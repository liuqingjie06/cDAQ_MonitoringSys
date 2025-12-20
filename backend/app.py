from __future__ import annotations

import os
import shutil
from pathlib import Path
from flask import Flask, jsonify, request, send_from_directory
from flask_socketio import SocketIO

import config as config_module
from daq import iot
from daq.manager import DeviceManager
from sensors.wind import WindService

app = Flask(__name__)
socketio = SocketIO(app, cors_allowed_origins="*")
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
FRONTEND_DIR = os.path.abspath(os.path.join(BASE_DIR, "..", "frontend"))
PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Runtime state
current_config = config_module.load_config()
device_manager: DeviceManager | None = None
wind_service: WindService | None = None


def _build_device_manager(cfg: dict) -> DeviceManager:
    """Stop any running devices and rebuild manager from config."""
    global device_manager
    if device_manager:
        try:
            device_manager.stop_all()
        except Exception:
            pass

    sys_cfg = {
        "sample_rate": cfg.get("sample_rate"),
        "effective_sample_rate": cfg.get("effective_sample_rate", cfg.get("sample_rate")),
        "samples_per_read": cfg.get("samples_per_read"),
        "fft_interval": cfg.get("fft_interval"),
    }
    device_manager = DeviceManager(
        socketio=socketio,
        devices_cfg=cfg.get("devices") or {},
        sys_cfg=sys_cfg,
        storage_cfg=cfg.get("storage") or {},
    )
    # Auto-start all devices on backend launch/config reload so system runs headless.
    try:
        device_manager.start_all()
    except Exception:
        pass
    return device_manager


def _apply_runtime_config(cfg: dict) -> None:
    """Apply config to services (DAQ devices, wind, IoT)."""
    global current_config, wind_service
    current_config = cfg
    iot.set_config(cfg.get("iot"))

    if wind_service is None:
        wind_service = WindService(socketio=socketio, cfg=cfg.get("wind"))
    wind_service.update_config(cfg.get("wind") or {})

    _build_device_manager(cfg)


# Initialize services using config file
_apply_runtime_config(current_config)


@app.route("/")
def index():
    return send_from_directory(FRONTEND_DIR, "index.html")


@app.route("/<path:filename>")
def frontend_static(filename):
    return send_from_directory(FRONTEND_DIR, filename)


@app.route("/api/config", methods=["GET"])
def api_get_config():
    return jsonify(current_config)


def _get_data_root() -> Path:
    """Resolve data directory from current config (storage.output_dir)."""
    storage_cfg = (current_config or {}).get("storage") or {}
    out_dir = storage_cfg.get("output_dir") or "data"
    p = Path(out_dir)
    if not p.is_absolute():
        backend_root = Path(__file__).resolve().parent  # backend/
        p = (backend_root / out_dir).resolve()
    return p


@app.route("/api/system/status", methods=["GET"])
def api_system_status():
    """Basic host status for dashboard: CPU, disk, data dir."""
    # CPU load
    cpu_percent = None
    try:
        import psutil

        cpu_percent = psutil.cpu_percent(interval=0.1)
    except Exception:
        try:
            import os as _os

            if hasattr(_os, "getloadavg"):
                # convert 1-min load to a rough percent guess (cores unknown)
                cpu_percent = float(_os.getloadavg()[0])
        except Exception:
            cpu_percent = None

    # Disk usage of project root
    try:
        disk_total, disk_used, disk_free = shutil.disk_usage(PROJECT_ROOT)
    except Exception:
        disk_total = disk_used = disk_free = None

    data_root = _get_data_root()
    return jsonify({
        "cpu_percent": cpu_percent,
        "disk": {
            "total": disk_total,
            "used": disk_used,
            "free": disk_free,
        },
        "data_dir": str(data_root),
    })


@app.route("/api/system/data", methods=["GET"])
def api_system_data():
    """List files under data directory (recursive by path argument)."""
    root = _get_data_root()
    rel_path = request.args.get("path") or ""
    target = (root / rel_path).resolve()

    # prevent escaping root
    try:
        target.relative_to(root)
    except Exception:
        return jsonify({"error": "invalid path"}), 400

    if not target.exists():
        return jsonify({"path": rel_path, "entries": []})

    entries = []
    try:
        for p in sorted(target.iterdir(), key=lambda x: x.name.lower()):
            try:
                st = p.stat()
                entries.append({
                    "name": p.name,
                    "is_dir": p.is_dir(),
                    "size": st.st_size if p.is_file() else None,
                    "mtime": st.st_mtime,
                    "path": str(p.relative_to(root)),
                })
            except Exception:
                entries.append({
                    "name": p.name,
                    "is_dir": p.is_dir(),
                    "error": "stat failed",
                    "path": str(p.relative_to(root)),
                })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    breadcrumbs = []
    curr = target
    while True:
        try:
            rel = curr.relative_to(root)
            breadcrumbs.append({"name": curr.name or str(rel), "path": str(rel)})
        except Exception:
            breadcrumbs.append({"name": root.name, "path": ""})
            break
        if curr == root:
            break
        curr = curr.parent
    breadcrumbs = list(reversed(breadcrumbs))

    return jsonify({
        "root": str(root),
        "path": str(target.relative_to(root)),
        "entries": entries,
        "breadcrumbs": breadcrumbs,
    })


@app.route("/api/config", methods=["POST"])
def api_save_config():
    payload = request.get_json(silent=True) or {}
    if not isinstance(payload, dict):
        return jsonify({"status": "error", "error": "Invalid JSON body"}), 400
    try:
        config_module.save_config(payload)
        cfg = config_module.load_config()
        _apply_runtime_config(cfg)
        return jsonify({"status": "ok", "config": cfg})
    except Exception as e:
        return jsonify({"status": "error", "error": str(e)}), 500


@app.route("/api/fatigue", methods=["GET"])
def api_get_fatigue():
    if not device_manager:
        return jsonify({})
    out = {}
    for name, dev in (device_manager.devices or {}).items():
        try:
            out[name] = dev.get_fatigue_snapshot()
        except Exception as e:
            out[name] = {"error": str(e)}
    return jsonify(out)


@app.route("/api/fatigue/reset", methods=["POST"])
def api_reset_fatigue():
    if not device_manager:
        return jsonify({"status": "ok", "result": {}})
    payload = request.get_json(silent=True) or {}
    target = payload.get("device") if isinstance(payload, dict) else None

    results = {}
    devices = device_manager.devices or {}
    selected = {target: devices.get(target)} if target else devices
    for name, dev in selected.items():
        if dev is None:
            continue
        try:
            results[name] = dev.reset_damage()
        except Exception as e:
            results[name] = {"error": str(e)}
    return jsonify({"status": "ok", "result": results})


@app.route("/api/wind", methods=["GET"])
def api_get_wind():
    if wind_service:
        try:
            return jsonify(wind_service.get_status())
        except Exception:
            pass
    return jsonify({
        "enabled": False,
        "mode": None,
        "connected": False,
        "sample": None,
        "stats": None,
    })


@socketio.on("start_device")
def start_device(msg):
    if device_manager:
        device_manager.start(msg["device"])


@socketio.on("stop_device")
def stop_device(msg):
    if device_manager:
        device_manager.stop(msg["device"])


@socketio.on("get_devices")
def get_devices(_msg=None):
    devices_cfg = current_config.get("devices") if isinstance(current_config, dict) else {}
    status = device_manager.get_status() if device_manager else {}
    socketio.emit("devices", {
        "devices": devices_cfg or {},
        "status": status,
    })


if __name__ == "__main__":
    socketio.run(
        app,
        host="0.0.0.0",
        port=5000,
        debug=False,      # Close debug for deployment
        use_reloader=False
    )
