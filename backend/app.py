from __future__ import annotations

import os
import shutil
import datetime
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

    requested_rate = cfg.get("sample_rate") or 0
    try:
        requested_rate = int(requested_rate)
    except Exception:
        requested_rate = 0
    if requested_rate <= 0:
        requested_rate = 1600

    effective_rate = cfg.get("effective_sample_rate") or requested_rate
    try:
        effective_rate = int(effective_rate)
    except Exception:
        effective_rate = requested_rate

    if requested_rate < 1600:
        hardware_rate = 1600
        effective_rate = requested_rate
    else:
        hardware_rate = requested_rate

    if effective_rate > hardware_rate:
        effective_rate = hardware_rate

    sys_cfg = {
        "sample_rate": hardware_rate,
        "effective_sample_rate": effective_rate,
        "samples_per_read": cfg.get("samples_per_read"),
        "fft_interval": cfg.get("fft_interval"),
        "fft_window_s": cfg.get("fft_window_s"),
        "disp_method": cfg.get("disp_method"),
    }
    device_manager = DeviceManager(
        socketio=socketio,
        devices_cfg=cfg.get("devices") or {},
        sys_cfg=sys_cfg,
        storage_cfg=cfg.get("storage") or {},
        wind_service=wind_service,
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
    try:
        display_names = []
        for name, dev_cfg in (cfg.get("devices") or {}).items():
            display = (dev_cfg or {}).get("display_name") or name
            if display:
                display_names.append(display)
        iot.set_control_display_names(display_names)
    except Exception:
        pass

    if wind_service is None:
        wind_service = WindService(socketio=socketio, cfg=cfg.get("wind"))
    wind_service.update_config(cfg.get("wind") or {})
    try:
        wind_service.set_publish_targets(display_names)
    except Exception:
        pass

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


def _resolve_data_path(rel_path: str) -> Path:
    """Resolve a relative data path safely under data root."""
    root = _get_data_root()
    rel = (rel_path or "").replace("\\", "/")
    target = (root / rel).resolve()
    target.relative_to(root)  # raises if escaping
    return target


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
    rel_path = (request.args.get("path") or "").replace("\\", "/")
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
                    "path": p.relative_to(root).as_posix(),
                })
            except Exception:
                entries.append({
                    "name": p.name,
                    "is_dir": p.is_dir(),
                    "error": "stat failed",
                    "path": p.relative_to(root).as_posix(),
                })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    breadcrumbs = []
    curr = target
    while True:
        try:
            rel = curr.relative_to(root)
            breadcrumbs.append({"name": curr.name or rel.as_posix(), "path": rel.as_posix()})
        except Exception:
            breadcrumbs.append({"name": root.name, "path": ""})
            break
        if curr == root:
            break
        curr = curr.parent
    breadcrumbs = list(reversed(breadcrumbs))

    return jsonify({
        "root": str(root),
        "path": target.relative_to(root).as_posix(),
        "entries": entries,
        "breadcrumbs": breadcrumbs,
    })


@app.route("/api/system/file", methods=["GET"])
def api_system_file():
    """Fetch file content for small text/CSV preview."""
    try:
        target = _resolve_data_path(request.args.get("path") or "")
    except Exception:
        return jsonify({"error": "invalid path"}), 400

    if not target.exists() or not target.is_file():
        return jsonify({"error": "not found"}), 404

    suffix = target.suffix.lower()
    if suffix == ".csv":
        import csv as _csv
        tail = request.args.get("tail", "").strip()
        limit = request.args.get("limit", "").strip()
        try:
            tail = int(tail)
        except Exception:
            tail = 0
        try:
            limit = int(limit)
        except Exception:
            limit = 200
        if tail < 0:
            tail = 0
        if tail > 20000:
            tail = 20000
        if limit == 0:
            limit = -1
        if limit > 20000:
            limit = 20000

        rows = []
        with target.open("r", encoding="utf-8") as f:
            reader = _csv.reader(f)
            if tail > 0:
                from collections import deque

                try:
                    header = next(reader)
                except StopIteration:
                    header = None
                buf = deque(maxlen=tail)
                for row in reader:
                    buf.append(row)
                if header:
                    rows.append(header)
                rows.extend(list(buf))
            else:
                for i, row in enumerate(reader):
                    rows.append(row)
                    if limit > 0 and i >= limit:
                        break
        return jsonify({"type": "csv", "rows": rows, "path": target.as_posix()})

    try:
        text = target.read_text(encoding="utf-8")
    except Exception:
        try:
            text = target.read_text(encoding="latin-1")
        except Exception as e:
            return jsonify({"error": str(e)}), 500
    # limit to avoid huge payload
    if len(text) > 20000:
        text = text[:20000] + "\n... truncated ..."
    return jsonify({"type": "text", "content": text, "path": target.as_posix()})


@app.route("/api/stats/daily_disp", methods=["GET"])
def api_daily_disp():
    """Daily max displacement per channel for cDAQ3."""
    date_str = (request.args.get("date") or "").strip()
    if not date_str:
        return jsonify({"error": "date required"}), 400
    try:
        day = datetime.date.fromisoformat(date_str)
    except Exception:
        return jsonify({"error": "invalid date"}), 400

    month = day.strftime("%Y%m")
    day_str = day.strftime("%d")
    try:
        target = _resolve_data_path(f"cDAQ3/{month}/{day_str}.csv")
    except Exception:
        return jsonify({"error": "invalid path"}), 400

    if not target.exists() or not target.is_file():
        return jsonify({"error": "not found"}), 404

    import csv as _csv

    result = {
        "date": date_str,
        "channels": {
            "0": {"max": None, "time": None},
            "1": {"max": None, "time": None},
        }
    }

    with target.open("r", encoding="utf-8") as f:
        reader = _csv.DictReader(f)
        for row in reader:
            if row.get("type") != "stat":
                continue
            try:
                ch = int(row.get("channel"))
            except Exception:
                continue
            if ch not in (0, 1):
                continue
            try:
                dmax = abs(float(row.get("disp_max") or 0))
                dmin = abs(float(row.get("disp_min") or 0))
            except Exception:
                continue
            val = max(dmax, dmin)
            key = str(ch)
            current = result["channels"][key]["max"]
            if current is None or val > current:
                result["channels"][key]["max"] = val
                result["channels"][key]["time"] = row.get("timestamp")

    return jsonify(result)


@app.route("/api/stats/daily_disp_recent", methods=["GET"])
def api_daily_disp_recent():
    """Recent daily max equivalent displacement (sum of X/Y) for cDAQ3."""
    try:
        days = int(request.args.get("days") or 30)
    except Exception:
        days = 30
    days = max(1, min(days, 90))
    today = datetime.date.today()
    series = []

    import csv as _csv

    for i in range(days):
        day = today - datetime.timedelta(days=days - 1 - i)
        month = day.strftime("%Y%m")
        day_str = day.strftime("%d")
        try:
            target = _resolve_data_path(f"cDAQ3/{month}/{day_str}.csv")
        except Exception:
            series.append({"date": day.isoformat(), "max_eq": None})
            continue

        if not target.exists() or not target.is_file():
            series.append({"date": day.isoformat(), "max_eq": None})
            continue

        max_eq = None
        by_ts = {}
        try:
            with target.open("r", encoding="utf-8") as f:
                reader = _csv.DictReader(f)
                for row in reader:
                    if row.get("type") != "stat":
                        continue
                    ts = row.get("timestamp") or ""
                    try:
                        ch = int(row.get("channel"))
                    except Exception:
                        continue
                    if ch not in (0, 1):
                        continue
                    try:
                        dmax = abs(float(row.get("disp_max") or 0))
                        dmin = abs(float(row.get("disp_min") or 0))
                    except Exception:
                        continue
                    val = max(dmax, dmin)
                    entry = by_ts.get(ts) or {0: 0.0, 1: 0.0}
                    entry[ch] = max(entry.get(ch, 0.0), val)
                    by_ts[ts] = entry
            for entry in by_ts.values():
                eq = abs(entry.get(0, 0.0)) + abs(entry.get(1, 0.0))
                if max_eq is None or eq > max_eq:
                    max_eq = eq
        except Exception:
            max_eq = None

        series.append({"date": day.isoformat(), "max_eq": max_eq})

    return jsonify({"series": series, "days": days})


@app.route("/api/system/tdms", methods=["GET"])
def api_system_tdms():
    """
    List TDMS channels or return waveform for a channel.
    Query: path=<relative-to-data>, channel=<name>, group=<group>
    """
    try:
        target = _resolve_data_path(request.args.get("path") or "")
    except Exception:
        return jsonify({"error": "invalid path"}), 400
    if not target.exists() or not target.is_file():
        return jsonify({"error": "not found"}), 404

    group = request.args.get("group") or None
    channel = request.args.get("channel") or None

    try:
        from nptdms import TdmsFile
    except Exception as e:
        return jsonify({"error": f"nptdms not installed: {e}"}), 500

    try:
        tf = TdmsFile.read(target)
    except Exception as e:
        return jsonify({"error": f"tdms read failed: {e}"}), 500

    if not channel:
        chans = []
        for ch in tf.groups():
            for c in ch.channels():
                chans.append({"group": ch.name, "channel": c.name})
        return jsonify({"channels": chans, "path": target.as_posix()})

    # read specific channel
    try:
        if group:
            cobj = tf[group][channel]
        else:
            # fallback: search by name
            cobj = None
            for g in tf.groups():
                for c in g.channels():
                    if c.name == channel:
                        cobj = c
                        group = g.name
                        break
                if cobj:
                    break
        if cobj is None:
            return jsonify({"error": "channel not found"}), 404
    except Exception as e:
        return jsonify({"error": f"channel access failed: {e}"}), 500

    try:
        data = cobj.data
        wf = cobj.properties
        fs = None
        try:
            if "wf_increment" in wf:
                fs = 1.0 / float(wf.get("wf_increment"))
            elif "sample_rate" in wf:
                fs = float(wf.get("sample_rate"))
        except Exception:
            fs = None

        # build time axis if possible
        t_axis = []
        if fs and len(data) > 0:
            t_axis = [i / fs for i in range(len(data))]

        # avoid huge payload: cap to 5000 points
        max_points = 5000
        if len(data) > max_points:
            step = len(data) // max_points
            data = data[::step]
            if t_axis:
                t_axis = t_axis[::step]

        return jsonify({
            "path": target.as_posix(),
            "group": group,
            "channel": channel,
            "fs": fs,
            "unit": wf.get("unit") or wf.get("unit_string") or "",
            "wf_props": {k: str(v) for k, v in wf.items()},
            "time": t_axis,
            "data": list(map(float, data)),
        })
    except Exception as e:
        return jsonify({"error": f"tdms parse failed: {e}"}), 500


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
