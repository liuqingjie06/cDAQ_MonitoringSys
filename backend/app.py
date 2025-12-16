from flask import Flask, send_from_directory, jsonify, request
from flask_socketio import SocketIO

from config import load_config, save_config
from daq.manager import DeviceManager
from daq import iot
from sensors.wind import WindService
import copy

# Serve the frontend files from ../frontend
app = Flask(__name__, static_folder="../frontend", static_url_path="")
socketio = SocketIO(app, cors_allowed_origins="*")

# Global config + manager
config_data = {}
sys_cfg = {}
devices_cfg = {}
device_manager: DeviceManager | None = None
wind_service: WindService | None = None


def _detect_devices_console():
    """Detect NI devices and print basic info to console."""
    try:
        from nidaqmx.system import System

        devs = list(System.local().devices)
        if not devs:
            print("[detect] No NI devices found.")
            return
        print("[detect] NI devices:")
        for dev in devs:
            try:
                name = dev.name
                prod = getattr(dev, "product_type", "unknown")
                serial = getattr(dev, "serial_num", "n/a")
                # Count AI physical channels if available
                try:
                    ai_count = len(getattr(dev, "ai_physical_chans", []))
                except Exception:
                    ai_count = "n/a"
                print(f"  - {name} ({prod}), serial={serial}, AI chans={ai_count}")
            except Exception as e:
                print("[detect] error reading device info:", e)
    except Exception as e:
        print("[detect] Could not enumerate NI devices:", e)


def _detect_and_merge_devices(cfg: dict) -> dict:
    """
    Detect NI devices; if found, only keep one chassis + its module name.
    If existing config already matches (device key + model), keep as-is.
    Otherwise rewrite the first device entry to detected chassis name and module name.
    """
    cfg = copy.deepcopy(cfg)
    try:
        from nidaqmx.system import System
        devs = list(System.local().devices)
    except Exception:
        return cfg

    if not devs:
        return cfg

    # Pick first chassis (non-Mod) as device name; first module (Mod) as model name
    # Also capture AI channel count from the chosen module (fallback to chassis)
    chassis = None
    module_name = None
    chosen_dev_for_ai = None
    for dev in devs:
        name = getattr(dev, "name", "")
        if "mod" in name.lower():
            if module_name is None:
                module_name = name
                chosen_dev_for_ai = dev
        elif "cdaq" in name.lower():
            if chassis is None:
                chassis = name
                if chosen_dev_for_ai is None:
                    chosen_dev_for_ai = dev
    if chassis is None and devs:
        chassis = devs[0].name
    if module_name is None and devs:
        module_name = getattr(devs[0], "product_type", devs[0].name)
        if chosen_dev_for_ai is None:
            chosen_dev_for_ai = devs[0]

    if chassis is None:
        return cfg

    try:
        ai_count = len(getattr(chosen_dev_for_ai, "ai_physical_chans", []))
    except Exception:
        ai_count = 0

    def _adjust_channels(ch_list, target):
        if target <= 0:
            return ch_list
        out = list(ch_list)
        if len(out) > target:
            out = out[:target]
        elif len(out) < target:
            base_ch = out[-1] if out else {"id": 0, "enabled": False}
            for idx in range(len(out), target):
                new_ch = dict(base_ch)
                new_ch["id"] = idx
                new_ch["enabled"] = False
                out.append(new_ch)
        return out

    devices_cfg = list((cfg.get("devices") or {}).items())
    channels = []
    if devices_cfg:
        channels = devices_cfg[0][1].get("channels", [])
        current_name = devices_cfg[0][0]
        current_model = devices_cfg[0][1].get("model")
        if current_name == chassis and current_model == module_name:
            if ai_count:
                channels = _adjust_channels(channels, ai_count)
                cfg["devices"][current_name]["channels"] = channels
            return cfg  # already matches

    # Adjust channels to detected ai_count
    if ai_count:
        channels = _adjust_channels(channels, ai_count)

    cfg["devices"] = {
        chassis: {
            "model": module_name,
            "channels": channels,
        }
    }
    return cfg


def apply_config(data: dict):
    """Update in-memory config and recreate device manager."""
    global config_data, sys_cfg, devices_cfg, device_manager, wind_service

    original = copy.deepcopy(data)
    merged = _detect_and_merge_devices(data)
    # If detection adjusted names/models/channels, persist back to config.json
    if merged != original:
        try:
            save_config(merged)
        except Exception as e:
            print("[config] failed to persist detected device info:", e)
    config_data = merged
    sys_cfg = {
        "sample_rate": config_data.get("sample_rate", 2000),
        "effective_sample_rate": config_data.get("effective_sample_rate", config_data.get("sample_rate", 2000)),
        "samples_per_read": config_data.get("samples_per_read", 4000),
        "fft_interval": config_data.get("fft_interval", 0.5),
    }
    devices_cfg = config_data.get("devices", {})
    iot.set_config(config_data.get("iot"))
    wind_cfg = config_data.get("wind") or {}

    # Print detected devices to console for visibility
    _detect_devices_console()

    # stop old manager if exists
    if device_manager:
        try:
            device_manager.stop_all()
        except Exception:
            pass
    if wind_service:
        try:
            wind_service.stop()
        except Exception:
            pass

    device_manager = DeviceManager(
        socketio=socketio,
        devices_cfg=devices_cfg,
        sys_cfg=sys_cfg,
    )
    wind_service = WindService(socketio=socketio, cfg=wind_cfg)
    try:
        wind_service.update_config(wind_cfg)
    except Exception:
        pass
    # Auto-start all devices on config load
    try:
        device_manager.start_all()
    except Exception:
        pass
    # Send a test IoT payload at startup/config reload
    try:
        iot.publish({
            "type": "startup_test",
            "ts": __import__("datetime").datetime.now().isoformat(),
            "message": "iot connectivity test",
            "devices": list(devices_cfg.keys())
        })
    except Exception as e:
        print("[iot] startup test publish error:", e)


# Initial load
apply_config(load_config())


@app.route("/")
def index():
    """Serve the SPA entrypoint instead of returning 404."""
    return send_from_directory(app.static_folder, "index.html")


@app.route("/api/config", methods=["GET", "POST"])
def api_config():
    """
    GET: return current config.json
    POST: overwrite config.json with posted JSON and re-instantiate DAQ manager.
    """
    if request.method == "GET":
        return jsonify(config_data)

    try:
        incoming = request.get_json(force=True)
        save_config(incoming)
        apply_config(load_config())
        return jsonify({"status": "ok", "config": config_data})
    except Exception as exc:
        return jsonify({"status": "error", "error": str(exc)}), 400


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
    socketio.emit("devices", {
        "devices": devices_cfg,
        "status": device_manager.get_status() if device_manager else {},
    })


@app.route("/api/fatigue", methods=["GET"])
def api_fatigue():
    """
    Return latest fatigue results per device if available.
    """
    results = {}
    if device_manager:
        for name, dev in device_manager.devices.items():
            results[name] = dev.get_fatigue_snapshot()
    return jsonify(results)


@app.route("/api/fatigue/reset", methods=["POST"])
def api_fatigue_reset():
    """
    Reset cumulative fatigue damage (all devices or a specific one).
    """
    device = None
    try:
        payload = request.get_json(silent=True) or {}
        device = payload.get("device")
    except Exception:
        device = None

    results = {}
    if device_manager:
        targets = device_manager.devices.keys() if not device else [device]
        for name in targets:
            dev = device_manager.devices.get(name)
            if dev:
                results[name] = dev.reset_damage()
    return jsonify({"status": "ok", "results": results})


@app.route("/api/wind", methods=["GET"])
def api_wind():
    if not wind_service:
        return jsonify({"enabled": False, "connected": False, "mode": "sim", "sample": None, "stats": None})
    return jsonify(wind_service.get_status())


if __name__ == "__main__":
    socketio.run(
        app,
        host="0.0.0.0",
        port=5000,
        debug=False,      # disable debug
        use_reloader=False  # disable reloader
    )
