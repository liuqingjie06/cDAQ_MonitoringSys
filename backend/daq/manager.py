from .device import DAQDevice
from util.logger import get_logger


def _safe_attr(obj, name, default=None):
    try:
        return getattr(obj, name)
    except Exception:
        return default


def _ai_channel_count(device):
    try:
        return len(device.ai_physical_chans)
    except Exception:
        return 0


def log_device_check(devices_cfg: dict) -> None:
    logger = get_logger("device_check")
    logger.info("Device check start")
    logger.info("Configured device count=%s", len(devices_cfg or {}))
    for name, cfg in (devices_cfg or {}).items():
        channels = cfg.get("channels") or []
        logger.info(
            "Config device name=%s model=%s channels=%s",
            name,
            cfg.get("model", ""),
            len(channels),
        )

    try:
        from nidaqmx.system import System
    except Exception as e:
        logger.error("Device check skipped (nidaqmx not available): %s", e)
        return

    try:
        system = System.local()
        devices = list(system.devices)
    except Exception as e:
        logger.error("Device check failed (DAQ system error): %s", e)
        return

    logger.info("Detected NI-DAQmx device count=%s", len(devices))
    for dev in devices:
        serial = _safe_attr(dev, "serial_num", None)
        product = _safe_attr(dev, "product_type", None)
        ai_count = _ai_channel_count(dev)
        logger.info(
            "Detected device name=%s product=%s serial=%s ai_channels=%s",
            _safe_attr(dev, "name", ""),
            product,
            serial,
            ai_count,
        )

    for name, cfg in (devices_cfg or {}).items():
        model = cfg.get("model", "")
        channels = cfg.get("channels") or []
        match = None
        for dev in devices:
            dev_name = _safe_attr(dev, "name", "")
            if dev_name == name or (model and dev_name == model):
                match = dev
                break
        if match is None and model:
            for dev in devices:
                dev_name = _safe_attr(dev, "name", "")
                if model in dev_name or dev_name in model:
                    match = dev
                    break

        if match is None:
            logger.warning("Config device not found name=%s model=%s", name, model)
            continue

        logger.info(
            "Config match name=%s actual_name=%s product=%s serial=%s ai_channels=%s config_channels=%s",
            name,
            _safe_attr(match, "name", ""),
            _safe_attr(match, "product_type", None),
            _safe_attr(match, "serial_num", None),
            _ai_channel_count(match),
            len(channels),
        )


class DeviceManager:
    def __init__(self, socketio, devices_cfg, sys_cfg, storage_cfg=None):
        self.socketio = socketio
        self.devices = {}
        storage_cfg = storage_cfg or {}
        storage_duration = storage_cfg.get("duration_s", 60)
        self.storage_service = None
        self.storage_cfg = storage_cfg
        log_device_check(devices_cfg or {})

        for name, dev_cfg in devices_cfg.items():
            self.devices[name] = DAQDevice(
                name=name,
                device_cfg=dev_cfg,
                sample_rate=sys_cfg["sample_rate"],
                effective_sample_rate=sys_cfg.get("effective_sample_rate", sys_cfg["sample_rate"]),
                samples_per_read=sys_cfg["samples_per_read"],
                fft_interval=sys_cfg["fft_interval"],
                fft_window_s=sys_cfg.get("fft_window_s"),
                disp_method=sys_cfg.get("disp_method"),
                storage_duration_s=storage_duration,
            )
            self.devices[name].socketio = socketio

        # Optional TDMS storage service
        if storage_cfg.get("enabled"):
            try:
                from .storage_worker import StorageService
                self.storage_service = StorageService(self, storage_cfg)
                self.storage_service.start()
            except Exception as e:
                # Non-fatal: continue without storage
                print("[storage] init error:", e)

    def start(self, name):
        if name in self.devices:
            self.devices[name].start()

    def start_all(self):
        """Start all configured devices."""
        for name in self.devices:
            self.start(name)

    def stop(self, name):
        if name in self.devices:
            self.devices[name].stop()

    def stop_all(self):
        """Stop all running devices."""
        for dev in self.devices.values():
            dev.stop()
        if self.storage_service:
            try:
                self.storage_service.stop()
            except Exception:
                pass

    def get_status(self):
        return {
            name: {
                "running": dev.running,
                "actual_rate": dev.actual_rate,
                "sample_rate": dev.sample_rate,
                "effective_sample_rate": getattr(dev, "effective_sample_rate", dev.sample_rate),
            }
            for name, dev in self.devices.items()
        }
