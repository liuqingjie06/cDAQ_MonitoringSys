from .device import DAQDevice


class DeviceManager:
    def __init__(self, socketio, devices_cfg, sys_cfg):
        self.socketio = socketio
        self.devices = {}

        for name, dev_cfg in devices_cfg.items():
            self.devices[name] = DAQDevice(
                name=name,
                device_cfg=dev_cfg,
                sample_rate=sys_cfg["sample_rate"],
                effective_sample_rate=sys_cfg.get("effective_sample_rate", sys_cfg["sample_rate"]),
                samples_per_read=sys_cfg["samples_per_read"],
                fft_interval=sys_cfg["fft_interval"],
            )
            self.devices[name].socketio = socketio

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
