import threading
import time
import queue

import nidaqmx
from nidaqmx.constants import (
    AcquisitionType,
    Coupling,
    AccelUnits,
)


class DAQRunner:
    """
    Handles NI task lifecycle and continuous sampling in its own thread.
    Emits raw samples to a callback and optional queue.
    """
    def __init__(self, name, channels_cfg, sample_rate, samples_per_read, on_samples, model=None):
        self.name = name
        self.channels_cfg = channels_cfg
        self.sample_rate = sample_rate
        self.samples_per_read = samples_per_read
        self.on_samples = on_samples
        self.model = model or ""
        self.actual_rate = None

        self.task = None
        self.running = False
        self.thread = None

    def _add_channel(self, task, ch: dict):
        ch_id = ch["id"]
        coupling = ch.get("coupling", "AC").upper()
        iepe = ch.get("iepe", False)

        if coupling == "DC" and iepe:
            raise ValueError(f"{self.name} CH{ch_id}: DC coupling cannot be used with IEPE ON")

        if self.model.startswith("9230"):
            if ch.get("type") != "acc":
                raise ValueError(f"{self.name} (NI-9230) only supports acceleration channels")

        ai = task.ai_channels.add_ai_accel_chan(
            physical_channel=f"{self.name}Mod1/ai{ch_id}",
            sensitivity=ch.get("sensitivity", 100.0),
            units=AccelUnits.G,
        )

        if coupling == "DC":
            ai.ai_coupling = Coupling.DC
        else:
            ai.ai_coupling = Coupling.AC

        if iepe:
            ai.ai_excit_val = ch.get("iepe_current", 0.004)
        else:
            ai.ai_excit_val = 0.0

    def start(self):
        if self.running:
            return

        self.task = nidaqmx.Task()
        for ch in self.channels_cfg:
            if not ch.get("enabled", True):
                continue
            self._add_channel(self.task, ch)

        self.task.timing.cfg_samp_clk_timing(
            rate=self.sample_rate,
            sample_mode=AcquisitionType.CONTINUOUS,
            samps_per_chan=self.samples_per_read * 5,
        )

        self.running = True
        self.thread = threading.Thread(target=self._loop, daemon=True)
        self.thread.start()
        try:
            actual_rate = self.task.timing.samp_clk_rate
        except Exception:
            actual_rate = self.sample_rate
        self.actual_rate = actual_rate
        print(f"[{self.name}] DAQ started, requested_rate={self.sample_rate}, actual_rate={actual_rate}")

    def stop(self):
        self.running = False
        if self.thread:
            self.thread.join(timeout=1.0)
            self.thread = None
        if self.task:
            try:
                self.task.stop()
                self.task.close()
            except Exception:
                pass
            self.task = None
        print(f"[{self.name}] DAQ stopped")

    def _loop(self):
        while self.running:
            try:
                data = self.task.read(
                    number_of_samples_per_channel=self.samples_per_read,
                    timeout=1.0  # avoid hang if device is unplugged
                )
                if self.on_samples:
                    self.on_samples(data)
            except Exception as e:
                print(f"[{self.name}] DAQ read error (device may be disconnected):", e)
                # Stop local loop without blocking on self.stop() (to avoid self-join deadlock)
                self.running = False
                try:
                    if self.task:
                        self.task.stop()
                        self.task.close()
                except Exception:
                    pass
                self.task = None
                break
            # small yield
            time.sleep(0.0)
