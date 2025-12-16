import time
from collections import deque

from .runner import DAQRunner
from .analysis_worker import AnalysisWorker
from .damage_logger import DamageLogger
from .analysis import acc_to_disp, build_sn_curve  # re-export if needed elsewhere
import numpy as np


class DAQDevice:
    """
    Orchestrates DAQ runner (sampling), analysis worker, and damage logger.
    """
    def __init__(
        self,
        name: str,
        device_cfg: dict,
        sample_rate: int,
        effective_sample_rate: int,
        samples_per_read: int,
        fft_interval: float,
    ):
        self.name = name
        self.model = device_cfg.get("model", "9230")
        self.channels = device_cfg["channels"]

        self.sample_rate = sample_rate
        self.effective_sample_rate = effective_sample_rate
        self.samples_per_read = samples_per_read
        self.fft_interval = fft_interval
        self.decimation = max(1, int(round(self.sample_rate / max(1, self.effective_sample_rate))))

        self.running = False
        # ring buffers for frontend streaming
        self.buffers = [
            deque(maxlen=sample_rate * 5)
            for _ in self.channels
        ]
        self.last_fft_time = 0.0

        self.damage_logger = DamageLogger(
            device_name=self.name,
            data_dir=self._data_dir
        )
        self.analysis_worker = AnalysisWorker(
            device_name=self.name,
            sample_rate=self.effective_sample_rate,
            log_interval=600.0,
            damage_logger=self.damage_logger,
            channels_cfg=self.channels,
        )
        self.runner = DAQRunner(
            name=self.name,
            channels_cfg=self.channels,
            sample_rate=self.sample_rate,
            samples_per_read=self.samples_per_read,
            on_samples=self._on_samples,
            model=self.model,
        )
        self.socketio = None

    @property
    def _data_dir(self):
        import pathlib
        return pathlib.Path(__file__).resolve().parent.parent / "data"

    def start(self):
        if self.running:
            return
        self.running = True
        self.analysis_worker.start()
        self.runner.start()
        self._update_decimation()

    def stop(self):
        self.running = False
        try:
            self.runner.stop()
        except Exception:
            pass
        try:
            self.analysis_worker.stop()
        except Exception:
            pass

    def _on_samples(self, data):
        # optional decimation for downstream processing, with simple low-pass (boxcar) to reduce aliasing
        decimated = []
        for ch_data in data:
            if self.decimation > 1:
                try:
                    arr = np.asarray(ch_data, dtype=float)
                    # boxcar low-pass then decimate
                    kernel = np.ones(self.decimation) / self.decimation
                    filt = np.convolve(arr, kernel, mode="same")
                    decimated.append(filt[:: self.decimation].tolist())
                except Exception:
                    decimated.append(ch_data)
            else:
                decimated.append(ch_data)

        # update buffers for streaming using decimated data
        for i, ch_data in enumerate(decimated):
            self.buffers[i].extend(ch_data)

        payload = self._build_payload()
        if self.socketio:
            self.socketio.emit(f"stream_{self.name}", payload)
            # send FFT (magnitude) for first two channels if available
            try:
                fft_payload = self._build_fft_payload(decimated)
                if fft_payload:
                    self.socketio.emit(f"spectrum_{self.name}", fft_payload)
            except Exception:
                pass

        # hand off to analysis thread with decimated data
        self.analysis_worker.submit(decimated)

    def _update_decimation(self):
        actual = self.runner.actual_rate or self.sample_rate
        target = self.effective_sample_rate or actual
        if target <= 0:
            target = actual
        self.decimation = max(1, int(round(actual / target)))

    # ==========================================================
    # Helpers for streaming
    # ==========================================================
    def _build_payload(self):
        n = self.samples_per_read
        time_data = [
            list(self.buffers[i])[-n:]
            for i in range(len(self.buffers))
        ]

        payload = {
            "device": self.name,
            "time_data": time_data,
        }
        # displacement (simple double integration) for first two channels
        try:
            disp = []
            for idx, ch in enumerate(time_data[:2]):
                arr = np.asarray(ch, dtype=float)
                if arr.size == 0:
                    disp.append([])
                    continue
                unit = ""
                try:
                    unit = (self.channels[idx].get("unit") or "").lower()
                except Exception:
                    unit = ""
                # convert g to m/s^2 if needed
                if unit == "g":
                    arr = arr * 9.80665
                disp.append(acc_to_disp(arr, fs=self.effective_sample_rate or self.sample_rate).tolist())
            payload["displacement"] = disp
        except Exception:
            payload["displacement"] = []

        now = time.time()
        if now - self.last_fft_time > self.fft_interval:
            self.last_fft_time = now
            payload["fft"] = self._calc_fft(n)

        return payload

    def _calc_fft(self, n: int):
        import numpy as np
        fft_result = []

        for buf in self.buffers:
            x = np.array(list(buf)[-n:])
            if len(x) < n:
                fft_result.append([])
                continue

            win = np.hanning(len(x))
            X = np.fft.rfft(x * win)
            fft_result.append(
                20 * np.log10(np.abs(X) + 1e-12).tolist()
            )

        return fft_result

    def _build_fft_payload(self, decimated_data):
        import numpy as np
        if not decimated_data or len(decimated_data) < 2:
            return None
        # Use effective sample rate for frequency axis
        fs = self.effective_sample_rate or self.sample_rate
        spectra = []
        freq = None
        for ch in decimated_data[:2]:
            arr = np.asarray(ch, dtype=float)
            if arr.size < 2:
                spectra.append([])
                continue
            win = np.hanning(arr.size)
            X = np.fft.rfft(arr * win)
            mag = 20 * np.log10(np.abs(X) + 1e-12)
            if freq is None:
                freq = np.fft.rfftfreq(arr.size, d=1.0 / fs).tolist()
            spectra.append(mag.tolist())
        return {
            "device": self.name,
            "freq": freq if freq is not None else [],
            "spectra": spectra,
        }

    def get_fatigue_snapshot(self):
        sn_sa, sn_n = build_sn_curve(et=2.05e5)
        params = {
            "fs": self.sample_rate,
            "k_disp2stress": 90.62 / 0.4,
            "et": 2.05e5,
            "dphi_deg": 5.0,
        }

        # load cumulative from logger
        cum_phi = self.damage_logger.cum_phi or []
        cum_damage = self.damage_logger.cum_damage or []
        d_cum_max = max(cum_damage) if cum_damage else 0.0
        phi_cum = cum_phi[cum_damage.index(d_cum_max)] if cum_damage else 0.0

        base = {
            "timestamp": self.damage_logger.cum_timestamp,
            "device": self.name,
            "Dmax": 0.0,
            "phi_deg": 0.0,
            "Sa_max": 0.0,
            "phi_deg_list": cum_phi,
            "D_phi": [],
            "D_phi_cum": cum_damage,
            "D_cum_max": d_cum_max,
            "phi_deg_cum": phi_cum,
            "params": params,
            "sn_curve": {"Sa": sn_sa, "N": sn_n},
        }

        lf = self.analysis_worker.last_fatigue
        if lf:
            result = dict(base)
            result.update(lf)
            result.setdefault("D_phi_cum", base["D_phi_cum"])
            result.setdefault("D_cum_max", base["D_cum_max"])
            result.setdefault("phi_deg_cum", base["phi_deg_cum"])
            result.setdefault("phi_deg_list", base["phi_deg_list"])
            result.setdefault("sn_curve", base["sn_curve"])
            result.setdefault("params", base["params"])
            return result

        return base

    @property
    def actual_rate(self):
        return getattr(self.runner, "actual_rate", None)

    def reset_damage(self):
        self.damage_logger.reset_cumulative()
        self.analysis_worker.last_fatigue = None
        return self.get_fatigue_snapshot()
