import time
from collections import deque

from .runner import DAQRunner
from .analysis_worker import AnalysisWorker
from .damage_logger import DamageLogger
from .analysis import acc_to_disp, build_sn_curve  # re-export if needed elsewhere
from . import iot
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
        fft_window_s: float | None = None,
        disp_method: str | None = None,
        storage_duration_s: float = 60.0,
    ):
        self.name = name
        self.model = device_cfg.get("model", "9230")
        self.display_name = device_cfg.get("display_name", name)
        self.channels = device_cfg["channels"]

        self.sample_rate = sample_rate
        self.effective_sample_rate = effective_sample_rate
        self.samples_per_read = samples_per_read
        self.fft_interval = fft_interval
        self.fft_window_s = float(fft_window_s) if fft_window_s else 30.0
        self.disp_method = disp_method or "fft"
        self.decimation = max(1, int(round(self.sample_rate / max(1, self.effective_sample_rate))))
        self._decim_kernel = None
        self._decim_state = [np.zeros(0, dtype=float) for _ in self.channels]

        self.running = False
        # ring buffers for frontend streaming (keep enough for the fixed 30s display window)
        eff_rate = max(1, int(self.effective_sample_rate or self.sample_rate))
        buf_len = max(int(eff_rate * self.fft_window_s), self.samples_per_read)
        self.buffers = [
            deque(maxlen=buf_len)
            for _ in self.channels
        ]
        self.disp_buffers = [
            deque(maxlen=buf_len)
            for _ in self.channels
        ]
        # ring buffers for storage snapshots (decimated)
        self.storage_duration_s = max(1.0, float(storage_duration_s))
        eff_rate = max(1, int(self.effective_sample_rate or self.sample_rate))
        self.storage_buffers = [
            deque(maxlen=int(eff_rate * self.storage_duration_s))
            for _ in self.channels
        ]
        self.last_fft_time = 0.0
        self.last_iot_stream_time = 0.0

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
            disp_method=self.disp_method,
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
        # optional decimation for downstream processing, apply low-pass before downsample
        decimated = []
        for ch_idx, ch_data in enumerate(data):
            if self.decimation > 1:
                try:
                    arr = np.asarray(ch_data, dtype=float)
                    kernel = self._decim_kernel
                    if kernel is None:
                        kernel = self._build_decimation_kernel()
                        self._decim_kernel = kernel
                    if kernel is not None:
                        state = self._decim_state[ch_idx]
                        pad_len = max(0, len(kernel) - 1)
                        if state.size != pad_len:
                            state = np.zeros(pad_len, dtype=float)
                        x = np.concatenate([state, arr])
                        # valid conv returns length == len(arr), aligned to current chunk
                        filt = np.convolve(x, kernel, mode="valid")
                        if pad_len > 0:
                            self._decim_state[ch_idx] = x[-pad_len:]
                        decimated.append(filt[:: self.decimation].tolist())
                    else:
                        decimated.append(arr[:: self.decimation].tolist())
                except Exception:
                    decimated.append(ch_data)
            else:
                decimated.append(ch_data)

        # update buffers for streaming using decimated data
        for i, ch_data in enumerate(decimated):
            self.buffers[i].extend(ch_data)
        # compute displacement on raw data, then downsample to match effective rate
        for i, ch_data in enumerate(data):
            try:
                arr = np.asarray(ch_data, dtype=float)
                if arr.size == 0:
                    continue
                unit = ""
                try:
                    unit = (self.channels[i].get("unit") or "").lower()
                except Exception:
                    unit = ""
                if unit == "g":
                    arr = arr * 9.80665
                disp_raw = acc_to_disp(arr, fs=self.sample_rate, method=self.disp_method)
                if self.decimation > 1:
                    disp_dec = disp_raw[:: self.decimation]
                else:
                    disp_dec = disp_raw
                self.disp_buffers[i].extend(disp_dec.tolist() if hasattr(disp_dec, "tolist") else list(disp_dec))
            except Exception:
                pass
        # update storage buffers using decimated data
        for i, ch_data in enumerate(decimated):
            self.storage_buffers[i].extend(ch_data)

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

        self._maybe_publish_iot()

        # hand off to analysis thread with decimated data
        self.analysis_worker.submit(decimated)

    def _update_decimation(self):
        actual = self.runner.actual_rate or self.sample_rate
        target = self.effective_sample_rate or actual
        if target <= 0:
            target = actual
        self.decimation = max(1, int(round(actual / target)))
        self._decim_kernel = self._build_decimation_kernel()
        self._decim_state = [np.zeros(0, dtype=float) for _ in self.channels]

    def _build_decimation_kernel(self):
        if self.decimation <= 1:
            return None
        fs_in = float(self.sample_rate or 1)
        fs_out = float(self.effective_sample_rate or fs_in)
        cutoff_hz = 0.45 * (fs_out / 2.0)
        fc = max(0.001, min(cutoff_hz / fs_in, 0.49))
        taps = max(31, self.decimation * 8 + 1)
        if taps % 2 == 0:
            taps += 1
        n = np.arange(taps, dtype=float)
        m = (taps - 1) / 2.0
        h = 2.0 * fc * np.sinc(2.0 * fc * (n - m))
        h *= np.hanning(taps)
        h /= np.sum(h)
        return h

    # ==========================================================
    # Helpers for streaming
    # ==========================================================
    def _build_payload(self):
        # use available samples (decimated) to avoid empty payload when samples_per_read is large
        time_data = []
        eff_rate = max(1, int(self.effective_sample_rate or self.sample_rate))
        n_limit = int(eff_rate * self.fft_window_s)
        for buf in self.buffers:
            n = min(n_limit, len(buf))
            time_data.append(list(buf)[-n:] if n > 0 else [])

        payload = {
            "device": self.name,
            "display_name": self.display_name,
            "time_data": time_data,
        }
        # displacement from raw->downsampled buffer
        try:
            disp = []
            n_limit = int(eff_rate * self.fft_window_s)
            for buf in self.disp_buffers[:2]:
                n = min(n_limit, len(buf))
                disp.append(list(buf)[-n:] if n > 0 else [])
            payload["displacement"] = disp
        except Exception:
            payload["displacement"] = []

        now = time.time()
        if now - self.last_fft_time > self.fft_interval:
            self.last_fft_time = now
            payload["fft"] = self._calc_fft(n)

        return payload

    def _iot_topic_base(self) -> str:
        base = (self.display_name or self.name or "").strip()
        return base or self.name

    def _maybe_publish_iot(self):
        window_s = max(1.0, float(self.fft_window_s or 1.0))
        now = time.time()
        if now - self.last_iot_stream_time < window_s:
            return

        eff_rate = max(1, int(self.effective_sample_rate or self.sample_rate))
        n_limit = int(eff_rate * window_s)
        if n_limit <= 1:
            return
        if len(self.buffers) < 2 or len(self.buffers[0]) < n_limit or len(self.buffers[1]) < n_limit:
            return

        data_x = list(self.buffers[0])[-n_limit:]
        data_y = list(self.buffers[1])[-n_limit:]
        ts = time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(now))
        topic_base = self._iot_topic_base()

        vib_payload = {
            "device": self.name,
            "display_name": self.display_name,
            "timestamp": ts,
            "sample_rate": eff_rate,
            "window_s": window_s,
            "data": {"x": data_x, "y": data_y},
        }
        iot.publish(vib_payload, topic=f"{topic_base}/stream/vib")

        disp_x = list(self.disp_buffers[0])[-n_limit:] if len(self.disp_buffers) > 0 else []
        disp_y = list(self.disp_buffers[1])[-n_limit:] if len(self.disp_buffers) > 1 else []
        disp_x = self._downsample_to_1hz(disp_x, eff_rate, window_s)
        disp_y = self._downsample_to_1hz(disp_y, eff_rate, window_s)
        disp_payload = {
            "device": self.name,
            "display_name": self.display_name,
            "timestamp": ts,
            "sample_rate": 1,
            "window_s": window_s,
            "data": {"x": disp_x, "y": disp_y},
        }
        iot.publish(disp_payload, topic=f"{topic_base}/stream/disp_track")

        freq_payload = self._build_iot_freq_payload(data_x, data_y, eff_rate, ts)
        if freq_payload:
            iot.publish(freq_payload, topic=f"{topic_base}/stream/freq")

        self.last_iot_stream_time = now

    def _downsample_to_1hz(self, data, eff_rate: int, window_s: float):
        if not data:
            return []
        step = int(round(eff_rate)) if eff_rate else 1
        if step <= 1:
            out = data
        else:
            out = data[::step]
        target_len = int(window_s)
        if target_len > 0 and len(out) > target_len:
            out = out[-target_len:]
        return out

    def _build_iot_freq_payload(self, data_x, data_y, fs: int, timestamp: str):
        import numpy as np

        n = min(len(data_x), len(data_y))
        if n < 2 or fs <= 0:
            return None
        x = np.asarray(data_x[-n:], dtype=float)
        y = np.asarray(data_y[-n:], dtype=float)
        win = np.hanning(n)
        X = np.fft.rfft(x * win)
        Y = np.fft.rfft(y * win)
        freq = np.fft.rfftfreq(n, d=1.0 / fs)
        if freq.size < 2:
            return None

        max_hz = 5.0
        idx = np.where(freq <= max_hz)[0]
        if idx.size == 0:
            return None

        df = float(freq[1] - freq[0])
        mag_x = 20 * np.log10(np.abs(X) + 1e-12)
        mag_y = 20 * np.log10(np.abs(Y) + 1e-12)

        return {
            "device": self.name,
            "display_name": self.display_name,
            "timestamp": timestamp,
            "count": int(idx.size),
            "df": df,
            "fmax_hz": max_hz,
            "values": {
                "x": mag_x[idx].tolist(),
                "y": mag_y[idx].tolist(),
            },
        }

    def _calc_fft(self, n: int):
        import numpy as np
        fft_result = []

        for buf in self.buffers:
            n_use = min(n, len(buf))
            x = np.array(list(buf)[-n_use:])
            if len(x) < 2:
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
        fs = self.effective_sample_rate or self.sample_rate
        if not fs:
            return None
        n_fft = int(fs * self.fft_window_s)
        if n_fft <= 1:
            return None
        if not self.buffers or len(self.buffers[0]) < n_fft:
            return None
        spectra = []
        freq = None
        for buf in self.buffers[:2]:
            arr = np.asarray(list(buf)[-n_fft:], dtype=float)
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
            "fs": self.effective_sample_rate,
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

    # ==========================================================
    # Storage snapshot helper
    # ==========================================================
    def capture_snapshot(self, duration_s: float | None = None):
        """
        Return last duration_s seconds of decimated data for each channel.
        """
        if duration_s is None or duration_s <= 0:
            duration_s = self.storage_duration_s
        eff_rate = self.effective_sample_rate or self.sample_rate
        count = int(duration_s * eff_rate)
        data = []
        for buf in self.storage_buffers:
            arr = list(buf)
            if count and len(arr) > count:
                arr = arr[-count:]
            data.append(arr)
        return {
            "device": self.name,
            "display_name": self.display_name,
            "sample_rate": eff_rate,
            "effective_sample_rate": eff_rate,
            "channels": self.channels,
            "data": data,
        }
