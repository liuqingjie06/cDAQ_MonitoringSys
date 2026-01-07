import threading
import time
import queue
import threading
import numpy as np

from .analysis import fatigue_damage, acc_to_disp
from . import iot


class AnalysisWorker:
    """
    Consumes sample batches, performs stats + fatigue analysis, writes logs via DamageLogger.
    """
    def __init__(self, device_name, sample_rate, log_interval, damage_logger, channels_cfg=None, disp_method="fft"):
        self.device_name = device_name
        self.sample_rate = sample_rate
        self.log_interval = log_interval
        self.damage_logger = damage_logger
        self.channels_cfg = channels_cfg or []
        self.disp_method = disp_method

        self.queue = queue.Queue(maxsize=3)
        self.thread = None
        self.running = False

        self.log_window_start = time.time()
        self.log_stats = []
        self.analysis_buffers = [[] for _ in (self.channels_cfg or [None, None])]
        self.last_fatigue = None
        self.last_chunk_len = None

    def start(self):
        if self.running:
            return
        self.running = True
        self.thread = threading.Thread(target=self._loop, daemon=True)
        self.thread.start()

    def stop(self):
        self.running = False
        if self.thread:
            self.thread.join(timeout=1.0)
            self.thread = None

    def submit(self, data):
        try:
            self.queue.put_nowait(data)
        except queue.Full:
            try:
                self.queue.get_nowait()
                self.queue.put_nowait(data)
            except Exception:
                pass

    def _ensure_stats_len(self, n):
        while len(self.log_stats) < n:
            self.log_stats.append({"min": float("inf"), "max": float("-inf"), "sumsq": 0.0, "count": 0})

    def _accumulate_stats(self, data):
        self._ensure_stats_len(len(data))
        for idx, ch_data in enumerate(data):
            st = self.log_stats[idx]
            if not ch_data:
                continue
            try:
                st["max"] = max(st["max"], max(ch_data))
                st["min"] = min(st["min"], min(ch_data))
                st["sumsq"] += sum(x * x for x in ch_data)
                st["count"] += len(ch_data)
            except Exception:
                pass

    def _compute_disp_stats(self, window_sec: float | None = None):
        disp_stats = []
        for idx, arr in enumerate(self.analysis_buffers):
            if not arr:
                disp_stats.append({"max": 0.0, "min": 0.0, "rms": 0.0, "p2p": 0.0})
                continue
            unit = ""
            try:
                unit = (self.channels_cfg[idx].get("unit") or "").lower()
            except Exception:
                unit = ""
            # Use full window (log_interval) by default; optionally limit if window_sec is provided.
            if window_sec is not None and self.sample_rate > 0:
                max_samples = int(self.sample_rate * window_sec)
                sliced = arr[-max_samples:] if len(arr) > max_samples else arr
            else:
                sliced = arr
            data = np.asarray(sliced, dtype=float)
            if unit == "g":
                data = data * 9.80665
            disp = acc_to_disp(data, fs=self.sample_rate, method=self.disp_method)
            if disp.size:
                dmax = float(np.max(disp))
                dmin = float(np.min(disp))
                rms = float(np.sqrt(np.mean(disp ** 2)))
                p2p = float(dmax - dmin)
                disp_stats.append({"max": dmax, "min": dmin, "rms": rms, "p2p": p2p})
            else:
                disp_stats.append({"max": 0.0, "min": 0.0, "rms": 0.0, "p2p": 0.0})
        return disp_stats

    def _dominant_freq_hz(self, data_arr: np.ndarray) -> float | None:
        """Return dominant frequency (Hz) for a real signal; None if insufficient data."""
        if data_arr.size < 2 or self.sample_rate <= 0:
            return None
        try:
            window = np.hanning(data_arr.size)
            X = np.fft.rfft(data_arr * window)
            freqs = np.fft.rfftfreq(data_arr.size, d=1.0 / self.sample_rate)
            if freqs.size < 2:
                return None
            mag = np.abs(X)
            # skip DC
            idx = int(np.argmax(mag[1:])) + 1
            return float(freqs[idx])
        except Exception:
            return None

    def _loop(self):
        while self.running:
            try:
                data = self.queue.get(timeout=0.5)
            except queue.Empty:
                continue

            now = time.time()
            self._accumulate_stats(data)
            try:
                if data and len(data[0]):
                    self.last_chunk_len = len(data[0])
            except Exception:
                pass

            # accumulate for fatigue on ch0/ch1
            if len(data) >= 2:
                try:
                    self.analysis_buffers[0].extend(data[0])
                    self.analysis_buffers[1].extend(data[1])
                except Exception:
                    pass

            if now - self.log_window_start >= self.log_interval:
                start_ts = self.log_window_start
                self.log_window_start = now
                fatigue = None
                try:
                    ax = np.array(self.analysis_buffers[0], dtype=float)
                    ay = np.array(self.analysis_buffers[1], dtype=float)
                    fatigue = fatigue_damage(
                        ax, ay,
                        fs=self.sample_rate,
                        k_disp2stress=90.62 / 0.4,
                        et=2.05e5,
                        disp_method=self.disp_method
                    )
                    import datetime
                    fatigue["timestamp"] = datetime.datetime.fromtimestamp(now).isoformat()
                    fatigue["device"] = self.device_name
                    fatigue = self.damage_logger.update_cumulative(
                        fatigue,
                        ts=datetime.datetime.fromtimestamp(now)
                    )
                    self.last_fatigue = fatigue
                except Exception as e:
                    print(f"[{self.device_name}] fatigue error:", e)

                # compute displacement stats for this window
                try:
                    disp_stats = self._compute_disp_stats()
                except Exception:
                    disp_stats = []

                self.damage_logger.write_window(
                    device_name=self.device_name,
                    stats=self.log_stats,
                    disp_stats=disp_stats,
                    fatigue=fatigue,
                    start_ts=start_ts
                )

                try:
                    cum = self.damage_logger.cum_damage or []
                    cum_phi = self.damage_logger.cum_phi or []
                    payload = {
                        "device": self.device_name,
                        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(now)),
                        "channels": [
                            {
                                "ch": i,
                                "acc_max": None if s["count"] == 0 else s["max"],
                                "acc_min": None if s["count"] == 0 else s["min"],
                                "acc_rms": None if s["count"] == 0 else (s["sumsq"] / s["count"]) ** 0.5,
                                "acc_p2p": None if s["count"] == 0 else (s["max"] - s["min"]),
                                "disp_max": disp_stats[i]["max"] if i < len(disp_stats) else None,
                                "disp_min": disp_stats[i]["min"] if i < len(disp_stats) else None,
                                "disp_rms": disp_stats[i]["rms"] if i < len(disp_stats) else None,
                                "disp_p2p": disp_stats[i]["p2p"] if i < len(disp_stats) else None,
                                "main_freq_hz": self._dominant_freq_hz(np.asarray(self.analysis_buffers[i], dtype=float)) if i < len(self.analysis_buffers) else None,
                            }
                            for i, s in enumerate(self.log_stats)
                        ],
                        "fatigue_cumulative": {
                            "phi_deg_list": cum_phi,
                            "D_phi_cum": cum,
                        }
                    }
                    iot.publish(payload)
                except Exception as e:
                    print(f"[{self.device_name}] iot publish error:", e)

                self.log_stats = []
                self.analysis_buffers = [[] for _ in (self.channels_cfg or [None, None])]
