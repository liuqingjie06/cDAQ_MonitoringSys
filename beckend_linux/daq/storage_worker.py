import threading
import time
import shutil
from pathlib import Path
from datetime import datetime, timezone

import numpy as np

from .analysis import acc_to_disp
from . import iot
from nptdms import TdmsWriter, ChannelObject


class StorageService:
    """
    Periodically captures recent raw data from devices and writes TDMS files.
    """

    def __init__(self, device_manager, cfg: dict, wind_service=None):
        self.device_manager = device_manager
        self.wind_service = wind_service
        self.enabled = bool(cfg.get("enabled", False))
        self.interval_s = float(cfg.get("interval_s", 600))
        self.duration_s = float(cfg.get("duration_s", 30))
        out_dir = cfg.get("output_dir") or "data"
        self.output_dir = self._resolve_output_dir(out_dir)
        self.filename_format = cfg.get("filename_format") or "{display_name}_{ts}.tdms"
        self.retention_months = int(cfg.get("retention_months", 3))

        self._thread = None
        self._stop = threading.Event()

    def _resolve_output_dir(self, out_dir):
        p = Path(out_dir)
        if p.is_absolute():
            return p
        backend_root = Path(__file__).resolve().parent.parent
        return (backend_root / p).resolve()

    def start(self):
        if not self.enabled:
            return
        if self._thread and self._thread.is_alive():
            return
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()
        print(f"[storage] started: interval={self.interval_s}s, duration={self.duration_s}s, dir={self.output_dir}")

    def stop(self):
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=2.0)
            self._thread = None
        print("[storage] stopped")

    def _loop(self):
        while not self._stop.is_set():
            start_ts = time.time()
            try:
                self._run_once()
            except Exception as e:
                print("[storage] run error:", e)
            # sleep remaining time
            elapsed = time.time() - start_ts
            wait_for = max(0.0, self.interval_s - elapsed)
            self._stop.wait(wait_for)

    def _run_once(self):
        ts = datetime.now(timezone.utc)  # store as UTC so viewers show local correctly
        ts_str = ts.strftime("%d%m%y_%H%M%S")
        # Cleanup old TDMS based on retention
        try:
            self._cleanup_old_tdms(ts)
        except Exception as e:
            print("[storage] cleanup error:", e)
        for name, dev in (self.device_manager.devices or {}).items():
            snap = dev.capture_snapshot(self.duration_s)
            if not snap or not snap.get("data"):
                continue
            try:
                self._write_tdms(ts, ts_str, snap)
            except Exception as e:
                print(f"[storage] write error for {name}:", e)
            try:
                self._publish_iot_data(ts, dev, snap)
            except Exception as e:
                print(f"[storage] iot publish error for {name}:", e)

        try:
            self._publish_wind_stats(ts)
        except Exception as e:
            print("[storage] wind publish error:", e)

    def _publish_iot_data(self, ts: datetime, dev, snap: dict):
        topic_base = (dev.display_name or dev.name or "").strip() or dev.name
        timestamp = ts.astimezone().strftime("%Y-%m-%dT%H:%M:%S")
        interval_s = float(self.interval_s)

        vib_payload = self._build_vib_stats_payload(dev, snap, timestamp, interval_s, self.duration_s)
        if vib_payload:
            iot.publish(vib_payload, topic=f"{topic_base}/data/vib")

        disp_payload = self._build_disp_stats_payload(dev, snap, timestamp, interval_s, self.duration_s)
        if disp_payload:
            iot.publish(disp_payload, topic=f"{topic_base}/data/disp")

        try:
            fatigue = dev.get_fatigue_snapshot()
        except Exception:
            fatigue = None
        if fatigue:
            cum_phi = fatigue.get("phi_deg_list")
            cum = fatigue.get("D_phi_cum")
            if cum_phi is None or cum is None:
                try:
                    cum_phi = dev.damage_logger.cum_phi
                    cum = dev.damage_logger.cum_damage
                except Exception:
                    cum_phi = None
                    cum = None
            payload = {
                "device": dev.name,
                "display_name": dev.display_name,
                "timestamp": timestamp,
                "interval_s": interval_s,
                "fatigue_cumulative": {
                    "phi_deg_list": cum_phi,
                    "D_phi_cum": cum,
                },
            }
            iot.publish(payload, topic=f"{topic_base}/data/fatigure")

    def _build_vib_stats_payload(self, dev, snap: dict, timestamp: str, interval_s: float, window_s: float):
        data = snap.get("data") or []
        if not data:
            return None
        channels = []
        ch_cfgs = snap.get("channels") or []
        for idx, ch_data in enumerate(data):
            arr = np.asarray(ch_data, dtype=float)
            if arr.size == 0:
                stats = {"acc_max": None, "acc_min": None, "acc_rms": None, "acc_p2p": None}
            else:
                acc_max = float(np.max(arr))
                acc_min = float(np.min(arr))
                acc_rms = float(np.sqrt(np.mean(arr ** 2)))
                acc_p2p = float(acc_max - acc_min)
                stats = {"acc_max": acc_max, "acc_min": acc_min, "acc_rms": acc_rms, "acc_p2p": acc_p2p}
            try:
                ch_id = ch_cfgs[idx].get("id", idx)
            except Exception:
                ch_id = idx
            channels.append({"ch": ch_id, **stats})
        return {
            "device": dev.name,
            "display_name": dev.display_name,
            "timestamp": timestamp,
            "interval_s": interval_s,
            "channels": channels,
            "window_s": float(window_s),
        }

    def _build_disp_stats_payload(self, dev, snap: dict, timestamp: str, interval_s: float, window_s: float):
        data = snap.get("data") or []
        if not data:
            return None
        ch_cfgs = snap.get("channels") or []
        disp_stats = []
        for idx, ch_data in enumerate(data):
            arr = np.asarray(ch_data, dtype=float)
            if arr.size == 0:
                disp_stats.append({"max": None, "min": None, "rms": None, "p2p": None})
                continue
            unit = ""
            try:
                unit = (ch_cfgs[idx].get("unit") or "").lower()
            except Exception:
                unit = ""
            if unit == "g":
                arr = arr * 9.80665
            fs = snap.get("effective_sample_rate") or snap.get("sample_rate")
            if not fs:
                disp_stats.append({"max": None, "min": None, "rms": None, "p2p": None})
                continue
            disp = acc_to_disp(arr, fs=fs, method=getattr(dev, "disp_method", "fft"))
            if disp.size:
                dmax = float(np.max(disp))
                dmin = float(np.min(disp))
                rms = float(np.sqrt(np.mean(disp ** 2)))
                p2p = float(dmax - dmin)
                disp_stats.append({"max": dmax, "min": dmin, "rms": rms, "p2p": p2p})
            else:
                disp_stats.append({"max": None, "min": None, "rms": None, "p2p": None})

        main_idx = 0
        if disp_stats:
            best = None
            for i, st in enumerate(disp_stats):
                if st["max"] is None or st["min"] is None:
                    continue
                peak = max(abs(st["max"]), abs(st["min"]))
                if best is None or peak > best[0]:
                    best = (peak, i)
            if best is not None:
                main_idx = best[1]

        try:
            ch_id = ch_cfgs[main_idx].get("id", main_idx)
        except Exception:
            ch_id = main_idx
        main_stats = disp_stats[main_idx] if disp_stats else {"max": None, "min": None, "rms": None, "p2p": None}
        return {
            "device": dev.name,
            "display_name": dev.display_name,
            "timestamp": timestamp,
            "interval_s": interval_s,
            "main_channel": ch_id,
            "disp_max": main_stats["max"],
            "disp_min": main_stats["min"],
            "disp_rms": main_stats["rms"],
            "disp_p2p": main_stats["p2p"],
            "window_s": float(window_s),
        }

    def _publish_wind_stats(self, ts: datetime):
        if not self.wind_service:
            return
        status = self.wind_service.get_status()
        stats = (status or {}).get("stats") or {}
        if not stats:
            return
        timestamp = ts.astimezone().strftime("%Y-%m-%dT%H:%M:%S")
        interval_s = float(self.interval_s)
        payload = {
            "timestamp": timestamp,
            "interval_s": interval_s,
            "speed_mean": stats.get("speed_mean"),
            "speed_max": stats.get("speed_max"),
            "speed_min": stats.get("speed_min"),
            "direction_mean_deg": stats.get("direction_mean_deg"),
            "n": stats.get("n"),
        }
        for name, dev in (self.device_manager.devices or {}).items():
            topic_base = (dev.display_name or dev.name or "").strip() or dev.name
            iot.publish(payload, topic=f"{topic_base}/data/wind")

    def _write_tdms(self, ts: datetime, ts_str: str, snap: dict):
        data = snap.get("data") or []
        if not any(len(ch) for ch in data):
            return
        display_name = snap.get("display_name") or snap.get("device") or "device"
        filename = self.filename_format.format(display_name=display_name, ts=ts_str)
        # Organize by month/day folders for cleaner data directory
        local_ts = ts.astimezone()
        month = local_ts.strftime("%Y%m")
        day = local_ts.strftime("%d")
        dest_dir = self.output_dir / month / day
        dest_dir.mkdir(parents=True, exist_ok=True)
        path = dest_dir / filename
        group_name = "Data"
        wf_start_time = snap.get("start_time") or ts  # timezone-aware UTC datetime
        fs = snap.get("effective_sample_rate") or snap.get("sample_rate")
        wf_increment = float(1.0 / fs) if fs else None
        # TDMS waveform metadata so viewers know time axis
        wf_start_offset = 0.0  # seconds from wf_start_time
        wf_start_index = 0
        wf_xname = "Time"
        wf_xunit_string = "s"

        channels = []
        ch_cfgs = snap.get("channels") or []
        for idx, ch_data in enumerate(data):
            try:
                ch_id = ch_cfgs[idx].get("id", idx)
                unit = ch_cfgs[idx].get("unit", "")
                remark = ch_cfgs[idx].get("remark", "")
                sensitivity = ch_cfgs[idx].get("sensitivity")
                coupling = ch_cfgs[idx].get("coupling")
                ch_type = ch_cfgs[idx].get("type")
                iepe = ch_cfgs[idx].get("iepe")
            except Exception:
                ch_id = idx
                unit = ""
                remark = ""
                sensitivity = None
                coupling = None
                ch_type = None
                iepe = None
            ch_name = f"CH{ch_id}"
            arr = np.asarray(ch_data, dtype=float)
            props = {
                "sample_rate": snap.get("sample_rate"),
                "effective_sample_rate": fs,
                "unit": unit,
                "unit_string": unit,  # TDMS waveform unit
                "remark": remark,
                "sensitivity": sensitivity,
                "coupling": coupling,
                "type": ch_type,
                "iepe": iepe,
            }
            if wf_increment:
                props["wf_increment"] = float(wf_increment)  # dt in seconds
                props["wf_start_time"] = wf_start_time  # datetime for TDMS waveform
                props["wf_start_offset"] = float(wf_start_offset)
                props["wf_start_index"] = int(wf_start_index)
                props["wf_samples"] = int(len(arr))
                props["wf_xname"] = wf_xname
                props["wf_xunit_string"] = wf_xunit_string
                props["wf_time_reference"] = "absolute"  # hint for readers: absolute time, not relative
            if wf_start_time:
                props.setdefault("wf_start_time", wf_start_time)
            channels.append(ChannelObject(group_name, ch_name, arr, properties=props))

        with TdmsWriter(path) as writer:
            writer.write_segment(channels)

        print(f"[storage] wrote {path}")

    def _cleanup_old_tdms(self, now: datetime):
        """Remove TDMS month folders older than retention_months."""
        if not self.retention_months or self.retention_months <= 0:
            return
        try:
            curr = now.astimezone()
        except Exception:
            curr = datetime.now()
        curr_val = curr.year * 12 + curr.month
        cutoff = curr_val - (self.retention_months - 1)
        if not self.output_dir.exists():
            return
        for month_dir in self.output_dir.iterdir():
            if not month_dir.is_dir():
                continue
            name = month_dir.name
            if len(name) != 6 or not name.isdigit():
                continue
            try:
                y = int(name[:4])
                m = int(name[4:6])
                val = y * 12 + m
            except Exception:
                continue
            if val < cutoff:
                try:
                    shutil.rmtree(month_dir)
                    print(f"[storage] removed old TDMS dir: {month_dir}")
                except Exception as e:
                    print(f"[storage] failed to remove {month_dir}: {e}")
