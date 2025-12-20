import threading
import time
from pathlib import Path
from datetime import datetime

import numpy as np
from nptdms import TdmsWriter, ChannelObject


class StorageService:
    """
    Periodically captures recent raw data from devices and writes TDMS files.
    """

    def __init__(self, device_manager, cfg: dict):
        self.device_manager = device_manager
        self.enabled = bool(cfg.get("enabled", False))
        self.interval_s = float(cfg.get("interval_s", 600))
        self.duration_s = float(cfg.get("duration_s", 30))
        out_dir = cfg.get("output_dir") or (Path(__file__).resolve().parent.parent / "data")
        self.output_dir = Path(out_dir)
        self.filename_format = cfg.get("filename_format") or "{display_name}_{ts}.tdms"

        self._thread = None
        self._stop = threading.Event()

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
        ts = datetime.now()
        ts_str = ts.strftime("%d%m%y_%H%M%S")
        for name, dev in (self.device_manager.devices or {}).items():
            snap = dev.capture_snapshot(self.duration_s)
            if not snap or not snap.get("data"):
                continue
            try:
                self._write_tdms(ts, ts_str, snap)
            except Exception as e:
                print(f"[storage] write error for {name}:", e)

    def _write_tdms(self, ts: datetime, ts_str: str, snap: dict):
        data = snap.get("data") or []
        if not any(len(ch) for ch in data):
            return
        display_name = snap.get("display_name") or snap.get("device") or "device"
        filename = self.filename_format.format(display_name=display_name, ts=ts_str)
        path = self.output_dir / filename
        group_name = "Data"
        wf_start_time = snap.get("start_time") or ts
        fs = snap.get("effective_sample_rate") or snap.get("sample_rate")
        wf_increment = 1.0 / fs if fs else None

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
                props["wf_increment"] = wf_increment  # dt in seconds
                props["wf_time_unit"] = "s"
            if wf_start_time:
                props["wf_start_time"] = wf_start_time
            channels.append(ChannelObject(group_name, ch_name, arr, properties=props))

        with TdmsWriter(path) as writer:
            writer.write_segment(channels)

        print(f"[storage] wrote {path}")
