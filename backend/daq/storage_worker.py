import threading
import time
from pathlib import Path
from datetime import datetime, timezone

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
        ts = datetime.now(timezone.utc)  # store as UTC so viewers show local correctly
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
