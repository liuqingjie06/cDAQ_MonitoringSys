from __future__ import annotations

import math
import random
import threading
import time
from collections import deque
from dataclasses import dataclass
from typing import Any, Deque, Dict, Optional, Tuple


@dataclass
class WindSample:
    ts: float
    speed_mps: float
    direction_deg: float


def _wrap_deg(deg: float) -> float:
    return deg % 360.0


def _clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def _circular_mean_deg(angles_deg) -> float:
    if not angles_deg:
        return 0.0
    s = sum(math.sin(math.radians(a)) for a in angles_deg)
    c = sum(math.cos(math.radians(a)) for a in angles_deg)
    if s == 0 and c == 0:
        return 0.0
    return _wrap_deg(math.degrees(math.atan2(s, c)))


class WindSensorBase:
    """Abstract wind sensor interface."""

    def connect(self) -> bool:
        return False

    def close(self) -> None:
        return None

    def read(self) -> WindSample:
        raise NotImplementedError


class SimulatedWindSensor(WindSensorBase):
    def __init__(self, seed: Optional[int] = None):
        self._rng = random.Random(seed)
        self._speed = 5.0
        self._direction = 90.0

    def connect(self) -> bool:
        return False  # no physical device; frontend should show not connected

    def read(self) -> WindSample:
        # Random-walk + gentle periodic component.
        t = time.time()
        self._speed += self._rng.gauss(0.0, 0.15)
        self._speed += 0.05 * math.sin(t / 15.0)
        self._speed = _clamp(self._speed, 0.0, 35.0)

        self._direction += self._rng.gauss(0.0, 1.5)
        self._direction += 1.0 * math.sin(t / 60.0)
        self._direction = _wrap_deg(self._direction)

        return WindSample(ts=t, speed_mps=float(self._speed), direction_deg=float(self._direction))


class Rs485WindSensor(WindSensorBase):
    """
    Placeholder for a real RS485/Modbus wind sensor.

    Expected config keys (example):
      - port: "COM3"
      - baudrate: 9600
      - slave_id: 1
      - protocol: "modbus_rtu"
      - registers: { speed: 0x0000, direction: 0x0001 }

    Implementation intentionally left as a stub until hardware is available.
    """

    def __init__(self, cfg: Dict[str, Any]):
        self.cfg = cfg

    def connect(self) -> bool:
        return False

    def read(self) -> WindSample:
        raise RuntimeError("RS485 wind sensor not implemented (no hardware connected)")


class WindService:
    """
    Samples wind speed/direction every `sample_interval_s`.
    Computes a stats snapshot every `stats_interval_s`.
    """

    def __init__(self, socketio=None, cfg: Optional[Dict[str, Any]] = None):
        self.socketio = socketio
        self.cfg = cfg or {}

        self._lock = threading.Lock()
        self._running = False
        self._thread: Optional[threading.Thread] = None

        self._sensor: WindSensorBase = SimulatedWindSensor()
        self.connected: bool = False
        self.mode: str = "sim"

        self.last_sample: Optional[WindSample] = None
        self.last_stats: Dict[str, Any] = {}

        self._window: Deque[WindSample] = deque()

    def update_config(self, cfg: Dict[str, Any]) -> None:
        self.cfg = cfg or {}

        enabled = bool(self.cfg.get("enabled", True))
        mode = (self.cfg.get("mode") or "sim").lower()
        self.mode = mode

        if not enabled:
            self.stop()
            return

        if mode == "rs485":
            self._sensor = Rs485WindSensor(self.cfg.get("rs485") or {})
        else:
            self._sensor = SimulatedWindSensor(seed=self.cfg.get("sim_seed"))

        self.start()

    def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._running = False
        if self._thread:
            self._thread.join(timeout=1.0)
            self._thread = None
        try:
            self._sensor.close()
        except Exception:
            pass

    def get_status(self) -> Dict[str, Any]:
        with self._lock:
            s = self.last_sample
            return {
                "enabled": bool(self.cfg.get("enabled", True)),
                "mode": self.mode,
                "connected": bool(self.connected),
                "sample_interval_s": float(self.cfg.get("sample_interval_s", 1.0)),
                "stats_interval_s": float(self.cfg.get("stats_interval_s", 600.0)),
                "sample": None if not s else {
                    "ts": s.ts,
                    "speed_mps": s.speed_mps,
                    "direction_deg": s.direction_deg,
                },
                "stats": self.last_stats or None,
            }

    def _emit(self, event: str, payload: Dict[str, Any]) -> None:
        if not self.socketio:
            return
        try:
            self.socketio.emit(event, payload)
        except Exception:
            pass

    def _compute_stats(self, samples: Tuple[WindSample, ...]) -> Dict[str, Any]:
        speeds = [s.speed_mps for s in samples]
        dirs = [s.direction_deg for s in samples]
        if not speeds:
            return {}
        return {
            "ts_start": samples[0].ts,
            "ts_end": samples[-1].ts,
            "speed_min": float(min(speeds)),
            "speed_max": float(max(speeds)),
            "speed_mean": float(sum(speeds) / len(speeds)),
            "direction_mean_deg": float(_circular_mean_deg(dirs)),
            "n": int(len(speeds)),
        }

    def _loop(self) -> None:
        sample_interval_s = float(self.cfg.get("sample_interval_s", 1.0))
        stats_interval_s = float(self.cfg.get("stats_interval_s", 600.0))
        stats_every_n = max(1, int(round(stats_interval_s / max(sample_interval_s, 1e-6))))

        # Initial connect attempt
        try:
            self.connected = bool(self._sensor.connect())
        except Exception:
            self.connected = False

        counter = 0
        while self._running:
            t0 = time.time()
            try:
                sample = self._sensor.read()
            except Exception:
                # Keep running; mark disconnected; no new sample.
                self.connected = False
                sample = None

            if sample is not None:
                with self._lock:
                    self.last_sample = sample
                    self._window.append(sample)
                    # Keep roughly one stats window of samples
                    while len(self._window) > stats_every_n:
                        self._window.popleft()
                self._emit("wind_sample", {
                    "connected": bool(self.connected),
                    "mode": self.mode,
                    "ts": sample.ts,
                    "speed_mps": sample.speed_mps,
                    "direction_deg": sample.direction_deg,
                })

                counter += 1
                if counter >= stats_every_n:
                    counter = 0
                    with self._lock:
                        window = tuple(self._window)
                    stats = self._compute_stats(window)
                    with self._lock:
                        self.last_stats = stats
                    self._emit("wind_stats", {
                        "connected": bool(self.connected),
                        "mode": self.mode,
                        "stats": stats,
                    })

            # Sleep to maintain cadence
            dt = time.time() - t0
            time.sleep(max(0.0, sample_interval_s - dt))

