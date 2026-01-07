from __future__ import annotations

import math
import random
import threading
import time
from collections import deque
from dataclasses import dataclass
from typing import Any, Deque, Dict, Optional, Tuple

import serial


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
      - bytesize: 8
      - parity: "N"
      - stopbits: 1
      - timeout_s: 0.5
      - protocol: "modbus_rtu"
      - registers: { speed: 0x0000, direction: 0x0001 }

    Implements Modbus RTU read for 5 registers (default) from register 0x0000.
    """

    def __init__(self, cfg: Dict[str, Any]):
        self.cfg = cfg
        self._ser: Optional[serial.Serial] = None

    @staticmethod
    def _crc16(data: bytes) -> bytes:
        crc = 0xFFFF
        for pos in data:
            crc ^= pos
            for _ in range(8):
                if (crc & 1) != 0:
                    crc >>= 1
                    crc ^= 0xA001
                else:
                    crc >>= 1
        return crc.to_bytes(2, "little")

    @staticmethod
    def _parity(value: str) -> str:
        key = str(value or "N").strip().upper()
        if key in ("E", "EVEN"):
            return serial.PARITY_EVEN
        if key in ("O", "ODD"):
            return serial.PARITY_ODD
        return serial.PARITY_NONE

    @staticmethod
    def _stopbits(value: float) -> float:
        if float(value) >= 2:
            return serial.STOPBITS_TWO
        return serial.STOPBITS_ONE

    def connect(self) -> bool:
        if self._ser and self._ser.is_open:
            return True
        port = self.cfg.get("port", "COM3")
        baudrate = int(self.cfg.get("baudrate", 9600))
        bytesize = int(self.cfg.get("bytesize", 8))
        parity = self._parity(self.cfg.get("parity", "N"))
        stopbits = self._stopbits(self.cfg.get("stopbits", 1))
        timeout_s = float(self.cfg.get("timeout_s", 0.5))
        try:
            self._ser = serial.Serial(
                port=port,
                baudrate=baudrate,
                bytesize=bytesize,
                parity=parity,
                stopbits=stopbits,
                timeout=timeout_s,
            )
        except Exception:
            self._ser = None
            return False
        try:
            self._ser.reset_input_buffer()
        except Exception:
            pass
        return True

    def close(self) -> None:
        if self._ser:
            try:
                self._ser.close()
            except Exception:
                pass
        self._ser = None

    def _build_request(self) -> bytes:
        slave_id = int(self.cfg.get("slave_id", 1))
        start_reg = int(self.cfg.get("start_register", 0))
        reg_count = int(self.cfg.get("register_count", 5))
        payload = bytes([
            slave_id & 0xFF,
            0x03,
            (start_reg >> 8) & 0xFF,
            start_reg & 0xFF,
            (reg_count >> 8) & 0xFF,
            reg_count & 0xFF,
        ])
        return payload + self._crc16(payload)

    def read(self) -> WindSample:
        if not self._ser or not self._ser.is_open:
            if not self.connect():
                raise RuntimeError("RS485 wind sensor not connected")
        if not self._ser:
            raise RuntimeError("RS485 wind sensor not connected")

        request = self._build_request()
        try:
            self._ser.reset_input_buffer()
        except Exception:
            pass
        self._ser.write(request)
        header = self._ser.read(3)
        if len(header) != 3:
            raise RuntimeError(f"RS485 response header length {len(header)} != 3")
        data_len = header[2]
        payload = self._ser.read(data_len + 2)
        response = header + payload

        if len(response) != 3 + data_len + 2:
            raise RuntimeError(f"RS485 response length {len(response)} != {3 + data_len + 2}")
        if response[1] != 0x03:
            raise RuntimeError(f"RS485 response function {response[1]:02X} != 03")
        if response[-2:] != self._crc16(response[:-2]):
            raise RuntimeError("RS485 CRC check failed")

        data = response[3:3 + data_len]
        if len(data) < 10:
            raise RuntimeError("RS485 response data too short")

        speed_raw = (data[0] << 8) | data[1]
        angle_raw = (data[6] << 8) | data[7]
        speed_mps = speed_raw / 10.0
        direction_deg = _wrap_deg(angle_raw / 10.0)
        return WindSample(ts=time.time(), speed_mps=float(speed_mps), direction_deg=float(direction_deg))


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
            if not self.connected:
                try:
                    self.connected = bool(self._sensor.connect())
                except Exception:
                    self.connected = False
            try:
                sample = self._sensor.read()
            except Exception:
                # Keep running; mark disconnected; no new sample.
                self.connected = False
                sample = None

            if sample is not None:
                self.connected = True
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

