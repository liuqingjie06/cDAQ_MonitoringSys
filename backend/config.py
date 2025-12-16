# backend/config.py
"""
Configuration loader/saver backed by JSON.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict

ROOT_DIR = Path(__file__).parent
CONFIG_FILE = ROOT_DIR / "config.json"

DEFAULT_CONFIG: Dict[str, Any] = {
    "sample_rate": 2000,
    "samples_per_read": 4000,
    "fft_interval": 0.5,
    "wind": {
        "enabled": True,
        "mode": "sim",  # sim / rs485
        "sample_interval_s": 1.0,
        "stats_interval_s": 600.0,
        "sim_seed": 1,
        "rs485": {
            "port": "COM3",
            "baudrate": 9600,
            "slave_id": 1,
        },
    },
    "iot": {
        "type": "log",          # log / mqtt / http (placeholder)
        "host": "127.0.0.1",
        "port": 1883,
        "topic": "cdaq/data",
        "username": "",
        "password": ""
    },
    "devices": {
        
    },
}


def _deep_merge_defaults(data: Dict[str, Any], defaults: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(defaults)
    for k, v in (data or {}).items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = _deep_merge_defaults(v, out[k])  # type: ignore[arg-type]
        else:
            out[k] = v
    return out


def ensure_config_file() -> None:
    """Create config file with defaults if it does not exist."""
    if not CONFIG_FILE.exists():
        save_config(DEFAULT_CONFIG)


def load_config() -> Dict[str, Any]:
    ensure_config_file()
    with CONFIG_FILE.open("r", encoding="utf-8") as f:
        data = json.load(f)
    # Backward compatible: add missing keys from defaults
    return _deep_merge_defaults(data, DEFAULT_CONFIG)


def save_config(data: Dict[str, Any]) -> None:
    CONFIG_FILE.parent.mkdir(parents=True, exist_ok=True)
    with CONFIG_FILE.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
