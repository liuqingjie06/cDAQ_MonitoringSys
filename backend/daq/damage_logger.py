import json
import datetime
from pathlib import Path


class DamageLogger:
    """
    Handles window stats logging and cumulative damage persistence.
    """
    def __init__(self, device_name: str, data_dir: Path, dphi_deg: float = 5.0):
        self.device_name = device_name
        self.data_dir = data_dir
        self.data_dir.mkdir(exist_ok=True)
        self.damage_file = self.data_dir / "damage_cumulative.txt"
        self.backup_file = self.data_dir / "damage_cumulative.bak"
        self.dphi_deg = dphi_deg

        self.cum_phi = None
        self.cum_damage = None
        self.cum_timestamp = None
        self._init_cumulative(dphi_deg)

    def _default_bins(self, dphi_deg: float):
        bins = int(360 / dphi_deg)
        # Start at 0°, step dphi (0, 5, 10, ...)
        phi = [i * dphi_deg for i in range(bins)]
        dmg = [0.0 for _ in phi]
        return phi, dmg

    def _try_load_json(self, path: Path):
        try:
            if not path.exists():
                return None
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return None

    def _init_cumulative(self, dphi_deg: float):
        data = self._try_load_json(self.damage_file)
        if not data:
            # Try backup if main file is corrupted/truncated (e.g. crash during write)
            data = self._try_load_json(self.backup_file)
            if data:
                try:
                    self.damage_file.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
                except Exception:
                    pass

        if data:
            self.cum_phi = data.get("phi_deg_list")
            self.cum_damage = data.get("D_phi_cum")
            self.cum_timestamp = data.get("timestamp")
            if isinstance(self.cum_phi, list) and isinstance(self.cum_damage, list) and len(self.cum_phi) and len(self.cum_damage):
                # If bins changed, keep existing damage by mapping to new bins.
                if len(self.cum_phi) != len(self.cum_damage):
                    self.cum_phi, self.cum_damage = self._default_bins(dphi_deg)
                    self.cum_timestamp = datetime.datetime.now().isoformat()
                    self._write_cumulative()
                return

        # default bins (new file or unrecoverable)
        self.cum_phi, self.cum_damage = self._default_bins(dphi_deg)
        self.cum_timestamp = datetime.datetime.now().isoformat()
        self._write_cumulative()

    def _write_cumulative(self):
        payload = {
            "timestamp": self.cum_timestamp,
            "device": self.device_name,
            "phi_deg_list": self.cum_phi,
            "D_phi_cum": self.cum_damage,
            "D_cum_max": max(self.cum_damage) if self.cum_damage else 0.0,
            "phi_deg_cum": self.cum_phi[self.cum_damage.index(max(self.cum_damage))] if self.cum_damage else 0.0,
        }
        data = json.dumps(payload, ensure_ascii=False, indent=2)
        # Best-effort backup of last good file, then atomic replace.
        try:
            if self.damage_file.exists():
                self.backup_file.write_text(self.damage_file.read_text(encoding="utf-8"), encoding="utf-8")
        except Exception:
            pass

        tmp = self.data_dir / (self.damage_file.name + ".tmp")
        tmp.write_text(data, encoding="utf-8")
        tmp.replace(self.damage_file)

    def update_cumulative(self, fatigue: dict, ts: datetime.datetime) -> dict:
        phi_list = fatigue.get("phi_deg_list") or []
        d_list = fatigue.get("D_phi") or []
        if not phi_list or not d_list:
            return fatigue

        if self.cum_phi is None or self.cum_damage is None:
            self._init_cumulative(5.0)

        if len(self.cum_damage) != len(d_list) or len(self.cum_phi) != len(phi_list):
            # Preserve existing cumulative damage by mapping old bins to new bins.
            old_phi = list(self.cum_phi) if self.cum_phi else []
            old_damage = list(self.cum_damage) if self.cum_damage else []
            self.cum_phi = list(phi_list)
            self.cum_damage = [0.0 for _ in phi_list]
            if old_phi and old_damage and len(old_phi) == len(old_damage):
                for i, new_phi in enumerate(self.cum_phi):
                    # circular closest-angle match
                    best_j = 0
                    best_dist = float("inf")
                    for j, p in enumerate(old_phi):
                        dist = abs(((p - new_phi + 180.0) % 360.0) - 180.0)
                        if dist < best_dist:
                            best_dist = dist
                            best_j = j
                    try:
                        self.cum_damage[i] = float(old_damage[best_j])
                    except Exception:
                        self.cum_damage[i] = 0.0

        self.cum_damage = [c + d for c, d in zip(self.cum_damage, d_list)]
        self.cum_timestamp = ts.isoformat()

        D_cum_max = max(self.cum_damage) if self.cum_damage else 0.0
        idx_max = self.cum_damage.index(D_cum_max) if self.cum_damage else 0
        phi_deg_cum = self.cum_phi[idx_max] if self.cum_phi else 0.0

        fatigue["D_phi_cum"] = list(self.cum_damage)
        fatigue["D_cum_max"] = D_cum_max
        fatigue["phi_deg_cum"] = phi_deg_cum

        self._write_cumulative()
        return fatigue

    def reset_cumulative(self):
        """Reset cumulative damage to zeros and persist."""
        self.cum_phi, self.cum_damage = self._default_bins(self.dphi_deg)
        self.cum_timestamp = datetime.datetime.now().isoformat()
        self._write_cumulative()
        return {
            "timestamp": self.cum_timestamp,
            "device": self.device_name,
            "phi_deg_list": self.cum_phi,
            "D_phi_cum": self.cum_damage,
            "D_cum_max": 0.0,
            "phi_deg_cum": 0.0,
        }

    def write_window(self, device_name: str, stats: list, fatigue: dict, start_ts: float):
        dt = datetime.datetime.fromtimestamp(start_ts)
        date_str = dt.strftime("%Y%m%d")
        file_path = self.data_dir / f"{date_str}.txt"

        lines = []
        for idx, st in enumerate(stats):
            if st["count"] == 0:
                continue
            rms = (st["sumsq"] / st["count"]) ** 0.5 if st["count"] else 0.0
            line = (
                f"{dt.strftime('%Y-%m-%d %H:%M:%S')},"
                f"device={device_name},ch={idx},"
                f"max={st['max']:.6f},min={st['min']:.6f},rms={rms:.6f}"
            )
            lines.append(line)

        if fatigue:
            lines.append(
                f"{dt.strftime('%Y-%m-%d %H:%M:%S')},device={device_name},"
                f"fatigue_Dmax={fatigue.get('Dmax',0):.6e},"
                f"phi_deg={fatigue.get('phi_deg',0):.2f},"
                f"Sa_max={fatigue.get('Sa_max',0):.4f}"
            )

        if lines:
            with file_path.open("a", encoding="utf-8") as f:
                for line in lines:
                    f.write(line + "\n")
