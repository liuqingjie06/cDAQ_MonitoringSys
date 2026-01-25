# backend/daq/analysis.py
import numpy as np
import math

try:
    import pywt

    PYWT_AVAILABLE = True
except ImportError:
    PYWT_AVAILABLE = False

try:
    from .. import config as _config

    DEFAULT_WAVELET_INTERVAL_S = float(getattr(_config, "FFT_WINDOW_S", 30.0))
except Exception:
    DEFAULT_WAVELET_INTERVAL_S = 30.0

def detrend_linear(x: np.ndarray) -> np.ndarray:
    if x.size == 0:
        return x
    t = np.arange(x.size, dtype=float)
    p = np.polyfit(t, x, 1)
    return x - (p[0] * t + p[1])


def wavelet_remove_trend(data: np.ndarray, wavelet: str = "db6", level: int | None = None) -> np.ndarray:
    if not PYWT_AVAILABLE:
        return data
    n = len(data)
    if n < 4:
        return data
    if level is None:
        level = int(np.log2(n)) - 3
        if level < 1:
            level = 1
    coeffs = pywt.wavedec(data, wavelet, mode="symmetric", level=level)
    coeffs[0] = np.zeros_like(coeffs[0])
    rec = pywt.waverec(coeffs, wavelet, mode="symmetric")
    if len(rec) > n:
        rec = rec[:n]
    return rec

def disp_freq_domain_window(
    acc_win: np.ndarray,
    fs: float,
    fc_hp: float,
    win_name: str,
    f0_reg: float = 0.03,
    hp_slope: float = 4.0,
) -> np.ndarray:
    """
    稳健频域积分（窗口内）
    - 正则化：1/(w^2 + w0^2)，避免低频发散
    - 平滑高通：W_hp = 1 / (1 + (fc/f)^hp_slope)，代替硬切
    - 位移输出再去趋势（仅影响位移，不动加速度显示）

    参数建议（0.2 Hz 主频）：
      f0_reg = 0.02~0.05 Hz  (越小越保留低频，但更易漂)
      fc_hp  = 0.02~0.08 Hz  (用于“抑制DC泄漏”的软高通)
      hp_slope = 4~8         (越大越接近硬切)
    """
    n = len(acc_win)
    if n < 128:
        return np.zeros_like(acc_win)

    a = acc_win.astype(float)
    a = a - np.mean(a)

    if win_name == "hann":
        w = np.hanning(n)
    elif win_name == "hamming":
        w = np.hamming(n)
    else:
        w = np.ones(n)

    A = np.fft.rfft(a * w)
    freqs = np.fft.rfftfreq(n, d=1.0 / fs)
    omega = 2.0 * np.pi * freqs

    w0 = 2.0 * np.pi * max(1e-6, float(f0_reg))
    denom = -(omega**2 + w0**2)

    f = freqs.copy()
    W_hp = np.zeros_like(f)
    eps = 1e-12
    valid = f > 0
    fc = max(0.0, float(fc_hp))
    if fc <= 0:
        W_hp[valid] = 1.0
    else:
        p = max(1.0, float(hp_slope))
        W_hp[valid] = 1.0 / (1.0 + (fc / (f[valid] + eps)) ** p)

    X = A * W_hp / denom
    X[0] = 0.0

    x = np.fft.irfft(X, n=n)

    w_rms = np.sqrt(np.mean(w**2))
    if w_rms > 1e-12:
        x = x / w_rms

    x = detrend_linear(x)

    return x


def _acc_to_disp_fft(acc: np.ndarray, fs: float) -> np.ndarray:
    """
    Double integration via FFT (windowed, regularized).
    """
    if acc.size == 0:
        return acc
    if fs <= 0:
        return acc
    acc = np.asarray(acc, dtype=float)
    return disp_freq_domain_window(acc, fs=fs, fc_hp=0.03, win_name="hann")


def _acc_to_disp_time(acc: np.ndarray, fs: float) -> np.ndarray:
    """
    Double integration in time domain with simple de-mean + detrend.
    """
    if acc.size == 0:
        return acc
    if fs <= 0:
        return acc
    acc = np.asarray(acc, dtype=float)
    acc = acc - np.mean(acc)
    vel = np.cumsum(acc) / fs
    vel = vel - np.mean(vel)
    disp = np.cumsum(vel) / fs
    t = np.arange(disp.size)
    p = np.polyfit(t, disp, 1)
    disp = disp - (p[0] * t + p[1])
    return disp


class Accel2DispKF:
    def __init__(
        self,
        fs: float,
        sigma_a: float = 0.05,
        sigma_b: float = 1e-6,
        sigma_x_pseudo: float = 2,
        wavelet_interval_s: float | None = None,
        wavelet: str = "db6",
    ):
        self.fs = float(fs)
        self.dt = 1.0 / self.fs
        self.x = np.zeros((3, 1))
        self.P = np.eye(3)
        self.wavelet_interval_s = float(
            DEFAULT_WAVELET_INTERVAL_S if wavelet_interval_s is None else wavelet_interval_s
        )
        self.wavelet = wavelet
        self._wavelet_buf: list[float] = []
        self.set_params(fs, sigma_a, sigma_b, sigma_x_pseudo)
        self.reset()

    def set_params(self, fs: float, sigma_a: float, sigma_b: float, sigma_x_pseudo: float):
        self.fs = float(fs)
        self.dt = 1.0 / self.fs
        dt = self.dt

        # State transition: [x, v, b]
        self.F = np.array(
            [
                [1.0, dt, -0.5 * dt**2],
                [0.0, 1.0, -dt],
                [0.0, 0.0, 1.0],
            ]
        )
        self.B = np.array([[0.5 * dt**2], [dt], [0.0]])

        # Process noise Q
        self.Q = np.zeros((3, 3))
        self.Q[0, 0] = 1e-8
        self.Q[1, 1] = 1e-7
        self.Q[2, 2] = (sigma_b**2) * dt

        # Pseudo measurement on displacement to suppress drift
        self.H = np.array([[1.0, 0.0, 0.0]])
        self.R = np.array([[sigma_x_pseudo**2]])

    def reset(self):
        self.x = np.zeros((3, 1))
        self.P = np.diag([0.1, 0.1, 0.01])
        self._wavelet_buf = []

    def force_state(self, x_val: float = 0.0, v_val: float = 0.0):
        self.x[0, 0] = x_val
        self.x[1, 0] = v_val

    def step_block(self, acc_block: np.ndarray) -> np.ndarray:
        out = np.zeros(len(acc_block))
        I = np.eye(3)
        for i, a_meas in enumerate(acc_block):
            # 1) Predict
            u = np.array([[a_meas]])
            self.x = self.F @ self.x + self.B @ u
            self.P = self.F @ self.P @ self.F.T + self.Q

            # 2) Weak constraint update (pull displacement toward 0)
            z = np.array([[0.0]])
            y = z - (self.H @ self.x)
            S = self.H @ self.P @ self.H.T + self.R
            K = self.P @ self.H.T / S[0, 0]
            self.x = self.x + K * y
            self.P = (I - K @ self.H) @ self.P

            out[i] = float(self.x[0, 0])

        if PYWT_AVAILABLE and self.wavelet_interval_s > 0:
            self._wavelet_buf.extend(out.tolist())
            target_len = int(self.wavelet_interval_s * self.fs)
            if target_len >= 2 and len(self._wavelet_buf) >= target_len:
                arr = np.asarray(self._wavelet_buf, dtype=float)
                clean = wavelet_remove_trend(arr, wavelet=self.wavelet)
                if clean.size >= 2:
                    dt = 1.0 / self.fs
                    last_x = float(clean[-1])
                    last_v = float((clean[-1] - clean[-2]) / dt)
                    self.force_state(last_x, last_v)
                if clean.size >= out.size:
                    out = clean[-out.size :]
                self._wavelet_buf = []

        return out


def _acc_to_disp_kf(acc: np.ndarray, fs: float) -> np.ndarray:
    if acc.size == 0:
        return acc
    if fs <= 0:
        return acc
    acc = np.asarray(acc, dtype=float)
    kf = Accel2DispKF(fs=fs)
    return kf.step_block(acc)


def acc_to_disp(acc: np.ndarray, fs: float, method: str = "fft") -> np.ndarray:
    """
    Displacement from acceleration. method: "fft", "time", or "kf".
    """
    m = str(method).lower() if method is not None else "fft"
    if m == "time":
        return _acc_to_disp_time(acc, fs)
    if m == "kf":
        return _acc_to_disp_kf(acc, fs)
    return _acc_to_disp_fft(acc, fs)


def rainflow_ranges_counts(sig: np.ndarray):
    """
    Basic ASTM rainflow counting: returns (ranges, counts).
    """
    if len(sig) < 2:
        return np.array([]), np.array([])

    # extract turning points
    x = np.asarray(sig, dtype=float)
    tp = [x[0]]
    for i in range(1, len(x) - 1):
        prev, curr, nxt = x[i - 1], x[i], x[i + 1]
        if (curr - prev) * (nxt - curr) <= 0:
            tp.append(curr)
    tp.append(x[-1])

    stack = []
    ranges = []
    counts = []

    for v in tp:
        stack.append(v)
        while len(stack) >= 3:
            s0, s1, s2 = stack[-3], stack[-2], stack[-1]
            r1 = abs(s1 - s0)
            r2 = abs(s2 - s1)
            if r1 <= r2:
                ranges.append(r1)
                counts.append(0.5)
                stack.pop(-2)
            else:
                break

    for i in range(len(stack) - 1):
        ranges.append(abs(stack[i + 1] - stack[i]))
        counts.append(0.5)

    return np.array(ranges), np.array(counts)


def asme_sn_cycles(Sa: float, et: float) -> float:
    """
    ASME S-N cycles based on provided MATLAB routine.
    Sa: stress amplitude (MPa)
    et: elastic modulus (MPa)
    """
    if Sa <= 0:
        return math.inf
    Y = math.log10(28300.0 * Sa / et)
    if 10 ** Y >= 20:
        X = (
            -4706.5245
            + 1813.6228 * Y
            + 6785.5644 / Y
            - 368.12404 * Y ** 2
            - 5133.7345 / Y ** 2
            + 30.708204 * Y ** 3
            + 1596.1916 / Y ** 3
        )
    else:
        X = (38.1309 - 60.1705 * Y ** 2 + 25.0352 * Y ** 4) / (
            1 + 1.80224 * Y ** 2 - 4.68904 * Y ** 4 + 2.26536 * Y ** 6
        )
    return 10 ** X


def build_sn_curve(et: float, s_min: float = 50.0, s_max: float = 500.0, points: int = 300):
    """
    Generate S-N curve samples for plotting (Sa linear spacing, N log-x as in MATLAB reference).
    """
    sa = np.linspace(s_min, s_max, points)
    n_vals = np.array([asme_sn_cycles(s, et) for s in sa])
    return sa.tolist(), n_vals.tolist()


def fatigue_damage(ax: np.ndarray, ay: np.ndarray, fs: float, k_disp2stress: float, et: float, disp_method: str = "fft"):
    """
    Compute directional fatigue damage from two-channel acceleration.
    """
    disp_x = acc_to_disp(ax, fs, method=disp_method)
    disp_y = acc_to_disp(ay, fs, method=disp_method)

    dphi = math.radians(5)
    phi_edges = np.arange(0, 2 * math.pi + dphi, dphi)
    phi_center = phi_edges[:-1] + dphi / 2

    D_phi = []
    Sa_max_list = []

    for phi in phi_center:
        u_phi = disp_x * math.cos(phi) + disp_y * math.sin(phi)
        u_phi = u_phi - np.mean(u_phi)
        stress_phi = k_disp2stress * u_phi

        ranges, counts = rainflow_ranges_counts(stress_phi)
        if ranges.size == 0:
            D_phi.append(0.0)
            Sa_max_list.append(0.0)
            continue

        Sa = ranges / 2.0
        Dk = 0.0
        for s, n in zip(Sa, counts):
            if s <= 0:
                continue
            if s < 48 or s > 3999:
                continue
            Ni = asme_sn_cycles(s, et)
            if Ni and Ni > 0:
                Dk += n / Ni
        D_phi.append(Dk)
        Sa_max_list.append(float(np.max(Sa)) if Sa.size else 0.0)

    D_phi = np.array(D_phi)
    Sa_max_list = np.array(Sa_max_list)

    if D_phi.size == 0:
        return {
            "Dmax": 0.0,
            "phi_deg": 0.0,
            "Sa_max": 0.0,
            "phi_deg_list": [],
            "D_phi": [],
            "params": {"fs": fs, "k_disp2stress": k_disp2stress, "et": et, "dphi_deg": math.degrees(dphi)},
        }

    imax = int(np.argmax(D_phi))
    sa_sn, n_sn = build_sn_curve(et=et)
    return {
        "Dmax": float(D_phi[imax]),
        "phi_deg": math.degrees(phi_center[imax]),
        "Sa_max": float(Sa_max_list[imax]),
        "phi_deg_list": (phi_center * 180 / math.pi).tolist(),
        "D_phi": D_phi.tolist(),
        "params": {"fs": fs, "k_disp2stress": k_disp2stress, "et": et, "dphi_deg": math.degrees(dphi)},
        "sn_curve": {"Sa": sa_sn, "N": n_sn},
    }
