# backend/daq/analysis.py
import numpy as np
import math


def _acc_to_disp_fft(acc: np.ndarray, fs: float) -> np.ndarray:
    """
    Double integration via FFT (per MATLAB acc2disp), suppressing low-frequency blow-up.
    """
    if acc.size == 0:
        return acc
    if fs <= 0:
        return acc

    acc = np.asarray(acc, dtype=float)
    N = acc.size
    A = np.fft.fft(acc)

    f = np.arange(N) * fs / N
    omega = 2 * math.pi * f
    # 防止低频发散：omega < 0.05 Hz -> set to inf
    omega = np.where(omega < 2 * math.pi * 0.05, np.inf, omega)
    # 去除直流分量
    if omega.size > 0:
        omega[0] = np.inf

    U = -A / (omega ** 2)
    u = np.fft.ifft(U).real

    # detrend (linear) to reduce drift
    t = np.arange(N)
    p = np.polyfit(t, u, 1)
    trend = p[0] * t + p[1]
    u = u - trend
    return u


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


def acc_to_disp(acc: np.ndarray, fs: float, method: str = "fft") -> np.ndarray:
    """
    Displacement from acceleration. method: "fft" or "time".
    """
    if method and str(method).lower() == "time":
        return _acc_to_disp_time(acc, fs)
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
