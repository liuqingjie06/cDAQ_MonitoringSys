# -*- coding: utf-8 -*-
import threading
import time
from collections import deque
import numpy as np
import tkinter as tk
from tkinter import ttk

import matplotlib

matplotlib.use("TkAgg")
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
from matplotlib.figure import Figure

# -----------------------------
#  依赖库检查
# -----------------------------
try:
    import nidaqmx
    from nidaqmx.constants import AcquisitionType, TerminalConfiguration, ExcitationSource, Coupling
    from nidaqmx.stream_readers import AnalogMultiChannelReader
    from nidaqmx.system import System

    NI_AVAILABLE = True
except Exception:
    NI_AVAILABLE = False

try:
    import pywt

    PYWT_AVAILABLE = True
except ImportError:
    PYWT_AVAILABLE = False
    print("Warning: PyWavelets not installed. Wavelet detrending will be disabled.")


# -----------------------------
#  工具函数
# -----------------------------
def detrend_linear(y: np.ndarray) -> np.ndarray:
    n = len(y)
    if n < 3:
        return y.copy()
    t = np.arange(n, dtype=float)
    A = np.vstack([t, np.ones(n)]).T
    k, b = np.linalg.lstsq(A, y, rcond=None)[0]
    return y - (k * t + b)


def wavelet_remove_trend(data: np.ndarray, wavelet='db6', level=None) -> np.ndarray:
    if not PYWT_AVAILABLE:
        return data

    n = len(data)
    if level is None:
        level = int(np.log2(n)) - 3
        if level < 1: level = 1

    coeffs = pywt.wavedec(data, wavelet, mode='symmetric', level=level)
    coeffs[0] = np.zeros_like(coeffs[0])
    rec = pywt.waverec(coeffs, wavelet, mode='symmetric')

    if len(rec) > n:
        rec = rec[:n]
    return rec


def disp_freq_domain_window(acc_win: np.ndarray,
                            fs: float,
                            fc_hp: float,
                            detrend_out: bool = True) -> np.ndarray:
    n = len(acc_win)
    if n < 128: return np.zeros_like(acc_win)

    a = detrend_linear(acc_win - np.mean(acc_win))
    Aw = np.fft.rfft(a)
    freqs = np.fft.rfftfreq(n, d=1.0 / fs)
    omega = 2.0 * np.pi * freqs

    Xw = np.zeros_like(Aw, dtype=complex)
    valid = omega > 0
    Xw[valid] = Aw[valid] / (-(omega[valid] ** 2))

    if fc_hp > 0:
        response = 1.0 / (1.0 + (fc_hp / (freqs + 1e-6)) ** 4)
        Xw *= response

    Xw[0] = 0.0
    x = np.fft.irfft(Xw, n=n)

    if detrend_out:
        x = detrend_linear(x)
    return x


# -----------------------------
#  时域两次积分
# -----------------------------
class TimeDomainIntegrator:
    def __init__(self, fs: float, bias_tau_s: float = 60.0, leak_fz_hz: float = 0.02):
        self.set_params(fs, bias_tau_s, leak_fz_hz)
        self.reset()

    def set_params(self, fs: float, bias_tau_s: float, leak_fz_hz: float):
        self.fs = float(fs)
        self.dt = 1.0 / self.fs
        self.bias_tau_s = max(0.0, float(bias_tau_s))
        self.leak_fz_hz = max(0.0, float(leak_fz_hz))

        if self.bias_tau_s <= 0:
            self.bias_alpha = 1.0
        else:
            self.bias_alpha = self.dt / (self.bias_tau_s + self.dt)

        self.leak_w = 2.0 * np.pi * self.leak_fz_hz

    def reset(self):
        self.bias = 0.0
        self.v = 0.0
        self.x = 0.0

    def step_block(self, acc_block: np.ndarray) -> np.ndarray:
        a = np.asarray(acc_block, dtype=float)
        n = len(a)
        out = np.zeros(n, dtype=float)

        dt = self.dt
        w = self.leak_w
        alpha = self.bias_alpha
        bias = self.bias
        v = self.v
        x = self.x

        for i in range(n):
            ai = a[i]
            bias = (1.0 - alpha) * bias + alpha * ai
            a_eff = ai - bias

            v = v + a_eff * dt - w * v * dt
            x = x + v * dt - w * x * dt
            out[i] = x

        self.bias, self.v, self.x = bias, v, x
        return out


# -----------------------------
#  卡曼滤波
# -----------------------------
class Accel2DispKF:
    def __init__(self, fs: float, sigma_a: float = 0.01, sigma_b: float = 1e-5, sigma_x_pseudo: float = 0.1):
        self.fs = float(fs)
        self.dt = 1.0 / self.fs
        self.x = np.zeros((3, 1))
        self.P = np.eye(3)
        self.set_params(fs, sigma_a, sigma_b, sigma_x_pseudo)
        self.reset()

    def set_params(self, fs: float, sigma_a: float, sigma_b: float, sigma_x_pseudo: float):
        self.fs = float(fs)
        self.dt = 1.0 / self.fs
        dt = self.dt

        self.F = np.array([
            [1.0, dt, -0.5 * dt ** 2],
            [0.0, 1.0, -dt],
            [0.0, 0.0, 1.0]
        ])
        self.B = np.array([[0.5 * dt ** 2], [dt], [0.0]])

        self.Q = np.zeros((3, 3))
        self.Q[0, 0] = 1e-8
        self.Q[1, 1] = 1e-7
        self.Q[2, 2] = (sigma_b ** 2) * dt

        self.H = np.array([[1.0, 0.0, 0.0]])
        self.R = np.array([[sigma_x_pseudo ** 2]])

    def reset(self):
        self.x = np.zeros((3, 1))
        self.P = np.diag([0.1, 0.1, 0.01])

    def force_state(self, x_val=0.0, v_val=0.0):
        self.x[0, 0] = x_val
        self.x[1, 0] = v_val

    def step_block(self, acc_block: np.ndarray) -> np.ndarray:
        out = np.zeros(len(acc_block))
        I = np.eye(3)
        for i, a_meas in enumerate(acc_block):
            u = np.array([[a_meas]])
            self.x = self.F @ self.x + self.B @ u
            self.P = self.F @ self.P @ self.F.T + self.Q

            z = np.array([[0.0]])
            y = z - (self.H @ self.x)
            S = self.H @ self.P @ self.H.T + self.R
            K = self.P @ self.H.T / S[0, 0]
            self.x = self.x + K * y
            self.P = (I - K @ self.H) @ self.P

            out[i] = float(self.x[0, 0])
        return out


def terminal_config_from_string(s: str):
    if not NI_AVAILABLE: return None
    if hasattr(TerminalConfiguration, "PSEUDODIFFERENTIAL"):
        return TerminalConfiguration.PSEUDODIFFERENTIAL
    if hasattr(TerminalConfiguration, "PSEUDO_DIFFERENTIAL"):
        return TerminalConfiguration.PSEUDO_DIFFERENTIAL
    if hasattr(TerminalConfiguration, "PSEUDODIFF"):
        return TerminalConfiguration.PSEUDODIFF
    return None


# -----------------------------
#  采集线程
# -----------------------------
class NIReaderThread(threading.Thread):
    def __init__(self, phys_chans, fs, chunk, term_cfg_str="RSE"):
        super().__init__(daemon=True)
        self.phys_chans = phys_chans
        self.fs = fs
        self.chunk = chunk
        self.term_cfg_str = term_cfg_str
        self._stop = threading.Event()
        self._lock = threading.Lock()
        self._latest = None

    def stop(self):
        self._stop.set()

    def get_latest(self):
        with self._lock:
            return self._latest

    def run(self):
        if not NI_AVAILABLE: return
        term_cfg = terminal_config_from_string(self.term_cfg_str)
        with nidaqmx.Task() as task:
            for ch in self.phys_chans:
                if term_cfg is None:
                    task.ai_channels.add_ai_voltage_chan(physical_channel=ch)
                else:
                    task.ai_channels.add_ai_voltage_chan(physical_channel=ch, terminal_config=term_cfg)

            task.timing.cfg_samp_clk_timing(rate=self.fs, sample_mode=AcquisitionType.CONTINUOUS,
                                            samps_per_chan=self.chunk * 10)
            reader = AnalogMultiChannelReader(task.in_stream)
            buf = np.zeros((len(self.phys_chans), self.chunk), dtype=np.float64)
            task.start()
            sample_index = 0

            while not self._stop.is_set():
                reader.read_many_sample(data=buf, number_of_samples_per_channel=self.chunk, timeout=2.0)
                ts = (np.arange(self.chunk) + sample_index) / self.fs
                sample_index += self.chunk
                with self._lock:
                    self._latest = (ts, buf.copy())


# -----------------------------
#  GUI
# -----------------------------
class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("NI-9230 2-CH Realtime Monitor (Optimized Display)")

        # ===== 采样设置 =====
        self.fs = 1600.0
        self.chunk = 400
        self.fs_hz = tk.DoubleVar(value=self.fs)
        self.downsample = tk.IntVar(value=1)
        self.downsample_n = 1
        self.proc_fs = self.fs

        # ===== NI Config =====
        self.mod_name = tk.StringVar(value="")
        self.ch_x = tk.IntVar(value=0)
        self.ch_y = tk.IntVar(value=1)
        self.term_cfg = tk.StringVar(value="RSE")
        self.sens_mV_per_g = tk.DoubleVar(value=1000.0)
        self.g0 = 9.80665/2

        # ===== Method Config =====
        self.method = tk.StringVar(value="time")

        # Time params
        self.td_bias_tau = tk.DoubleVar(value=60.0)
        self.td_leak_fz = tk.DoubleVar(value=0.02)

        # Freq params
        self.fd_fc_hp = tk.DoubleVar(value=0.04)
        self.fd_win_s = tk.DoubleVar(value=80.0)
        self.fd_win_name = tk.StringVar(value="hann")
        self.fd_detrend_out = tk.BooleanVar(value=False)

        # KF params
        self.kf_sigma_a = tk.DoubleVar(value=0.02)
        self.kf_sigma_b = tk.DoubleVar(value=1e-6)
        self.kf_sigma_init_x = tk.DoubleVar(value=0.3)
        self.kf_sigma_init_v = tk.DoubleVar(value=0.1)
        self.kf_sigma_init_b = tk.DoubleVar(value=0.05)

        # Wavelet Params
        self.wavelet_interval_s = 120.0
        self.wavelet_buf_x = []
        self.wavelet_buf_y = []

        # Display params
        self.time_window_s = tk.DoubleVar(value=70.0)

        # Buffers for Realtime Plot
        self.t_buf = deque()
        self.ax_buf = deque()
        self.ay_buf = deque()
        self.x_buf = deque()
        self.y_buf = deque()
        self.r_buf = deque()

        # Buffers for Static 60s Plot
        self.batch_t = []
        self.batch_ax = []
        self.batch_ay = []
        self.batch_start_t = None

        self.t0 = None
        self._last_ts_end = None

        # Integrators
        self.td_x = TimeDomainIntegrator(self.fs)
        self.td_y = TimeDomainIntegrator(self.fs)
        self.kf_x = Accel2DispKF(self.fs)
        self.kf_y = Accel2DispKF(self.fs)

        self._fd_len = int(self.fd_win_s.get() * self.proc_fs)
        self._fd_ax = deque(maxlen=self._fd_len)
        self._fd_ay = deque(maxlen=self._fd_len)

        self.reader = None
        self._running = False

        self._build_ui()
        self._on_refresh_click()
        self.after(50, self._update_loop)

    def _refresh_modules(self):
        if not NI_AVAILABLE: return []
        mods = []
        try:
            sys = System.local()
            for dev in sys.devices:
                ai = getattr(dev, "ai_physical_chans", None)
                if ai is not None and len(ai) > 0:
                    mods.append(dev.name)
        except:
            pass
        mods = sorted(list(set(mods)))
        if mods and (self.mod_name.get() not in mods):
            self.mod_name.set(mods[0])
        return mods

    def _get_phys_chans(self):
        mod = self.mod_name.get().strip()
        if not mod: return None
        return [f"{mod}/ai{int(self.ch_x.get())}", f"{mod}/ai{int(self.ch_y.get())}"]

    def _on_refresh_click(self):
        if not NI_AVAILABLE:
            self.status.set("nidaqmx 不可用")
            return
        mods = self._refresh_modules()
        self.cb_mod["values"] = mods
        if mods:
            self.status.set(f"发现模块：{', '.join(mods[:8])}")
        else:
            self.status.set("未发现带 AI 的设备")

    def _build_ui(self):
        frm = ttk.Frame(self)
        frm.pack(fill="both", expand=True)

        ctl = ttk.Frame(frm)
        ctl.pack(side="top", fill="x")

        # Top Control Bar
        ttk.Label(ctl, text="Module:").pack(side="left", padx=(6, 0))
        self.cb_mod = ttk.Combobox(ctl, textvariable=self.mod_name, width=15, state="readonly")
        self.cb_mod.pack(side="left", padx=4)

        ttk.Label(ctl, text="X=ai").pack(side="left")
        ttk.Spinbox(ctl, from_=0, to=31, textvariable=self.ch_x, width=3).pack(side="left", padx=2)
        ttk.Label(ctl, text="Y=ai").pack(side="left")
        ttk.Spinbox(ctl, from_=0, to=31, textvariable=self.ch_y, width=3).pack(side="left", padx=2)

        ttk.Label(ctl, text="Term:").pack(side="left", padx=(10, 0))
        ttk.Combobox(ctl, textvariable=self.term_cfg, values=["RSE", "NRSE", "DIFF"], width=6, state="readonly").pack(
            side="left", padx=4)

        ttk.Button(ctl, text="Refresh", command=self._on_refresh_click).pack(side="left", padx=6)
        ttk.Label(ctl, text="mV/g:").pack(side="left", padx=(10, 0))
        ttk.Entry(ctl, textvariable=self.sens_mV_per_g, width=8).pack(side="left", padx=4)

        ttk.Label(ctl, text="Fs:").pack(side="left", padx=(10, 0))
        ttk.Entry(ctl, textvariable=self.fs_hz, width=6).pack(side="left", padx=4)
        ttk.Label(ctl, text="Ds:").pack(side="left", padx=(5, 0))
        ttk.Spinbox(ctl, from_=1, to=100, textvariable=self.downsample, width=3).pack(side="left", padx=4)

        ttk.Label(ctl, text="Win(s):").pack(side="left", padx=(10, 0))
        ttk.Entry(ctl, textvariable=self.time_window_s, width=6).pack(side="left", padx=4)

        ttk.Button(ctl, text="Start", command=self.start).pack(side="left", padx=6)
        ttk.Button(ctl, text="Stop", command=self.stop).pack(side="left", padx=6)
        ttk.Button(ctl, text="Reset", command=self._reset_display).pack(side="right", padx=6)

        # Params frame
        mfrm = ttk.LabelFrame(frm, text="Method & Params")
        mfrm.pack(side="top", fill="x", padx=6, pady=4)

        sub_m = ttk.Frame(mfrm)
        sub_m.pack(side="top", fill="x", padx=4, pady=2)
        ttk.Radiobutton(sub_m, text="时域法", value="time", variable=self.method).pack(side="left", padx=8)
        ttk.Radiobutton(sub_m, text="频域法", value="freq", variable=self.method).pack(side="left", padx=8)
        ttk.Radiobutton(sub_m, text="卡曼滤波(含小波去势)", value="kf", variable=self.method).pack(side="left", padx=8)

        pfrm = ttk.Frame(mfrm)
        pfrm.pack(side="top", fill="x", padx=4, pady=2)

        # Time params
        tbox = ttk.LabelFrame(pfrm, text="Time params")
        tbox.pack(side="left", fill="x", expand=True, padx=4)
        ttk.Label(tbox, text="bias_tau").grid(row=0, column=0)
        ttk.Entry(tbox, textvariable=self.td_bias_tau, width=6).grid(row=0, column=1)
        ttk.Label(tbox, text="leak_fz").grid(row=0, column=2)
        ttk.Entry(tbox, textvariable=self.td_leak_fz, width=6).grid(row=0, column=3)

        # Freq params
        fbox = ttk.LabelFrame(pfrm, text="Freq params")
        fbox.pack(side="left", fill="x", expand=True, padx=4)
        ttk.Label(fbox, text="win_s").grid(row=0, column=0)
        ttk.Entry(fbox, textvariable=self.fd_win_s, width=6).grid(row=0, column=1)
        ttk.Label(fbox, text="fc_hp").grid(row=0, column=2)
        ttk.Entry(fbox, textvariable=self.fd_fc_hp, width=6).grid(row=0, column=3)
        ttk.Checkbutton(fbox, text="detrend", variable=self.fd_detrend_out).grid(row=1, column=0, columnspan=2)

        # KF params
        kbox = ttk.LabelFrame(pfrm, text="KF params")
        kbox.pack(side="left", fill="x", expand=True, padx=4)
        ttk.Label(kbox, text="sig_a").grid(row=0, column=0)
        ttk.Entry(kbox, textvariable=self.kf_sigma_a, width=6).grid(row=0, column=1)
        ttk.Label(kbox, text="sig_b").grid(row=0, column=2)
        ttk.Entry(kbox, textvariable=self.kf_sigma_b, width=6).grid(row=0, column=3)
        ttk.Label(kbox, text="P_R").grid(row=1, column=0)
        ttk.Entry(kbox, textvariable=self.kf_sigma_init_x, width=6).grid(row=1, column=1)

        # Plots - 3x2 Grid
        fig = Figure(figsize=(11, 8.5), dpi=100)

        self.ax1 = fig.add_subplot(3, 2, 1)
        self.ax1.set_title("Realtime Acc (m/s²)")
        self.ax2 = fig.add_subplot(3, 2, 2)
        self.ax2.set_title("Realtime Disp (m)")

        self.ax3 = fig.add_subplot(3, 2, 3)
        self.ax3.set_title("Total r(t)")
        self.ax4 = fig.add_subplot(3, 2, 4, projection="polar")
        self.ax4.set_title("Realtime Trajectory")

        # Row 3: Static 60s Report
        self.ax5 = fig.add_subplot(3, 1, 3)
        self.ax5.set_title("Static 60s: Total Displacement Magnitude (Downsampled View)")
        self.ax5.set_xlabel("Time (s)")

        self.l1x, = self.ax1.plot([], [], label="X")
        self.l1y, = self.ax1.plot([], [], label="Y")
        self.ax1.legend(loc='upper right', fontsize='small')

        self.l2x, = self.ax2.plot([], [], label="X")
        self.l2y, = self.ax2.plot([], [], label="Y")
        self.ax2.legend(loc='upper right', fontsize='small')

        self.l3, = self.ax3.plot([], [], label="r")
        self.l4, = self.ax4.plot([], [], lw=1.2)

        self.l5, = self.ax5.plot([], [], 'b-', lw=1.2, label="|r|")
        self.ax5.legend(loc='upper right')

        fig.tight_layout()
        self.canvas = FigureCanvasTkAgg(fig, master=frm)
        self.canvas.get_tk_widget().pack(side="top", fill="both", expand=True)

        self.status = tk.StringVar(value="Ready.")
        ttk.Label(frm, textvariable=self.status).pack(side="bottom", fill="x")

    def start(self):
        if self._running: return
        if not NI_AVAILABLE:
            self.status.set("Err: NI-DAQmx not found")
            return
        phys = self._get_phys_chans()
        if not phys: return

        fs = float(self.fs_hz.get())
        if fs <= 0: fs = 1600.0
        ds = int(self.downsample.get())
        if ds < 1: ds = 1

        self.fs = fs
        self.downsample_n = ds
        self.proc_fs = self.fs / ds

        self._reset_display()
        self._running = True
        self.status.set(f"Running... {phys} Fs={fs}")

        self.reader = NIReaderThread(phys, self.fs, self.chunk, term_cfg_str=self.term_cfg.get())
        self.reader.start()

    def stop(self):
        self._running = False
        if self.reader:
            self.reader.stop()
            self.reader = None
        self.status.set("Stopped.")

    def _reset_display(self):
        self.t_buf.clear();
        self.ax_buf.clear();
        self.ay_buf.clear()
        self.x_buf.clear();
        self.y_buf.clear();
        self.r_buf.clear()
        self.t0 = None
        self._last_ts_end = None

        self.batch_t = []
        self.batch_ax = []
        self.batch_ay = []
        self.batch_start_t = None

        self.l5.set_data([], [])
        self.ax5.relim();
        self.ax5.autoscale_view()

        self.td_x.reset();
        self.td_y.reset()
        self.kf_x.reset();
        self.kf_y.reset()
        self.wavelet_buf_x = [];
        self.wavelet_buf_y = []

        self._fd_len = int(max(5.0, float(self.fd_win_s.get())) * self.proc_fs)
        self._fd_ax = deque(maxlen=self._fd_len)
        self._fd_ay = deque(maxlen=self._fd_len)

        self.canvas.draw_idle()

    def _volts_to_mps2(self, v):
        sens = self.sens_mV_per_g.get() / 1000.0
        if sens <= 0: sens = 1.0
        return (v / sens) * self.g0

    def _append_block(self, ts, ax, ay, x, y):
        # 1. Realtime buffer
        win_s = max(5.0, float(self.time_window_s.get()))
        maxlen = int(win_s * self.proc_fs)

        if self.t_buf:
            last_t = self.t_buf[-1]
            if np.isfinite(last_t):
                dt = 1.0 / self.proc_fs
                if ts[0] - last_t > 1.5 * dt:
                    for b in [self.t_buf, self.ax_buf, self.ay_buf, self.x_buf, self.y_buf, self.r_buf]:
                        b.append(np.nan)

        self.t_buf.extend(ts)
        self.ax_buf.extend(ax)
        self.ay_buf.extend(ay)
        self.x_buf.extend(x)
        self.y_buf.extend(y)
        self.r_buf.extend(np.hypot(x, y))

        while len(self.t_buf) > maxlen:
            self.t_buf.popleft();
            self.ax_buf.popleft();
            self.ay_buf.popleft()
            self.x_buf.popleft();
            self.y_buf.popleft();
            self.r_buf.popleft()

        # 2. Static 60s Batch
        self.batch_t.extend(ts)
        self.batch_ax.extend(ax)
        self.batch_ay.extend(ay)

        if self.batch_start_t is None and len(self.batch_t) > 0:
            self.batch_start_t = self.batch_t[0]

        if self.batch_start_t is not None and len(self.batch_t) > 0:
            current_dur = self.batch_t[-1] - self.batch_start_t
            if current_dur >= 60.0:
                self._process_static_60s_block()

    def _process_static_60s_block(self):
        """
        触发多线程处理：
        """
        print("[Info] 60s Batch Reached. Offloading calculation to background thread...")

        t_arr = np.array(self.batch_t)
        ax_arr = np.array(self.batch_ax)
        ay_arr = np.array(self.batch_ay)

        kf_params = {
            'proc_fs': self.proc_fs,
            'sig_a': self.kf_sigma_a.get(),
            'sig_b': self.kf_sigma_b.get(),
            'init_x': self.kf_sigma_init_x.get(),
            'fc_hp': max(0.1, float(self.fd_fc_hp.get()))
        }

        # 启动计算线程
        calc_thread = threading.Thread(
            target=self._thread_calc_static_60s,
            args=(t_arr, ax_arr, ay_arr, kf_params),
            daemon=True
        )
        calc_thread.start()

        # 立即重置 Batch
        self.batch_t = []
        self.batch_ax = []
        self.batch_ay = []
        self.batch_start_t = None

    def _thread_calc_static_60s(self, t_arr, ax_arr, ay_arr, params):
        """
        后台线程：执行计算 + 【关键】执行数据降采样
        """
        proc_fs = params['proc_fs']

        # 1. 完整数据计算 (保证精度)
        kf_temp_x = Accel2DispKF(proc_fs, params['sig_a'], params['sig_b'], params['init_x'])
        kf_temp_y = Accel2DispKF(proc_fs, params['sig_a'], params['sig_b'], params['init_x'])

        x_raw = kf_temp_x.step_block(ax_arr)
        y_raw = kf_temp_y.step_block(ay_arr)

        def apply_hpf(data, fs, fc):
            n = len(data)
            if n < 2: return data
            freqs = np.fft.rfftfreq(n, d=1.0 / fs)
            gain = 1.0 / (1.0 + (fc / (freqs + 1e-9)) ** 4)
            gain[0] = 0.0
            spec = np.fft.rfft(data)
            spec *= gain
            return np.fft.irfft(spec, n=n)

        fc = params['fc_hp']
        sx = apply_hpf(x_raw, proc_fs, fc)
        sy = apply_hpf(y_raw, proc_fs, fc)
        sr = np.hypot(sx, sy)
        t_rel = t_arr - t_arr[0]

        # 2. 【核心修改】数据降采样 (Visual Downsampling)
        # 每 20 个点取 1 个 (1600Hz -> 相当于显示 80Hz 的数据密度)
        # 这将把 96,000 个点减少到 4,800 个点，极大减轻 matplotlib 绘图压力
        view_step = 20

        t_view = t_rel[::view_step]
        r_view = sr[::view_step]

        # 3. 调度主线程更新 (只传递降采样后的数据)
        self.after(0, lambda: self._update_static_graph(t_view, r_view))

    def _update_static_graph(self, t, r):
        """
        主线程回调：绘制轻量级数据
        """
        print(f"[Info] Calculation done. Updating plot with {len(r)} points.")
        self.l5.set_data(t, r)

        if len(t) > 0:
            self.ax5.set_xlim(0, t[-1])
            min_v = np.min(r)
            max_v = np.max(r)
            margin = (max_v - min_v) * 0.1
            if margin == 0: margin = 1e-6
            self.ax5.set_ylim(min_v - margin, max_v + margin)

        self.canvas.draw_idle()

    def _integrate_xy(self, ax_block, ay_block):
        m = self.method.get()

        if m == "time":
            self.td_x.set_params(self.proc_fs, self.td_bias_tau.get(), self.td_leak_fz.get())
            self.td_y.set_params(self.proc_fs, self.td_bias_tau.get(), self.td_leak_fz.get())
            return self.td_x.step_block(ax_block), self.td_y.step_block(ay_block)

        if m == "kf":
            self.kf_x.set_params(self.proc_fs, self.kf_sigma_a.get(), self.kf_sigma_b.get(), self.kf_sigma_init_x.get())
            self.kf_y.set_params(self.proc_fs, self.kf_sigma_a.get(), self.kf_sigma_b.get(), self.kf_sigma_init_x.get())

            x_block = self.kf_x.step_block(ax_block)
            y_block = self.kf_y.step_block(ay_block)

            if PYWT_AVAILABLE:
                self.wavelet_buf_x.extend(x_block)
                self.wavelet_buf_y.extend(y_block)
                target_len = int(self.wavelet_interval_s * self.proc_fs)

                if len(self.wavelet_buf_x) >= target_len:
                    print(f"[Info] 2分钟触发：KF小波去势校准...")
                    arr_x = np.array(self.wavelet_buf_x)
                    arr_y = np.array(self.wavelet_buf_y)
                    clean_x = wavelet_remove_trend(arr_x, wavelet='db6')
                    clean_y = wavelet_remove_trend(arr_y, wavelet='db6')

                    dt = 1.0 / self.proc_fs
                    last_x = clean_x[-1]
                    last_vx = (clean_x[-1] - clean_x[-2]) / dt
                    last_y = clean_y[-1]
                    last_vy = (clean_y[-1] - clean_y[-2]) / dt

                    self.kf_x.force_state(last_x, last_vx)
                    self.kf_y.force_state(last_y, last_vy)

                    L = len(x_block)
                    x_block = clean_x[-L:]
                    y_block = clean_y[-L:]
                    self.wavelet_buf_x = []
                    self.wavelet_buf_y = []

            return x_block, y_block

        # Freq
        win_s = max(5.0, float(self.fd_win_s.get()))
        win_n = int(win_s * self.proc_fs)
        f_reg = float(self.fd_fc_hp.get())

        self._fd_ax.extend(ax_block.tolist())
        self._fd_ay.extend(ay_block.tolist())
        if len(self._fd_ax) < win_n:
            return np.full_like(ax_block, np.nan), np.full_like(ay_block, np.nan)

        ax_win = np.array(list(self._fd_ax)[-win_n:], dtype=float)
        ay_win = np.array(list(self._fd_ay)[-win_n:], dtype=float)

        x_win = disp_freq_domain_window(ax_win, self.proc_fs, f_reg, detrend_out=self.fd_detrend_out.get())
        y_win = disp_freq_domain_window(ay_win, self.proc_fs, f_reg, detrend_out=self.fd_detrend_out.get())

        return x_win[-len(ax_block):], y_win[-len(ax_block):]

    def _update_loop(self):
        if self._running and self.reader:
            latest = self.reader.get_latest()
            if latest:
                ts, buf = latest
                ts = np.asarray(ts, dtype=float)
                vx, vy = buf[0], buf[1]
                ds = int(self.downsample_n)
                if ds > 1: ts, vx, vy = ts[::ds], vx[::ds], vy[::ds]

                if len(ts) > 0:
                    if self._last_ts_end and ts[0] <= self._last_ts_end:
                        pass
                    else:
                        if self.t0 is None: self.t0 = ts[0]
                        ax = self._volts_to_mps2(vx)
                        ay = self._volts_to_mps2(vy)
                        x, y = self._integrate_xy(ax, ay)
                        self._append_block(ts, ax, ay, x, y)
                        self._last_ts_end = ts[-1]

        self._redraw_realtime()
        self.after(50, self._update_loop)

    def _redraw_realtime(self):
        if len(self.t_buf) < 10: return
        t = np.array(self.t_buf) - self.t0
        self.l1x.set_data(t, self.ax_buf);
        self.l1y.set_data(t, self.ay_buf)
        self.ax1.relim();
        self.ax1.autoscale_view()
        self.l2x.set_data(t, self.x_buf);
        self.l2y.set_data(t, self.y_buf)
        self.ax2.relim();
        self.ax2.autoscale_view()
        self.l3.set_data(t, self.r_buf)
        self.ax3.relim();
        self.ax3.autoscale_view()

        x, y = np.array(self.x_buf), np.array(self.y_buf)
        valid = np.isfinite(x) & np.isfinite(y)
        if np.any(valid):
            x, y = x[valid], y[valid]
            theta = np.arctan2(y, x)
            rad = np.hypot(x, y)
            self.l4.set_data(theta, rad)
            rmax = np.max(rad) if len(rad) > 0 else 1.0
            self.ax4.set_rlim(0, max(rmax * 1.1, 1e-6))
        self.canvas.draw_idle()


if __name__ == "__main__":
    app = App()
    app.mainloop()