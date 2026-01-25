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
#  NI-DAQmx
# -----------------------------
try:
    import nidaqmx
    from nidaqmx.constants import AcquisitionType, TerminalConfiguration
    from nidaqmx.stream_readers import AnalogMultiChannelReader
    from nidaqmx.system import System
    NI_AVAILABLE = True
except Exception:
    NI_AVAILABLE = False


# -----------------------------
#  工具：线性去趋势（可选用）
# -----------------------------
def detrend_linear(y: np.ndarray) -> np.ndarray:
    n = len(y)
    if n < 3:
        return y.copy()
    t = np.arange(n, dtype=float)
    A = np.vstack([t, np.ones(n)]).T
    k, b = np.linalg.lstsq(A, y, rcond=None)[0]
    return y - (k * t + b)


# -----------------------------
#  纯频域二次积分（窗口内）
# -----------------------------
def disp_freq_domain_window(acc_win: np.ndarray,
                             fs: float,
                             fc_hp: float,
                             detrend_out: bool = True) -> np.ndarray:
    n = len(acc_win)
    if n < 128: return np.zeros_like(acc_win)

    # 1. 强力去趋势（替代窗函数）
    a = detrend_linear(acc_win - np.mean(acc_win))

    # 2. FFT
    Aw = np.fft.rfft(a)
    freqs = np.fft.rfftfreq(n, d=1.0 / fs)
    omega = 2.0 * np.pi * freqs

    # 3. 积分算子
    Xw = np.zeros_like(Aw, dtype=complex)
    valid = omega > 0
    
    # 核心公式
    Xw[valid] = Aw[valid] / (-(omega[valid] ** 2))
    
    # 4. 软高通滤波（Butterworth 响应替代硬截断）
    # 这种方式可以极大减少截止频率带来的震荡
    if fc_hp > 0:
        # 二阶高通滤波器响应： (s^2) / (s^2 + 1.414*s*w0 + w0^2)
        # 简单处理：使用 4 阶衰减
        response = 1.0 / (1.0 + (fc_hp / (freqs + 1e-6))**4)
        Xw *= response

    Xw[0] = 0.0
    x = np.fft.irfft(Xw, n=n)

    if detrend_out:
        x = detrend_linear(x)
    return x


# -----------------------------
#  时域两次积分（漂移受控：bias + leak）
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

        # 状态转移矩阵: [x, v, b]
        self.F = np.array([
            [1.0, dt, -0.5 * dt**2],
            [0.0, 1.0, -dt],
            [0.0, 0.0,  1.0]
        ])
        self.B = np.array([[0.5 * dt**2], [dt], [0.0]])
        
        # 过程噪声 Q
        self.Q = np.zeros((3, 3))
        self.Q[0, 0] = 1e-8
        self.Q[1, 1] = 1e-7
        self.Q[2, 2] = (sigma_b**2) * dt 
        
        # 观测模型: 观测位移 x，并假设其趋向于 0 (抑制漂移的关键)
        self.H = np.array([[1.0, 0.0, 0.0]])
        # 观测噪声 R: 值越大，位移越自由（幅值准），但漂移风险增加
        self.R = np.array([[sigma_x_pseudo**2]])

    def reset(self):
        self.x = np.zeros((3, 1))
        self.P = np.diag([0.1, 0.1, 0.01])

    def step_block(self, acc_block: np.ndarray) -> np.ndarray:
        out = np.zeros(len(acc_block))
        I = np.eye(3)
        for i, a_meas in enumerate(acc_block):
            # 1. 预测
            u = np.array([[a_meas]])
            self.x = self.F @ self.x + self.B @ u
            self.P = self.F @ self.P @ self.F.T + self.Q

            # 2. 弱约束更新 (把位移拉向 0)
            z = np.array([[0.0]]) 
            y = z - (self.H @ self.x)
            S = self.H @ self.P @ self.H.T + self.R
            K = self.P @ self.H.T / S[0,0]
            self.x = self.x + K * y
            self.P = (I - K @ self.H) @ self.P

            out[i] = float(self.x[0, 0])
        return out
    
def terminal_config_from_string(s: str):
    """
    NI-9230 仅支持 Pseudo-Differential
    这里强制返回 PSEUDODIFFERENTIAL，避免 -200077
    """
    if not NI_AVAILABLE:
        return None

    # nidaqmx 不同版本命名不同
    if hasattr(TerminalConfiguration, "PSEUDODIFFERENTIAL"):
        return TerminalConfiguration.PSEUDODIFFERENTIAL
    if hasattr(TerminalConfiguration, "PSEUDO_DIFFERENTIAL"):
        return TerminalConfiguration.PSEUDO_DIFFERENTIAL
    if hasattr(TerminalConfiguration, "PSEUDODIFF"):
        return TerminalConfiguration.PSEUDODIFF

    # 实在找不到就不设，让 DAQmx 用默认
    return None


# -----------------------------
#  采集线程：两通道
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
        if not NI_AVAILABLE:
            return

        term_cfg = terminal_config_from_string(self.term_cfg_str)

        with nidaqmx.Task() as task:
            for ch in self.phys_chans:
                if term_cfg is None:
                    task.ai_channels.add_ai_voltage_chan(physical_channel=ch)
                else:
                    task.ai_channels.add_ai_voltage_chan(
                        physical_channel=ch,
                        terminal_config=term_cfg
                    )

            task.timing.cfg_samp_clk_timing(
                rate=self.fs,
                sample_mode=AcquisitionType.CONTINUOUS,
                samps_per_chan=self.chunk * 10
            )

            reader = AnalogMultiChannelReader(task.in_stream)
            buf = np.zeros((len(self.phys_chans), self.chunk), dtype=np.float64)

            task.start()
            sample_index = 0

            while not self._stop.is_set():
                reader.read_many_sample(
                    data=buf,
                    number_of_samples_per_channel=self.chunk,
                    timeout=2.0
                )

                ts = (np.arange(self.chunk) + sample_index) / self.fs
                sample_index += self.chunk

                with self._lock:
                    self._latest = (ts, buf.copy())  # buf shape (2, chunk)


# -----------------------------
#  GUI
# -----------------------------
class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("NI-9230 2-CH Acc->Disp (X/Y) + Total + Polar")

        # ===== 采样设置 =====
        self.fs = 1600.0
        self.chunk = 400  # 每次读 0.25 s
        self.fs_hz = tk.DoubleVar(value=self.fs)
        self.downsample = tk.IntVar(value=1)
        self.downsample_n = 1
        self.proc_fs = self.fs

        # ===== 选择：模块名 + 通道号 =====
        self.mod_name = tk.StringVar(value="")
        self.ch_x = tk.IntVar(value=0)  # 0->X
        self.ch_y = tk.IntVar(value=1)  # 1->Y
        self.term_cfg = tk.StringVar(value="RSE")  # RSE / NRSE / DIFF

        # ===== 传感器灵敏度（mV/g）=====
        self.sens_mV_per_g = tk.DoubleVar(value=1000.0)  # 改成你的（例如 500mV/g）
        self.g0 = 9.80665

        # ===== 方法选择 =====
        self.method = tk.StringVar(value="time")  # time / freq / kf

        # 时域参数
        self.td_bias_tau = tk.DoubleVar(value=60.0)
        self.td_leak_fz = tk.DoubleVar(value=0.02)

        # 频域参数
        self.fd_fc_hp = tk.DoubleVar(value=0.04)
        self.fd_win_s = tk.DoubleVar(value=80.0)
        self.fd_win_name = tk.StringVar(value="hann")
        self.fd_detrend_out = tk.BooleanVar(value=False)

        # KF 参数
        self.kf_sigma_a = tk.DoubleVar(value=0.02)
        self.kf_sigma_b = tk.DoubleVar(value=1e-6)
        self.kf_sigma_init_x = tk.DoubleVar(value=0.3)
        self.kf_sigma_init_v = tk.DoubleVar(value=0.1)
        self.kf_sigma_init_b = tk.DoubleVar(value=0.05)

        # 显示窗口（秒）
        self.time_window_s = tk.DoubleVar(value=70.0)

        # buffers
        self.t_buf = deque()
        self.ax_buf = deque()
        self.ay_buf = deque()
        self.x_buf = deque()
        self.y_buf = deque()
        self.r_buf = deque()

        self.t0 = None
        self._last_ts_end = None

        # integrators
        self.td_x = TimeDomainIntegrator(self.fs, self.td_bias_tau.get(), self.td_leak_fz.get())
        self.td_y = TimeDomainIntegrator(self.fs, self.td_bias_tau.get(), self.td_leak_fz.get())
        # 仅保留 fs, sigma_a, sigma_b 和一个新参数 (原本的 sigma_init_x 位置)
        self.kf_x = Accel2DispKF(self.fs, self.kf_sigma_a.get(), self.kf_sigma_b.get(), self.kf_sigma_init_x.get())
        self.kf_y = Accel2DispKF(self.fs, self.kf_sigma_a.get(), self.kf_sigma_b.get(), self.kf_sigma_init_x.get())
        # freq rolling window
        self._fd_len = int(self.fd_win_s.get() * self.proc_fs)
        self._fd_ax = deque(maxlen=self._fd_len)
        self._fd_ay = deque(maxlen=self._fd_len)

        # reader
        self.reader = None
        self._running = False

        self._build_ui()
        self._on_refresh_click()  # 初始刷新设备列表
        self.after(50, self._update_loop)

    # ---------- 设备枚举 ----------
    def _refresh_modules(self):
        if not NI_AVAILABLE:
            return []
        mods = []
        sys = System.local()
        for dev in sys.devices:
            ai = getattr(dev, "ai_physical_chans", None)
            if ai is not None and len(ai) > 0:
                mods.append(dev.name)  # e.g., cDAQ1Mod1
        mods = sorted(list(set(mods)))
        if mods and (self.mod_name.get() not in mods):
            self.mod_name.set(mods[0])
        return mods

    def _get_phys_chans(self):
        mod = self.mod_name.get().strip()
        if not mod:
            return None
        return [f"{mod}/ai{int(self.ch_x.get())}", f"{mod}/ai{int(self.ch_y.get())}"]

    def _on_refresh_click(self):
        if not NI_AVAILABLE:
            self.status.set("nidaqmx 不可用：请确认安装 NI-DAQmx + nidaqmx，并在同一环境运行。")
            return
        mods = self._refresh_modules()
        self.cb_mod["values"] = mods
        if mods:
            self.status.set(f"发现模块：{', '.join(mods[:8])}" + (" ..." if len(mods) > 8 else ""))
        else:
            self.status.set("未发现带 AI 的设备/模块：请先在 NI MAX 确认可见。")

    # ---------- UI ----------
    def _build_ui(self):
        frm = ttk.Frame(self)
        frm.pack(fill="both", expand=True)

        ctl = ttk.Frame(frm)
        ctl.pack(side="top", fill="x")

        ttk.Label(ctl, text="Module:").pack(side="left", padx=(6,0))
        self.cb_mod = ttk.Combobox(ctl, textvariable=self.mod_name, width=18, state="readonly")
        self.cb_mod.pack(side="left", padx=4)

        ttk.Label(ctl, text="X=ai").pack(side="left")
        ttk.Spinbox(ctl, from_=0, to=31, textvariable=self.ch_x, width=3).pack(side="left", padx=2)
        ttk.Label(ctl, text="Y=ai").pack(side="left")
        ttk.Spinbox(ctl, from_=0, to=31, textvariable=self.ch_y, width=3).pack(side="left", padx=2)

        ttk.Label(ctl, text="Term:").pack(side="left", padx=(10,0))
        ttk.Combobox(ctl, textvariable=self.term_cfg, values=["RSE", "NRSE", "DIFF"], width=6, state="readonly").pack(side="left", padx=4)

        ttk.Button(ctl, text="Refresh", command=self._on_refresh_click).pack(side="left", padx=6)

        ttk.Label(ctl, text="mV/g:").pack(side="left", padx=(10,0))
        ttk.Entry(ctl, textvariable=self.sens_mV_per_g, width=8).pack(side="left", padx=4)

        ttk.Label(ctl, text="Fs(Hz):").pack(side="left", padx=(10,0))
        ttk.Entry(ctl, textvariable=self.fs_hz, width=7).pack(side="left", padx=4)
        ttk.Label(ctl, text="Downsample:").pack(side="left", padx=(10,0))
        ttk.Spinbox(ctl, from_=1, to=100, textvariable=self.downsample, width=4).pack(side="left", padx=4)

        ttk.Label(ctl, text="TimeWindow(s):").pack(side="left", padx=(10,0))
        ttk.Entry(ctl, textvariable=self.time_window_s, width=6).pack(side="left", padx=4)

        ttk.Button(ctl, text="Start", command=self.start).pack(side="left", padx=6)
        ttk.Button(ctl, text="Stop", command=self.stop).pack(side="left", padx=6)
        ttk.Frame(ctl).pack(side="left", fill="x", expand=True)
        ttk.Button(ctl, text="Reset", command=self._reset_display).pack(side="right", padx=6)

        mfrm = ttk.LabelFrame(frm, text="Method")
        mfrm.pack(side="top", fill="x", padx=6, pady=4)
        ttk.Radiobutton(mfrm, text="时域法", value="time", variable=self.method).pack(side="left", padx=8)
        ttk.Radiobutton(mfrm, text="频域法(纯积分)", value="freq", variable=self.method).pack(side="left", padx=8)
        ttk.Radiobutton(mfrm, text="卡曼滤波", value="kf", variable=self.method).pack(side="left", padx=8)

        pfrm = ttk.Frame(frm)
        pfrm.pack(side="top", fill="x", padx=6)

        tbox = ttk.LabelFrame(pfrm, text="Time params")
        tbox.pack(side="left", fill="x", expand=True, padx=4)
        ttk.Label(tbox, text="bias_tau(s)").grid(row=0, column=0, sticky="w")
        ttk.Entry(tbox, textvariable=self.td_bias_tau, width=8).grid(row=0, column=1, sticky="w", padx=4)
        ttk.Label(tbox, text="leak_fz(Hz)").grid(row=0, column=2, sticky="w")
        ttk.Entry(tbox, textvariable=self.td_leak_fz, width=8).grid(row=0, column=3, sticky="w", padx=4)

        fbox = ttk.LabelFrame(pfrm, text="Freq params")
        fbox.pack(side="left", fill="x", expand=True, padx=4)
        ttk.Label(fbox, text="win_s").grid(row=0, column=0, sticky="w")
        ttk.Entry(fbox, textvariable=self.fd_win_s, width=8).grid(row=0, column=1, sticky="w", padx=4)
        ttk.Label(fbox, text="fc_hp(Hz)").grid(row=0, column=2, sticky="w")
        ttk.Entry(fbox, textvariable=self.fd_fc_hp, width=8).grid(row=0, column=3, sticky="w", padx=4)
        ttk.Label(fbox, text="window").grid(row=0, column=4, sticky="w")
        ttk.Combobox(fbox, textvariable=self.fd_win_name, values=["hann", "hamming", "rect"], width=8, state="readonly").grid(row=0, column=5, padx=4)
        ttk.Checkbutton(fbox, text="detrend_out", variable=self.fd_detrend_out).grid(row=1, column=0, columnspan=2, sticky="w", padx=4)

        kbox = ttk.LabelFrame(pfrm, text="KF params")
        kbox.pack(side="left", fill="x", expand=True, padx=4)
        ttk.Label(kbox, text="sigma_a").grid(row=0, column=0, sticky="w")
        ttk.Entry(kbox, textvariable=self.kf_sigma_a, width=10).grid(row=0, column=1, padx=3)
        ttk.Label(kbox, text="sigma_b").grid(row=0, column=2, sticky="w")
        ttk.Entry(kbox, textvariable=self.kf_sigma_b, width=10).grid(row=0, column=3, padx=3)
        ttk.Label(kbox, text="init_x").grid(row=1, column=0, sticky="w")
        ttk.Entry(kbox, textvariable=self.kf_sigma_init_x, width=10).grid(row=1, column=1, padx=3)
        ttk.Label(kbox, text="init_v").grid(row=1, column=2, sticky="w")
        ttk.Entry(kbox, textvariable=self.kf_sigma_init_v, width=10).grid(row=1, column=3, padx=3)
        ttk.Label(kbox, text="init_b").grid(row=1, column=4, sticky="w")
        ttk.Entry(kbox, textvariable=self.kf_sigma_init_b, width=10).grid(row=1, column=5, padx=3)

        # ----- plots -----
        fig = Figure(figsize=(11, 6.5), dpi=100)
        self.ax1 = fig.add_subplot(2, 2, 1)
        self.ax2 = fig.add_subplot(2, 2, 2)
        self.ax3 = fig.add_subplot(2, 2, 3)
        self.ax4 = fig.add_subplot(2, 2, 4, projection="polar")

        self.ax1.set_title("Acceleration RAW (m/s²)  X/Y")
        self.ax1.set_xlabel("Time (s)")
        self.ax1.set_ylabel("a (m/s²)")

        self.ax2.set_title("Displacement (m)  X/Y")
        self.ax2.set_xlabel("Time (s)")
        self.ax2.set_ylabel("x,y (m)")

        self.ax3.set_title("Total Displacement r(t) (m)")
        self.ax3.set_xlabel("Time (s)")
        self.ax3.set_ylabel("r (m)")

        self.ax4.set_title("Polar Trajectory (r, theta)")
        self.ax4.set_theta_zero_location("E")
        self.ax4.set_theta_direction(1)

        self.l1x, = self.ax1.plot([], [], label="ax")
        self.l1y, = self.ax1.plot([], [], label="ay")
        self.ax1.legend(loc="upper right")

        self.l2x, = self.ax2.plot([], [], label="x")
        self.l2y, = self.ax2.plot([], [], label="y")
        self.ax2.legend(loc="upper right")

        self.l3, = self.ax3.plot([], [], label="r")
        self.ax3.legend(loc="upper right")

        self.l4, = self.ax4.plot([], [], lw=1.2)

        self.canvas = FigureCanvasTkAgg(fig, master=frm)
        self.canvas.get_tk_widget().pack(side="top", fill="both", expand=True)

        # status
        self.status = tk.StringVar(value="Ready.")
        ttk.Label(frm, textvariable=self.status).pack(side="bottom", fill="x")

    # ---------- start/stop ----------
    def start(self):
        if self._running:
            return
        if not NI_AVAILABLE:
            self.status.set("nidaqmx 不可用：请安装 NI-DAQmx + nidaqmx 并在同一环境运行。")
            return

        phys = self._get_phys_chans()
        if not phys:
            self.status.set("请先选择 Module（例如 cDAQ1Mod1），再启动。")
            return

        fs = float(self.fs_hz.get())
        if fs <= 0:
            fs = 1600.0
            self.fs_hz.set(fs)
        ds = int(self.downsample.get())
        if ds < 1:
            ds = 1
            self.downsample.set(ds)

        self.fs = fs
        self.downsample_n = ds
        self.proc_fs = self.fs / ds

        self._running = True
        self.status.set(f"Running... {phys[0]} , {phys[1]}  Term={self.term_cfg.get()}  Fs={self.fs:.1f}  Ds={ds}")

        # reset buffers
        self.t_buf.clear(); self.ax_buf.clear(); self.ay_buf.clear()
        self.x_buf.clear(); self.y_buf.clear(); self.r_buf.clear()
        self.t0 = None
        self._last_ts_end = None

        # reset integrators
        self.td_x.reset(); self.td_y.reset()
        self.kf_x.reset(); self.kf_y.reset()

        # reset freq rolling
        self._fd_len = int(max(5.0, float(self.fd_win_s.get())) * self.proc_fs)
        self._fd_ax = deque(maxlen=self._fd_len)
        self._fd_ay = deque(maxlen=self._fd_len)

        # reader thread
        self.reader = NIReaderThread(phys, self.fs, self.chunk, term_cfg_str=self.term_cfg.get())
        self.reader.start()

    def stop(self):
        self._running = False
        if self.reader is not None:
            self.reader.stop()
            self.reader = None
        self.status.set("Stopped.")
        self._reset_display()

    def _reset_display(self):
        # Clear buffers and restart time origin for a clean plot.
        self.t_buf.clear(); self.ax_buf.clear(); self.ay_buf.clear()
        self.x_buf.clear(); self.y_buf.clear(); self.r_buf.clear()
        self.t0 = None
        self._last_ts_end = None

        self.td_x.reset(); self.td_y.reset()
        self.kf_x.reset(); self.kf_y.reset()

        self._fd_len = int(max(5.0, float(self.fd_win_s.get())) * self.proc_fs)
        self._fd_ax = deque(maxlen=self._fd_len)
        self._fd_ay = deque(maxlen=self._fd_len)

        self.l1x.set_data([], []); self.l1y.set_data([], [])
        self.l2x.set_data([], []); self.l2y.set_data([], [])
        self.l3.set_data([], []); self.l4.set_data([], [])
        for ax in (self.ax1, self.ax2, self.ax3):
            ax.relim()
            ax.autoscale_view()
        self.ax4.set_rlim(0, 1.0)
        self.canvas.draw_idle()

    # ---------- conversion ----------
    def _volts_to_mps2(self, v: np.ndarray) -> np.ndarray:
        sens_V_per_g = (self.sens_mV_per_g.get() / 1000.0)  # mV/g -> V/g
        if sens_V_per_g <= 0:
            sens_V_per_g = 1.0
        g_val = v / sens_V_per_g
        return g_val * self.g0

    # ---------- buffer append ----------
    def _append_block(self, ts, ax, ay, x, y):
        win_s = max(5.0, float(self.time_window_s.get()))
        maxlen = int(win_s * self.proc_fs)

        if self.t_buf:
            last_t = self.t_buf[-1]
            if np.isfinite(last_t):
                dt = 1.0 / self.proc_fs
                gap = float(ts[0]) - float(last_t)
                if gap < (-0.5 * dt):
                    # Time jumped backwards; restart the display to avoid mixing segments.
                    self.t_buf.clear(); self.ax_buf.clear(); self.ay_buf.clear()
                    self.x_buf.clear(); self.y_buf.clear(); self.r_buf.clear()
                    self.t0 = float(ts[0])
                elif gap > (1.5 * dt):
                    # Break lines across gaps to prevent diagonal bridges.
                    self.t_buf.append(np.nan)
                    self.ax_buf.append(np.nan)
                    self.ay_buf.append(np.nan)
                    self.x_buf.append(np.nan)
                    self.y_buf.append(np.nan)
                    self.r_buf.append(np.nan)

        for i in range(len(ts)):
            self.t_buf.append(float(ts[i]))
            self.ax_buf.append(float(ax[i]))
            self.ay_buf.append(float(ay[i]))
            self.x_buf.append(float(x[i]))
            self.y_buf.append(float(y[i]))
            self.r_buf.append(float(np.hypot(x[i], y[i])))

        while len(self.t_buf) > maxlen:
            self.t_buf.popleft(); self.ax_buf.popleft(); self.ay_buf.popleft()
            self.x_buf.popleft(); self.y_buf.popleft(); self.r_buf.popleft()

    # ---------- integrate ----------
    def _integrate_xy(self, ax_block, ay_block):
        m = self.method.get()

        if m == "time":
            self.td_x.set_params(self.proc_fs, self.td_bias_tau.get(), self.td_leak_fz.get())
            self.td_y.set_params(self.proc_fs, self.td_bias_tau.get(), self.td_leak_fz.get())
            return self.td_x.step_block(ax_block), self.td_y.step_block(ay_block)

        if m == "kf":
            # 这里对应 Accel2DispKF.set_params 的 4 个参数
            # 我们暂且借用 GUI 上的 kf_sigma_init_x 作为 sigma_x_pseudo 参数
            self.kf_x.set_params(self.proc_fs, self.kf_sigma_a.get(), self.kf_sigma_b.get(), self.kf_sigma_init_x.get())
            self.kf_y.set_params(self.proc_fs, self.kf_sigma_a.get(), self.kf_sigma_b.get(), self.kf_sigma_init_x.get())
            return self.kf_x.step_block(ax_block), self.kf_y.step_block(ay_block)
        

        
        # freq：滚动长窗 + 强重叠，只输出本块末段，保证连续
        win_s = max(5.0, float(self.fd_win_s.get()))
        win_n = int(win_s * self.proc_fs)
        f_reg = float(self.fd_fc_hp.get())
        wn = self.fd_win_name.get()
        if wn == "rect":
            wn = "rect"

        self._fd_ax.extend(ax_block.tolist())
        self._fd_ay.extend(ay_block.tolist())

        if len(self._fd_ax) < win_n:
            return np.full_like(ax_block, np.nan), np.full_like(ay_block, np.nan)

        ax_win = np.array(list(self._fd_ax)[-win_n:], dtype=float)
        ay_win = np.array(list(self._fd_ay)[-win_n:], dtype=float)

        x_win = disp_freq_domain_window(
            ax_win, self.proc_fs, f_reg, wn,
            remove_mean=True, detrend_out=self.fd_detrend_out.get()
        )
        y_win = disp_freq_domain_window(
            ay_win, self.proc_fs, f_reg, wn,
            remove_mean=True, detrend_out=self.fd_detrend_out.get()
        )

        L = len(ax_block)
        return x_win[-L:], y_win[-L:]

    # ---------- update loop ----------
    def _update_loop(self):
        if self._running and self.reader is not None:
            latest = self.reader.get_latest()
            if latest is not None:
                ts, buf = latest  # ts (chunk,), buf (2, chunk)
                ts = np.asarray(ts, dtype=float)
                vx = buf[0, :]
                vy = buf[1, :]

                ds = int(self.downsample_n)
                if ds > 1:
                    ts = ts[::ds]
                    vx = vx[::ds]
                    vy = vy[::ds]
                if len(ts) == 0:
                    self.after(50, self._update_loop)
                    return

                if self._last_ts_end is not None:
                    if ts[-1] <= self._last_ts_end:
                        self.after(50, self._update_loop)
                        return
                    if ts[0] <= self._last_ts_end:
                        dt = 1.0 / self.proc_fs
                        start = int(np.searchsorted(ts, self._last_ts_end + 0.5 * dt, side="right"))
                        ts = ts[start:]
                        vx = vx[start:]
                        vy = vy[start:]
                        if len(ts) == 0:
                            self.after(50, self._update_loop)
                            return

                if self.t0 is None:
                    self.t0 = float(ts[0])

                ax = self._volts_to_mps2(vx)
                ay = self._volts_to_mps2(vy)

                x, y = self._integrate_xy(ax, ay)
                self._append_block(ts, ax, ay, x, y)
                self._last_ts_end = float(ts[-1])

        self._redraw()
        self.after(50, self._update_loop)

    # ---------- redraw ----------
    def _redraw(self):
        if len(self.t_buf) < 10:
            return

        t = np.array(self.t_buf, dtype=float)
        ax = np.array(self.ax_buf, dtype=float)
        ay = np.array(self.ay_buf, dtype=float)
        x = np.array(self.x_buf, dtype=float)
        y = np.array(self.y_buf, dtype=float)
        r = np.array(self.r_buf, dtype=float)

        tt = t - self.t0

        self.l1x.set_data(tt, ax)
        self.l1y.set_data(tt, ay)
        self.ax1.relim(); self.ax1.autoscale_view()

        self.l2x.set_data(tt, x)
        self.l2y.set_data(tt, y)
        self.ax2.relim(); self.ax2.autoscale_view()

        self.l3.set_data(tt, r)
        self.ax3.relim(); self.ax3.autoscale_view()

        theta = np.arctan2(y, x)
        self.l4.set_data(theta, r)

        finite_r = r[np.isfinite(r)]
        if finite_r.size:
            rmax = float(np.max(finite_r))
        else:
            rmax = 1.0
        if rmax < 1e-6:
            rmax = 1.0
        self.ax4.set_rlim(0, rmax * 1.05)

        finite_theta = theta[np.isfinite(theta)]
        finite_r_last = finite_r[-1] if finite_r.size else np.nan
        ang_deg = float(np.degrees(finite_theta[-1])) if finite_theta.size else float("nan")
        self.status.set(
            f"Method={self.method.get()} | r={finite_r_last:.3f} m | theta={ang_deg:.1f}° | Mod={self.mod_name.get()}"
        )

        self.canvas.draw_idle()


if __name__ == "__main__":
    app = App()
    app.mainloop()
