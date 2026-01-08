const intervalMs = 10000;
const maxPoints = 144;
const towerProfiles = {
    t1: { label: "1号塔", vibScale: 1.0, dispScale: 1.0, fatigueScale: 1.0 },
    t2: { label: "2号塔", vibScale: 1.15, dispScale: 0.9, fatigueScale: 1.3 },
    t3: { label: "3号塔", vibScale: 0.85, dispScale: 1.2, fatigueScale: 0.8 }
};
let activeTower = "t1";
let selectedDate = null;

let configData = {};
let deviceConfigs = {};
let fatigueData = {};
let windState = { connected: false, mode: "sim", sample: null, stats: null };
const socket = io();
let spectrumDevice = null;
let spectrumRangeHz = 5;
const streamDisp = { x: [], y: [], fs: null };
const streamAcc = { x: [], y: [], fs: null };

const rawSeries = { x: [], vib0: [], vib1: [], disp0: [], disp1: [] };
const series = { x: [], vib0: [], vib1: [], disp0: [], disp1: [], dispEq: [] };
const windowState = { maxVib: 0, maxDisp: 0 };
let fatigueRaw = null;

function formatNum(val, digits = 2) {
    if (val === undefined || val === null || Number.isNaN(val)) return "--";
    const num = Number(val);
    return Number.isFinite(num) ? num.toFixed(digits) : "--";
}

function getDeviceLabel(deviceKey) {
    const cfg = (deviceConfigs || {})[deviceKey] || {};
    return cfg.display_name || deviceKey;
}

function getAccUnit() {
    return "m/s^2";
}

function getDispUnit() {
    return "mm";
}

function getAccScale() {
    const dev = (configData.devices || {}).cDAQ3 || {};
    const channels = Array.isArray(dev.channels) ? dev.channels : [];
    for (const ch of channels) {
        if (!ch || !ch.unit) continue;
        const unit = String(ch.unit).toLowerCase();
        if (unit === "g" || unit === "gal") {
            return 9.80665;
        }
        if (unit.includes("m/s") || unit.includes("m/s^2") || unit.includes("m/s2")) {
            return 1.0;
        }
    }
    return 9.80665;
}

function getDispScale() {
    return 1000.0;
}

function getDateRangeMs() {
    if (selectedDate) {
        const start = new Date(selectedDate.getFullYear(), selectedDate.getMonth(), selectedDate.getDate());
        const end = new Date(start.getTime() + 24 * 60 * 60 * 1000 - 1);
        return { startMs: start.getTime(), endMs: end.getTime(), custom: true };
    }
    const now = Date.now();
    return { startMs: now - 24 * 60 * 60 * 1000, endMs: now, custom: false };
}

function toDateStringLocal(d) {
    const y = d.getFullYear();
    const m = String(d.getMonth() + 1).padStart(2, "0");
    const day = String(d.getDate()).padStart(2, "0");
    return `${y}-${m}-${day}`;
}

function getSelectedDayRangeMs() {
    if (selectedDate) {
        const start = new Date(selectedDate.getFullYear(), selectedDate.getMonth(), selectedDate.getDate());
        const end = new Date(start.getTime() + 24 * 60 * 60 * 1000 - 1);
        return { startMs: start.getTime(), endMs: end.getTime() };
    }
    if (series.x.length) {
        const last = series.x[series.x.length - 1];
        const start = new Date(last.getFullYear(), last.getMonth(), last.getDate());
        const end = new Date(start.getTime() + 24 * 60 * 60 * 1000 - 1);
        return { startMs: start.getTime(), endMs: end.getTime() };
    }
    const now = new Date();
    const start = new Date(now.getFullYear(), now.getMonth(), now.getDate());
    const end = new Date(start.getTime() + 24 * 60 * 60 * 1000 - 1);
    return { startMs: start.getTime(), endMs: end.getTime() };
}

function getSelectedDateString() {
    if (selectedDate) {
        return toDateStringLocal(selectedDate);
    }
    if (series.x.length) {
        return toDateStringLocal(series.x[series.x.length - 1]);
    }
    return toDateStringLocal(new Date());
}

function updateKpi() {
    const vibEl = document.getElementById("kpi-vibration");
    const dispEl = document.getElementById("kpi-displacement");
    const windowEl = document.getElementById("kpi-window");
    const windowEl2 = document.getElementById("kpi-window-2");
    const range = getDateRangeMs();
    let rangeText = `${new Date(range.startMs).toLocaleString()} - ${new Date(range.endMs).toLocaleString()}`;
    if (!range.custom && series.x.length) {
        const first = series.x[0];
        const last = series.x[series.x.length - 1];
        rangeText = `${first.toLocaleString()} - ${last.toLocaleString()}`;
    }

    if (vibEl) vibEl.textContent = formatNum(windowState.maxVib, 3);
    if (dispEl) dispEl.textContent = formatNum(windowState.maxDisp, 4);
    if (windowEl) windowEl.textContent = rangeText;
    if (windowEl2) windowEl2.textContent = rangeText;
}

function updateVibrationChart() {
    const chart = document.getElementById("vibration-trend");
    if (!chart) return;
    const unit = getAccUnit();
    Plotly.react(chart, [{
        x: series.x,
        y: series.vib0,
        name: "X",
        mode: "lines+markers",
        line: { color: "#e76f51", width: 2 },
        marker: { size: 5 }
    }, {
        x: series.x,
        y: series.vib1,
        name: "Y",
        mode: "lines+markers",
        line: { color: "#2a9d8f", width: 2 },
        marker: { size: 5 }
    }], {
        margin: { t: 35, b: 45, l: 60, r: 20 },
        paper_bgcolor: "rgba(0,0,0,0)",
        plot_bgcolor: "rgba(0,0,0,0)",
        xaxis: { title: "时间", type: "date", tickformat: "%H:%M" },
        yaxis: { title: `最大振动 (${unit})` }
    }, { displayModeBar: false, responsive: true });
}

function updateDisplacementChart() {
    const chart = document.getElementById("displacement-trend");
    if (!chart) return;
    const unit = getDispUnit();
    Plotly.react(chart, [{
        x: series.x,
        y: series.dispEq,
        name: "等效位移",
        mode: "lines+markers",
        line: { color: "#6c5ce7", width: 2 },
        marker: { size: 5 }
    }], {
        margin: { t: 35, b: 45, l: 60, r: 20 },
        paper_bgcolor: "rgba(0,0,0,0)",
        plot_bgcolor: "rgba(0,0,0,0)",
        xaxis: { title: "时间", type: "date", tickformat: "%H:%M" },
        yaxis: { title: `最大位移 (${unit})` },
        showlegend: false
    }, { displayModeBar: false, responsive: true });
}

function renderSpectrumPlaceholder() {
    const chart = document.getElementById("acc-spectrum");
    if (!chart) return;
    const unit = getAccUnit();
    const x = Array.from({ length: 51 }, (_, i) => i * 0.1);
    const zeros = x.map(() => 0);
    Plotly.react(chart, [{
        x,
        y: zeros,
        name: "X",
        mode: "lines",
        line: { color: "#e76f51", width: 2 }
    }, {
        x,
        y: zeros,
        name: "Y",
        mode: "lines",
        line: { color: "#2a9d8f", width: 2 }
    }], {
        margin: { t: 35, b: 45, l: 60, r: 20 },
        paper_bgcolor: "rgba(0,0,0,0)",
        plot_bgcolor: "rgba(0,0,0,0)",
        xaxis: { title: "频率 (Hz)", range: [0, 5] },
        yaxis: { title: `幅值 (${unit})` },
        annotations: [{
            text: "暂无频谱数据",
            x: 0.5,
            y: 0.5,
            xref: "paper",
            yref: "paper",
            showarrow: false,
            font: { color: "#7b8a97" }
        }]
    }, { displayModeBar: false, responsive: true });
}

function renderRealtimeVibPlaceholder() {
    const chart = document.getElementById("realtime-vibration");
    if (!chart) return;
    const unit = getAccUnit();
    Plotly.react(chart, [], {
        margin: { t: 30, b: 40, l: 60, r: 20 },
        paper_bgcolor: "rgba(0,0,0,0)",
        plot_bgcolor: "rgba(0,0,0,0)",
        xaxis: { title: "时间 (s)" },
        yaxis: { title: `振动 (${unit})` },
        annotations: [{
            text: "等待实时数据",
            x: 0.5,
            y: 0.5,
            xref: "paper",
            yref: "paper",
            showarrow: false,
            font: { color: "#7b8a97" }
        }]
    }, { displayModeBar: false, responsive: true });
}

function updateRealtimeVibChart() {
    const chart = document.getElementById("realtime-vibration");
    if (!chart) return;
    const x0 = streamAcc.x || [];
    const y1 = streamAcc.y || [];
    const n = Math.max(x0.length, y1.length);
    if (!n) {
        renderRealtimeVibPlaceholder();
        return;
    }
    const fs = Number(streamAcc.fs) || 1;
    const idxCount = Math.max(x0.length, y1.length);
    const x = Array.from({ length: idxCount }, (_, i) => i / fs);
    const profile = towerProfiles[activeTower] || towerProfiles.t1;
    const scale = getAccScale() * profile.vibScale;
    const unit = getAccUnit();
    Plotly.react(chart, [{
        x,
        y: x0.map(v => v * scale),
        name: "X",
        mode: "lines",
        line: { color: "#e76f51", width: 1.8 }
    }, {
        x,
        y: y1.map(v => v * scale),
        name: "Y",
        mode: "lines",
        line: { color: "#2a9d8f", width: 1.8 }
    }], {
        margin: { t: 30, b: 40, l: 60, r: 20 },
        paper_bgcolor: "rgba(0,0,0,0)",
        plot_bgcolor: "rgba(0,0,0,0)",
        xaxis: { title: "时间 (s)" },
        yaxis: { title: `振动 (${unit})` },
        showlegend: true
    }, { displayModeBar: false, responsive: true });
}

function renderTrajectoryPlaceholder() {
    const chart = document.getElementById("disp-trajectory");
    if (!chart) return;
    const unit = getDispUnit();
    Plotly.react(chart, [], {
        margin: { t: 35, b: 45, l: 60, r: 20 },
        paper_bgcolor: "rgba(0,0,0,0)",
        plot_bgcolor: "rgba(0,0,0,0)",
        polar: {
            radialaxis: { title: `位移幅值 (${unit})` },
            angularaxis: { direction: "counterclockwise", rotation: 0 }
        },
        annotations: [{
            text: "暂无轨迹数据",
            x: 0.5,
            y: 0.5,
            xref: "paper",
            yref: "paper",
            showarrow: false,
            font: { color: "#7b8a97" }
        }]
    }, { displayModeBar: false, responsive: true });
}

function updateTrajectoryChart() {
    const chart = document.getElementById("disp-trajectory");
    if (!chart) return;
    const rawX = streamDisp.x || [];
    const rawY = streamDisp.y || [];
    const len = Math.min(rawX.length, rawY.length);
    if (!len) {
        renderTrajectoryPlaceholder();
        return;
    }
    const profile = towerProfiles[activeTower] || towerProfiles.t1;
    const scale = getDispScale() * profile.dispScale;
    const fs = Number(streamDisp.fs) || 1;
    const step = Math.max(1, Math.round(fs)); // downsample to 1s per point
    const xs = [];
    const ys = [];
    for (let i = 0; i < len; i += step) {
        xs.push(rawX[i] * scale);
        ys.push(rawY[i] * scale);
    }
    const smoothWindow = 5;
    const smoothSeries = (arr, window) => {
        if (window <= 1 || arr.length < 2) return arr.slice();
        const half = Math.floor(window / 2);
        return arr.map((_, i) => {
            const start = Math.max(0, i - half);
            const end = Math.min(arr.length, i + half + 1);
            let sum = 0;
            for (let j = start; j < end; j += 1) sum += arr[j];
            return sum / (end - start);
        });
    };
    const xsSmooth = smoothSeries(xs, smoothWindow);
    const ysSmooth = smoothSeries(ys, smoothWindow);
    const mags = [];
    const thetas = [];
    let maxMag = 0;
    let maxDir = 0;
    for (let i = 0; i < xsSmooth.length; i += 1) {
        const x = xsSmooth[i];
        const y = ysSmooth[i];
        const m = Math.hypot(x, y);
        let ang = Math.atan2(y, x) * (180 / Math.PI);
        if (ang < 0) ang += 360;
        mags.push(m);
        thetas.push(ang);
        if (m > maxMag) {
            maxMag = m;
            maxDir = ang;
        }
    }
    const unit = getDispUnit();
    const colorStops = [
        [0, 60, 0],
        [220, 20, 20]
    ];
    const toColor = (t) => {
        const clamped = Math.max(0, Math.min(1, t));
        const r = Math.round(colorStops[0][0] + (colorStops[1][0] - colorStops[0][0]) * clamped);
        const g = Math.round(colorStops[0][1] + (colorStops[1][1] - colorStops[0][1]) * clamped);
        const b = Math.round(colorStops[0][2] + (colorStops[1][2] - colorStops[0][2]) * clamped);
        return `rgb(${r},${g},${b})`;
    };
    const traces = [];
    const maxRef = maxMag || 1;
    for (let i = 1; i < mags.length; i += 1) {
        const t = mags[i] / maxRef;
        traces.push({
            r: [mags[i - 1], mags[i]],
            theta: [thetas[i - 1], thetas[i]],
            type: "scatterpolar",
            mode: "lines",
            line: { color: toColor(t), width: 2 },
            hoverinfo: "skip"
        });
    }
    if (!traces.length) {
        renderTrajectoryPlaceholder();
        return;
    }
    traces.push({
        r: [mags[mags.length - 1]],
        theta: [thetas[thetas.length - 1]],
        type: "scatterpolar",
        mode: "markers",
        marker: { size: 4, color: "#dc2f02" },
        hovertemplate: `R: %{r:.3f} ${unit}<br>θ: %{theta:.1f} °<extra></extra>`
    });
    Plotly.react(chart, traces, {
        margin: { t: 35, b: 45, l: 60, r: 20 },
        paper_bgcolor: "rgba(0,0,0,0)",
        plot_bgcolor: "rgba(0,0,0,0)",
        polar: {
            radialaxis: { title: `位移幅值 (${unit})` },
            angularaxis: { direction: "counterclockwise", rotation: 0 }
        },
        showlegend: false
    }, { displayModeBar: false, responsive: true });
    const maxEl = document.getElementById("traj-max");
    if (maxEl) maxEl.textContent = formatNum(maxMag, 3);
    const dirEl = document.getElementById("traj-max-dir");
    if (dirEl) dirEl.textContent = `${formatNum(maxDir, 1)} °`;
}

function renderSpectrumFromPayload(freq = [], spectra = []) {
    const chart = document.getElementById("acc-spectrum");
    if (!chart) return;
    if (!Array.isArray(freq) || !freq.length || !Array.isArray(spectra) || !spectra.length) {
        renderSpectrumPlaceholder();
        return;
    }
    const maxHz = Number(spectrumRangeHz) > 0 ? Number(spectrumRangeHz) : null;
    let plotFreq = freq;
    let plotSpectra = spectra;
    if (maxHz) {
        const keepIdx = [];
        for (let i = 0; i < freq.length; i += 1) {
            if (freq[i] <= maxHz) keepIdx.push(i);
        }
        plotFreq = keepIdx.map(i => freq[i]);
        plotSpectra = spectra.map(arr => keepIdx.map(i => arr[i]));
    }
    Plotly.react(chart, [{
        x: plotFreq,
        y: plotSpectra[0] || [],
        name: "X",
        mode: "lines",
        line: { color: "#e76f51", width: 2 }
    }, {
        x: plotFreq,
        y: plotSpectra[1] || [],
        name: "Y",
        mode: "lines",
        line: { color: "#2a9d8f", width: 2 }
    }], {
        margin: { t: 35, b: 45, l: 60, r: 20 },
        paper_bgcolor: "rgba(0,0,0,0)",
        plot_bgcolor: "rgba(0,0,0,0)",
        xaxis: { title: "频率 (Hz)", range: maxHz ? [0, maxHz] : undefined },
        yaxis: { title: "幅值 (dB)" }
    }, { displayModeBar: false, responsive: true });
}

function initSpectrumSocket() {
    if (spectrumDevice) return;
    const keys = Object.keys(deviceConfigs || {});
    const device = keys[0] || "cDAQ3";
    spectrumDevice = device;
    socket.on(`spectrum_${device}`, payload => {
        renderSpectrumFromPayload(payload?.freq || [], payload?.spectra || []);
    });
    socket.on(`stream_${device}`, payload => {
        const timeData = payload?.time_data || [];
        streamAcc.x = Array.isArray(timeData[0]) ? timeData[0] : [];
        streamAcc.y = Array.isArray(timeData[1]) ? timeData[1] : [];
        const disp = payload?.displacement || [];
        streamDisp.x = Array.isArray(disp[0]) ? disp[0] : [];
        streamDisp.y = Array.isArray(disp[1]) ? disp[1] : [];
        const fs = payload?.effective_sample_rate || payload?.sample_rate || streamDisp.fs;
        streamDisp.fs = fs;
        streamAcc.fs = fs;
        updateRealtimeVibChart();
        updateTrajectoryChart();
    });
}

socket.on("connect", () => {
    fetchWind();
});
socket.on("wind_sample", (payload) => {
    updateWindSample(payload);
});
socket.on("wind_stats", (payload) => {
    updateWindStats(payload);
});

async function fetchDailyDispStats() {
    const chart = document.getElementById("daily-disp-chart");
    if (!chart) return;
    try {
        const res = await fetch(`/api/stats/daily_disp_recent?days=30&nocache=${Date.now()}`, {
            cache: "no-store"
        });
        if (!res.ok) {
            renderDailyDispChart(null);
            return;
        }
        const data = await res.json();
        renderDailyDispChart(data);
    } catch (err) {
        renderDailyDispChart(null);
    }
}

function renderDailyDispChart(stats) {
    const chart = document.getElementById("daily-disp-chart");
    if (!chart) return;
    const unit = getDispUnit();
    if (!stats || !Array.isArray(stats.series)) {
        Plotly.react(chart, [], {
            margin: { t: 20, b: 40, l: 60, r: 20 },
            paper_bgcolor: "rgba(0,0,0,0)",
            plot_bgcolor: "rgba(0,0,0,0)",
            xaxis: { title: "日期" },
            yaxis: { title: `最大等效位移 (${unit})` },
            annotations: [{
                text: "暂无数据",
                x: 0.5,
                y: 0.5,
                xref: "paper",
                yref: "paper",
                showarrow: false,
                font: { color: "#7b8a97" }
            }]
        }, { displayModeBar: false, responsive: true });
        return;
    }

    const profile = towerProfiles[activeTower] || towerProfiles.t1;
    const scale = getDispScale() * profile.dispScale;
    const x = stats.series.map(item => item.date);
    const y = stats.series.map(item => (item.max_eq != null ? item.max_eq * scale : null));
    Plotly.react(chart, [{
        x,
        y,
        type: "bar",
        marker: { color: "#6c5ce7" },
        hovertemplate: `%{x}: %{y:.3f} ${unit}<extra></extra>`
    }], {
        margin: { t: 20, b: 40, l: 60, r: 20 },
        paper_bgcolor: "rgba(0,0,0,0)",
        plot_bgcolor: "rgba(0,0,0,0)",
        xaxis: { title: "日期", tickformat: "%m/%d" },
        yaxis: { title: `最大位移 (${unit})` }
    }, { displayModeBar: false, responsive: true });
}

function applyTowerProfile() {
    const profile = towerProfiles[activeTower] || towerProfiles.t1;
    const accScale = getAccScale();
    const dispScale = getDispScale();
    series.x = rawSeries.x.slice();
    series.vib0 = rawSeries.vib0.map(v => (v == null ? null : v * profile.vibScale * accScale));
    series.vib1 = rawSeries.vib1.map(v => (v == null ? null : v * profile.vibScale * accScale));
    series.disp0 = rawSeries.disp0.map(v => (v == null ? null : v * profile.dispScale * dispScale));
    series.disp1 = rawSeries.disp1.map(v => (v == null ? null : v * profile.dispScale * dispScale));
    series.dispEq = series.disp0.map((v, i) => {
        const x = Math.abs(v ?? 0);
        const y = Math.abs(series.disp1[i] ?? 0);
        return x + y;
    });

    const lastIdx = series.x.length - 1;
    if (lastIdx >= 0) {
        const lastVib = Math.max(series.vib0[lastIdx] ?? 0, series.vib1[lastIdx] ?? 0);
        const lastDisp = series.dispEq[lastIdx] ?? 0;
        windowState.maxVib = lastVib;
        windowState.maxDisp = lastDisp;
    } else {
        windowState.maxVib = 0;
        windowState.maxDisp = 0;
    }

    updateKpi();
    updateVibrationChart();
    updateDisplacementChart();
    renderSpectrumPlaceholder();
    updateTrajectoryChart();
    renderFatigue();
}

function renderWindCard() {
    const speed = windState.sample?.speed_mps;
    const dir = windState.sample?.direction_deg;
    const stats = windState.stats || {};

    const speedEl = document.getElementById("wind-speed");
    const dirEl = document.getElementById("wind-direction");
    const meanEl = document.getElementById("wind-mean");
    const minmaxEl = document.getElementById("wind-minmax");
    const dirMeanEl = document.getElementById("wind-dir-mean");
    const updatedEl = document.getElementById("wind-updated");
    const connectionEl = document.getElementById("wind-connection");

    if (speedEl) speedEl.textContent = formatNum(speed, 2);
    if (dirEl) dirEl.textContent = formatNum(dir, 1);
    if (meanEl) meanEl.textContent = `${formatNum(stats.speed_mean, 2)} m/s`;
    if (minmaxEl) minmaxEl.textContent = `${formatNum(stats.speed_min, 2)} / ${formatNum(stats.speed_max, 2)} m/s`;
    if (dirMeanEl) dirMeanEl.textContent = `${formatNum(stats.direction_mean_deg, 1)} °`;
    if (updatedEl) updatedEl.textContent = windState.sample?.ts ? new Date(windState.sample.ts * 1000).toLocaleTimeString() : "--";
    if (connectionEl) {
        connectionEl.textContent = windState.connected ? "已连接" : "未连接";
        connectionEl.classList.toggle("soft", !windState.connected);
    }

    const needle = document.getElementById("wind-needle");
    const angle = Number.isFinite(Number(dir)) ? Number(dir) : 0;
    if (needle) {
        needle.style.transformOrigin = "100px 100px";
        needle.style.transform = `rotate(${angle}deg)`;
    }
}

function updateWindSample(payload) {
    if (!payload) return;
    windState.connected = !!payload.connected;
    windState.mode = payload.mode || windState.mode;
    windState.sample = {
        ts: payload.ts,
        speed_mps: payload.speed_mps,
        direction_deg: payload.direction_deg,
    };
    renderWindCard();
}

function updateWindStats(payload) {
    if (!payload) return;
    windState.connected = !!payload.connected;
    windState.mode = payload.mode || windState.mode;
    windState.stats = payload.stats || null;
    renderWindCard();
}

async function fetchWind() {
    try {
        const res = await fetch("/api/wind");
        const data = await res.json();
        windState = {
            connected: !!data.connected,
            mode: data.mode || "sim",
            sample: data.sample || null,
            stats: data.stats || null,
        };
        renderWindCard();
    } catch (err) {
        // ignore
    }
}

async function fetchConfig() {
    try {
        const res = await fetch("/api/config");
        configData = await res.json();
        deviceConfigs = configData.devices || {};
        const trajLabel = document.getElementById("traj-window-label");
        if (trajLabel) {
            trajLabel.textContent = String(configData.fft_window_s || 30);
        }
        const realtimeLabel = document.getElementById("realtime-window-label");
        if (realtimeLabel) {
            realtimeLabel.textContent = String(configData.fft_window_s || 30);
        }
        document.querySelectorAll("#interval-label, .interval-label").forEach(el => {
            el.textContent = String(intervalMs / 1000);
        });
        initSpectrumSocket();
    } catch (err) {
        // ignore
    }
}

async function fetchFatigueFromStorage() {
    try {
        const curve = await fetchDamageCurve();
        const latestFatigue = await fetchLatestFatigueFromCsv();
        if (latestFatigue) {
            fatigueRaw = {
                ...latestFatigue,
                phi_deg_list: curve?.phi_deg_list || [],
                D_phi_cum: curve?.D_phi_cum || [],
            };
        } else {
            fatigueRaw = null;
        }
        fatigueData = fatigueRaw ? { cDAQ3: fatigueRaw } : {};
        renderFatigue();
    } catch (err) {
        // ignore
    }
    setTimeout(fetchFatigueFromStorage, 30000);
}

function renderFatigue() {
    const board = document.getElementById("fatigue-board");
    const status = document.getElementById("fatigue-updated");
    if (!board) return;
    board.innerHTML = "";
    const names = Object.keys(fatigueData || {});
    if (!names.length) {
        const empty = document.createElement("div");
        empty.className = "empty-state";
        empty.textContent = "暂无疲劳评估数据";
        board.appendChild(empty);
        if (status) status.textContent = "等待数据";
        return;
    }

    names.forEach(name => {
        const profile = towerProfiles[activeTower] || towerProfiles.t1;
        const rawItem = fatigueData[name] || {};
        const item = {
            ...rawItem,
            Dmax: Number(rawItem.Dmax) * profile.fatigueScale,
            D_cum_max: Number(rawItem.D_cum_max) * profile.fatigueScale,
            Sa_max: Number(rawItem.Sa_max) * profile.fatigueScale
        };
        const card = document.createElement("div");
        card.className = "fatigue-card";
        const dmax = item.D_cum_max ?? item.Dmax;
        const phi = item.phi_deg_cum ?? item.phi_deg;
        const polarId = `fatigue-polar-${name}`;
        card.innerHTML = `
            <div class="fatigue-title">${getDeviceLabel(name)}</div>
            <div class="fatigue-meta">${item.timestamp || "时间未知"}</div>
            <div class="fatigue-row">
                <div class="fatigue-metrics">
                    <div class="fatigue-metric"><span>Dmax</span><strong>${formatNum(dmax, 4)}</strong></div>
                    <div class="fatigue-metric"><span>方向</span><strong>${formatNum(phi, 1)} °</strong></div>
                    <div class="fatigue-metric"><span>最大应力幅</span><strong>${formatNum(item.Sa_max, 3)} MPa</strong></div>
                </div>
                <div class="fatigue-polar" id="${polarId}"></div>
            </div>
        `;
        board.appendChild(card);

        const phiList = item.phi_deg_list || [];
        const dList = (item.D_phi_cum || item.D_phi || []).map(v => Number(v) * profile.fatigueScale);
        const polarDiv = document.getElementById(polarId);
        if (polarDiv) {
            if (phiList.length && dList.length) {
                const pairs = phiList
                    .map((p, i) => [Number(p), Number(dList[i])])
                    .filter(p => Number.isFinite(p[0]) && Number.isFinite(p[1]))
                    .sort((a, b) => a[0] - b[0]);
                const closedPhi = pairs.map(p => p[0]);
                const closedD = pairs.map(p => p[1]);
                if (closedPhi.length) {
                    closedPhi.push(closedPhi[0]);
                    closedD.push(closedD[0]);
                }
                Plotly.newPlot(polarDiv, [{
                    type: "scatterpolar",
                    r: closedD,
                    theta: closedPhi,
                    mode: "lines+markers",
                    fill: "toself",
                    line: { color: "#e76f51", width: 2 },
                    marker: { size: 4 }
                }], {
                    margin: { t: 10, b: 10, l: 10, r: 10 },
                    polar: {
                        radialaxis: { title: "损伤因子 D", showline: true, linewidth: 1 },
                        angularaxis: { direction: "counterclockwise", rotation: 0 }
                    }
                }, { displayModeBar: false, responsive: true });
            } else {
                polarDiv.textContent = "暂无方向损伤数据";
            }
        }
    });

    if (status) status.textContent = `更新于 ${new Date().toLocaleTimeString()}`;
}

function updateClock() {
    const nowEl = document.getElementById("now-time");
    if (nowEl) nowEl.textContent = new Date().toLocaleTimeString();
}

function initChart() {
    const vibChart = document.getElementById("vibration-trend");
    if (vibChart) {
        const unit = getAccUnit();
        Plotly.newPlot(vibChart, [], {
            margin: { t: 35, b: 45, l: 60, r: 20 },
            paper_bgcolor: "rgba(0,0,0,0)",
            plot_bgcolor: "rgba(0,0,0,0)",
            xaxis: { title: "时间", type: "date" },
            yaxis: { title: `最大振动 (${unit})` }
        }, { displayModeBar: false, responsive: true });
    }

    const dispChart = document.getElementById("displacement-trend");
    if (dispChart) {
        const unit = getDispUnit();
        Plotly.newPlot(dispChart, [], {
            margin: { t: 35, b: 45, l: 60, r: 20 },
            paper_bgcolor: "rgba(0,0,0,0)",
            plot_bgcolor: "rgba(0,0,0,0)",
            xaxis: { title: "时间", type: "date" },
            yaxis: { title: `最大位移 (${unit})` }
        }, { displayModeBar: false, responsive: true });
    }
}

function parseTimestamp(text) {
    if (!text) return null;
    const normalized = String(text).replace(" ", "T");
    const dt = new Date(normalized);
    return Number.isNaN(dt.getTime()) ? null : dt;
}

function rowsToObjects(rows) {
    if (!rows.length) return [];
    const header = rows[0];
    return rows.slice(1).map(row => {
        const obj = {};
        header.forEach((key, idx) => {
            obj[key] = row[idx];
        });
        return obj;
    });
}

function buildTrendPoints(rows) {
    const range = getDateRangeMs();
    const mapRange = new Map();
    const mapAll = new Map();
    const updateMap = (map, ts, ch, accMax, dispMax) => {
        const key = ts.getTime();
        const current = map.get(key) || {
            time: ts,
            vib0: null,
            vib1: null,
            disp0: null,
            disp1: null
        };
        if (ch === 0) {
            current.vib0 = accMax;
            current.disp0 = dispMax;
        } else if (ch === 1) {
            current.vib1 = accMax;
            current.disp1 = dispMax;
        }
        map.set(key, current);
    };
    rows.forEach(r => {
        if (r.type !== "stat") return;
        const ts = parseTimestamp(r.timestamp);
        if (!ts) return;
        const ms = ts.getTime();
        const ch = Number(r.channel);
        if (!Number.isFinite(ch)) return;

        const accMax = Math.max(Math.abs(Number(r.acc_max) || 0), Math.abs(Number(r.acc_min) || 0));
        const dispMax = Math.max(Math.abs(Number(r.disp_max) || 0), Math.abs(Number(r.disp_min) || 0));

        updateMap(mapAll, ts, ch, accMax, dispMax);
        if (ms >= range.startMs && ms <= range.endMs) {
            updateMap(mapRange, ts, ch, accMax, dispMax);
        }
    });

    const useMap = range.custom ? mapRange : (mapRange.size ? mapRange : mapAll);
    const points = Array.from(useMap.values()).sort((a, b) => a.time - b.time);
    if (points.length > maxPoints) {
        return points.slice(points.length - maxPoints);
    }
    return points;
}

async function fetchDataEntries(path) {
    try {
        const res = await fetch(`/api/system/data?path=${encodeURIComponent(path)}`);
        return await res.json();
    } catch (err) {
        return { entries: [] };
    }
}

async function fetchCsvRows(path) {
    try {
        const res = await fetch(`/api/system/file?path=${encodeURIComponent(path)}&limit=0&nocache=${Date.now()}`, {
            cache: "no-store"
        });
        const data = await res.json();
        if (data.type !== "csv" || !Array.isArray(data.rows)) return [];
        return rowsToObjects(data.rows);
    } catch (err) {
        return [];
    }
}

async function findRecentCsvFiles() {
    const root = await fetchDataEntries("cDAQ3");
    const months = (root.entries || []).filter(e => e.is_dir).map(e => e.name).sort();
    if (!months.length) return [];

    const recentMonths = months.slice(-2);
    const csvEntries = [];
    for (const month of recentMonths) {
        const data = await fetchDataEntries(`cDAQ3/${month}`);
        (data.entries || []).forEach(e => {
            if (!e.is_dir && String(e.name).toLowerCase().endsWith(".csv")) {
                csvEntries.push({ path: e.path, name: e.name });
            }
        });
    }

    return csvEntries.sort((a, b) => a.name.localeCompare(b.name)).slice(-3).map(e => e.path);
}

async function fetchRecentCsvRows() {
    const csvPaths = await findRecentCsvFiles();
    if (!csvPaths.length) return [];
    const results = await Promise.all(csvPaths.map(fetchCsvRows));
    return results.flat();
}

async function loadCsvWindowData() {
    const rows = await fetchRecentCsvRows();
    if (!rows.length) {
        rawSeries.x = [];
        rawSeries.vib0 = [];
        rawSeries.vib1 = [];
        rawSeries.disp0 = [];
        rawSeries.disp1 = [];
        applyTowerProfile();
        fetchDailyDispStats();
        return;
    }

    const points = buildTrendPoints(rows);
    rawSeries.x = points.map(p => p.time);
    rawSeries.vib0 = points.map(p => p.vib0);
    rawSeries.vib1 = points.map(p => p.vib1);
    rawSeries.disp0 = points.map(p => p.disp0);
    rawSeries.disp1 = points.map(p => p.disp1);
    applyTowerProfile();
    fetchDailyDispStats();
}

async function fetchLatestFatigueFromCsv() {
    const rows = await fetchRecentCsvRows();
    const fatigueRows = rows
        .filter(r => r.type === "fatigue" && r.timestamp)
        .sort((a, b) => String(a.timestamp).localeCompare(String(b.timestamp)));
    if (!fatigueRows.length) return null;

    const latest = fatigueRows[fatigueRows.length - 1];
    return {
        timestamp: latest.timestamp,
        Dmax: Number(latest.fatigue_Dmax),
        phi_deg: Number(latest.fatigue_phi_deg),
        Sa_max: Number(latest.fatigue_Sa_max),
    };
}

async function fetchDamageCurve() {
    try {
        const res = await fetch(`/api/system/file?path=${encodeURIComponent("cDAQ3/damage_cumulative.json")}`);
        const data = await res.json();
        if (!data.content) return null;
        return JSON.parse(data.content);
    } catch (err) {
        return null;
    }
}

const dateInput = document.getElementById("history-date");
const dateDisplay = document.getElementById("history-date-display");
if (dateInput) {
    dateInput.addEventListener("change", () => {
        selectedDate = dateInput.value ? new Date(dateInput.value) : null;
        if (dateDisplay) {
            dateDisplay.value = dateInput.value || "";
        }
        loadCsvWindowData();
    });
}
if (dateDisplay && dateInput) {
    dateDisplay.addEventListener("click", () => {
        if (typeof dateInput.showPicker === "function") {
            dateInput.showPicker();
        } else {
            dateInput.focus();
            dateInput.click();
        }
    });
}

initChart();
updateClock();
setInterval(updateClock, 1000);
fetchConfig();
fetchWind();
fetchFatigueFromStorage();
loadCsvWindowData();
renderTrajectoryPlaceholder();
renderRealtimeVibPlaceholder();

if (dateDisplay && dateInput?.value) {
    dateDisplay.value = dateInput.value;
}
setInterval(loadCsvWindowData, 60000);
setInterval(fetchWind, 30000);

document.querySelectorAll(".tower-tab").forEach(btn => {
    btn.addEventListener("click", () => {
        const id = btn.dataset.tower || "t1";
        activeTower = towerProfiles[id] ? id : "t1";
        document.querySelectorAll(".tower-tab").forEach(el => {
            el.classList.toggle("active", el.dataset.tower === activeTower);
        });
        applyTowerProfile();
    });
});
window.addEventListener("resize", () => {
    const vibChart = document.getElementById("vibration-trend");
    const dispChart = document.getElementById("displacement-trend");
    if (vibChart) Plotly.Plots.resize(vibChart);
    if (dispChart) Plotly.Plots.resize(dispChart);
    const dailyChart = document.getElementById("daily-disp-chart");
    if (dailyChart) Plotly.Plots.resize(dailyChart);
    const spectrum = document.getElementById("acc-spectrum");
    if (spectrum) Plotly.Plots.resize(spectrum);
    const traj = document.getElementById("disp-trajectory");
    if (traj) Plotly.Plots.resize(traj);
    const realtime = document.getElementById("realtime-vibration");
    if (realtime) Plotly.Plots.resize(realtime);
});
