## cDAQ Monitoring (MVP)

面向结构健康监测的最小可用版，包含采集、实时展示、疲劳分析、存储、IoT 推送。

### 功能概览
- 实时监测：基于 NI cDAQ，Socket.IO 向前端推送时域、频谱、位移和设备运行状态。
- 配置管理：前端表单读写 `backend/config.json`，支持采样率、通道、风速、存储、IoT 等配置。保存后后端自动重载并重建设备，自动启动采集。
- 疲劳评估：每个设备独立计算方向性疲劳（雨流计数 + ASME S-N），累计结果持久化并可在前端查看。
- 数据存储：
  - TDMS：按月/日目录归档，包含 waveform 元数据（start_time、dt、x-unit 等）。
  - 窗口统计 CSV：每设备独立，按月/日归档。
- 设备状态：前端“设备状态”页展示 CPU、磁盘占用和 data 目录文件树，可下钻子目录。
- IoT 推送：按窗口发布加速度/位移统计（max/min/rms/p2p）、主频、疲劳累计到 MQTT；也会记录到本地日志（log 模式）。

### 目录与持久化
- 配置：`backend/config.json`
- TDMS：`backend/data/<YYYYMM>/<DD>/<文件名>.tdms`
- 统计 CSV：`backend/data/<设备名>/<YYYYMM>/<DD>.csv`
- 疲劳累计：`backend/data/<设备名>/damage_cumulative.json`（含 .bak）
- IoT 日志（log 模式或 MQTT 回执）：`backend/data/iot_log.jsonl`
- 旧的 `backend/data/damage_cumulative.txt` 已废弃。
- TDMS 保留：按配置 `storage.retention_months` 清理超期月份（默认 3 个月）。

### 运行步骤
1) 安装依赖  
   `pip install -r requirements.txt`

2) 启动后端  
   `python backend/app.py`  
   默认监听 `0.0.0.0:5000`，启动后自动加载配置并启动所有设备。

3) 访问前端  
   浏览器打开 `http://<主机IP>:5000`，同一局域网手机/电脑均可访问。

4) 防火墙/网络  
   需要放行 TCP 5000（见先前 netsh 规则）。前端 Socket.IO 已使用当前主机，支持局域网访问。

### 前端页面
- 监测：设备列表、通道时域图/位移、频谱切换。
- 配置：表单编辑系统参数、设备/通道、风速、存储、IoT 等，保存后立即生效。
- 疲劳：每设备显示最近疲劳结果与累计分布，支持重置累计。
- 设备状态：CPU、磁盘、data 目录浏览（可点击子目录）。

### IoT 配置
`config.json` 的 `iot` 节点：
- type: `mqtt` / `log`；mqtt 需要 host/port/topic/cert/key 等。
- 证书默认路径示例：`backend/license/...`（相对路径会被解析到项目根）。
- MQTT 发布周期同窗口统计周期（默认 10 分钟）。

### 数据存储细节
- TDMS 按月/日目录，文件名格式可配置 `storage.filename_format`。
- 每设备 CSV 按月/日拆分，首行表头包含窗口统计和疲劳字段。
- 疲劳累计按设备独立文件，线程安全写入，崩溃时可从 .bak 恢复。

### git 忽略与提交
- `.gitignore` 已忽略 `backend/data/` 与 `data/`。若之前已被跟踪，可执行 `git rm -r --cached backend/data` 后提交，以免 data 变化出现在状态中。

### 常见问题
- 看不到疲劳/配置接口：确认后端已运行，前端 Socket.IO 未指向 127.0.0.1。
- data 目录未创建：运行后自动创建；未运行前目录为空属于正常。
- 多设备混写：已按设备分目录存储，避免互相覆盖。
