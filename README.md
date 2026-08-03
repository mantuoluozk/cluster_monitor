# Cluster Monitor

面向海光 DCU 超节点推理测试的轻量监控脚本。脚本运行在跳板机，通过 SSH 并行采集多个计算节点的数据，支持 **PD 分离**和 **IFB** 两种部署方式。

它会记录完整测试周期，并在结束后生成 CSV、汇总表和单页 HTML 仪表盘。

## 能采集什么

- CPU：利用率、频率、温度、功耗
- 主机内存：占用量、总量、利用率
- 每张 DCU：利用率、显存、功耗、温度
- 计算节点：整机功耗
- InfiniBand：吞吐、包速率、链路状态和错误计数
- 服务端口：可访问/不可访问时间标记

每个计算节点可以单独查看，P、D、IFB 节点也会分类保存。

## 效果预览

全节点汇总与跨节点关键指标对比：

![全节点监控汇总](docs/images/dashboard-overview.png)

按 P、D、IFB 和节点切换的完整时间曲线：

![单节点完整时间曲线](docs/images/dashboard-node-detail.png)

## 环境要求

跳板机需要：

- Python 3
- 可以免密 SSH 到所有计算节点

计算节点不需要 Python，只需要：

- Bash、`base64`
- `hy-smi`
- `ipmitool`
- 可读取的 `/proc` 和 `/sys`

## 快速开始

### 1. 修改配置

编辑 `monitor_config.jsonc`。PD 分离示例：

```jsonc
"deployment": {
  "mode": "PD",
  "groups": {
    "P": ["p1c0", "p1c1"],
    "D": ["p1c2", "p1c3"],
    "IFB": []
  }
}
```

IFB 示例：

```jsonc
"deployment": {
  "mode": "IFB",
  "groups": {
    "P": [],
    "D": [],
    "IFB": ["p1c0", "p1c1", "p1c2", "p1c3"]
  }
}
```

`mode: "PD"` 只使用 P、D 数组；`mode: "IFB"` 只使用 IFB 数组。

### 2. 设置服务端口标记（可选）

```jsonc
"route_probe": {
  "enabled": true,
  "host": "p1c0",
  "port": 8000,
  "interval_s": 1,
  "connect_timeout_s": 0.5,
  "successes_to_reachable": 1,
  "failures_to_unreachable": 3
}
```

端口状态只用于在 CSV 和曲线中标记服务启动、停止时间，不会控制数据采集，也不会改变统计区间。

### 3. 启动监控

```bash
python3 cluster_monitor.py --model DeepSeek-V3
```

脚本启动后立即记录。按 `Ctrl+C` 结束并生成汇总和可视化。

也可以固定监控时长：

```bash
python3 cluster_monitor.py --model DeepSeek-V3 --duration 600
```

`--model` 会覆盖 JSONC 中的 `model_name`，并用于结果目录名称。`--duration 0` 表示一直运行到 `Ctrl+C`。

## 结果目录

```text
results/DeepSeek-V3_run_YYYYMMDD_HHMMSS/
├── dashboard.html              # 单页可视化入口
├── summary.csv                 # 全部节点和DCU汇总
├── route_events.csv            # 端口状态事件
├── effective_config.json       # 本次实际配置
├── run_metadata.json
├── P/
│   └── p1c0/
│       ├── host.csv            # CPU、内存、整机数据
│       ├── dcu_cards.csv       # 四张DCU逐卡数据
│       ├── summary.csv
│       └── visualization.svg
└── D/...
```

IFB 模式会生成 `IFB/` 目录。

仪表盘分为三部分：

1. 全节点平均值/最大值汇总表
2. 跨节点关键指标对比
3. 可切换节点的完整时间曲线

## 查看结果

### 方式一：下载结果文件夹

用 SCP/SFTP 下载对应的**整个结果文件夹**，然后在本地打开 `dashboard.html`。

不能只下载 HTML，因为页面还需要同目录中的 SVG 文件。

### 方式二：通过 SSH 隧道直接查看

在本地终端执行下面的命令，并替换其中的大括号参数：

```powershell
ssh -p {SSH_PORT} -L 18080:127.0.0.1:18080 {USER}@{JUMP_HOST} "cd {PROJECT_PATH}/results && python3 -m http.server 18080 --bind 127.0.0.1"
```

例如项目路径是 `/root/dcu_monitor`，则 `{PROJECT_PATH}` 替换为该路径。下面是一条完整示例命令：假设跳板机 SSH 地址为 `192.0.2.10`、用户为 `root`、SSH 端口为 `2222`。

```powershell
ssh -p 2222 -L 18080:127.0.0.1:18080 root@192.0.2.10 "cd /root/dcu_monitor/results && python3 -m http.server 18080 --bind 127.0.0.1"
```

`192.0.2.10` 和 `2222` 是文档示例，请替换成真实跳板机 IP 和 SSH 端口。保持终端运行，然后浏览器访问：

```text
http://127.0.0.1:18080/
```

按 `Ctrl+C` 即可停止结果服务和 SSH 隧道。服务只绑定服务器的 `127.0.0.1`，不会直接暴露结果目录。

## 统计口径

- 平均值和最大值基于本次运行期间的全部有效样本。
- 每个节点从第一条成功样本统计到结束前最后一条样本。
- 缺失值不会按 0 参与计算。
- 路由端口事件只做标记，不裁剪统计区间。
- DCU 利用率只采用 `hy-smi --showhcuutil` 的最近1秒 HCU active ratio。
- 节点 DCU 总功耗先在每个时刻汇总全部卡，再计算全程平均值和最大值。

## 常用配置

| 配置项 | 说明 |
|---|---|
| `model_name` | 默认模型名称，可被 `--model` 覆盖 |
| `monitor_duration_s` | `0` 表示运行到 Ctrl+C |
| `sample_interval_s` | CPU、内存等基础采样周期 |
| `dcu_utilization_interval_s` | DCU 利用率刷新周期 |
| `dcu_memory_interval_s` | 显存刷新周期 |
| `dcu_temperature_interval_s` | DCU 温度刷新周期 |
| `cpu_power_interval_s` | CPU 功耗刷新周期 |
| `node_power_interval_s` | 整机功耗刷新周期 |
| `ssh_options` | 跳板机连接计算节点的 SSH 参数 |

较慢指标在独立周期刷新，中间样本复用最近一次成功值。`hy-smi --showhcuutil` 在部分环境中耗时约4秒，因此使用独立 SSH 通道，不阻塞基础采样。

## 常见问题

### 计算节点没有 Python

不影响使用。Python 只在跳板机运行，计算节点执行 Bash、`hy-smi`、`ipmitool` 和 sysfs 读取命令。

### 某个字段显示为空

表示采集命令没有返回该指标，或当前输出格式未被解析器识别。空值不代表 0。

### Ctrl+C 后能生成结果吗

可以。`Ctrl+C` 是正常结束方式，脚本会写出汇总、SVG 和 `dashboard.html`。

### 监控会明显影响推理吗

脚本使用持久 SSH，传输的是少量文本。显存、温度、BMC 功耗和较慢的 DCU 利用率命令使用较长刷新周期，通常不会形成大量网络流量。需要进一步降低干扰时，可以增大 JSONC 中的采样周期。

<details>
<summary><strong>采集命令与计算口径（排查时查看）</strong></summary>

### CPU与内存

```bash
cat /proc/stat
cat /proc/cpuinfo
cat /proc/meminfo
cat /proc/loadavg
```

- CPU利用率通过相邻两次 `/proc/stat` 累计计数差计算。
- `CPU利用率 = 100 × (1 - Δ(idle+iowait) / Δ全部CPU时间)`。
- `内存已用 = MemTotal - MemAvailable`。
- `内存利用率 = 内存已用 / MemTotal × 100%`。

CPU温度读取：

```bash
grep -H . /sys/class/hwmon/hwmon*/name /sys/class/hwmon/hwmon*/temp*_label /sys/class/hwmon/hwmon*/temp*_input 2>/dev/null
```

CPU功耗：

```bash
sudo ipmitool sensor get CPU_POWER | awk '/Sensor Reading/ {print $4; exit}'
```

整机功耗：

```bash
ipmitool dcmi power reading
```

### DCU

```bash
hy-smi --showuse --showmemuse --showpower --json
hy-smi --showhcuutil
hy-smi --showmeminfo vram --json
hy-smi --showtemp --json
```

- 利用率统一来自 `hy-smi --showhcuutil`。
- 单卡功耗来自 `Average Graphics Package Power (W)`。
- 显存利用率为 `已用显存 / 总显存 × 100%`。
- 温度保留 edge、junction、memory、core，汇总图默认使用 junction。

### InfiniBand

脚本读取：

```text
/sys/class/infiniband/*/ports/*/state
/sys/class/infiniband/*/ports/*/phys_state
/sys/class/infiniband/*/ports/*/rate
/sys/class/infiniband/*/ports/*/counters/*
```

包括发送/接收数据量、包数、接收错误、发送丢弃、符号错误、链路中断等计数器。吞吐通过相邻样本差值除以实际时间间隔计算。

### 路由端口

跳板机使用 Python `socket.create_connection()` 进行 TCP 建连探测。建连成功仅表示端口可以访问，不代表一次推理已经完成。

</details>
