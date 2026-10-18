# racewong-app — RaceBox 跨平台同步桌面应用

把 `../src/racebox.py`（扫描 → 蓝牙下载 → 解析 → 入库 → 清理设备内存）重构为一个小体积跨平台桌面应用。
先做 Windows，macOS/Linux 只依赖换 BLE 后端，协议层完全复用。

## 1. 功能范围

1. BLE 扫描/连接 RaceBox（记住上次设备地址，优先回连，否则全盘扫描）
2. 下载：按 ublox 私有协议收 `0xFF` 报文，解析 80 字节记录，按 `0x26` 切分轨迹段
3. 落库：两种 sink，二选一（设置页可切换，也可同时开启）
   - **PostgreSQL**：直连写 `imp_racebox` / `lc_racebox`，SQL 与 Python 版逐字一致
   - **MQTT**：按 des kwong RBX2 二进制帧发布到 `deskwong/racebox/data`，QoS 1，**收到 PUBACK 才算成功**
4. 清理：上传全部确认后，发擦除命令并按 `0x24` 报的进度推进到 `0x02` ACK
5. 失败兜底：sink 不可用时写本地 spool 队列（替代 Python 版的 Excel 兜底），恢复后重放

## 2. 协议常量（来自 `deskwong/doc/SPECIFICATIONS.md` §3/§4 与本仓库 Python 版）

### 2.1 BLE

| 项 | 值 |
| --- | --- |
| Service UART | `6e400001-b5a3-f393-e0a9-e50e24dcca9e` |
| RX 写 | `6e400002-b5a3-f393-e0a9-e50e24dcca9e` |
| TX 通知 | `6e400003-b5a3-f393-e0a9-e50e24dcca9e` |
| 查询状态 | `B5 62 FF 22 00 00 21 62` |
| 下载 | `B5 62 FF 23 00 00 22 65` |
| 擦除 | `B5 62 FF 24 00 00 23 68` |

下载时序：连接 → `start_notify(TX)` → 写下载命令 → 收 `0x23`(总数上界) / `0x21`、`0x01`(记录) / `0x26`(段边界) / `0x02`(完成 ACK) → 停 notify → 断开。
报文帧：`B5 62 | class | id | len(u16 LE) | payload | ck_a | ck_b`，校验 `sum([2..-2))`。

状态查询回复 `FF 22`：`recording, memory_level, security_flags, stored(u32), max(u32)`。
擦除回复 `FF 24`：payload[0] 为进度百分比；`FF 02` ACK payload 为 `FF 24` 表示完成；`FF 03` NACK。

### 2.2 80 字节记录（`0x21`/`0x01` payload 前 80 字节，小端）

以下偏移与字段来自 des kwong `racebox_test.c` 的样例数据与 `racebox_service.c` 的入库换算：

| 偏移 | 字段 | 入库换算 |
| --- | --- | --- |
| 0 | iTOW u32 | 原值 |
| 4 | year u16 | 原值 |
| 5 | month u8 | 原值 |
| 6 | day u8 | 原值 |
| 7 | hour u8 | 原值 |
| 8 | minute u8 | 原值 |
| 9 | second u8 | 原值 |
| 10 | time_accuracy u16 | 原值 |
| 12 | nanoseconds i32 | 原值 |
| 13 | reserved | 忽略 |
| 14 | fix u8 | `fix == 0` 丢弃 |
| 15 | reserved | 忽略 |
| 16 | longitude i32 (1e-7) | ÷1e7 |
| 20 | latitude i32 (1e-7) | ÷1e7 |
| 24 | wgs_altitude i32 mm | ÷1000 |
| 28 | msl_altitude i32 mm | ÷1000 |
| 32 | horizontal_accuracy u32 mm | ÷1000 |
| 36 | vertical_accuracy u32 mm | ÷1000 |
| 40 | speed i32 mm/s | `×0.6` → km/h（= Python `parsed_data[20] / 100 * 60`） |
| 44 | heading i32 (1e-5 度) | ÷1e5 |
| 48 | speed_accuracy u32 | 原值 |
| 52 | heading_accuracy u32 (1e-5) | ÷1e5 |
| 56 | pdop u16 (0.01) | ÷100 |
| 60 | reserved | 忽略 |
| 62 | reserved | 忽略 |
| 64 | gforce_x i16 (0.001) | ÷1000 |
| 66 | gforce_y i16 (0.001) | ÷1000 |
| 68 | gforce_z i16 (0.001) | ÷1000 |
| 70 | rotation_rate_x i16 (0.01) | ÷100 |
| 72 | rotation_rate_y i16 (0.01) | ÷100 |
| 74 | rotation_rate_z i16 (0.01) | ÷100 |
| 76..80 | 补零 | 忽略 |

`fix == 0` 的记录在解码时直接返回 `None`（与 Python 一致，落库前过滤）。
代码实现：`src/protocol.rs` 的 `Record::decode`，有 `record_decodes_all_fields` / `record_without_fix_is_dropped` 测试锁定字段含义。

### 2.3 PostgreSQL

沿用 `../script/datastruct.sql` 的 `imp_racebox` / `lc_racebox` 两张表，SQL 与 Python 版一致：

- 幂等：`select count(1) from imp_racebox where file_name = %s`
- 头表：`insert into imp_racebox(imp_stamp, file_name, duration)`
- 明细：`insert into lc_racebox(...) values %s`（批量 `execute_values`，page_size 1000）
- 文件名：`首记录年月日时分秒_末记录年月日时分秒`（如 `20241006103045_20241006104512`）

### 2.4 MQTT RBX2 帧（des kwong §4）

头 136 字节 + N×80 字节记录，消息长 `136 + N×80`，批次不可跨轨迹段。
协议上限 N ≤ 720，但 broker 普遍限制单包 10KB（实测 db.wongcw.cn:1883 为 10240 字节），
因此实现按 `RBX2_MAX_BATCH_RECORDS = 120` 分批（帧约 9.7KB），超限会被 broker 直接断连。

| 偏移 | 长度 | 字段 |
| ---: | ---: | --- |
| 0 | 4 | magic `RBX2` |
| 4 | 1 | version = 2 |
| 5 | 1 | flags：bit0 同步末批，bit1 轨迹段末批 |
| 6 | 2 | header_size = 136 |
| 8 | 2 | record_size = 80 |
| 10 | 2 | record_count |
| 12 | 4 | sync_offset（本批在本次同步的首序号） |
| 16 | 4 | sync_total（末批为实际总数，否则为 `0x23` 上界） |
| 20 | 4 | sync_date 本地日期 `YYYYMMDD` |
| 24 | 16 | sync_id 每次同步随机 UUID 原始字节 |
| 40 | 48 | device 名称/序列号，NUL 结尾 |
| 88 | 4 | session_index |
| 92 | 4 | session_offset |
| 96 | 4 | session_total（段尾前 0） |
| 100 | 8 | session_start_utc `YYYYMMDDhhmmss` 数值 |
| 108 | 8 | session_end_utc（段尾前 0） |
| 116 | 4 | session_start_itow |
| 120 | 4 | session_start_nano（有符号） |
| 124 | 4 | reserved 0 |
| 128 | 4 | payload_crc32 = CRC32-IEEE(records) |
| 132 | 4 | header_crc32 = CRC32-IEEE(frame[0..132)) |
| 136 | N×80 | 原始记录 |

发布用 QoS 1，轮询/等待 PUBACK 成功后才把该批次标记完成，才能进入擦除。默认主题 `deskwong/racebox/data`，在应用设置里可改。

## 3. 模块划分（语言无关）

```
core/   protocol.rs|py|cs   帧解析、校验、记录解码、RBX2 组帧、CRC32
        ble/               扫描、连接、notify、命令下发（平台后端隔离）
        sink/              PgSink | MqttSink | SpoolSink（统一 trait/interface）
        queue/             本地 spool 目录 + 断点续传索引
app/    ui/                设置页 + 主页（设备、进度、日志）
        config             配置文件读写（%APPDATA% 或用户目录，不依赖仓库 conf/）
```

## 4. 配置项

- BLE：设备名过滤 `RaceBox`、上次连接地址缓存
- sink 模式：`database` / `mqtt` / `both`，失败回落 spool 开关
- PostgreSQL：host / port / database / user / password
- MQTT：broker / port / user / password / topic / client id / QoS
- 行为：`auto_erase`（上传全部成功后是否自动清理设备内存）、
  `spool_on_failure`（失败转本地缓存）、`scan_timeout_secs`（蓝牙扫描超时，10–120 秒）
- MQTT：`keepalive_secs`（保活间隔，10–300 秒）
- 外观：`theme` = `dark` / `light`（设置页“外观”卡片切换，切换即保存；
  旧配置文件没有该字段时按 `serde(default)` 视为 `dark`，不会丢配置）

界面为 egui 自绘的卡片式布局，暗色 / 亮色两套主题共用同一套配色变量
（`src/style.rs` 的 `Palette`），改颜色只动这一处。

### 界面结构

- 顶部栏：主页右侧是“设置”小角标；设置页右侧是关闭 X（有未保存更改会弹窗确认）
- 底部状态栏：左下角是运行状态（空闲 / 运行中 · 当前阶段），右下角是日志抽屉开关、
  待重传数和版本号
- 日志是底部可折叠抽屉（可拖高），不再是独立标签页
- 同步页：左侧是大字号阶段 / 设备 / 进度条，右侧是主操作（开始同步 / 查询状态 /
  清除内存）；任务运行中只保留红色 **取消同步**（点击后变“取消中...”），worker 在
  扫描、MQTT 连接、下载、上传、擦除各检查点中断，设备数据不会被误删
- 查询状态 / 清除内存不会连接 MQTT，只有完整同步才建立上传通道，启动更快
- 设置页：上传方式用分段选择器；行为开关是 iOS 风格开关；设备扫描超时、
  MQTT 保活间隔用滑块调节（均落盘到 config.json）

## 5. 实现顺序

1. 配置模型 + 设置页
2. BLE 扫描连接 + 状态查询
3. 下载解析（含 0x26 分段、进度展示）
4. PostgreSQL sink（对齐现有表结构与幂等）
5. MQTT sink（RBX2 组帧 + PUBACK 确认）
6. spool 队列与重放
7. 擦除流程与整体状态机
8. 打包（Windows 优先）

## 6. 图标

| 文件 | 用途 |
| --- | --- |
| `assets/icon.ico` | exe 资源图标（16/24/32/48/64/256，32 位带透明），由 `winres` 在 `build.rs` 中嵌入 |
| `assets/icon256.png` | 运行时窗口图标，`main.rs` 里 `include_bytes!` + `eframe::icon_data::from_png_bytes` |

两个图标都由 `app/tools/make_icon.py` 生成（Pillow 绘制：深蓝圆角方底 + 白色赛道走线 + 起点/终点标记）：

```powershell
.venv\Scripts\python.exe app\tools\make_icon.py
```

`build.rs` 行为：

- Windows 原生构建 → 调 `rc.exe`，版本信息用中文（`#pragma code_page(65001)`）
- WSL 交叉构建（x86_64-pc-windows-msvc）→ 自动把 LLVM 的 `llvm-rc` 软链成 `rc.exe` 给 `winres` 用；此时版本信息退化成英文（llvm-rc 不认 65001 代码页的中文字符），图标本身不受影响
- 两个平台都不再因为缺 rc 编译器而失败，最差只是少一个 exe 图标

## 7. 构建与运行

技术栈：Rust + egui/eframe（自绘 UI，无 WebView、无 Qt），Windows 单文件 exe 约 5.3 MB。
BLE 用 `btleplug`（Windows 走 WinRT），MQTT 用 `rumqttc`，PostgreSQL 用 `tokio-postgres`（NoTls）。

### 产物

- `app\target\x86_64-pc-windows-msvc\release\racewong-app.exe`：约 5.7 MB，单文件、PE subsystem=GUI（**不会弹控制台窗口**），exe 资源里已嵌入图标和版本信息
- release 版把 `windows_subsystem` 设为 `windows`，debug 版仍带控制台，方便 `cargo run` 看输出

### 连通性自检（不开窗口）

```powershell
racewong-app.exe --check
```

按当前配置依次测 PostgreSQL（真发一次 `select 1`）和 MQTT（等 CONNACK），
失败会打印服务端原文，例如：

```
PostgreSQL fn.wongcw.cn:8432/eadm ... 失败: password authentication failed for user "user_eadm" [28P01]
```

设置页的“测试连接”也是走这两条路径，结果会同时显示在同步页的“提示”行、日志页，
以及设置页对应卡片按钮旁边（进行中显示转圈，结束显示成功 / 失败 / 超时）。
PostgreSQL 测试整体有 15 秒超时，主机不可达时不会一直转圈；MQTT 测试等 CONNACK，
内部 10 秒超时。测试成功后 MQTT 的连接会保留到下一次测试或退出，避免打出误导性的断开错误。

MQTT 连接的健壮性（`src/sink/mqtt.rs`）：

- 每次连接的 client_id 都会加随机后缀，避免和设置页的探测连接
  （或其它实例）client_id 相同被 broker 踢掉
- eventloop 任务**除主动停止外绝不退出**：rumqttc 的 eventloop 只要继续 poll
  就会在断线后自动重连；一旦任务退出，request channel 断开，之后所有 publish
  都会报 `Failed to send mqtt requests to eventloop`（这就是之前同步时
  所有批次上传失败的根因）
- 连接断开时 `wait_ack` 会快速失败并给出“MQTT 连接已断开”而不是傻等 60 秒；
  重连成功后的批次照常等 PUBACK

### 本地（Windows）构建
无 MSVC Build Tools / C 盘紧张时，用 WSL + cargo-xwin 交叉编译到 `x86_64-pc-windows-msvc`，
只下载 MSVC CRT/SDK 到 WSL（可指定到 D 盘），不碰 C 盘、不用 MinGW：

```bash
# WSL Ubuntu 24.04 内
apt install clang lld
rustup target add x86_64-pc-windows-msvc
cargo install cargo-xwin
# 产物在 app/target/x86_64-pc-windows-msvc/release/racewong-app.exe
XWIN_CACHE_DIR=/mnt/d/xwin-cache cargo xwin build --release --target x86_64-pc-windows-msvc
```

`rumqttc` 当前 `default-features = false`（不开 rustls），因此 MQTT 仅支持明文 1883
——这是为了绕开 ring 交叉编译需要 MSVC `llvm-lib` 的限制；后续如需 TLS，在装有
MSVC Build Tools 的机器上把默认特性打开即可。

### 原生调试（WSL 内，Linux 目标）

```bash
cargo check   # 类型检查
cargo test    # src/protocol.rs 的协议单测（帧解析、记录解码、RBX2 组帧、CRC32）
cargo build --release
```

### 配置位置

`%APPDATA%\wongcw\racewong\config\config.json`（首次运行自动生成默认值；`conf/db.cnf`
不参与打包，密码在设置页填写）。本地 spool 在 `...\racewong\config\spool\`，日志在
`...\config\racewong.log`。

## 8. 发布脚本与 CI（Windows / macOS / Linux）

发布统一走 `script/release.ps1`（PowerShell 5.1 与 7+ 均可）或 `script/release.sh`（bash，Linux / macOS / WSL / Git Bash）：

```powershell
# Windows：自动 patch +1 → 改 app/Cargo.toml → 提交 → 打标签 → 推送 → 建 Release → 本机打包
.\script\release.ps1
.\script\release.ps1 -Version v0.2.0      # 指定版本
.\script\release.ps1 -NoBuild             # 只打标签建 Release，包全交给 CI
.\script\release.ps1 -BuildOnly           # 只构建当前平台的安装包
```

```bash
./script/release.sh                       # 同上的 bash 版
./script/release.sh --version v0.2.0
./script/release.sh --no-build
./script/release.sh --build-only --out-dir dist
```

说明：

- 自动版本：取最新 `v*.*.*` 标签 patch +1，没有标签就用 `app/Cargo.toml` 的版本
- Release 正文来自 **git 提交记录**（`上一个标签..HEAD` 的 `git log`）
- 建 Release 用 `gh`；没装 `gh` 时可用 `GITHUB_TOKEN` 走 REST
- 本机只构建当前平台那一个包，产物形如：
  - `racewong-<ver>-windows-x86_64.zip`
  - `racewong-<ver>-macos-aarch64.tar.gz`（在 macOS 上还会出 `.dmg`，内含 `RaceBox 同步.app`）
  - `racewong-<ver>-linux-x86_64.tar.gz`（有 `dpkg-deb` 时还会出 `.deb`）

`.github/workflows/release.yml` 在三个 runner 上跑同一个脚本的 `-BuildOnly` / `--build-only`：

| Job | Runner | 目标 | 产物 |
| --- | --- | --- | --- |
| windows-x86_64 | windows-latest | `x86_64-pc-windows-msvc` | zip |
| macos-aarch64 | macos-latest | `aarch64-apple-darwin` | tar.gz + dmg |
| linux-x86_64 | ubuntu-latest | `x86_64-unknown-linux-gnu` | tar.gz + deb |

推 `v*` tag 时自动把三个包挂到同一个 Release；也可在 Actions 页面手动 `workflow_dispatch`。

## 9. 与 Python 版本的差异

| 项 | Python (`../src/racebox.py`) | 本应用 |
| --- | --- | --- |
| 上传目标 | 仅 PostgreSQL | PostgreSQL / MQTT / 两者，设置页可切 |
| 上传确认 | 无（插入即视为成功） | MQTT 必须等 PUBACK，失败转本地 spool |
| 设备清理 | `clean_memory` 恒为 False | `auto_erase` 默认开，且**上传全部成功才擦除** |
| 收尾数据段 | 最后一个 0x26 之后的数据丢弃 | 下载 ACK 后会把尾段也落库 |
| 无定位记录 | `fix != 0` 过滤 | 同样过滤，并在日志里报告跳过条数 |
| 兜底 | 导出 Excel 待重传 | spool 目录 + 启动时自动重放 |
