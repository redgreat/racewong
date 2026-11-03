# racewong — RaceBox 蓝牙同步工具

通过蓝牙（BLE）连接 RaceBox（Mini / Mini S），把设备 **Standalone 模式**下记录的轨迹数据下载、解析，
写入 PostgreSQL 或通过 MQTT 上传，可选同步到 RaceBox 官方云；全部上传成功后再清理设备内存。

仓库里有两套实现：

| 目录 | 实现 | 状态 |
| --- | --- | --- |
| `app/` | Rust + egui 跨平台桌面应用（Windows / macOS / Linux） | **主力**，持续维护、按版本发布 |
| `src/racebox.py` | Python 命令行脚本（bleak + psycopg2） | 早期版本，仅保留作参考 / 兜底 |

## 功能

- **BLE 扫描与连接**：记住上次连接的设备，优先回连，找不到再全盘扫描
- **下载与解析**：按 RaceBox 私有 ublox 协议接收记录，解码 80 字节数据包，按会话边界切分轨迹段，丢弃无定位（`fix == 0`）记录
- **上传通道**（二选一）
  - PostgreSQL 直连：写 `imp_racebox`（头表）/ `lc_racebox`（明细），按文件名幂等
  - MQTT：RBX2 二进制帧，QoS 1，**收到 PUBACK 才算成功**
- **RaceBox 云**（附加通道）：登录官方账号，把同步数据或本地 GPX / Garmin FIT 文件转成会话上传到 racebox.pro
- **同步前自检**：动设备之前先验证上传通道（PG 试写回滚 / MQTT 等 CONNACK），不通就不下载
- **失败兜底**：上传失败自动重试，仍失败则写本地 spool 缓存，启动后自动重放
- **安全擦除**：只有全部上传成功才清理设备内存（`auto_erase` 可关）
- **桌面体验**：暗色 / 亮色主题、10 种界面语言、托盘常驻、单实例、在线静默升级

## 下载安装

到 [Releases](https://github.com/redgreat/racewong/releases) 下载对应平台的安装包：

| 平台 | 文件 |
| --- | --- |
| Windows 10+ x86_64 | `racewong-<版本>-windows-x86_64.msi`（按用户安装，无需管理员，覆盖升级） |
| macOS Apple Silicon | `racewong-<版本>-macos-aarch64.dmg`（ad-hoc 签名，首次运行需在系统设置中放行） |
| Linux x86_64 | `racewong-<版本>-linux-x86_64.deb` |

安装后在设置页填写 PostgreSQL 或 MQTT 连接信息即可使用。应用内「检查并更新」会自动下载并安装新版本。

配置文件位置（首次运行自动生成）：

- 配置：`%APPDATA%\wongcw\racewong\config\config.json`
- 本地缓存：`...\racewong\config\spool\`
- 日志：`...\racewong\config\racewong.log`

## 从源码构建（桌面应用）

需要 Rust stable 工具链。

```powershell
# Windows
.\script\build.ps1            # release 构建
.\script\build.ps1 -Debug     # debug 构建（带控制台）
.\script\build.ps1 -Test -Run # 先跑单测，编完启动
```

```bash
# Linux / macOS / WSL（WSL 下默认交叉编译出 Windows exe）
./script/build.sh
./script/build.sh --native --test
```

或者直接用 cargo：

```bash
cd app
cargo test --bins
cargo build --release
```

Windows 上没有完整 MSVC Build Tools 时，可先 `. .\script\rust-env.ps1` 借用 VS 自带工具链，
或在 WSL 中用 `cargo-xwin` 交叉编译，详见 [`app/README.md`](app/README.md#7-构建与运行)。

不开窗口做连通性自检：

```powershell
racewong-app.exe --check
```

## Python 脚本（旧版）

```powershell
python -m venv .venv
.venv\Scripts\pip install -r requirements.txt
```

在 `conf/db.cnf` 中配置数据库（该目录已被 `.gitignore` 忽略，不会提交）：

```ini
[racebox]
host = ...
port = 5432
database = ...
user = ...
password = ...
auto_erase = false

[amap]
amap_key = ...
```

运行：

```powershell
.\run.ps1           # 下载（默认）
.\run.ps1 status    # 查询设备状态
.\run.ps1 erase     # 清除设备内存
.\run.ps1 full      # 下载 + 清除
```

Linux / macOS 用 `./run.sh`，参数相同。

## 数据库

表结构见 [`script/datastruct.sql`](script/datastruct.sql)（`imp_racebox` 导入批次头表、`lc_racebox` 明细表），
常用查询见 `script/sqlscript.sql`，清理数据见 `script/deletedata.sql`。

## 目录结构

```
app/                  Rust 桌面应用
  src/protocol.rs     BLE 帧解析、80 字节记录解码、RBX2 组帧、CRC32
  src/ble.rs          扫描、连接、下载、擦除
  src/worker.rs       同步流程状态机
  src/sink/           上传通道：pg / mqtt / racebox(云) / spool(本地缓存)
  src/racebox/        RaceBox 云：API、会话文件编码、GPX/FIT 导入
  src/ui.rs, style.rs 界面与主题
  src/i18n*           多语言
  installer/          WiX 安装包定义、许可协议、品牌图
  tools/              图标 / 安装图生成脚本（Python）
src/racebox.py        Python 旧版脚本
script/               构建、发布、SQL 脚本
doc/                  协议文档与设计说明
.github/workflows/    发布流水线（推 v* 标签自动构建三平台安装包）
```

## 文档

- [`app/README.md`](app/README.md)：桌面应用详细设计（协议常量、RBX2 帧格式、配置项、安装包、构建与发布）
- [`doc/racebox-parsing.md`](doc/racebox-parsing.md)：RaceBox 数据解析知识库
- [`doc/racebox-cloud-api.md`](doc/racebox-cloud-api.md)：RaceBox 云 API 与会话文件格式
- [`doc/cloud-upload-design.md`](doc/cloud-upload-design.md)：云上传功能设计
- `doc/RaceBox BLE Protocol Description rev 8.pdf`：官方 BLE 协议文档

## 发布

```powershell
.\script\release.ps1                  # 自动 patch +1、打标签、推送、建 Release
.\script\release.ps1 -Version v0.2.0  # 指定版本
```

标签推送后由 GitHub Actions 构建 Windows / macOS / Linux 三个安装包并挂到 Release。

## 许可

[MIT](LICENSE)
