# AGENTS.md

给在本仓库工作的智能体 / 开发者的说明。详细设计看 `app/README.md`，本文只记必须知道的约定。

## 项目概览

RaceBox BLE 同步工具：蓝牙下载设备 Standalone 记录 → 解析 → 上传（PostgreSQL / MQTT / RaceBox 云）→ 上传全部成功后擦除设备内存。

- `app/`：**主力实现**，Rust 2021 + egui/eframe 0.31 + tokio。crate 名 `racewong-app`，另有 `src/bin/uninstall.rs` 卸载程序
- `src/racebox.py`：Python 旧版脚本，基本不再改动；改协议时以 Rust 版为准
- `script/`：构建（`build.ps1` / `build.sh`）、发布（`release.ps1` / `release.sh`）、SQL
- `doc/`：协议与解析文档（`racebox-parsing.md`、`racebox-cloud-api.md`、`cloud-upload-design.md`）

## 常用命令

所有 cargo 命令在 `app/` 目录下执行。

```powershell
. .\script\rust-env.ps1          # Windows 本机：借用 VS 自带 MSVC 工具链（缺 Build Tools 时）
cd app
cargo check --all-targets        # 类型检查
cargo test --bins                # 单元测试（protocol / i18n / racebox 等）
cargo build --release
```

或用脚本：`.\script\build.ps1 [-Debug] [-Test] [-Run] [-Check]`，`./script/build.sh [--debug] [--test] [--native]`。

连通性自检（不开窗口）：`racewong-app.exe --check`（release 版是 GUI 子系统，看不到输出时用 debug 构建）。

## 验证要求

改 Rust 代码后至少跑：

1. `cargo check --all-targets`，保持 0 error 0 warning
2. `cargo test --bins`，全部通过

改协议解析（`protocol.rs`、`racebox/session.rs`、`racebox/import.rs`）必须补/改对应单测锁定字段含义。

## 代码约定

- **注释、文档、提交信息用中文**，注释解释"为什么"，保留已有注释
- 提交信息遵循 Conventional Commits：`feat(scope): ...` / `fix(racebox): ...` / `docs: ...` / `chore(release): ...`
- **界面文案必须走 i18n**：静态文案用 `t!("中文", "English")`，带参数用 `tr!("已上传 {} 条", "{} records uploaded", n)`；
  其余 8 种语言加到 `src/i18n/table_ui.rs`（静态）或 `table_msg.rs`（模板）。查表 key 是 `(中文, English)` 对，不要只用中文
- 颜色只改 `src/style.rs` 的 `Palette`，不要在界面代码里写死颜色
- 新的上传目标实现 `sink::DataSink` trait，复用分段、进度回调、失败转 spool、取消逻辑
- 配置字段新增时加 `#[serde(default)]`，旧 `config.json` 不能因缺字段解析失败
- 平台相关依赖放在 `Cargo.toml` 的 `[target.'cfg(...)'.dependencies]` 下（托盘仅 Windows / macOS）
- `rumqttc` 关闭了默认特性（无 TLS），不要随意打开，会破坏 WSL 交叉编译

## 关键行为不能破坏

- 设备擦除只能在**全部上传成功**之后进行
- 完整同步必须先做上传通道自检，失败直接中止，不碰设备
- MQTT 必须等 PUBACK 才算成功；RBX2 单批 ≤ 120 条（broker 单包 10KB 限制）
- PostgreSQL 写入 SQL 与表结构需与 `script/datastruct.sql` 保持一致，按 `file_name` 幂等

## 发布

- 版本号在 `app/Cargo.toml`，`Cargo.lock` 中本 crate 版本需同步（CI 使用 `--locked`）
- 用 `script/release.ps1` / `release.sh` 发布，不要手工改版本打标签；推 `v*` 标签即触发 `.github/workflows/release.yml` 构建三平台安装包
- PowerShell 脚本读写文件统一用 .NET UTF-8 API（5.1 的 `Get-Content` 会按 GBK 读坏中文）

## 安全

- `conf/` 已被 gitignore，`conf/db.cnf` 含数据库密码，**不要提交、不要在输出里打印内容**
- 桌面应用的密码 / 云账号凭据保存在用户配置目录，不进仓库
- 不要擅自推送、打标签或创建 Release，除非用户明确要求
