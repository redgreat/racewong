# RaceBox 云上传功能：设计与交接文档

> **给接手的人（或智能体）**：本文件说明这个功能要做什么、架构怎么搭、
> 代码写到了哪一步、还有哪些坑。看完它和 [`racebox-cloud-api.md`](./racebox-cloud-api.md)
> 就能接着干，不用再去翻反编译过程。

## 0. 一句话目标

把设备同步（BLE）和本地轨迹文件（GPX / Garmin FIT）都转成 RaceBox 官网认的
二进制会话，上传到 `racebox.pro`，登录自己的账号就能在官网上看到。

## 1. 现状（重要，先看这个）

**已编译通过、31 个单测全绿、并在本机实跑验证过云页面布局与配置落盘。**

上一轮是「盲写」——本机没有 Rust 工具链。这一轮先把工具链补齐了：
`rustup` 装的 `stable-x86_64-pc-windows-msvc`，链接器借用 VS Community 自带的
`ScopeCppSDK\vc15`（cl/link/rc 14.12）。设置脚本见 [`script/rust-env.ps1`](../script/rust-env.ps1)：

```powershell
. .\script\rust-env.ps1        # 一次性把 PATH / LIB / INCLUDE 配好
cd app
cargo check --all-targets      # 0 error 0 warning
cargo test                     # 31 passed
```

注意 Git for Windows 的 `/usr/bin/link.exe` 会顶掉 MSVC 的 `link.exe`
（症状是 `link: extra operand ...`），所以工具链目录必须排在 PATH 前面。
CI（GitHub Actions）的 runner 自带完整 MSVC，不需要这些。

盲写期实际踩到的 5 个编译错误（都已修，改法留在注释里）：

1. `cloud_ui.rs` 4 处 `Button::ui()` —— 少了 `use eframe::egui::Widget;`
2. `session.rs` 头校验和那行 `write_u32(&mut h, 312, crc32(&h[..312]))` ——
   同一句里既有可变借用又有只读借用，E0502；先算完再写
3~5. `racebox/mod.rs` 几个 `pub use` 没被用到（warning）、`account_card` 多了个 `rt` 参数

剩下的唯一警告是 `未能嵌入 exe 图标`（本机 winres 找不到 rc.exe），只影响 exe 图标，
CI 上正常。

## 2. 功能拆分（与需求对应）

| 需求 | 状态 |
| --- | --- |
| 同步时把定位数据也传到 RaceBox 云 | 已实现（附加 sink，见 §4.3） |
| 本地 GPX / FIT 文件选择并上传 | 已实现（内置目录浏览 + 拖放） |
| 桌面端加账号登录 | 已实现（云页面，见 §4.4） |
| 官网数据查看 | 只做「云端会话列表 + 官网链接」，完整图表在官网看 |
| 视频相关 | **明确不做** |
| 赛道拆圈（lap timing） | **明确不做**（用户没赛车，留作以后，见 §6） |

## 3. 文件清单（这次新增/改动的）

新增：

```
app/src/racebox/mod.rs       模块入口：Record→采样点、生成会话字节
app/src/racebox/session.rs   RB V2 会话文件编码器（含单测）
app/src/racebox/import.rs    GPX / FIT 解析 → 采样点（含单测）
app/src/racebox/api.rs       云端 HTTP：登录 / 上传 / 列表（含单测）
app/src/racebox/store.rs     账号凭据持久化（cloud.json）
app/src/sink/racebox.rs      把云上传做成一条 DataSink
app/src/cloud_ui.rs          「RaceBox 云」整页 UI
docs/racebox-cloud-api.md    逆向协议（本会话格式的权威说明）
docs/cloud-upload-design.md  本文件
```

改动：

```
app/src/config.rs   新增 CloudConfig（cloud 字段，默认关闭）+ save_cloud（只合并写 cloud 一节）
app/src/worker.rs   build_sinks 里把云 sink 加进去
app/src/sink/mod.rs 注册 racebox 模块、SinkError::Cloud 变体
app/src/ui.rs       新增 Tab::Cloud、App.cloud 字段、顶栏云图标、页面分发
app/src/style.rs    新增 cloud_button 图标
app/src/main.rs     注册 mod racebox / mod cloud_ui
script/rust-env.ps1 本机 Rust 编译环境设置（借用 VS 自带的 MSVC 工具链）
```

## 4. 设计决策（为什么这么搭）

### 4.1 云上传是「附加通道」，不是 sink_mode 的第三选项

`sink_mode`（PostgreSQL / MQTT 二选一）是「把数据送到自己的库」。云上传是
「顺带往官网传一份」，性质不同，所以做成独立开关 `Config.cloud.enabled`，
和那两个并存。失败只记日志，**不阻断同步**（官网那份是锦上添花）。

### 4.2 云上传直接复用 DataSink 接口

BLE 协议里 `MSG_SESSION_BOUNDARY` 就是设备自己的会话边界，`sink::Session` 正好
对应一个设备会话。所以把 `RaceBoxSink` 做成 `DataSink`，就能白捡一整套现成逻辑：
按会话分段、段内进度回调、失败转本地缓存、取消时提前返回。代码在 `app/src/sink/racebox.rs`。

### 4.3 凭据不进 config.json

密码和 auth token 单独存 `cloud.json`（同 `config.json` 目录），Unix 上顺手 0600。
`config.json` 只放 `cloud` 开关、设备序列号、时区、上次导入目录——这些不敏感。

### 4.4 页面改动「立即生效」

设置页有「保存 / 放弃」心智，云页面没有：开关一改就写回 config.json。
原因是这里没有“改一半退出”的场景。

### 4.5 不引第三方解析库

GPX 只是带属性的 `trkpt`，FIT 是自描述二进制。手写解析器比引依赖好控制——
尤其 FIT，各库 API 版本差异大，而我们只要 record 消息里的几个字段。
**这也保证了：本次改动没有新增任何 crate 依赖，不会动 CI。**

### 4.6 不用原生文件对话框

`rfd` 之类的库在 Linux 上要处理 GTK / portal 的问题，而 Linux 正是 CI 里最脆的那个
（之前就栽过一次）。所以用内置目录浏览（输入路径 + 扫描 + 勾选）+ 拖放，
零依赖、三平台行为一致、还天然支持批量导入。

## 5. 已知风险点（最可能要修的地方）

前 5 条是盲写期列的，括号内是核对结果：

1. **`cloud_ui.rs` 里的 egui API 名字**（✅ 全部命中，除 `Widget` trait 未引入）。
   egui 0.31 里 `id_salt`、`ScrollArea::show`、`add_enabled`、`Grid`、`toggle_row`、
   `outline_button` 都对得上。

2. **`session.rs` 的浮点→整型**（✅ 无问题，单测覆盖头部自洽与记录流对齐）。

3. **`import.rs` 的 FIT 解析**（⚠️ 仍未拿真实 `.fit` 验证，只有手工拼的样例）。
   接个真实 Garmin 文件测一下。

4. **`chrono` 的调用**（✅ 锁的是 0.4 新版，`from_timestamp` / `from_timestamp_millis` 都有）。

5. **单测里的时间戳常量**（✅ 全部通过，未改动）。

6. **多语言**。新 UI 字符串用的是 `t!` / `tr!`，中英 OK；其它语言走 `i18n` 查表，
   表里**没有**这些词条，会回退成英文。要补的话往 `app/src/i18n/table_ui.rs` 加。

7. **未验证：真实上传链路**。`api.rs` 的登录/上传/列表没有跑过真实网络请求
   （协议是逆向来的，Python 版 `rbcloud.py` 已验证过同一套接口）。第一次真机登录
   前先确认 `%USERPROFILE%\.rbcloud.json` 里的账号还能用。

## 6. 明确没做、但以后可能要做的

- **赛道拆圈**：下载官方赛道库（`/track/all`），用起终点线做判圈，把圈事件按
  8 字节/条写进会话文件尾部，并更新头部计数与 CRC。见 API 文档 §4.5。
- **原生文件对话框**：要的话加 `rfd`，注意 Linux 的 feature 选择（避开 GTK）。
- **分享链接**：`POST /session/{id}/sharing` 已逆向，可以做「上传后一键生成分享链接」。
- **固件更新**：`/device/checkForUpdates` + `/device/downloadUpdate`。
- **凭据自动续期**：auth 有效期 1 天，可以做成过期前自动用存的密码重新登录。

## 7. 构建与验证

```powershell
# 0. 先配环境（本机没装完整 MSVC 时必做）
. .\script\rust-env.ps1

# 1. 过编译 + 单测
cd app
cargo check --all-targets
cargo test

# 2. 出包并起客户端
.\script\build.ps1 --run
```

验证清单（✅ = 已实测）：

- [x] `cargo check` / `cargo test` 全绿（32 passed / 仅剩「未能嵌入 exe 图标」一条，本机缺 rc.exe）
- [x] 主页面：只剩齿轮 / 信息两个顶栏图标；「从本地文件导入」卡在主页（截图
      `dist/ui-idle.png`、`dist/ui-final-settings.png`）
- [x] 设置页：上传方式 / 关闭行为改成选项卡片；密码行显示「圆点 + 已设置 · 修改」，
      弹窗填写（`dist/ui-dialog-cred.png`）；云账号卡 + 上传配置（截图
      `dist/ui-settings-scroll.png`）；云登录弹窗（`dist/ui-dialog-login.png`）
- [x] 关于窗口：标题栏可拖动、外部点击关闭、版本号可点检查更新（`dist/ui-about-final.png`）
- [x] 云页面布局：卡片等宽撑满、980×720 与 780×620 两种窗口尺寸都无溢出/重叠/截断
      （截图见 `dist/cloud-page-03.png`、`dist/cloud-page-narrow.png`）
- [x] 上传配置落盘：序列号框输入 `12345` → `config.json` 的 `cloud.device_serial` 立即写入，
      其余字段原样保留（走 `config::save_cloud`，不会顺带把设置页未保存的改动写进去）
- [ ] 云页面能登录（真账号），退出登录后能再登
- [ ] 开「同步时自动上传」后同步一次，`racebox.pro` 能看到新会话
- [ ] 拖一个 `.gpx` 进窗口，勾选上传，官网能看到
- [ ] 云端列表能刷新出最近会话

## 8.1 这一轮改的布局 / 配置问题

| 问题 | 改法 |
| --- | --- |
| 云页面套了两层 `ScrollArea`（中央面板一层、`cloud_ui::page` 又一层）→ 双滚动条、内容被内层截掉一截 | 去掉 `page()` 里那层，交给 `ui.rs` 的中央面板统一滚 |
| 卡片不撑满宽度（设置页每张卡开头都有 `ui.set_width(ui.available_width())`，云页面漏了）→ 三张卡宽窄不一 | 每张卡补上同样一句；实测三张卡边界一致（x 25–970） |
| 卡片越画越宽：每张卡都用 `ui.set_width(ui.available_width())`，右边顶到窗口边框（越靠下的卡越明显，第 5 张开始完全贴边） | 见下面 8.2 的第 3 条，统一换成 `style::fill_card_width(ui)` |
| 输入框 / 路径框写死 280 / 380 px，窄窗口会挤出卡片 | 换成 `field_width()` 随可用宽度变化（180–320 / 200–∞），按钮行改 `horizontal_wrapped` 自动换行 |
| 序列号 / 时区框每帧拿配置值重建 → 正在敲的内容被吞（敲到一半的数字会消失） | 输入原文缓存在 `CloudState` 里；认得出来才写回配置，失焦时按配置值复位 |
| 路径改了必须再点「扫描」才会记住 | 回车或失焦即重扫并落盘，敲字过程中不重复扫 |
| 云页面「立即生效」时整份 `Config` 落盘，会把设置页还没保存的改动一起写进去，导致「放弃修改」失效 | 新增 `config::save_cloud()`：只读回磁盘 JSON、替换 `cloud` 一个键再写回 |

### 8.2 配置文件读写的两条硬规矩（踩过坑）

1. **写 `config.json` 不要用 PowerShell 的 `ConvertTo-Json | Set-Content -Encoding utf8`。**
   PS 5.1 的 utf8 带 BOM（EF BB BF），serde_json 读到会直接报
   `expected value at line 1 column 1`，**整份配置被丢掉、静默退回默认值**
   （mqtt/pg 凭据、设备地址全没了，且应用不弹任何提示）。
   要写就用：
   ```powershell
   [System.IO.File]::WriteAllText($p, $json, (New-Object System.Text.UTF8Encoding($false)))
   ```
2. **读的时候必须剥 BOM。** `config::load()` 已经做了
   `trim_start_matches('\u{feff}')`，`save_cloud()` 同样处理；
   有单测 `bom_prefixed_json_still_parses` 盯着（它也断言「不剥 BOM 确实会失败」）。
   另外 `save_cloud()` 遇到读不回原文 / 格式异常时**返回 Err 而不是写空对象**——
   宁可这次云配置存不下，也不能把磁盘上那份配置清掉；UI 侧会把错误显示出来。

顺带：`script/rust-env.ps1` 这种带中文注释的脚本**必须存成 UTF-8 with BOM**，
否则 PS 5.1 按 ANSI 解析会直接语法报错（文件里已用 BOM）。

3. **卡片要撑满宽度，别用 `ui.set_width(ui.available_width())`。**
   egui 的 `set_width` 走的是「下一个控件占多大地方」那套逻辑，会把
   `item_spacing`（默认 8px）一起算进 `min_rect`，卡片因此比可用宽度宽 8px；
   更麻烦的是它还会顺手把**外层 ui 的可用宽度顶宽**，一页下来越靠后的卡片越往右溢出
   （实测：980 宽的窗口，第 1 张卡右边还有 16px，第 5 张开始直接贴到窗口边框）。
   现在统一用 `style::fill_card_width(ui)`——它只 `expand_to_include_rect`，
   不加间距也不动 `max_rect`，实测首尾卡片边界一致（x 25–970）。
   同理，`ui.horizontal` 里想给两个卡留 12px 缝，直接设
   `ui.spacing_mut().item_spacing.x = 12.0`，**不要再 `add_space(12)`**：
   `add_space` 本身是个控件，egui 会在它前后各插一次默认间距，整行就多出 16px。

### 8.3 页面结构（云页面已撤销，功能拆进两个页面）

「RaceBox 云」这个独立标签页去掉了，只留**同步主页**和**设置页**：

| 原来在云页面 | 现在在哪 |
| --- | --- |
| 账号登录 / 退出 | 设置页「RaceBox 云」卡，点「登录」弹窗填邮箱密码（页面上不再有密码框） |
| 上传配置（开关 / 序列号 / 时区） | 设置页同一张卡的下半部分 |
| 本地文件导入（路径、扫描、勾选、上传） | **同步主页**最后一张卡（和「同步设备」是同一种入口：把数据送出去） |
| 云端会话列表 | 暂不提供（登录信息只用于上传） |

顶栏从三个图标（云 / 齿轮 / 信息）减到两个（齿轮 / 信息）；`style::cloud_button`
和 `style::radio_button` 都删掉了。

### 8.4 同步进度：下载完分两路上传，各报各的进度

`worker::spawn_flush` 以前是串行遍历 sinks（先 PostgreSQL 再云），两路共用一条
进度回调 → 段内进度会先走一遍再倒回去重走，视觉上来回跳。现在：

- 每一路各起一个 `tokio::spawn` 并行上传，互不等待；
- 每路有自己的 `ChannelCounter`（`FlushCounters::new(sinks)` 按通道数分配）；
- 新增事件 `ChannelList(Vec<String>)`（任务开始时发一次，界面据此重建进度条、
  顺带清掉上一次的残留）和 `ChannelProgress { name, uploaded, failed, total }`；
- 界面在主卡圆钮下方画「上传通道」：一路一条 `ProgressBar`，显示 `已传 / 总数`，
  有失败的整条变红。只有任务跑过（`running || phase.is_final()`）才展开。

顺带删掉了 `UiEvent::SessionProgress`（段内 x/y）：分路之后它会被两条通道交替刷新，
保留只会让数字乱跳。

### 8.5b 敏感字段一律进弹窗

设置页上不再出现任何账号 / 密码 / 连接串输入框，页面上只留「摘要 + 修改」：

| 卡片 | 页面显示 | 弹窗字段 |
| --- | --- | --- |
| PostgreSQL | 连接 `user@host:port/db`、账号、密码（圆点 · 已设置）+「修改」 | 主机 / 端口 / 数据库 / 用户 / 密码（+ 显示密码） |
| MQTT | 连接 `broker:port → topic`、账号、密码 +「修改」 | Broker / 端口 / 保活间隔 / 用户 / 密码 / 主题 / Client ID |
| RaceBox 云 | 「未登录」+「登录」 | 邮箱 / 密码（+ 显示密码） |

弹窗统一由 `App::show_dialog()` 画（`Dialog::Connection` / `Dialog::CloudLogin`）：
- `.open(&mut bool)` 让 egui 在标题栏右上角画出 ×；
- 底部两个按钮走 `style::dialog_buttons()`：等宽（各占一半再留 12px 缝）、整体居中；
- 「确定」在关键信息没填时置灰（连接弹窗要求主机非空且端口能解析成 `u16`）。

原来单独改密码用的 `Dialog::Credential` / `CredTarget` 已经删掉——密码现在跟着
连接参数一起在 `Dialog::Connection` 里改，省一份代码。

### 8.5 说明文字：卡片右上角的「?」

所有常驻的灰色小字提示都收进了 `style::help_icon()`：卡片标题右边一个小圆圈「?」，
点一下才在下方弹出说明（点别处收起），不再占版面。
标题 + 问号一起用的封装是 `style::card_title_with_help()`。
弹层用 `egui::popup::popup_below_widget` + `PopupCloseBehavior::CloseOnClickOutside`。

### 8.6 关于窗口

- 带 egui 原生标题栏（可拖动；拖动过之后 egui 记住位置，下次打开还在那儿），右侧 × 也能关；
- 首次打开居中（`default_pos`），窗口矩形记在 `App::about_rect`；
- 只有点在 `about_rect` **之外**（或按 Esc）才关闭，点窗体内部不会关；
- 版本号那一行就是「检查更新」入口（蓝色链接样式，busy 时显示「检查中…」），
  点击调 `App::check_update()`，结果直接显示在窗口里。

## 8. 相关账号与凭据

- 测试账号：`rubygreat@msn.com`（官网 racebox.pro）。**密码别写进仓库**，
  本机已存到 `%USERPROFILE%\.rbcloud.json`（被 `rbcloud.py` 用）。
- 分析过程目录 `D:\wangcw\Documents\racebox-re\`（在本仓库之外，不进 git）：
  里面有反编译源码、工具 `tools/rbcloud.py`（命令行版上传工具，可作为行为参照）。
