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
- sink 模式：`database` / `mqtt` 二选一（只能选一种，不做“同时上传”；
  旧配置里的 `both` 会被当成 `database`，不会导致整份配置解析失败）
- PostgreSQL：host / port / database / user / password
- MQTT：broker / port / user / password / topic / client id / QoS
- 行为：`auto_erase`（上传全部成功后是否自动清理设备内存）、
  `spool_on_failure`（失败转本地缓存）、`scan_timeout_secs`（蓝牙扫描超时，10–120 秒）
- MQTT：`keepalive_secs`（保活间隔，10–300 秒）
- 外观：`theme` = `dark` / `light`（设置页“外观”卡片切换，切换即保存；
  旧配置文件没有该字段时按 `serde(default)` 视为 `dark`，不会丢配置）
- 语言：`lang` = `zh` / `en` / `ja` / `ko` / `pt` / `es` / `ru` / `fr` / `de` / `it`
  （设置页“语言 / Language”卡片里的下拉列表，切换即保存；缺省为 `zh`）
- 关闭行为：`close_action` = `Quit` / `Tray`（缺省 `Quit`）

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
- 设置页：上传方式、关闭行为用单选控件（一组里只能选一个），语言是下拉列表
  （10 种语言，横排单选会挤成一坨）；行为开关是
  iOS 风格开关；设备扫描超时、MQTT 保活间隔是数字输入框（均落盘到 config.json）

### 同步前的通道自检

完整同步会在**动设备之前**先检测上传通道：

- PostgreSQL：走一遍真实写入链路（连库 → 开事务 → 用两条假记录试写 → 回滚），
  20 秒超时。表缺列、列类型不匹配、没写权限这些都会在这一步暴露
- MQTT：等到 broker 的 CONNACK 才算通过

任一项不通过就直接中止，不会去下载设备数据——以前是先把几分钟的数据下完，
到写库时才失败转本地缓存，等于白跑一趟。

### 上传性能与取消行为

- PostgreSQL 批量写入按 1000 行一批，并且**按批大小缓存预处理语句**。
  一段几万条的数据会拆成几十批，每批 29 列 × 1000 行 = 29000 个占位符；
  如果每批都直接 `execute(&sql, ..)`，客户端每批都要重新 prepare（解析 + 规划）
  一遍这条两万多参数的语句，一段 5 万条要跑三四分钟。缓存后解析只发生一次。
  每段的实际吞吐会写进日志文件：`PostgreSQL 写入完成: <文件名>, N 条, 耗时 X.Xs (Y 条/秒)`
- 上传过程中界面会显示段内进度（`第 N 段上传中 3000 / 50054 条`），不再只有
  “一段 0% 然后突然 100%”
- 点“取消同步”会**立刻**生效：已经下载但还没上传的那一段直接写本地缓存
  （毫秒级），不再等上传跑完；已经在飞的上传任务会继续跑完，但结果不再往界面上刷，
  免得取消之后还一条条往外冒

### 多语言

`src/i18n.rs` 提供 `t!` / `tr!` 两个宏，**中文 / 英文两版就地写在调用点**：

```rust
ui.colored_label(p.muted, t!("主机", "Host"));
let s = tr!("已上传 {} 条", "{} records uploaded", n);
```

其余 8 种语言（日 / 韩 / 葡 / 西 / 俄 / 法 / 德 / 意）放在 `src/i18n/` 下的三张表里：

| 文件 | 内容 |
| --- | --- |
| `i18n/table.rs` | `Row` 结构、`row()` 构造、`(中文, English)` → 译文的索引 |
| `i18n/table_ui.rs` | 静态文案（按钮、标题、阶段名…），142 条 |
| `i18n/table_msg.rs` | 带 `{}` 占位符的模板，36 条 |

一行一条，10 个参数按位置传：

```rust
row("主机", "Host", "ホスト", "호스트", "Host", "Host", "Хост", "Hôte", "Host", "Host"),
```

要点：

- **查表 key 是 `(中文, English)` 这一对，不是只用中文**。`主题` 在 MQTT 配置里是
  Topic、在外观里是 Theme，只用中文当 key 会串；`失败` 也有 `failed` / `Failed` 两种大小写。
- **查不到就回退英文**（不是中文）：非中文用户读英文比读中文有用。
- `tr!` 的模板只能用裸 `{}`。用**命名捕获**的调用点（`tr!("…: {e}", "…: {e}")`）拿不到
  参数列表，这类文案只有中英两版，其他语言回退英文 —— 全项目还剩 34 条，都是
  `xxx 失败: {e}` 这种技术性错误详情和后台日志。想让某条也支持，把调用点改成显式传参
  （`tr!("失败: {}", "failed: {}", e)`）再往 `table_msg.rs` 加一行就行，`render_other`
  里有占位符个数校验兜底，配错了也只会回退英文，不会渲染出缺值的半句话。
- 字体：微软雅黑（msyh）有汉字和西里尔字母，但**没有韩文谚文**，所以 `setup_fonts`
  会额外加载 Malgun Gothic（韩）/ Yu Gothic（日），按家族列表逐字回退。
- 窗口标题固定是 `RaceBox`：标题栏由窗口系统绘制、不跟界面语言变，带上“同步”就得
  为 10 种语言各维护一份，收益为零。
- 同步页的大圆钮（直径 ~90px）里的文字走 `round_action_button` 的自适应排版：先在圆内
  可用宽度上**按词换行**，还塞不下就**逐档缩小字号**（13.8px → 9px），所以德语
  `Synchronisieren`、俄语 `Синхронизация` 这种长词也不会捅出圆外。「开始同步 /
  取消同步」在非中文语言里特意用了单词形式（`Sincronizar` / `Parar`），整句
  （`iniciar sincronização`）会缩到 10px 以下；完整说明在悬停提示里。

语言存在全局原子量里（后台线程也要产出提示文字），`config::load()` 会顺手同步。
切换语言后，已经产生的日志/结果文字仍是生成时的语言，这是有意为之（历史记录不改写）。

`cargo test --bins` 里 `i18n` 有 7 个单测兜底：语言码往返、serde 往返、文案 key 不重复、
每条都翻全了、占位符个数和中英一致、缺译文回退英文。

## 5. 实现顺序

1. 配置模型 + 设置页
2. BLE 扫描连接 + 状态查询
3. 下载解析（含 0x26 分段、进度展示）
4. PostgreSQL sink（对齐现有表结构与幂等）
5. MQTT sink（RBX2 组帧 + PUBACK 确认）
6. spool 队列与重放
7. 擦除流程与整体状态机
8. 打包（Windows 优先）

## 8. 安装包、在线更新与签名

- Windows Release 只发布 `racewong-<版本>-windows-x86_64.msi`。MSI 按用户安装到
  `%LOCALAPPDATA%\RaceBox Sync`，同一 `UpgradeCode` 的新版本会直接覆盖升级，无需先卸载，
  也不要求管理员权限。
- macOS Release 发布 `racewong-<版本>-macos-aarch64.dmg`，内含 ad-hoc 签名的 `.app`。
  当前未配置 Developer ID 和 Apple 公证，因此首次运行仍需用户在系统设置中手动放行。
- 设置页的“检查并更新”调用 GitHub `releases/latest`。发现更高的语义化版本后自动下载
  当前平台的 MSI/DMG；Windows **完全静默覆盖升级**（`msiexec /qn`，不弹任何向导、
  不要求点“下一步”），macOS 打开 DMG，用户把应用拖入 Applications 覆盖旧版本。

### Windows 静默升级的细节

- **安装位置沿用上一次**：更新器把当前 exe 所在目录作为 `INSTALLFOLDER=` 传给 MSI；
  MSI 自己也会在安装时把路径写进 `HKCU\Software\racewong\RaceBoxSync\InstallDir`，
  升级时用 `RegistrySearch` 读回来兜底（手动双击 MSI 升级走的是这条路）。
  两条路径都保证“装哪儿就在哪儿升”，不会因为静默安装没人能改目录而被搬回默认位置。
- **为什么不在进程里直接调 msiexec**：MSI 要覆盖的正是当前正在运行的 exe，Windows 上
  被占用的文件无法替换。更新器会写一个短命 `cmd` 脚本到
  `%TEMP%\racewong-update\apply-update.cmd`，等 2 秒让本进程退出、文件锁释放，
  再执行 `msiexec /qn /norestart`，装完把程序重新拉起来。
- **失败怎么查**：静默安装的完整日志在 `%TEMP%\racewong-update\install.log`。

### 安装许可协议

安装向导里有一页中文许可协议（欢迎 → **许可协议** → 安装目录 → 确认安装）。

- 正文维护在 `app/installer/license.zh.txt`（UTF-8，人读）
- 打包时由 `script/wix-license.ps1` 的 `ConvertTo-LicenseRtf` 转成 RTF 传给 WiX 的
  `WixUILicenseRtf`——WiX 的许可页是 RichText 控件，只认 RTF，中文必须写成
  `\uNNNN?` 转义，否则就是乱码
- 改完协议不用手动转，`release.ps1` 生成 MSI 时会现转；想单独看产物：
  `. .\script\wix-license.ps1` 后调 `ConvertTo-LicenseRtf`

### 安装向导里的选项与品牌图

向导一共四页：欢迎 → 许可协议 → **安装目录（含选项）** → 确认安装，装完是完成页。

| 位置 | 内容 | 默认 |
| --- | --- | --- |
| 安装目录页 | `创建桌面快捷方式` 勾选框 | 勾上 |
| 完成页 | `安装完成后启动 RaceBox同步` 勾选框 | 勾上 |

- **桌面快捷方式**绑在 `DESKTOPSHORTCUT` 属性上（`CheckBoxValue="1"`），快捷方式单独放在
  `DesktopShortcut` 组件里、组件条件是 `DESKTOPSHORTCUT = 1`，卸载时随组件一起消失。
  取消勾选时 MSI 会把属性**清空**（不是置 0），所以条件写成 `= 1` 而不是 `<> 0`。
  这个选择不跨版本记忆：属性被清空后再写进整型注册表值会直接让安装报错，不值得。
- **安装目录页是自己重写的**：`WixUI_InstallDir` 没有能插控件的位置，而 WiX 把**所有**
  对话框打在一个 `ui.wixlib` 里——只要引了它，`InstallDirDlg` 就必然被拉进来，同名覆盖
  会报 `LGHT0091 Duplicate symbol`。所以 `.wxs` 里不再 `UIRef` 那个序列，而是把
  WiX 3.14 的 `WixUI_InstallDir.wxs` 整段搬过来自己写（`<UI Id="RaceBoxUI">`），
  末尾只留 `UIRef WixUI_Common` 取字体/位图/UIText，其余对话框仍按 Id 从扩展库取。
  - 自己的对话框 Id 是 `RaceBoxInstallDirDlg`，**必须改名**：把对话框改名、但
    `Publish` 里仍写 `Dialog="InstallDirDlg"` 时，light 会把引用绑到 wixlib 那份，
    自己的定义反而成了重复符号。所有指向它的 `Publish` 已同步改名。
  - 11 个原有控件的 Id / 类型 / 坐标 / 文案与 WiX 官方那份逐属性一致
    （`ChangeFolder`、`Next`、`Cancel` 的跳转全靠 `Publish` 按 Id 挂载，一个都不能改）。
  - 改完应比对 MSI 表确认没动到导航：`InstallUISequence` 41 行、`ControlEvent` 774 行，
    与改动前的基线完全一致，只多出勾选框那两个控件。
- **品牌图**：`app/installer/banner.bmp`（493×58，各页顶部横幅）和
  `dialog.bmp`（493×312，欢迎/完成页背景），由 `app/tools/make_installer_art.py` 生成，
  和 exe 图标共用 `app/tools/racebox_mark.py` 里的同一份矢量数据。
  两张图都按 493 宽设计、由 MSI 缩到 370 显示（WiX 自带素材也是这个套路）。
  横幅上**只能把 logo 放右边**：WixUI 把每页的标题（X=15,Y=6）和描述（X=25,Y=23，
  宽 280）透明地画在横幅上，左边是文字区，放 logo 会压字。
- **其它常规项**：控制面板里的图标 / 说明 / 帮助链接 / 更新说明链接（`ARP*`）、
  `Win+R` 能直接敲 `racewong-app`（App Paths，卸载时清掉）、
  Windows 10 以下直接拒绝安装（蓝牙走 WinRT，`VersionNT >= 1000`）。

#### 本地验证安装包

改 `.wxs` 后不必等 CI（一轮约 4 分钟）。WiX 3.14 的二进制包解压即用，免安装：

```powershell
# https://github.com/wixtoolset/wix3/releases/download/wix3141rtm/wix314-binaries.zip
candle.exe -nologo -dVersion=0.1.9 -dExePath=<exe> -dIconPath=app/assets/icon.ico `
  -dLicenseRtf=<rtf> -dBannerBmp=app/installer/banner.bmp -dDialogBmp=app/installer/dialog.bmp `
  -out out/r.wixobj app/installer/racewong.wxs
light.exe -nologo -sval -ext WixUIExtension -ext WixUtilExtension -cultures:zh-cn `
  -out out/r.msi out/r.wixobj
```

想确认向导没被改坏，用同包的 `Microsoft.Deployment.WindowsInstaller.dll` 读表比对
改动前后的 MSI：`Dialog` / `InstallUISequence` / `ControlEvent` 三张表应与基线一致
（只多出新增的控件）。注意 MSI SQL 里 `Sequence`、`Property`、`Condition`、`Text`
是保留字，`SELECT Sequence, ...` 会报语法错误，用 `SELECT *` 再自己拆；
记录要取 `.ToString()`，`GetString(i)` 取出来是空串。
`dark.exe` 反编译出的 wxs 只保留 `<UIRef>`，看不到 UI 表，别指望它。

ad-hoc 签名只保证 Apple Silicon 上应用包结构完整，并不建立 Gatekeeper 信任。以后取得
Developer ID 后，应恢复正式签名和公证，再取消手动放行提示。

## 6. 图标

| 文件 | 用途 |
| --- | --- |
| `assets/icon.ico` | exe 资源图标（16/24/32/48/64/256，32 位带透明），由 `winres` 在 `build.rs` 中嵌入 |
| `assets/icon256.png` | 运行时窗口图标，`main.rs` 里 `include_bytes!` + `eframe::icon_data::from_png_bytes` |

两个图标都由 `app/tools/make_icon.py` 生成。图形直接用官网的品牌标记：从
`https://www.racebox.pro/assets/img/RaceBoxLogo.svg` 里取出那条 marker path，
按 M/C/S/V/H/L/Z 拍平成多边形（三次贝塞尔每段采样 24 点）后填充品牌红
`#EA2127`、透明底，8 倍超采样再缩到目标尺寸——和官网 favicon 是同一个形状。
不引 SVG 渲染库，是因为只为一枚图标装 cairosvg / resvg 不划算。

拍平逻辑和标记数据单独放在 `app/tools/racebox_mark.py`，安装向导的两张品牌图
（`installer/banner.bmp` / `installer/dialog.bmp`，见 §8）也用它，形状保证一致。

```powershell
.venv\Scripts\python.exe app\tools\make_icon.py            # exe / 窗口 / 托盘图标
.venv\Scripts\python.exe app\tools\make_installer_art.py  # 安装向导横幅与背景
```

界面主题色也取自官网：主色 `#EA2127`，中性色 `#2C2C2C` / `#666` / `#808186` /
`#F1F1F3` / `#E4E4EC`，见 `app/src/style.rs` 的 `palette()`。

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

按当前配置依次测 PostgreSQL（开事务、用两条假记录走一遍真实写入再回滚，
表缺列 / 列类型对不上 / 没有写权限都会当场报出来）和 MQTT（等 CONNACK），
失败会打印服务端原文，例如：

```
PostgreSQL fn.wongcw.cn:8432/eadm ... 失败: password authentication failed for user "user_eadm" [28P01]
```

设置页的“测试连接”也是走这两条路径，结果会同时显示在同步页的“提示”行、日志页，
以及设置页对应卡片按钮旁边（进行中显示转圈，结束显示成功 / 失败 / 超时）。
PostgreSQL 测试整体有 20 秒超时，主机不可达时不会一直转圈；MQTT 测试等 CONNACK，
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

上面的命令已经包进 `script/build.sh`，平时直接用脚本就行（自动判平台、打印产物路径）：

```bash
./script/build.sh              # WSL 下默认交叉编译出 Windows exe；其它平台编本机原生版
./script/build.sh --debug      # debug 构建（带控制台，方便看日志）
./script/build.sh --check      # 编完顺手跑 --check（PG 试写回滚 + MQTT CONNACK）
./script/build.sh --run        # 编完直接启动客户端
./script/build.sh --native --test --clean   # 只编 Linux 版、先跑单测、先 clean
```

Windows 原生构建用 `.\script\build.ps1`，参数是开关形式（`-Debug` `-Run` `-Check` `-Test` `-Clean`）；
本机没装 Rust 工具链时它会自动转到 WSL 里调 `build.sh`（`-NoWsl` 可关掉这个回退）。

两个脚本都会在编译前结束还在跑的客户端（`-NoKill` / `--no-kill` 可关掉）。原因是
Windows 上正在运行的 exe 无法被删除或覆盖，rustc 写产物时会报：

```
error: failed to remove file `...\release\racewong-app.exe`
Caused by: Input/output error (os error 5)
```

`os error 5` 就是 `ERROR_ACCESS_DENIED`——客户端还在跑（尤其是设置里选了「隐藏到托盘」，
关窗口不退进程）。手工处理：托盘菜单里点「退出」，或任务管理器结束 `racewong-app.exe`。
另外 Windows 目标加 `--run` 时是用 `cmd start` 后台拉起的，关掉终端不会把客户端一起带走。

`rumqttc` 当前 `default-features = false`（不开 rustls），因此 MQTT 仅支持明文 1883
——这是为了绕开 ring 交叉编译需要 MSVC `llvm-lib` 的限制；后续如需 TLS，在装有
MSVC Build Tools 的机器上把默认特性打开即可。

### 原生调试（WSL 内，Linux 目标）

```bash
cargo check   # 类型检查
cargo test    # src/protocol.rs 的协议单测（帧解析、记录解码、RBX2 组帧、CRC32）
cargo build --release
```

### 关闭窗口的行为（托盘常驻）

设置 → 行为 → 「点关闭按钮时」，二选一：

- **直接退出应用**（默认）：点右上角 × 就退出进程。
- **隐藏到托盘，后台继续运行**：点 × 只把窗口藏起来，程序留在 Windows 通知区域
  （macOS 是菜单栏），正在跑的同步任务不会中断。托盘菜单：`显示主窗口` / `退出`，
  点 `退出` 才会真正结束进程。

托盘图标只在选“隐藏到托盘”时才常驻；切回“直接退出”会立刻摘掉，不会留一个隐形图标。

| | Windows | macOS |
| --- | --- | --- |
| 位置 | 通知区域（右下角，可能在 `^` 折叠区） | 菜单栏右上角 |
| 恢复窗口 | 左键点图标，或右键 → 显示主窗口 | 点图标 → 显示主窗口 |
| 退出 | 右键 → 退出 | 菜单 → 退出，或 Cmd+Q |
| 进程残留 | 无 | 仍会出现在 Dock 与 Cmd+Tab 里 |

**为什么托盘菜单要直接调 Win32**：窗口用 `ViewportCommand::Visible(false)` 藏起来之后，
egui 侧的 `Visible(true)` / `Close` 命令经常落不了地——表现就是右键菜单里
「显示主窗口」「退出」点了没反应，托盘图标赖在通知区域里，只能去任务管理器结束进程。
所以 `src/tray.rs` 在菜单回调里直接用 HWND 调 `ShowWindow` / `PostMessage(WM_CLOSE)`，
不依赖 egui 是否还在出帧；HWND 由 `main.rs` 从 `CreationContext::window_handle()` 取出后
传给 `App`。队列只用来同步界面内部状态（`hidden_to_tray`），另外左键点图标也能唤回窗口，
是菜单事件万一没送到时的备用路径。

macOS 想做成「纯菜单栏应用」（不占 Dock），在 `script/release.sh` 生成的 `Info.plist`
里加一行 `<key>LSUIElement</key><true/>` 即可；代价是 Dock 和 Cmd+Tab 里都看不到它，
只能从菜单栏图标操作。

**单实例**：`src/single_instance.rs` 在启动时抢一个命名互斥体
（`Local\racewong-RaceBoxSync-single-instance`）。托盘常驻时窗口只是藏起来、进程还在，
用户再点一次开始菜单快捷方式就会起第二个进程——两个进程各注册一个托盘图标，通知区域
里于是出现两个一模一样的「RaceBox同步」，而且旧那个的窗口是隐藏的、点了也没反应。
现在第二个进程发现互斥体已存在，就用 `FindWindowW("RaceBox")` 找到旧窗口、
`ShowWindow` + `SetForegroundWindow` 把它拉回前台，然后自己退出（不弹任何提示）。
互斥体建不出来或找不到旧窗口时一律放行，宁可多开也不能让用户点了打不开。

**名称**：通知区域的 tooltip、关于面板、安装包与开始菜单快捷方式统一叫「RaceBox同步」
（无空格）；其它语言显示 `RaceBox Sync`，品牌名不翻译。

Linux 暂不支持托盘（`tray-icon` 的 Linux 后端要 GTK / AppIndicator 系统库，会把
`cargo test` 和原生构建拖上系统依赖），选这一项会退回直接退出，设置页会给出提示。

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
.\script\release.ps1 -Build               # 强制本机构建当前平台的包
.\script\release.ps1 -BuildOnly           # 只构建当前平台的安装包（CI 用）
```

```bash
./script/release.sh                       # 同上的 bash 版
./script/release.sh --version v0.2.0
./script/release.sh --no-build
./script/release.sh --build               # 强制本机构建
./script/release.sh --build-only --out-dir dist
```

说明：

- 自动版本：取最新 `v*.*.*` 标签 patch +1，没有标签就用 `app/Cargo.toml` 的版本
- Release 正文来自 **git 提交记录**（`上一个标签..HEAD` 的 `git log`）
- 建 Release 用 `gh`；没装 `gh` 时可用 `GITHUB_TOKEN` 走 REST
- **标签推上去就等于发布**：`.github/workflows/release.yml` 会并行编 win/mac/linux 三个包并上传。
  所以本机的构建只是「让当前平台的包早几分钟出现」，而且**只有本机能上传时才有意义**
  —— `gh` 未登录且没有 `GITHUB_TOKEN` 时脚本会自动跳过本机构建（否则编出来的包无处可去），
  想强制构建加 `-Build` / `--build`。本机没有 cargo 时会回退到 WSL 里的 `script/build.sh`。
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
