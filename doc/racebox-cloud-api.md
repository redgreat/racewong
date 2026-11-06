# RaceBox 云 API 与会话文件格式（逆向结果）

> 本文档是对 RaceBox 官方云（`api.racebox.pro`）和会话二进制文件的逆向分析结果，
> 用于在本应用（racewong）里实现「上传会话到官网 racebox.pro」。
>
> 来源：Android 版 RaceBox 2.3.0（包名 `pro.RaceBox.androidapp`，.NET MAUI）反编译，
> 配合对真实账号的网络抓包 / 接口实测。**不是官方文档**，字段含义以实测为准。
>
> 分析过程的完整笔记在 `D:\wangcw\Documents\racebox-re\README-cloud-api.md`（本仓库之外），
> 本文是浓缩后的「够用」版本。
>
> 本文只管**云接口**和**会话容器的逐字节表**。BLE 帧、80 字节记录、GPX/FIT
> 导入、iTOW 与 CRC 的坑，见 [`racebox-parsing.md`](racebox-parsing.md)。

## 1. 账号与鉴权

- 基址 `https://api.racebox.pro/v1/`
- **鉴权方式是 Cookie，不是 Bearer。** 登录返回的 token 要放进
  `Cookie: racebox-api-auth=<token>`，域 `racebox.pro`，有效期约 1 天。
- 请求带任意 `User-Agent` 即可；官方 App 后台抓取会额外加 `X-RaceBox-BG: true`。

### 登录

```
POST /v1/login
Content-Type: application/json
```

```json
{
  "email": "you@example.com",
  "password": "...",
  "clientId": "<GUID，必填，设备唯一标识>",
  "platform": "Android",
  "version": "2.3.0",
  "devManufacturer": "samsung",
  "devModel": "SM-G991B",
  "osVersion": "Android 14"
}
```

响应：

```json
{
  "user": { "id": "66ca...", "email": "you@example.com", "name": "wangcw", "acceptedLegalDocumentsOn": 1733405844 },
  "auth": "9e6f...（64 位 hex）",
  "error_code": 0,
  "error_text": ""
}
```

- `clientId` 必填，服务端用它区分同一账号下的多台设备；桌面端给随机 GUID 即可。
- `auth` 既出现在响应体，也出现在 `Set-Cookie` 里，两处取到同一个值。
- `error_code != 0` 就是失败（账号或密码不对时 `error_text` 可能是空串）。

## 2. 会话相关端点

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/session` | **上传会话文件**。`Content-Type: application/octet-stream`，附 `If-Unmodified-Since`（RFC1123）。服务端解析后回填圈速 / 极速 / 最大 G |
| GET | `/session/{id}/data` | 下载原始会话文件（字节与设备上完全一致） |
| GET | `/session/{id}/meta` | 会话元数据 |
| POST | `/session/syncmeta` | 增量同步会话列表；也用来改收藏 / 备注 / 车辆 / 天气 / 标记删除 |
| POST | `/session/markDownloaded` | 标记已下载 |
| POST | `/session/syncvideos` | 视频绑定（本应用不做） |
| GET/POST | `/session/{id}/sharing` | 分享设置：公开 / 按邮箱邀请 / 移除 |

### 上传示例

```
POST /v1/session
Content-Type: application/octet-stream
If-Unmodified-Since: Sat, 31 Oct 2026 06:20:00 GMT
Cookie: racebox-api-auth=<token>

<会话文件裸字节>
```

成功响应（服务端已经把文件解析了，统计数据是它算的）：

```json
{
  "session": {
    "id": "43a66e2c4cf4e751bdc0e720",
    "sessionType": 1,
    "deviceSerialNumber": 3242704435,
    "uploadOK": true,
    "records": 362,
    "duration": 531,
    "maxSpeed": 57.5,
    "minSpeed": 0.01,
    "maxG": 1.041,
    "track": { "name": "No Track", ... }
  },
  "error_code": 0
}
```

### 会话列表

`POST /session/syncmeta`，body `{"lastSynced": 0, "sessions": []}`，
`lastSynced=0` 就是全量。实测一个账号 1280 条。

## 3. 其它端点（扩展能力，本应用暂不做）

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/track/all?sampleDrivingLines=1&type=all` | 官方赛道库（3.4 MB），含起终点线 / 分段线 |
| GET | `/track/outlines?since=N` | 赛道几何轮廓（54 MB） |
| GET | `/track/{id}?sampleDrivingLines=1&type=all` | 单条赛道 |
| POST | `/track/sync?type=all` | 自定义赛道同步 |
| POST | `/device/sync` | 设备列表同步 |
| GET | `/device/checkForUpdates` / `/device/downloadUpdate` | 固件更新 |
| DELETE | `/device/{id}` | 解绑设备 |
| POST | `/vehicle/sync` / DELETE `/vehicle/{id}` | 车辆档案 |
| POST | `/user/change-password` 等 | 账号管理 |
| GET | `/video-storage-subscription/plans` / `/status` | 视频存储订阅 |

## 4. 会话文件格式（RB V2 容器）

**这是和 App 交互最关键的部分。** 官网只认设备原生的二进制会话文件，
上传接口收到的是裸字节。GPX / FIT 必须先转成这个格式。

```
[0 .. HeaderLength)                      头部（自带 CRC32）
[HeaderLength .. +SensorsDataLength)     传感器记录流
[.. + FileSuffixLength)                  尾部（赛道会话的圈 / 分段事件，每条 8 字节）
```

魔数 `"RB"` + 版本 `02 01`（V2）。老版本（V1）是裸 300 字节头，App 按前 4 字节区分，
本实现只生成 V2。

### 4.1 头部（小端）

| 偏移 | 长度 | 字段 |
| --- | --- | --- |
| 0x00 | 4 | `"RB"` + FileVersionMajor(2) + FileVersionMinor(1) |
| 0x04 | 2 | HeaderLength（**随类型变**：Drag = 96 + 12×目标点数，默认 144；Track 固定 316） |
| 0x06 | 1 | RecorderVersionMajor（固件主版本，真实样本 1） |
| 0x07 | 1 | RecorderVersionMinor（真实样本 11） |
| 0x08 | 2 | OtherBytesLength（本实现 0） |
| 0x0a | 2 | FileSuffixLength（本实现 0，不写圈事件） |
| 0x0c | 4 | SensorsDataLength |
| 0x10 | 1 | Type：**0=直线加速（Drag），1=赛道（Track）**。本实现固定 0（见下） |
| 0x11 | 1 | IndexInTheDay |
| 0x12 | 1 | SensorRecordsTimeDelta |
| 0x13 | 1 | 保留 0 |
| 0x14 | 4 | NumberOfSensorRecords |
| 0x18 | 4 | NumberOfSystemDataRecords |
| 0x1c | 4 | NumberOfEventRecords |
| 0x20 | 2 | TZOffset，**分钟**（+480 = UTC+8） |
| 0x22 | 6 | 本地时间：年-2000, 月, 日, 时, 分, 秒 |
| 0x28 | 6 | UTC 时间，同上 |
| 0x2e | 12 | SessionID（云端的 id 就是这 12 字节的 hex） |
| 0x3a | 2 | 保留 |
| 0x3c | 8 | DeviceSerialNumber（i64，0 表示不指定） |
| 0x44 | 88 | **仅 Track**：Track（无关联时 `"No Track"`） |
| 0x44+88 | 128 | **仅 Track**：TrackConfiguration（`"Default"`） |
| 之后 | 20 | **仅 Track**：赛道统计（DurationMs / 圈数 / 最速 / 最大 G 等） |
| 之后 | 1+n | 传感器类型表：数量 + 每项 1 字节（GPS=1, GForce=2, Gyro=3） |
| 之后 | 4 | DataChecksum = CRC32(记录流 + 尾部) |
| 之后 | 4 | HeaderChecksum = CRC32(头部除自身外，即前 `HeaderLength - 4` 字节) |

**Drag 会话从 0x44 开始是另一套结构**，不是 Track 那三段：

```
0x44 (68)  u8   SessionMode         1 = SpeedMetric
0x45 (69)  u8   FtRolloutEnabled    0
0x46 (70)  u8   SetpointCount       N
0x47 (71)  u8   ChristmasTree       (倒计时 << 4) | 模式
0x48 (72)  i32  首点纬度 ×1e7
0x4c (76)  i32  首点经度 ×1e7
0x50 (80)  i16  TopSpeed ×100（km/h，上限 327.67）
0x52 (82)  u16  NumberOfRuns
0x54 (84)  N × 12 字节：目标点
```

每个目标点 12 字节：

```
u16 StartSpeed        起始速度（km/h）
u16 EndSpeed|Distance 目标速度（km/h）；距离模式下是距离（米）
u8  flags             bit0=已配置 bit1=距离模式 bit2=起始速度用英里
                      bit4-7=单位（15=km/h，14=mph）
u8  保留
i32 BestResults       最好成绩（毫秒）；0 = 还没成绩，交给服务端算
i16 BestResultsRuns   成绩所在跑次；-1 = 没有
```

`HeaderLengthCompute = 68 + 会话专属段 + OtherBytesLength + 1 + SensorTypes.Count + 8`，  
默认 4 个目标点就是 `68 + 64 + 0 + 1 + 3 + 8 = 144`。

**只改 0x10 的类型字节、头部还按 Track 排，服务端会回 `Failed to parse session`。**

CRC32 用标准多项式（等同 `zlib.crc32` / Rust `crc32fast`）。

赛道统计那 20 字节（小端）：

```
DurationMs u32
NumberOfLapEvents u8
NumberOfLaps u8
NumberOfSectorEvents u16
BestLapTime u32 (ms)
MaxSpeed i16 (×100 → km/h)
MinSpeed i16 (×100)
MaxGForce i16 (×1000 → g)
BestLapNumber i8
AutoEnd u8（bit0 = 0 表示自动结束）
```

赛道统计由服务端在上传时**重新计算**，头部这些字段填个说得过去的值即可
（我们把采样数据算一遍写进去，保持一致）。

### 4.2 Track / TrackConfiguration 固定模板（仅 Track 会话）

Drag 会话没有这两段。要写 Track 会话时：不关联赛道就整段照抄，服务端实测接受：

- **Track（88 字节）**：12 字节 id（全 0） + 偏移 12 起的 32 字节名称 `"No Track"`，其余 0。
- **TrackConfiguration（128 字节）**：12 字节 id（全 0） + 偏移 12 起的 32 字节名称 `"Default"`，
  **偏移 45 处是 flags = 2**，其余 0。

### 4.3 传感器记录流

三种记录，靠首字节区分：

**键记录（首字节 0，共 35 字节）**——全量采样点：

```
iTOW        u32    GPS 周内毫秒
Latitude    i32    ×1e7
Longitude   i32    ×1e7
Altitude    i32    ×1000（米）
Speed       u32    mm/s
Heading     u16    ×100（度）
GForceX/Y/Z i16    ×1000（g）
GyroX/Y/Z   i16    ×100（deg/s）
```

**增量记录（首字节 1..239，共 25 字节）**——相对上一个键记录：

```
索引        u8     1..239
ΔiTOW       u16
ΔLat        i16    ×1e7
ΔLng        i16    ×1e7
ΔAlt        i16    ×1000
ΔSpeed      i16    mm/s
Heading     u16    ×100
GForceX/Y/Z i16
GyroX/Y/Z   i16
```

**系统记录（首字节 240，共 8 字节）**——电池 / 天线 / 星数 / 精度，本实现不写。

**何时重发键记录**（固件 `SensorsDataRecord.WriteToStream` 的规则）：
首个点、间隔 > 2000 ms、时间倒流、增量序号 > 200、任一增量超出 ±16384。

### 4.4 iTOW 的含义

u-blox 的 GPS 周内毫秒数：

```
iTOW = (Unix毫秒 - 本周日00:00UTC) + 18000，再对一周（604800000 ms）取模
```

GPS 时比 UTC 快 18 秒（2017 年起的闰跳）。实测样本与该公式只差不到 1 秒，
差值来自 UTC 字段只存整秒。GPS 周从周日 00:00 UTC 起算。

### 4.5 尾部（赛道圈 / 分段事件）

每条 8 字节。本实现 `NumberOfEventRecords = 0`、`FileSuffixLength = 0`。

**类型固定写 Drag（`0x10 = 0`）**，不写圈事件：

- 我们传的是摩托车 / 汽车的加速数据，本来就没有起终点线，圈速无从谈起；
- 写成 Track 时云端会拿 `"No Track"` 去匹配赛道，匹配不上就归进「未关联赛道」，
  webapp 的 `sessions?type=track` 列表里看不到，等于白传；
- Drag 不要求赛道，服务端直接从速度 / G 力算 0-100、0-200、分段加速，
  正是需要的分析，且 `sessions?type=drag` 能正常列出。

要改成赛道会话（带圈速）得用官方赛道库的起终点线做判圈，把结果按 8 字节事件
写进尾部并更新头部计数和 CRC——本应用不做这件事。

## 5. 已验证的事实

- 用 GPX 生成会话上传后，服务端完整解析：`records / duration / maxSpeed / minSpeed / maxG` 全部回填正确
- 上传的会话立刻出现在 `syncmeta` 列表里
- 写成 Track（`0x10 = 1`）+ `"No Track"` 时，云端视为「未关联赛道」，webapp 的
  赛道列表里不显示；写成 Drag（`0x10 = 0`）才出现在 `sessions?type=drag` 里
- BLE 下发的每条 80 字节记录（见 `app/src/protocol.rs` 的 `Record`）与该格式的
  采样点几乎一一对应：iTOW、经纬度、海拔、速度、航向、G 力、角速度全都有

## 6. 客户端标识与网页版详情页（2026-11-05）

对照同一账号里「真机/官方工具上传」和「本应用上传」的会话，发现两处必须和
官方一致，否则手机 App 能看、网页版详情页却是一片 0：

1. **请求头 / 登录身份**。官方 `BaseApiClient.PrepareRequestMessage` 对
   racebox.pro 的每个请求都带 `X-RaceBox-App: 2.3.0`，UA 是
   `RaceBox/2.3.0 (Android 14)`，登录体上报 `platform=Android,
   version=2.3.0, devModel=SM-G991B, osVersion=Android 14`。
   本应用早期只带 Cookie、登录上报 `Windows / 0.1.x / desktop`：
   `syncmeta` 里 `runsCount`、`bestResults` 都正常，但 **`weather` 一直是 null**
   （账号里 1235 条有天气的会话没有一条是这种传法）。
   `tools/rbcloud.py` 用官方身份上传的测试会话则有天气。
   服务端为什么按客户端区分处理没有公开说明，按「和官方一致」处理即可。

2. **243 跑次开始标记的编号**。`GET /session/{id}/data` 拉回真机 Drag 会话
   （`6ae47b30…`，5 跑）实测：记录流里只有 **4 条** 243 标记，编号是
   **1、2、3、4**——第 0 跑是隐含的，会话开头不写标记。本应用以前给每一跑
   （含第 0 跑）都写一条，编号 0..N-1，比真机多一条且整体错一位。
   官方 `RaceBox.Core.dll` 的 `DragSession.Load` 对两种布局都返回 `Load=True`
   （`RunResults` 条数正确），所以手机 App 看不出来；网页版按标记切跑次就会对不上。
   现在 `app/src/racebox/session.rs::encode_records` 从第 1 跑开始写、编号 1..N-1，
   单测 `run_start_markers_start_at_one` 钉住这个布局。

3. **`deviceSerialNumber` 不能是 0（相关性最强）**。对同一账号
   `syncmeta` 的 1531 条会话做离线统计：

   | deviceSerialNumber | 条数 | weather 非空 | vehicleId 非空 |
   | --- | --- | --- | --- |
   | 3242704435 | 822 | 788 (95.9%) | 92%+ |
   | 2254300997 | 471 | 445 (94.5%) | 92%+ |
   | 0 | 238 | 2 (0.8%) | 0 (0%) |

   两个非 0 号都是用户自己的真机（旧设备 / RaceBox Mini S），**不是填错**。
   详情页空白的 `15b29b73…` 正是
   `deviceSerialNumber=0 / vehicleId=null / weather=null / runsCount=14`。
   两组会话的键集合完全相同，差别只在值：没有有效设备号时服务端不做天气 /
   车辆等后处理，网页版详情页依赖这些服务端补齐的字段；手机 App 用官方
   解析器直接读二进制，对 null 不敏感。这解释了「手机正常、网页空白」。

   `config.rs` 里 `device_serial` 默认值是 0，新用户不填就上传会重演这个
   问题。后续应加上传前校验（非 0 才允许上传，或至少在 UI 明确告警）。

其它观察到的差异（`dragMode`：真机多为 `Custom=0`、我们是 `SpeedMetric=1`；
`ftRollout`：真机 true、我们 false；`indexInTheDay` 我们固定 1）暂未确认
与详情页有关，先不动。

网页详情页的实际数据源仍未直接抓到：未登录时
`GET /webapp/session/{id}` 一律 `302 → /webapp/login`（旧的
`rb_network.json` 可离线复核），登录页有 Cloudflare Turnstile；公开静态资源
里只有通用脚本，抓过一次的 `main-menu.js` 是 384 字节、自注 `OBSOLETE` 的
滚动菜单效果。要确认前端取哪个字段，只能由已登录的浏览器在 F12 里看
Console / Network。

> 排查时别反复登录 / 刷页面：短时间内多次 `POST /v1/login` 会被源站
> 临时 403；`www.racebox.pro/webapp/login` 还有 Cloudflare Turnstile，
> 自动化浏览器拿不到 token。一轮只做一次登录 + 少量 API GET，
> 剩下的靠 `GET /session/{id}/meta`、`/data` 和本地官方解析器离线比对。

## 7. 部署验证的前置条件（2026-11-05 补记）

改完代码不等于改完用户正在跑的程序。构建产物在
`app/target/release/racewong-app.exe`，实际运行的是
`%LOCALAPPDATA%\RaceBox Sync\racewong-app.exe`，两者不是同一个文件；
应用又是单实例、关窗口只收进托盘（`close_action=Tray`），所以验证新版必须：

1. `Stop-Process -Name racewong-app -Force`；
2. 备份旧 exe，把 release 产物复制过去（比对 MD5）；
3. 再启动，确认日志时间戳是新的。

否则日志里新上传的会话仍是旧代码生成的，怎么测都不变。另注意旧云端
会话不会因本地代码更新而改变，必须**重新上传一条新会话**才能验证。

## 8. 网页版「灰色跑次」与设备序列号兜底（2026-11-06）

用户反馈：桌面应用上传的 Drag 会话详情页跑次下拉里大半是灰色、点不开；
手机上传的会话不管几跑都能点。样本：

- 桌面 `6aed1c3e89fbc8ebcb04d198`（11 跑，灰的是 1/2/3/6/7/9/10）
- 手机 `6adc5c13612e52057d021f7b`（4 跑，全部可点）

复用已有登录态各发 2 个 GET 拉回 `/meta` 和 `/data`（**没请求网页**），
逐条解析记录流与尾部后定位：

| | NumberOfRuns | 242 事件 | 零事件跑次 | 网页表现 |
|---|---|---|---|---|
| 手机 | 4 | 6 | 0 | 4 跑全可点 |
| 桌面 | 11 | 5 | 7 | 灰的正好是零事件那 7 跑 |

**结论：网页版按 `NumberOfRuns` 渲染下拉，但一项能不能点取决于这一跑有没有
`242` 目标点成绩事件。** 服务端不会替我们补事件，前端也不给空跑兜底。

我们的会话为什么会这样：

1. `detect_runs()` 按「速度 ≥1 km/h 开跑、连续 10 条静止收跑」切分，
   遇到红绿灯 / GPS 抖动会切出大量碎片：实测那 11 跑里有 7 跑条数
   `1~200`、极速 `1.2~21.6 km/h`，连 0-30 都到不了。尾部照样给每跑
   写了 68 字节，但一条 242 都没有 → 灰。
2. 第 0 跑（4117 条、极速 55.9 km/h）本该有成绩却也是 0：
   `track_setpoint` 要求「向上穿越起始速度」，BLE 数据段从运动中开始时
   第一条前面没有静止采样，这个判定永远等不到，整趟零成绩。

真机能全可点还有一层原因：它的第 4 个目标点是 **100 米距离目标**
（头部 `from=0, to=100, flags=0x13`，`isDistance=true`），几乎每跑都命中；
我们是 4 个纯速度目标，最后一个 0-150 km/h 基本不可能命中。

### 改动（`session.rs`）

- `encode_run_suffix()` 先算好每跑的成绩，**只把至少命中一个目标点的跑
  写进跑次表**，返回过滤后的 `runs`；`NumberOfRuns`、尾部、`243` 标记、
  `BestResultsRuns` 统一用过滤后的下标。采样记录仍全部留在记录流里，
  只是不再被标记为跑次起点。
- `track_setpoint()` 对 `run.start == 0` 且起点已在运动的情况，把跑次起点
  直接当计时起点（只处理这一种边缘情况），保住长数据段。
- 新增单测 `runs_without_any_result_are_dropped` /
  `first_segment_starting_in_motion_still_scores`。

预期：这条桌面会话重传后变成 5 跑（第 0 跑被救回 + 原来 4 跑），每一跑
都有 242 事件，下拉里不再有灰项。碎片跑的数据仍完整上传，只是不作为
独立跑次展示。

> 暂未改目标点定义（仍是 0-30/0-60/0-100/0-150）。改成真机那套
> 0-30/0-50/0-80/100m 能让低速跑也有成绩，但要动距离模式和头部 flags，
> 是另一处风险，等这版验证通过再说。

### 设备序列号兜底

同一条桌面会话的 `deviceSerialNumber` 仍是 0（本机 `config.json` 已填
`2254300997`，怀疑上传时配置未生效或来自另一实例）。为避免再出现
`weather=null / vehicleId=null`，改成三级取值：

配置 `cloud.device_serial`（非 0）→ 本次 `Session.device`（BLE 设备名）→
`cfg.last_device_name` → 0 并打 warn。
`racebox::serial_from_device_name()` 从
`RaceBox Mini S 2254300997 (E8:D9:EE:6D:DC:57)` 里截 10 位数字；只认
带 `RaceBox` 字样且数字 ≥8 位，避免把本地导入文件名里的 `20261106`
误当序列号。上传日志会打印「设备序列号 xxx（来源）」，便于一眼确认。
