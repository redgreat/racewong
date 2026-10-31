# RaceBox 云 API 与会话文件格式（逆向结果）

> 本文档是对 RaceBox 官方云（`api.racebox.pro`）和会话二进制文件的逆向分析结果，
> 用于在本应用（racewong）里实现「上传会话到官网 racebox.pro」。
>
> 来源：Android 版 RaceBox 2.3.0（包名 `pro.RaceBox.androidapp`，.NET MAUI）反编译，
> 配合对真实账号的网络抓包 / 接口实测。**不是官方文档**，字段含义以实测为准。
>
> 分析过程的完整笔记在 `D:\wangcw\Documents\racebox-re\README-cloud-api.md`（本仓库之外），
> 本文是浓缩后的「够用」版本。

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
| 0x04 | 2 | HeaderLength（本实现固定 316） |
| 0x06 | 1 | RecorderVersionMajor（固件主版本，真实样本 1） |
| 0x07 | 1 | RecorderVersionMinor（真实样本 11） |
| 0x08 | 2 | OtherBytesLength（本实现 0） |
| 0x0a | 2 | FileSuffixLength（本实现 0，不写圈事件） |
| 0x0c | 4 | SensorsDataLength |
| 0x10 | 1 | Type：**0=直线加速（Drag），1=赛道（Track）** |
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
| 0x44 | 88 | Track（无关联时 `"No Track"`） |
| 0x44+88 | 128 | TrackConfiguration（`"Default"`） |
| 之后 | 20 | 赛道统计（DurationMs / 圈数 / 最速 / 最大 G 等） |
| 之后 | 1+n | 传感器类型表：数量 + 每项 1 字节（GPS=1, GForce=2, Gyro=3） |
| 之后 | 4 | DataChecksum = CRC32(记录流 + 尾部) |
| 之后 | 4 | HeaderChecksum = CRC32(头部除自身外，即前 312 字节) |

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

### 4.2 Track / TrackConfiguration 固定模板

不关联赛道时整段照抄，服务端实测接受：

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

每条 8 字节。本实现 `NumberOfEventRecords = 0`、`FileSuffixLength = 0`，
上传后官网显示「整段一圈」。要支持拆圈，需要用官方赛道库的起终点线做判圈，
把结果按这个格式写进尾部，并更新头部计数和 CRC——**这是当前唯一没做的增强**。

## 5. 已验证的事实

- 用 GPX 生成会话上传后，服务端完整解析：`records / duration / maxSpeed / minSpeed / maxG` 全部回填正确
- 上传的会话立刻出现在 `syncmeta` 列表里
- 用 `"No Track"` / `"Default"` 上传的会话在云端显示为未关联赛道的普通会话，圈数为 1
- BLE 下发的每条 80 字节记录（见 `app/src/protocol.rs` 的 `Record`）与该格式的
  采样点几乎一一对应：iTOW、经纬度、海拔、速度、航向、G 力、角速度全都有
