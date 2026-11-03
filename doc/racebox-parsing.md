# RaceBox 数据解析知识库

> 这份文档是「以后改解析代码时该先看的东西」：数据从哪来、每个字节是什么意思、  
> 单位怎么换算、哪些地方踩过坑。
>
> 三份文档的分工：
>
> | 文档                                                 | 内容                                    |
> | -------------------------------------------------- | ------------------------------------- |
> | **本文** `racebox-parsing.md`                        | 解析链路、BLE 协议、80 字节记录、GPX/FIT、时间处理、坑清单  |
> | [`racebox-cloud-api.md`](racebox-cloud-api.md)     | 云接口（登录/上传/列表）、会话容器 RB V2 的逐字节表         |
> | [`cloud-upload-design.md`](cloud-upload-design.md) | 云上传功能的设计决策与交接说明                       |
>
> 所有格式都来自对 Android 版 RaceBox 2.3.0 的反编译 + 真机抓包 + 实测，  
> **不是官方文档**。字段含义以实测为准；有疑问先看第 10 节的坑清单。

---

## 0. 速查表

### 0.1 单位换算

| 量       | 原始存储                 | 换算                  | 代码位置                 |
| ------- | -------------------- | ------------------- | -------------------- |
| 经纬度     | i32，×1e7             | `v as f64 / 1e7`    | `protocol.rs`        |
| 海拔      | i32，毫米               | `/ 1000.0` → 米      | `protocol.rs`        |
| **速度**  | u32，**毫米每秒**          | `v / 1000 * 3.6` → km/h | `SPEED_KMH_PER_UNIT` |
| 航向      | i32，×1e5             | `/ 1e5` → 度         | `protocol.rs`        |
| 航向精度    | u32，×1e5             | `/ 1e5` → 度         | `protocol.rs`        |
| 水平/垂直精度 | u32，毫米               | `/ 1000.0` → 米      | `protocol.rs`        |
| G 力     | i16，×1000            | `/ 1000.0` → g      | `protocol.rs`        |
| 角速度     | i16，×100             | `/ 100.0` → °/s     | `protocol.rs`        |
| 会话文件速度  | u32，**mm/s**         | 同上，直接写 mm/s      | `racebox/session.rs` |

**速度是毫米每秒，两边一样**：反编译字段名就叫 `SpeedMMpS`，  
`UnitsConverterCore.SpeedMMpStoKPH(v) = v / 1000 * 3.6`。  
`SPEED_KMH_PER_UNIT` 就是 `3.6 / 1000 = 0.0036`，别再写成 0.6（那是错的，见第 10 节坑 3）。
| 会话文件海拔  | i32，毫米               | `× 1000`            | `racebox/session.rs` |

**速度单位踩过坑，务必按毫米每秒处理**。曾经的实现当成「0.6 km/h 一个单位」，  
速度整体放大 167 倍（实测 51 km/h 算成 8500 km/h），写进 Drag 会话头时又被  
i16 截到 327.67 km/h。验证方法：用相邻两点 GPS 位移反算速度，  
`位移速度 / 原始速度` 中位数应当约等于 1.000。

### 0.2 关键常量

```text
RECORD_SIZE            = 80      // BLE 单条记录
RBX2_HEADER_SIZE       = 136     // 转发帧头
RBX2_RECORD_SIZE       = 80
RBX2_MAX_BATCH_RECORDS = 120     // 单帧最多 120 条 ≈ 9.7KB
HEADER_LEN             = 144     // Drag 会话头（96 + 12 × 目标点数，默认 4 个）
                                 // Track 会话固定 316，我们不写
KEY_RECORD_LEN         = 35      // 会话文件全量记录
DELTA_RECORD_LEN       = 25      // 会话文件增量记录
```

### 0.3 常见故障 → 先查哪里

| 现象         | 先查                               |
| ---------- | -------------------------------- |
| 云端传上去了但不显示 | 会话类型字节（第 6.2 节），是不是写成了 Track     |
| 记录数比设备显示少  | `fix_status == 0` 的点被丢了（第 3.2 节） |
| 上传报 `Failed to parse session` | 头部结构跟会话类型对不上（第 6.2 节），Drag 却写了 Track 的头 |
| 速度明显对不上    | 速度单位当成了 0.6 km/h（第 3.2 节）      |
| 服务端算出的时长错乱 | 增量记录超限没重发键记录（第 6.3 节）            |
| 文件被拒「损坏」   | 头部 CRC 覆盖范围写错（第 6.3 节）           |
| 时间差 18 秒   | iTOW 的闰秒（第 7 节）                  |
| 轨迹跳到上周     | iTOW 跨周回绕（第 7 节）                 |

---

## 1. 数据从哪来：四条链路

```
来源                                统一形态                  去向
──────────────────────────────────────────────────────────────────────────
BLE 通知帧  → FrameReader/解码 ┐
GPX / FIT   → import.rs        ├→ Record → Session ─┬→ PostgreSQL（raw 原样）
云端会话    → （本应用未实现） ┘                     ├→ MQTT（RBX2 帧）
                                                    └→ session.rs → RaceBox 云
```

1. **BLE 实时 / 历史下载**（`ble.rs` + `worker.rs`）：设备把 80 字节记录推上来，  
   我们解码成 `Record`，按会话边界切段，边下边传。
2. **本地文件导入**（`racebox/import.rs`）：`.gpx` / `.fit` → `TrackPoint`，  
   再补成 `Record`，走同一套上传逻辑。
3. **云端会话**（接口 `GET /session/{id}/data`）：拿到的是设备原生容器，  
   理论上可以直接转发给 PostgreSQL/MQTT。**本应用目前没实现这条链路**，  
   接口只在 `racebox-cloud-api.md` 里记着，将来要做「云端备份」时用得上。
4. **上传**（`sink/*`）：PostgreSQL、MQTT、RaceBox 云三条通道并行，互不阻塞。

**设计上只有一个「记录」概念**：`Record`（80 字节 + 解码后的物理量）。  
不管数据从哪来，最后都变成 `Record`，下游不用区分来源。

---

## 2. BLE 传输层

### 2.1 服务与特征

| 用途      | UUID                                   |
| ------- | -------------------------------------- |
| 服务      | `6e400001-b5a3-f393-e0a9-e50e24dcca9e` |
| 写命令（RX） | `6e400002-b5a3-f393-e0a9-e50e24dcca9e` |
| 通知（TX）  | `6e400003-b5a3-f393-e0a9-e50e24dcca9e` |

写命令优先 `WriteType::WithResponse`，失败再退 `WithoutResponse`  
（不同固件对写类型的容忍度不一样）。

### 2.2 帧格式（UBX 风格）

```
偏移  长度  内容
0     2    同步头 B5 62
2     1    class（设备只用 0xFF）
3     1    id（消息号，见下表）
4     2    payload 长度（小端）
6     N    payload
6+N   1    CK_A
7+N   1    CK_B
```

校验是 UBX 那套「累加再累加」：

```rust
let (mut a, mut b) = (0u8, 0u8);
for &byte in &buf[2..buf.len() - 2] {
    a = a.wrapping_add(byte);
    b = b.wrapping_add(a);
}
// a == buf[len-2] && b == buf[len-1]
```

覆盖范围是 **class 到 payload 末尾**（不含同步头，不含校验本身）。

### 2.3 消息号

| ID     | 名称               | 方向    | payload        |
| ------ | ---------------- | ----- | -------------- |
| `0x22` | STATUS           | 设备→主机 | 12 字节设备状态      |
| `0x23` | DOWNLOAD_START   | 设备→主机 | u32 预计总条数      |
| `0x21` | RECORD_HISTORY   | 设备→主机 | 80 字节记录        |
| `0x01` | RECORD_LIVE      | 设备→主机 | 80 字节记录        |
| `0x26` | SESSION_BOUNDARY | 设备→主机 | 会话分界（见第 4 节）   |
| `0x24` | ERASE_PROGRESS   | 设备→主机 | 擦除百分比          |
| `0x02` | ACK              | 设备→主机 | `[0xFF, 命令ID]` |
| `0x03` | NACK             | 设备→主机 | 拒绝原因           |

### 2.4 命令（主机→设备，8 字节定长）

| 命令  | 字节                        | 说明                   |
| --- | ------------------------- | -------------------- |
| 查状态 | `B5 62 FF 22 00 00 21 62` | 回 `0x22`             |
| 下载  | `B5 62 FF 23 00 00 22 65` | 回 `0x23` + 一串 `0x21` |
| 擦除  | `B5 62 FF 24 00 00 23 68` | 回 `0x24` 进度          |

### 2.5 状态包（`0x22`）解析

```
0     1    是否正在录制（非 0 = 是）
1     1    内存占用百分比 0-100
2     1    bit0 = 是否启用安全锁
3     1    保留
4..8  4    已存条数 u32
8..12 4    最大可存条数 u32
```

界面上显示成「N/M 条」，M 为 0 时表示设备没报这个数。

### 2.6 分帧的两个坑

- **一个通知可能装半帧，也可能装多帧**：必须用 `FrameReader` 累积缓冲，  
  不要假设「一次通知 = 一个包」。
- **校验失败只丢一个字节，不要整包丢**：数据流里出现半截帧时，  
  丢一个字节重新找 `B5 62` 比丢整段更不容易丢数据。  
  `FrameReader::next_packet` 就是这么写的。

---

## 3. 80 字节记录（核心）

### 3.1 逐字节布局（小端）

| 偏移 | 长度 | 类型  | 字段                  | 换算                 |
| -- | -- | --- | ------------------- | ------------------ |
| 0  | 4  | u32 | iTOW                | GPS 周内毫秒，见第 7 节    |
| 4  | 2  | u16 | year                | 直接是年份，如 2026       |
| 6  | 1  | u8  | month               | 1-12               |
| 7  | 1  | u8  | day                 | 1-31               |
| 8  | 1  | u8  | hour                | 0-23               |
| 9  | 1  | u8  | minute              | 0-59               |
| 10 | 1  | u8  | second              | 0-59               |
| 11 | 1  | —   | 保留                  |                    |
| 12 | 4  | u32 | time_accuracy       | 未使用                |
| 16 | 4  | i32 | nanoseconds         | 纳秒部分，**可能为负**      |
| 20 | 1  | u8  | **fix_status**      | **0 = 无定位，整条记录作废** |
| 21 | 2  | —   | 保留                  |                    |
| 23 | 1  | u8  | satellites          | 卫星数                |
| 24 | 4  | i32 | longitude           | /1e7 → 度           |
| 28 | 4  | i32 | latitude            | /1e7 → 度           |
| 32 | 4  | i32 | wgs_altitude        | /1000 → 米（椭球高）     |
| 36 | 4  | i32 | msl_altitude        | /1000 → 米（海拔高）     |
| 40 | 4  | u32 | horizontal_accuracy | /1000 → 米          |
| 44 | 4  | u32 | vertical_accuracy   | /1000 → 米          |
| 48 | 4  | u32 | speed               | **mm/s，`/1000*3.6` → km/h** |
| 52 | 4  | i32 | heading             | /1e5 → 度           |
| 56 | 4  | u32 | speed_accuracy      | 未使用                |
| 60 | 4  | u32 | heading_accuracy    | /1e5 → 度           |
| 64 | 2  | u16 | pdop                |                    |
| 66 | 2  | —   | 保留              |            |
| 68 | 2  | i16 | gforce_x        | /1000 → g  |
| 70 | 2  | i16 | gforce_y        | /1000 → g  |
| 72 | 2  | i16 | gforce_z        | /1000 → g  |
| 74 | 2  | i16 | rotation_rate_x | /100 → °/s |
| 76 | 2  | i16 | rotation_rate_y | /100 → °/s |
| 78 | 2  | i16 | rotation_rate_z | /100 → °/s |

`Record.raw` 原样保存这 80 字节：MQTT 通道直接转发它，PostgreSQL 也存原始块，  
所以解码只是「给界面和云上传用的视图」，不能改动 raw。

### 3.2 四个必须知道的坑

**① `fix_status == 0` 直接丢弃。** `Record::decode` 返回 `None`，  
下载计数里算「收到」但不算「有效」，界面上显示成「（跳过无定位 N 条）」。  
这是**故意的**：没有定位的点经纬度是 0,0，传给云端会把统计拉歪。  
代价是记录数会比设备显示的少，用户会问，所以界面必须明确写出跳过了多少条。

**② 速度是毫米每秒，不是「0.6 km/h 一个单位」。** 反编译确认字段名 `SpeedMMpS`，  
`km/h = mm/s / 1000 * 3.6`。会话文件里也是同一个单位，直接抄过去就行。  
写错成 `× 0.6` 会让速度放大 167 倍，会话头里的极速再被 i16 截到 327.67。

**③ 航向是 ×1e5，不是 ×100。** 和 G 力、角速度的精度不一样，  
这四组值的换算系数各不相同，抄的时候别看串行。

**④ 两个海拔都要留着。** `wgs_altitude` 是 WGS84 椭球高，`msl_altitude`  
是海拔高（大地水准面之上），两者差几十米，取决于当地的大地水准面起伏。  
会话文件用 MSL（`record_to_point` 里的优先级），椭球高只在 MSL 为 0 时兜底。

### 3.3 时间字段是「冗余」的

一条记录里同时有 iTOW 和年月日时分秒。两者本该一致，实测差不到 1 秒——  
因为日期时间字段只存整秒，iTOW 有毫秒。**不要试图用一个去修正另一个**，  
按用途取：排序 / 分日期用日期字段，算相对时间用 iTOW。

---

## 4. 会话边界与分段上传

设备下载是「一大串记录流」，中间用 `0x26 SESSION_BOUNDARY` 切开会话。

```text
DOWNLOAD_START(总条数)
  记录 记录 记录 ...          ← 第 1 个会话
  SESSION_BOUNDARY
  记录 记录 记录 ...          ← 第 2 个会话
  SESSION_BOUNDARY
  ...
  ACK [FF 23]                ← 下载结束
```

**为什么要按边界切段上传**：一个会话可能有几万条记录（几十 MB），  
攒到最后一次性传，中途断网就全丢了；按会话切段可以「下完一段传一段」，  
下载和上传并行，失败也只影响那一段。

`worker.rs` 里的处理：

- 收到 `0x26` → 把 `current` 里攒的记录交给 `spawn_flush` 后台任务，清空继续收；
- 收到 `ACK [FF 23]` → 下载结束，把最后一段也交出去；
- 用户取消 → 把手头这段直接落盘（`abort_and_spool`），不丢数据；
- 两道超时：整轮 `DOWNLOAD_TIMEOUT = 1800s`（半小时还没下完就算了），  
  单次等待 `DOWNLOAD_IDLE_TIMEOUT = 120s`（两分钟没有通知就当设备假死）。

---

## 5. RBX2 转发帧（发给 PostgreSQL / MQTT）

一条 `Record` 一个 80 字节块，多块拼成一帧。帧头 136 字节：

| 偏移  | 长度 | 字段                 | 说明                         |
| --- | -- | ------------------ | -------------------------- |
| 0   | 4  | magic              | `"RBX2"`                   |
| 4   | 1  | version            | 2                          |
| 5   | 1  | flags              | bit0 = 整轮同步结束；bit1 = 本会话结束 |
| 6   | 2  | header_size        | 136                        |
| 8   | 2  | record_size        | 80                         |
| 10  | 2  | count              | 本帧记录数                      |
| 12  | 4  | sync_offset        | 本帧在整轮同步里的起始序号              |
| 16  | 4  | sync_total         | 整轮同步预计总数                   |
| 20  | 4  | sync_date          | 形如 `20261011`              |
| 24  | 16 | sync_id            | 整轮同步的 UUID                 |
| 40  | 48 | device             | 设备名，NUL 填充                 |
| 88  | 4  | session_index      | 当天第几个会话（从 1 起）             |
| 92  | 4  | session_offset     | 本帧在会话内的起始序号                |
| 96  | 4  | session_total      | 本会话总条数                     |
| 100 | 8  | session_start_utc  | 会话起始时间，形如 `20261011220830` |
| 108 | 8  | session_end_utc    | 会话结束时间                     |
| 116 | 4  | session_start_itow | 首条记录的 iTOW                 |
| 120 | 4  | session_start_nano | 首条记录的纳秒                    |
| 124 | 4  | —                  | 保留                         |
| 128 | 4  | payload_crc        | CRC32(记录流)                 |
| 132 | 4  | header_crc         | CRC32(帧头前 132 字节)          |

**两个 CRC 覆盖范围不一样，写错就是整帧被判损坏**：  
payload 只算记录流，header 只算前 132 字节（不含自己）。

单帧最多 120 条（`RBX2_MAX_BATCH_RECORDS`）：协议上限是 720，但 broker  
普遍限制单包 10KB，`136 + 120×80 ≈ 9.7KB`，留了余量。

---

## 6. RaceBox 会话容器（RB V2）

完整逐字节表在 [`racebox-cloud-api.md`](racebox-cloud-api.md) 第 4 节，  
这里只列改代码时最常动的几个。

### 6.1 结构

头部长度**随会话类型变**，服务端就是按 `HeaderLength` 定位记录流的：

```
Drag（我们用）  HeaderLength = 96 + 12N，默认 N=4 → 144
Track（不用）   HeaderLength = 316

[0 .. header_len)             头部（自带 CRC32）
[header_len .. +data_len)     记录流：键记录 35 字节 / 增量记录 25 字节
[.. + suffix_len)             尾部事件（本实现长度 0）
```

### 6.2 类型字节：Drag 还是 Track（重要）

偏移 `0x10`（十进制 16）一个字节：

| 值 | 类型             | 云端行为                             |
| - | -------------- | -------------------------------- |
| 0 | **Drag（直线加速）** | 不要求赛道关联，直接算 0-100 / 0-200 / 分段加速 |
| 1 | Track（赛道）      | 拿会话里的赛道去匹配赛道库，匹配不上归进「未关联赛道」      |

**本应用固定写 0（Drag）。** 之前写 1 的后果是：会话带了 `"No Track"`，  
云端匹配不到赛道，`sessions?type=track` 列表里根本不显示，传了等于白传。  
我们的数据本来就没有起终点线、没有圈速可言，写 Drag 才是对的。

**改类型不能只改这一个字节——两种类型的头部结构完全不同。**  
0.1.20 就栽在这：类型写了 0，头部还按 Track 的 316 字节排，  
服务端按 Drag 布局读到 `68 + 16 + 12×N` 就发现位置对不上 `HeaderLength`，  
直接回 `Failed to parse session`，一条都传不上去。

#### Drag 头部（偏移 68 起，共 `16 + 12N` 字节）

| 偏移  | 长度 | 字段                | 我们填的值                                  |
| --- | -- | ----------------- | -------------------------------------- |
| 68  | 1  | SessionMode       | `1` = SpeedMetric（按速度区间算成绩）           |
| 69  | 1  | FtRolloutEnabled  | `0`（不启用一英尺起步）                          |
| 70  | 1  | SetpointCount     | `4`（目标点个数 N）                           |
| 71  | 1  | ChristmasTree     | `0`（`倒计时 << 4 \| 模式`，不要起跑灯）           |
| 72  | 4  | 首点纬度 i32 ×1e7     | 记录流第一个点                                |
| 76  | 4  | 首点经度 i32 ×1e7     | 记录流第一个点                                |
| 80  | 2  | TopSpeed i16 ×100 | 极速 km/h × 100，i16 上限 327.67 km/h       |
| 82  | 2  | NumberOfRuns u16  | `0`，交给服务端算                            |
| 84  | 12N | 目标点数组            | 见下                                     |

每个目标点 12 字节 = 定义 6 字节 + 成绩 4 字节 + 跑次 2 字节：

```
u16 StartSpeed        起始速度（km/h）
u16 EndSpeed|Distance 目标速度（km/h）；距离模式下是距离（米）
u8  flags             bit0=已配置 bit1=距离模式 bit2=起始速度用英里
                      bit4-7=单位（15=km/h，14=mph）
u8  保留              0
i32 BestResults       最好成绩（毫秒）；0 = 还没成绩
i16 BestResultsRuns   成绩所在跑次；-1 = 没有
```

默认 4 个目标点 `0→60 / 0→100 / 0→150 / 0→200 km/h`，flags 全填 `0xF1`  
（已配置 + km/h），成绩填 `0` / `-1` 让服务端自己算。

紧跟其后是传感器类型表和两个 CRC：

```
header_len-12      传感器类型数 = 3
header_len-11..-8  类型：1=GPS 2=GForce 3=Gyroscope
header_len-8       DataChecksum
header_len-4       HeaderChecksum
```

`HeaderLengthCompute = 68 + Drag段 + OtherBytesLength + 1 + SensorTypes.Count + 8`，  
`OtherBytesLength = 0`、`SensorTypes.Count = 3`，所以默认正好 144。

### 6.3 记录流的编码规则

会话文件不存绝对坐标，而是「键记录 + 增量记录」混合，省空间：

- **键记录（首字节 0，35 字节）**：全量字段。
- **增量记录（首字节 1-200，25 字节）**：只存相对上一个键记录的差值。

**什么时候必须重发键记录**（照抄固件 `SensorsDataRecord.WriteToStream`）：

| 条件                   | 原因                |
| -------------------- | ----------------- |
| 第一条                  | 没有基准              |
| 与上一条间隔 > 2000 ms     | 增量时间字段是 u16，超了算不下 |
| 时间倒流（dt < 0）         | 增量无符号，负数会变成巨大值    |
| 连续增量超过 200 条         | 首字节只能表达到 200      |
| 经纬度/海拔/速度的增量超 ±16384 | i16 装不下           |

漏掉任何一条，服务端算出来的时间轴就会错乱。

字段单位（逐字节表见 [`racebox-cloud-api.md`](racebox-cloud-api.md) 4.3）：
纬度/经度/海拔/速度在增量记录里是**差值**，**航向是绝对值 `u16 ×100`**。
航向别用 i16——360° 要 36000，超过 i16 上限 32767，328°-360° 会被截成 327.67°。

### 6.4 CRC 覆盖范围

```
header_len-8   DataChecksum   = CRC32(记录流 + 尾部事件)
header_len-4   HeaderChecksum = CRC32(头部前 header_len-4 字节)
```

Track 会话就是 `312` / `316` 那组数；Drag 是 `140` / `144`。**别写死常量**，  
一律用 `header_len` 算。

头部 CRC **不含自己那 4 字节**。先把其他字段都填好，再算 CRC 写入——  
代码里之所以要 `let header_crc = crc32(&h[..header_len - 4]);` 单独一行，  
是因为同一句里既可变借用 `h` 又只读借用会被借用检查拦下来。

### 6.5 空会话不要发

采样点为空时 `encode_session` 返回 `None`，`sink/racebox.rs` 直接报  
「这一段没有有效记录」。服务端对空会话会返回错误，发了也没用。

---

## 7. 时间：iTOW / GPS 周 / 闰秒

`iTOW` = GPS 周内毫秒数（u-blox 的叫法，Time Of Week）。

```text
iTOW = (Unix毫秒 - 本周日00:00UTC) + 18000，再对一周取模
一周 = 604800000 ms
```

三条要注意：

1. **GPS 周从周日 00:00 UTC 起算**，不是周一，也不是 1970 年。  
   1970-01-01 是周四，所以第一个周日是 1970-01-04。
2. **+18000 是闰秒**：GPS 时不跳秒，比 UTC 快 18 秒（2017 年起）。  
   不加这 18 秒，算出来的 iTOW 会和真实设备差 18 秒。
3. **iTOW 会跨周回绕**：`u32` 存一周毫秒（604800000 < 2^32），  
   周日零点归零。相邻两条记录的 iTOW 差值超过半周，就说明跨周了，  
   要补一整周。`records_from_points` 里就是这么处理的。


设备下发的记录里 iTOW 和日期时间**都**有，正常情况下直接用；
只有从 GPX/FIT 导入时才需要自己算（`itow_from_unix`）。

---

## 8. GPX 解析

`racebox/import.rs`，手写不用库——GPX 只是「一堆带属性的 trkpt」。

### 8.1 规则

- 依次尝试 `trkpt` → `rtept` → `wpt`，哪个有数据用哪个
  （有些设备只写航点）。
- **必须有 `<time>`**，格式 RFC3339。没有时间的点直接丢，
  因为时间轴是算速度和 G 力的基础。
- 自闭合的 `<trkpt lat=".." lon=".." />` 跳过：没有子元素，海拔和时间都拿不到。
- `<ele>` 可选，缺省 0。

### 8.2 补出来的量（GPX / FIT 共用 `finalize`）

GPX 里只有坐标和时间，其余全是差分算的；FIT 可能自带速度（字段 6 / 78）：

| 量 | 算法 |
| --- | --- |
| 速度 | haversine 距离 / 时间差（文件自带速度时优先用文件） |
| 航向 | 相邻两点的 bearing |
| 纵向 G (`gx`) | 速度变化 / 时间差 / 9.80665，clamp 到 ±2 |
| `gy` / `gz` | 0 / 1.0（没有加速度计，横向 G 无从得知） |
| 角速度 | 全 0 |

`gz = 1.0` 是刻意的：静止时重力加速度在 z 轴上，写 0 会让「最大 G」
统计变成 0。写 1.0 表示「按正常重力算」。

### 8.3 清洗

- 坐标非有限值、或正好是 `(0,0)` 的点丢掉；
- 时间戳非递增的点丢掉（同一毫秒重复点会算出 0 速度）；
- 清洗后不足 2 个点就报错——少于两点算不出速度。

---

## 9. FIT 解析

Garmin 的二进制格式，自描述，但只需要 `record` 消息里的几个字段。

### 9.1 文件结构

```
[0..header_size)     头部（12 或 14 字节）
[header_size..+data_size)  数据区
```

头部关键字段：

| 偏移 | 内容 |
| --- | --- |
| 0 | header_size（12 或 14） |
| 4..8 | data_size（u32） |
| 8..12 | 签名 `".FIT"` |

### 9.2 数据消息

每条消息第一个字节是「记录头」：

| 位 | 含义 |
| --- | --- |
| bit7 | 1 = 压缩时间戳头（省掉时间字段） |
| bit6 | 1 = definition 消息（定义后续数据布局） |
| bit5 | 1 = 带开发者字段 |
| bit0-3 | local message type（0-15） |

**definition 消息**：后面跟 `reserved(1) + arch(1) + global_num(2) + n(1)`，
然后 n 组「字段号(1) + 大小(1) + 基类型(1)」。arch=1 表示大端。

**数据消息**：按 definition 里记录的字段顺序依次读，每个字段占它声明的大小。

**压缩时间戳头**：bit7 置位时 local type 在 bit5-6，且不存时间字段。
我们按普通数据消息处理——`record` 消息里本来就有完整的 timestamp。

### 9.3 只认 global 20（record）

| 字段号 | 含义 | 换算 |
| --- | --- | --- |
| 253 | timestamp | u32，加 FIT epoch |
| 0 | position_lat | sint32，× semicircle |
| 1 | position_long | sint32，× semicircle |
| 2 | altitude | u16，`/5 - 500` → 米 |
| 73 | enhanced_altitude | u32，`/5 - 500` → 米 |
| 6 | speed | u16，`/1000` → m/s |
| 78 | enhanced_speed | u32，`/1000` → m/s |

两个常量：

```text
FIT_EPOCH_UNIX = 631065600        // 1989-12-31T00:00:00Z
SEMICIRCLE     = 180 / 2^31       // 经纬度单位
```

**FIT 的 epoch 不是 1970**，忘了加这个偏移，时间会跑到 1989 年。

---

## 10. 坑清单（都踩过）

| # | 现象 | 原因 | 处理 |
| --- | --- | --- | --- |
| 1 | 云端传上去了，app 里看不到 | 类型写成 Track，赛道匹配不上 | 固定写 Drag（6.2） |
| 2 | `Failed to parse session`，一条都传不上去 | 类型改成 Drag，头部还是 Track 的 316 字节 | 整个头部按 Drag 重排，`header_len = 96 + 12N`（6.2） |
| 3 | 记录数比设备少几十条 | `fix_status == 0` 的点被丢 | 界面明示「跳过无定位 N 条」 |
| 4 | 速度放大 167 倍 | 当成「0.6 km/h 一个单位」 | 是 mm/s，`/1000*3.6`（0.1） |
| 5 | 航向差 1000 倍 | 当成 ×100 换算 | 是 ×1e5 |
| 6 | 服务端时长错乱 | 增量记录没按规则重发键记录 | 6.3 的五条规则 |
| 7 | 文件被判损坏 | CRC 覆盖范围算错 | 头部前 `header_len-4`，记录流单独算 |
| 8 | 时间差 18 秒 | 忘了 GPS 闰秒 | 7 节 |
| 9 | 轨迹跳到上周 | iTOW 跨周回绕没处理 | 差值超半周补一周 |
| 10 | GPX 一个点都读不到 | 点没有 `<time>`，或全是自闭合标签 | 8.1 |
| 11 | FIT 时间跑到 1989 | 忘了加 FIT epoch | 9.3 |
| 12 | 空会话上传报错 | 采样点为空 | `encode_session` 返回 None 时提前返回 |
| 13 | 单帧超过 10KB 被 broker 拒 | 一次塞了太多记录 | 上限 120 条 |
| 14 | 航向 328°-360° 的点方向全错 | 会话文件里航向写成 i16 ×100，上限 327.67° | 改成 u16 ×100（6.3） |

---

## 11. 调试与验证

### 11.1 日志

Windows 上在 `%APPDATA%\RaceBox Sync\racewong.log`，按级别滚动。
云上传会打印：

```text
RaceBox 云: Drag 会话 <id> 已上传，<n> 条记录，<秒> 秒
第4段 RaceBox 云 上传失败，2 秒后重试（第 1/2 次）: ...
```

「传了但看不到」先看这行，确认类型是 Drag、条数不是 0。

### 11.2 单测

解析逻辑的单测都在各模块底部（`#[cfg(test)]`），改完先跑：

```bash
cd app && cargo test
```

值得注意的几个：

| 测试 | 守住什么 |
| --- | --- |
| `record_decodes_all_fields` | 80 字节记录的每个偏移 |
| `record_without_fix_is_dropped` | fix_status=0 必须丢 |
| `session_type_is_drag` | 类型字节 + 三个圈字段为 0 |
| `header_is_consistent` | 会话头长度 / CRC / 类型表 |
| `records_walk_to_the_end` | 记录流能被逐条走完 |
| `long_gap_forces_key_record` | 间隔 2 秒必须重发键记录 |
| `itow_matches_real_device_sample` | iTOW 公式对得上真机样本 |
| `fit_minimal_roundtrip` | FIT 手工拼的最小文件能解出来 |

### 11.3 本机没有 MSVC 时

`cargo build` / `cargo test` 要链接器，`build.rs` 里 winres 需要 `link.exe`。
只想做类型检查的话，临时把 `build.rs` 挪开：

```bash
cd app
mv build.rs build.rs.tmpcheck
cargo check --release --locked --offline
mv build.rs.tmpcheck build.rs
```

要出安装包交给 CI（推 `v*` 标签，`windows-latest` 自带 MSVC + WiX）。

### 11.4 验云接口

上传成功但列表里没有，先用 curl 确认账号侧：

```bash
# 登录拿 auth
curl -s -X POST https://api.racebox.pro/v1/login \
  -H 'Content-Type: application/json' \
  -d '{"email":"...","password":"...","clientId":"<GUID>","platform":"Windows","version":"1.0","devManufacturer":"x","devModel":"y","osVersion":"Windows"}'

# 同步会话列表（注意是 POST，body 格式见 racebox-cloud-api.md 第 2 节）
curl -s -X POST https://api.racebox.pro/v1/session/syncmeta \
  -H 'Cookie: racebox-api-auth=<auth>' \
  -H 'Content-Type: application/json' -d '{}'
```

网页端按类型筛：`https://www.racebox.pro/webapp/sessions?type=drag`。

---

## 12. 代码地图

| 文件 | 职责 | 改这里之前看 |
| --- | --- | --- |
| `app/src/protocol.rs` | BLE 帧、80 字节记录、RBX2 帧 | 本文 2、3、5 节 |
| `app/src/ble.rs` | 蓝牙连接、订阅、写命令 | 2.1 |
| `app/src/worker.rs` | 下载循环、会话切段、上传调度 | 4 |
| `app/src/racebox/mod.rs` | `Record` ↔ `TrackPoint` 换算 | 0.1 单位表 |
| `app/src/racebox/session.rs` | RB V2 容器编码 | 6、7 |
| `app/src/racebox/import.rs` | GPX / FIT 解析 | 8、9 |
| `app/src/racebox/api.rs` | 云接口、错误分类 | `racebox-cloud-api.md` |
| `app/src/racebox/store.rs` | 云端账号凭据（单独文件，不进 config.json） | — |
| `app/src/sink/*.rs` | PostgreSQL / MQTT / 云三条上传通道 | `cloud-upload-design.md` |

---

## 13. 还没做的

- **赛道会话（带圈速）**：要用官方赛道库的起终点线做判圈，
  把结果按 8 字节事件写进尾部并更新头部计数和 CRC。当前固定 Drag，不做。
- **流式上传**：RaceBox 云是整段一次性 POST，服务端不报进度，
  所以这一路只有「上传中…」和结束后的平均速度。
  要真实时进度得改 `Body::wrap_stream` + chunked，有被服务端拒收的风险。
- **横向 G / 角速度**：GPX / FIT 导入的数据里没有，只能给 0 / 1.0。
  设备直连下来的记录里有真值。
- **会话文件里的航向**：`write_key` / `write_delta` 都写 `u16(heading * 100)`，
  和 [`racebox-cloud-api.md`](racebox-cloud-api.md) 4.3 的字段表一致。
  曾经误写成 `i16`，328°-360° 会溢出（i16 上限 327.67°），
  实测 2902 个点里有 547 个被截断——已修，改完记得跑
  `session_header_is_drag` 之外再抽查一段真实数据的航向分布。
