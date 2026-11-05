//! RaceBox 会话文件（RB V2 容器）编码器。
//!
//! 为什么需要自己写：RaceBox 官网只认设备原生的二进制会话文件，
//! 上传接口 `POST /v1/session` 收到的是裸字节，服务端解析后回填圈速、
//! 极速、最大 G。GPX / FIT 这些通用格式它不认识，必须先转成这个容器。
//!
//! 格式来自对 Android 版 2.3.0 的反编译（`RaceBox.Core.SessionData.V2`），
//! 完整字段表见 `docs/racebox-cloud-api.md`。
//!
//! 类型固定写 **Drag（直线加速）**：我们传的是摩托车 / 汽车的加速数据，
//! 本来就没有起终点线、没有圈速可言。写成 Track 会让云端去匹配赛道，
//! 匹配不上（`"No Track"`）的会话在 app 里干脆不显示——等于白传。
//!
//! **两种类型的头部结构完全不同，不是只改一个类型字节**：
//! - Track：`Track(88) + TrackConfiguration(128) + 统计(20)`，头长固定 316；
//! - Drag：`SessionMode / FT rollout / 目标点数量 / 圣诞树 / 首点坐标 / 极速 / 跑次`
//!   共 16 字节，后面跟 N 个加速目标点（每个占 12 字节），头长 `96 + 12N`。
//!
//! 把 Track 的头塞进 Drag 会话，服务端读完 `HeaderLength` 就发现位置对不上，
//! 直接回 `Failed to parse session`——这就是改成 Drag 后一条都传不上去的原因。
//!
//! 布局（Drag，N = 目标点数量）：
//! ```text
//! [0 .. 68)                    基础头（版本 / 时间 / 会话 id / 设备序列号）
//! [68 .. 68+16+12N)            Drag 专属段（含 N 个加速目标点）
//! [.. + 1+3)                   传感器类型表：数量 + GPS / GForce / Gyroscope
//! [.. + 8)                     DataChecksum + HeaderChecksum
//! [header_len .. + data_len)   传感器记录流（键记录 35 字节 / 增量记录 25 字节）
//! ```
//!
//! 尾部事件长度为 0：Drag 不需要圈事件，服务端按记录流自己算加速成绩。

use crate::protocol::Record;
use chrono::{Datelike as _, Timelike as _};
use crc32fast::hash as crc32;

/// 会话类型：0 是直线加速（Drag），1 是赛道（Track）。
///
/// 一律写 Drag。云端对 Track 会话会去找起终点线匹配赛道，我们传的会话
/// 关联的是 `"No Track"`，匹配不上就被归进「未关联赛道」，webapp 的
/// 赛道列表里看不到。Drag 不要求赛道、也不算圈速，服务端直接出
/// 0-100 / 0-200 这些加速分析，正是我们要的。
const SESSION_TYPE_DRAG: u8 = 0;

/// `DragSessionMode::SpeedMetric`：目标点按「从起始速度加到目标速度」定义。
const DRAG_MODE_SPEED_METRIC: u8 = 1;
/// `DeviceUnits::KillometersPerHour`，写在目标点 units 半字节里。
const UNITS_KPH: u8 = 15;

/// Drag 专属段的固定部分：SessionMode / FT rollout / 目标点数量 / 圣诞树 /
/// 首点纬度 / 首点经度 / 极速 / 跑次，共 16 字节。
const DRAG_FIXED_LEN: usize = 16;
/// 每个加速目标点在头部占 12 字节：6 字节定义 + 4 字节最好成绩 + 2 字节成绩所在跑次。
const DRAG_PER_SETPOINT: usize = 12;
/// 传感器类型数量：GPS / GForce / Gyroscope。
const SENSOR_TYPE_COUNT: usize = 3;

/// 默认加速目标点：(起始速度, 目标速度)，单位 km/h。
///
/// 只声明「想算什么」，成绩由我们自己按记录流算好写进尾部/事件（头部那两个字段也要填）。
/// 一次加速跑同时命中多个目标就记多条。
///
/// 选 0-30 / 0-60 / 0-100 / 0-150：市区骑行极速常在 60-80 km/h，只有 0-60 的话
/// 大部分跑次点进去是空的；0-30 几乎每跑都能命中，详情页才有内容可看。
const DEFAULT_SETPOINTS_KPH: [(u16, u16); 4] = [(0, 30), (0, 60), (0, 100), (0, 150)];

/// Drag 会话的头部长度：68 基础 + Drag 段 + 类型表(1+3) + 两个 CRC。
///
/// 赛道会话固定 316，Drag 随目标点数量变——服务端按这个值定位记录流，
/// 算错就是 `Failed to parse session`。
pub fn header_len() -> usize {
    68 + DRAG_FIXED_LEN + DEFAULT_SETPOINTS_KPH.len() * DRAG_PER_SETPOINT + 1 + SENSOR_TYPE_COUNT + 8
}

const KEY_RECORD_LEN: usize = 35;
const DELTA_RECORD_LEN: usize = 25;
const RECORD_PREFIX_KEY: u8 = 0;
const MAX_DELTA_RECORDS: u8 = 200;
const MAX_DELTA_MS: i64 = 2000;
const MAX_DELTA_RAW: f64 = 16384.0;

/// 一个采样点。字段单位与文件里的原始整数一一对应，转换放在编码时做，
/// 这样上层（BLE 记录 / GPX / FIT）只要填物理量就行。
#[derive(Clone, Copy, Debug, Default)]
pub struct TrackPoint {
    /// GPS 周内毫秒（u-blox 的 iTOW）。由时间戳换算，见 [`itow_from_unix`]。
    pub itow: u32,
    pub lat: f64,
    pub lng: f64,
    /// 海拔，米（写进文件时 ×1000）。
    pub alt: f64,
    /// 速度，毫米每秒。
    pub speed_mmps: u32,
    /// 航向，度。
    pub heading: f64,
    pub gx: f64,
    pub gy: f64,
    pub gz: f64,
    /// 角速度，度每秒。
    pub rx: f64,
    pub ry: f64,
    pub rz: f64,
    /// 水平定位精度，米。写进记录流里的系统记录（240），云端详情页显示「GPS Accuracy」。
    /// 0 表示未知——GPX / FIT 导入没有这个量，就不写系统记录了。
    pub hacc_m: f64,
    /// 参与定位的卫星数。同上。
    pub sats: u8,
}

/// 会话头部的元信息。
pub struct SessionMeta {
    /// 12 字节会话 id，云端返回的 id 就是它的十六进制。
    pub session_id: [u8; 12],
    /// UTC 起始时间 (年, 月, 日, 时, 分, 秒)。
    pub utc: (i32, u32, u32, u32, u32, u32),
    /// 时区偏移，分钟（+480 = UTC+8）。
    pub tz_offset_min: i16,
    /// 设备序列号。0 表示不指定，官网会显示成未知设备。
    pub device_serial: u64,
    /// 记录器固件版本（主, 次）。沿用真实设备样本的 1.11。
    pub recorder: (u8, u8),
}

/// GPS 周起点（周日 00:00 UTC）的 Unix 秒。
///
/// 1970-01-01 是周四，所以第一个周日是 1970-01-04（偏移 3 天）。
fn gps_week_start_unix(unix_secs: i64) -> i64 {
    const DAY: i64 = 86_400;
    const FIRST_SUNDAY: i64 = 3 * DAY;
    if unix_secs >= FIRST_SUNDAY {
        FIRST_SUNDAY + (unix_secs - FIRST_SUNDAY) / (7 * DAY) * 7 * DAY
    } else {
        FIRST_SUNDAY - ((FIRST_SUNDAY - unix_secs + 7 * DAY - 1) / (7 * DAY)) * 7 * DAY
    }
}

/// Unix 毫秒 → iTOW。
///
/// GPS 时比 UTC 快 18 秒（2017 年起的闰跳），设备写进文件的 iTOW 是 GPS 周内毫秒，
/// 所以要先加 18 秒再对一周取模。实测样本与这条公式只差不到 1 秒，
/// 差值来自 UTC 字段只存整秒。
pub fn itow_from_unix(unix_ms: i64) -> u32 {
    const WEEK_MS: i64 = 7 * 86_400_000;
    const GPS_UTC_LEAP_MS: i64 = 18_000;
    let secs = unix_ms.div_euclid(1000);
    let week_start = gps_week_start_unix(secs) * 1000;
    let v = (unix_ms - week_start + GPS_UTC_LEAP_MS).rem_euclid(WEEK_MS);
    v as u32
}

/// f64 → i16，超出范围就饱和（Rust 的 float→int 转换本身就饱和，不会 panic）。
fn i16_of(v: f64) -> i16 {
    v as i16
}

/// 判断当前点能不能写成增量记录；不能就重发键记录。
///
/// 规则照抄固件 `SensorsDataRecord.WriteToStream`：首个点、间隔超过 2 秒、
/// 时间倒流、增量序号超过 200、任一增量超出 ±16384，都要重发键记录。
fn needs_key(last: Option<&TrackPoint>, p: &TrackPoint, idx: u8) -> bool {
    let Some(l) = last else { return true };
    let dt = p.itow as i64 - l.itow as i64;
    dt > MAX_DELTA_MS
        || dt < 0
        || idx > MAX_DELTA_RECORDS
        || ((p.lat - l.lat) * 1e7).abs() > MAX_DELTA_RAW
        || ((p.lng - l.lng) * 1e7).abs() > MAX_DELTA_RAW
        || ((p.alt - l.alt) * 1000.0).abs() > MAX_DELTA_RAW
        || (p.speed_mmps as i64 - l.speed_mmps as i64).abs() > MAX_DELTA_RAW as i64
}

/// 航向写成 **u16 ×100**（0..36000）。
///
/// 别用 i16：360° 需要 36000，超过 i16 上限 32767，328°-360° 会被截成 327.67°。
/// 实测一段 2902 点的数据里有 547 个点落在这个区间，方向全错。
fn u16_heading(deg: f64) -> u16 {
    let d = if deg.is_finite() {
        deg.rem_euclid(360.0)
    } else {
        0.0
    };
    (d * 100.0).round() as u16
}

fn write_key(out: &mut Vec<u8>, p: &TrackPoint) {
    out.push(RECORD_PREFIX_KEY);
    out.extend_from_slice(&p.itow.to_le_bytes());
    out.extend_from_slice(&((p.lat * 1e7) as i32).to_le_bytes());
    out.extend_from_slice(&((p.lng * 1e7) as i32).to_le_bytes());
    out.extend_from_slice(&((p.alt * 1000.0) as i32).to_le_bytes());
    out.extend_from_slice(&p.speed_mmps.to_le_bytes());
    out.extend_from_slice(&u16_heading(p.heading).to_le_bytes());
    write_motion(out, p);
}

fn write_delta(out: &mut Vec<u8>, p: &TrackPoint, last: &TrackPoint, idx: u8) {
    out.push(idx);
    out.extend_from_slice(&((p.itow - last.itow) as u16).to_le_bytes());
    out.extend_from_slice(&i16_of((p.lat - last.lat) * 1e7).to_le_bytes());
    out.extend_from_slice(&i16_of((p.lng - last.lng) * 1e7).to_le_bytes());
    out.extend_from_slice(&i16_of((p.alt - last.alt) * 1000.0).to_le_bytes());
    out.extend_from_slice(&i16_of(p.speed_mmps as f64 - last.speed_mmps as f64).to_le_bytes());
    // 航向是绝对值，不参与增量
    out.extend_from_slice(&u16_heading(p.heading).to_le_bytes());
    write_motion(out, p);
}

/// G 力与角速度在两种记录里都是绝对值，不参与增量。
fn write_motion(out: &mut Vec<u8>, p: &TrackPoint) {
    out.extend_from_slice(&i16_of(p.gx * 1000.0).to_le_bytes());
    out.extend_from_slice(&i16_of(p.gy * 1000.0).to_le_bytes());
    out.extend_from_slice(&i16_of(p.gz * 1000.0).to_le_bytes());
    out.extend_from_slice(&i16_of(p.rx * 100.0).to_le_bytes());
    out.extend_from_slice(&i16_of(p.ry * 100.0).to_le_bytes());
    out.extend_from_slice(&i16_of(p.rz * 100.0).to_le_bytes());
}

/// 把导入的轨迹点补成设备记录（连 raw 一起填好）。
///
/// `.gpx` / `.fit` 解析出来只有物理量 + iTOW，没有设备记录里的日期时间字段；
/// 而 PostgreSQL 要按年月日时分秒入库、MQTT 要原样转发记录块，所以这里按
/// [`Record::decode`] 的布局反着写一回，让本地文件走上传通道时
/// 和设备下来的数据长得一模一样。
///
/// `start_unix_ms` 是首个采样点的 UTC 毫秒，后面每个点按 iTOW 的增量推时间——
/// iTOW 跨周会回绕，所以差值超过半周就补（减）一整周。
pub fn records_from_points(points: &[TrackPoint], start_unix_ms: i64) -> Vec<Record> {
    const WEEK_MS: i64 = 7 * 86_400_000;
    if points.is_empty() {
        return Vec::new();
    }
    let first = points[0].itow as i64;
    points
        .iter()
        .map(|p| {
            let mut delta = p.itow as i64 - first;
            if delta < -WEEK_MS / 2 {
                delta += WEEK_MS;
            } else if delta > WEEK_MS / 2 {
                delta -= WEEK_MS;
            }
            let unix_ms = start_unix_ms + delta;
            let dt = chrono::DateTime::<chrono::Utc>::from_timestamp_millis(unix_ms)
                .unwrap_or(chrono::DateTime::<chrono::Utc>::UNIX_EPOCH);
            record_from_point(p, unix_ms, dt)
        })
        .collect()
}

/// 单个点 -> 一条记录：先按布局写 raw，再把解码后的字段填回去。
fn record_from_point(
    p: &TrackPoint,
    unix_ms: i64,
    dt: chrono::DateTime<chrono::Utc>,
) -> Record {
    let mut raw = [0u8; crate::protocol::RECORD_SIZE];
    let year = dt.year().max(0) as u16;
    let nanos = ((unix_ms.rem_euclid(1000)) * 1_000_000) as i32;
    // 文件里没有定位质量和星数：写 0 表示未知，比编一个好看的数诚实
    raw[20] = 1; // fix_status：0 会被 decode 判成无效点
    put_u32(&mut raw, 0, p.itow);
    put_u16(&mut raw, 4, year);
    raw[6] = dt.month() as u8;
    raw[7] = dt.day() as u8;
    raw[8] = dt.hour() as u8;
    raw[9] = dt.minute() as u8;
    raw[10] = dt.second() as u8;
    put_i32(&mut raw, 16, nanos);
    put_i32(&mut raw, 24, i32_of(p.lng * 1e7));
    put_i32(&mut raw, 28, i32_of(p.lat * 1e7));
    put_i32(&mut raw, 32, i32_of(p.alt * 1000.0));
    put_i32(&mut raw, 36, i32_of(p.alt * 1000.0));
    // 记录里速度是毫米每秒，点里也是毫米每秒，直接写
    put_i32(&mut raw, 48, i32_of(p.speed_mmps as f64));
    put_i32(&mut raw, 52, i32_of(p.heading * 1e5));
    put_i16(&mut raw, 68, i16_of(p.gx * 1000.0));
    put_i16(&mut raw, 70, i16_of(p.gy * 1000.0));
    put_i16(&mut raw, 72, i16_of(p.gz * 1000.0));
    put_i16(&mut raw, 74, i16_of(p.rx * 100.0));
    put_i16(&mut raw, 76, i16_of(p.ry * 100.0));
    put_i16(&mut raw, 78, i16_of(p.rz * 100.0));

    Record {
        raw,
        itow: p.itow,
        year,
        month: dt.month() as u8,
        day: dt.day() as u8,
        hour: dt.hour() as u8,
        minute: dt.minute() as u8,
        second: dt.second() as u8,
        time_accuracy: 0,
        nanoseconds: nanos,
        fix_status: 1,
        satellites: 0,
        longitude: p.lng,
        latitude: p.lat,
        wgs_altitude: p.alt,
        msl_altitude: p.alt,
        horizontal_accuracy: 0.0,
        vertical_accuracy: 0.0,
        speed_kmh: p.speed_mmps as f64 / 1000.0 * 3.6,
        heading: p.heading,
        speed_accuracy: 0,
        heading_accuracy: 0.0,
        pdop: 0,
        gforce_x: p.gx,
        gforce_y: p.gy,
        gforce_z: p.gz,
        rotation_rate_x: p.rx,
        rotation_rate_y: p.ry,
        rotation_rate_z: p.rz,
    }
}

/// 非有限值（NaN / 无穷）写进二进制会变成一个莫名其妙的巨大整数，统一当 0。
fn i32_of(v: f64) -> i32 {
    if v.is_finite() {
        v as i32
    } else {
        0
    }
}

fn put_u16(buf: &mut [u8], at: usize, v: u16) {
    buf[at..at + 2].copy_from_slice(&v.to_le_bytes());
}

fn put_u32(buf: &mut [u8], at: usize, v: u32) {
    buf[at..at + 4].copy_from_slice(&v.to_le_bytes());
}

fn put_i32(buf: &mut [u8], at: usize, v: i32) {
    buf[at..at + 4].copy_from_slice(&v.to_le_bytes());
}

fn put_i16(buf: &mut [u8], at: usize, v: i16) {
    buf[at..at + 2].copy_from_slice(&v.to_le_bytes());
}

/// 记录流里的「系统数据」记录：`240 + 电量 + 存储 + 天线/卫星 + 水平精度`，共 8 字节。
///
/// 会话详情页的「GPS Accuracy 0.51 m / 15 sat」就来自这里——记录流本身不存
/// 精度和卫星数，只能靠这种旁挂记录带上去。手机 App 每 2 秒左右写一条。
const RECORD_PREFIX_SYSTEM: u8 = 240;
const SYSTEM_RECORD_LEN: usize = 8;
/// 每隔多少条采样写一条系统记录（25 Hz 下约 2 秒，和真机一致）。
const SYSTEM_RECORD_EVERY: usize = 50;

/// 记录流里的「目标点成绩」记录：`242 + 目标点 + 成绩 + 坡度 + 区间`，共 14 字节。
///
/// 详情页每个跑次下面的「0-60 kph 19.55」「100 m 15.59」这些行只认这条记录，
/// 尾部里的同名字段是给服务端汇总用的，不写 242 页面上就只有极速。
const RECORD_PREFIX_EVENT: u8 = 242;
const EVENT_RECORD_LEN: usize = 14;

/// 编码记录流，返回 (字节流, 采样条数, 系统记录条数)。
///
/// 三种「旁挂记录」按位置插进流里，它们都**不计入** `NumberOfSensorRecords`：
/// - `243 + u16 序号`：跑次开始，服务端靠它和 `NumberOfRuns` 对齐；
/// - `242 ...`：目标点成绩，详情页的跑次明细；
/// - `240 ...`：电量 / 卫星数 / 定位精度。
pub fn encode_records(
    points: &[TrackPoint],
    runs: &[Run],
    events: &[RunEvent],
) -> (Vec<u8>, u32, u32) {
    let mut out = Vec::with_capacity(
        points.len() * DELTA_RECORD_LEN
            + KEY_RECORD_LEN
            + runs.len() * RUN_START_RECORD_LEN
            + events.len() * EVENT_RECORD_LEN,
    );
    let mut last: Option<TrackPoint> = None;
    let mut idx: u8 = 0;
    let mut next_run = 0usize;
    let mut next_event = 0usize;
    let mut n_sys = 0u32;
    for (i, p) in points.iter().enumerate() {
        // 跑次开始：先写一条 243 记录，再写这一条采样记录。
        //
        // 真机文件（云端下载的 Drag 会话，5 跑 / 4 条 243）里**第 0 跑没有
        // 标记**：会话开头那趟是隐含的，从第 1 跑起才写，标记里的序号就是
        // 跑次号 1、2、3…。我们以前连第 0 跑也写了个 0 号标记，比真机多一条、
        // 序号整体错一位；手机 App 用官方解析器能容错，网页版详情页按标记
        // 切跑次时就会对不上。
        while next_run < runs.len() && runs[next_run].start == i {
            if next_run > 0 {
                out.push(RECORD_PREFIX_RUN_START);
                out.extend_from_slice(&(next_run as u16).to_le_bytes());
            }
            next_run += 1;
        }
        if needs_key(last.as_ref(), p, idx) {
            write_key(&mut out, p);
            last = Some(*p);
            idx = 0;
        } else {
            idx += 1;
            let l = last.expect("增量记录一定有上一个键记录");
            write_delta(&mut out, p, &l, idx);
        }
        // 成绩事件写在「区间最后一条记录」之后：服务端读到的
        // SessionSensorRecordIndex 就是这个位置，靠它把事件归到跑次上
        while next_event < events.len() && events[next_event].after == i {
            let e = &events[next_event];
            out.push(RECORD_PREFIX_EVENT);
            out.push(e.setpoint);
            out.extend_from_slice(&e.time_ms.to_le_bytes());
            out.extend_from_slice(&e.slope_x10.to_le_bytes());
            out.extend_from_slice(&e.start_idx_in_run.to_le_bytes());
            out.extend_from_slice(&e.n_records.to_le_bytes());
            out.extend_from_slice(&e.distance_m.to_le_bytes());
            next_event += 1;
        }
        if i % SYSTEM_RECORD_EVERY == 0 && p.sats > 0 && p.hacc_m > 0.0 {
            let mut rec = [0u8; SYSTEM_RECORD_LEN];
            rec[0] = RECORD_PREFIX_SYSTEM;
            // 电量 / 存储未知：0 表示没数据，别编一个好看的数
            rec[3] = (p.sats & 0x7F) << 1; // bit0 = 外接天线（未知，写 0）
            rec[4..8].copy_from_slice(&((p.hacc_m * 1000.0).round().max(0.0) as u32).to_le_bytes());
            out.extend_from_slice(&rec);
            n_sys += 1;
        }
    }
    (out, points.len() as u32, n_sys)
}

fn write_u16(buf: &mut [u8], at: usize, v: u16) {
    buf[at..at + 2].copy_from_slice(&v.to_le_bytes());
}

// ---------------------------------------------------------------- 加速跑（run）

/// 固件里的静止阈值：277.778 mm/s（= 1 km/h）。低于它按「没动」算。
const STATIONARY_MMPS: f64 = 277.778;
/// 连续这么多条「静止」记录，一趟加速跑就结束
/// （固件 `NumberOfConsecutiveStationaryRecordsToEndRun`）。
const STATIONARY_RECORDS_TO_END_RUN: usize = 10;
/// 记录流里的「跑次开始」记录：`243 + u16 跑次序号`，共 3 字节。
const RECORD_PREFIX_RUN_START: u8 = 243;
const RUN_START_RECORD_LEN: usize = 3;
/// 尾部每一趟的固定部分：起始记录 / 条数 / 极速 / 最大 G / 反应时间 /
/// FT rollout 记录号与耗时 / 保留，共 20 字节。
const RUN_SUFFIX_FIXED: usize = 20;
/// 尾部每一趟里每个目标点占 12 字节。
const RUN_SUFFIX_PER_SETPOINT: usize = 12;
/// 尾部长度字段只有 u16，跑次太多会溢出。超了就只保留前这么多趟。
const MAX_RUNS_PER_SESSION: usize =
    (u16::MAX as usize) / (RUN_SUFFIX_FIXED + DEFAULT_SETPOINTS_KPH.len() * RUN_SUFFIX_PER_SETPOINT);

/// 一趟加速跑：记录流里 `[start, end]` 闭区间。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Run {
    pub start: usize,
    pub end: usize,
}

/// 固件先把速度去抖动：低于 1 km/h 的一律当 0。
fn filtered_mmps(p: &TrackPoint) -> f64 {
    let v = p.speed_mmps as f64;
    if v < STATIONARY_MMPS {
        0.0
    } else {
        v
    }
}

/// 按固件的状态机划分加速跑。
///
/// 规则（`DragRealTimeProcessor.CheckSessionState`）：
/// - 速度一旦 ≥ 1 km/h 就开一趟；
/// - 连续 10 条以上「静止」记录，这一趟结束；
/// - 结束后再超过 1 km/h 就是新的一趟。
///
/// 不要求真的从静止地板油起步——市区骑行每个红绿灯都会算一趟，
/// 和官方 App 的行为一致。**服务端不会自己分跑**，它只读文件里的跑次，
/// 所以这里不划出来，云端就是「0 runs」，列表里干脆看不到。
pub fn detect_runs(points: &[TrackPoint]) -> Vec<Run> {
    let mut runs = Vec::new();
    let mut running = false;
    let mut stationary = 0usize;
    let mut start = 0usize;
    for (i, p) in points.iter().enumerate() {
        if !running {
            if filtered_mmps(p) >= STATIONARY_MMPS {
                running = true;
                start = i;
                stationary = 0;
            }
        } else if filtered_mmps(p) >= STATIONARY_MMPS {
            stationary = 0;
        } else {
            stationary += 1;
            if stationary > STATIONARY_RECORDS_TO_END_RUN {
                // 这一趟结束在第一个静止记录之前
                let end = i.saturating_sub(stationary);
                if end >= start {
                    runs.push(Run { start, end });
                }
                running = false;
            }
        }
    }
    if running {
        runs.push(Run {
            start,
            end: points.len() - 1,
        });
    }
    runs
}

/// 球面距离，米。只用来填「加速距离」和坡度，haversine 够了。
fn distance_m(a: (f64, f64), b: (f64, f64)) -> f64 {
    const R: f64 = 6_371_000.0;
    let p1 = a.0.to_radians();
    let p2 = b.0.to_radians();
    let dp = p2 - p1;
    let dl = (b.1 - a.1).to_radians();
    let h = (dp / 2.0).sin().powi(2) + p1.cos() * p2.cos() * (dl / 2.0).sin().powi(2);
    2.0 * R * h.sqrt().asin()
}

/// 一个目标点在某一趟里的成绩。
#[derive(Debug, Clone, Copy, Default)]
struct SetpointResult {
    time_ms: u32,
    slope_x10: i16,
    /// 起始记录在**本跑内**的下标。服务端 `GetRecordsRange` 用它定位成绩区间：
    /// `起始 = 跑的 StartRecordIndex + 本值`，再取 `n_records` 条。
    start_idx_in_run: u16,
    n_records: u16,
    distance_m: u16,
    /// 区间最后一条采样在整段里的绝对下标，用来决定 242 记录插在哪。
    end_idx: usize,
}

/// 一条「目标点成绩」事件（记录流里的 242 记录）。
#[derive(Debug, Clone, Copy)]
pub struct RunEvent {
    /// 写在第几条采样之后。服务端读到的 `SessionSensorRecordIndex` 是 `after + 1`，
    /// 它据此判断这条成绩属于哪一跑。
    after: usize,
    setpoint: u8,
    time_ms: u32,
    slope_x10: i16,
    start_idx_in_run: u16,
    n_records: u16,
    distance_m: u16,
}

/// 在一趟里找「从 start_kph 加到 end_kph」的那次穿越。
///
/// 起终点都按速度线性插值补到亚采样精度，和固件的
/// `SubTenthTimeCorrectionMS` / `CreateInterpolated` 等价。
fn track_setpoint(
    points: &[TrackPoint],
    run: &Run,
    start_kph: f64,
    end_kph: f64,
) -> Option<SetpointResult> {
    let start_th = start_kph * 1000.0 / 3.6;
    let end_th = end_kph * 1000.0 / 3.6;
    // 第 0 条没有「上一条」，固件那边同样处理不了
    let from = run.start.max(1);
    if from > run.end {
        return None;
    }
    let mut started = false;
    let mut start_itow = 0i64;
    let mut start_at = run.start;
    let mut start_ll = (0.0f64, 0.0f64);
    let mut start_alt = 0.0f64;
    for i in from..=run.end {
        let prev = filtered_mmps(&points[i - 1]);
        let new = filtered_mmps(&points[i]);
        if !started {
            // 起步：向上穿过起始速度；起速为 0 时要求真的动起来
            if prev <= start_th && start_th <= new && (start_th != 0.0 || new != 0.0) {
                started = true;
                // 穿越发生在 i-1 与 i 之间，起点按 i-1 算；跑次第一条之前没有采样点，
                // 就退到跑次起点（固件那边 StartRecordIndex = recordIndex - 1 也是这个意思）
                start_at = (i - 1).max(run.start);
                start_itow = points[start_at].itow as i64;
                start_ll = (points[start_at].lat, points[start_at].lng);
                start_alt = points[start_at].alt;
            }
            continue;
        }
        if prev <= end_th && end_th <= new && prev < new {
            let frac = (end_th - prev) / (new - prev);
            let dt = points[i].itow as i64 - points[i - 1].itow as i64;
            let end_itow = points[i - 1].itow as i64 + (dt as f64 * frac) as i64;
            let dist = distance_m(start_ll, (points[i].lat, points[i].lng));
            let slope = if dist > 0.0 {
                (points[i].alt - start_alt) * 100.0 / dist
            } else {
                0.0
            };
            return Some(SetpointResult {
                time_ms: (end_itow - start_itow).max(0) as u32,
                slope_x10: i16_of(slope * 10.0),
                // 起止都相对本跑起点：服务端拿这两个值切成绩区间
                start_idx_in_run: (start_at - run.start).min(u16::MAX as usize) as u16,
                n_records: (i - start_at + 1).min(u16::MAX as usize) as u16,
                distance_m: dist.round().clamp(0.0, u16::MAX as f64) as u16,
                end_idx: i,
            });
        }
    }
    None
}

/// 编码尾部：每趟 `20 + 12×目标点数` 字节，同时算出头部要填的最好成绩，
/// 以及要插进记录流的目标点成绩事件（242）。
///
/// 返回 (尾部字节, 每个目标点的最好成绩 ms, 成绩所在的跑次, 事件列表)。
fn encode_run_suffix(
    points: &[TrackPoint],
    runs: &[Run],
) -> (Vec<u8>, Vec<i32>, Vec<i16>, Vec<RunEvent>) {
    let n = DEFAULT_SETPOINTS_KPH.len();
    let mut out =
        Vec::with_capacity(runs.len() * (RUN_SUFFIX_FIXED + n * RUN_SUFFIX_PER_SETPOINT));
    let mut best = vec![0i32; n];
    let mut best_run = vec![-1i16; n];
    let mut events = Vec::new();

    for (ri, run) in runs.iter().enumerate() {
        let n_records = (run.end - run.start + 1) as u32;
        let seg = &points[run.start..=run.end];
        let top = seg
            .iter()
            .map(|p| filtered_mmps(p) / 1000.0 * 3.6)
            .fold(0.0f64, f64::max);
        // 最大 G 只算水平分量：z 轴压着 1 g 的重力，算进去就永远 ≥1 g。
        // 真机样本对得上：0.538 / 0.547 与 sqrt(gx²+gy²) 的最大值完全一致。
        let max_g = seg
            .iter()
            .map(|p| (p.gx * p.gx + p.gy * p.gy).sqrt())
            .fold(0.0f64, f64::max);
        out.extend_from_slice(&(run.start as u32).to_le_bytes());
        out.extend_from_slice(&n_records.to_le_bytes());
        out.extend_from_slice(&i16_of(top * 100.0).to_le_bytes());
        out.extend_from_slice(&i16_of(max_g * 1000.0).to_le_bytes()); // MaxGForce
        out.extend_from_slice(&i16::MIN.to_le_bytes()); // ReactionTime 无效
        out.extend_from_slice(&0u16.to_le_bytes()); // FtRolloutRecordIndex
        out.extend_from_slice(&u16::MAX.to_le_bytes()); // FtRolloutTime 无效
        out.extend_from_slice(&0u16.to_le_bytes()); // 保留

        for (si, (from, to)) in DEFAULT_SETPOINTS_KPH.iter().enumerate() {
            match track_setpoint(points, run, *from as f64, *to as f64) {
                Some(r) => {
                    out.extend_from_slice(&r.time_ms.to_le_bytes());
                    out.extend_from_slice(&r.slope_x10.to_le_bytes());
                    out.extend_from_slice(&r.start_idx_in_run.to_le_bytes());
                    out.extend_from_slice(&r.n_records.to_le_bytes());
                    out.extend_from_slice(&r.distance_m.to_le_bytes());
                    events.push(RunEvent {
                        after: r.end_idx,
                        setpoint: si as u8,
                        time_ms: r.time_ms,
                        slope_x10: r.slope_x10,
                        start_idx_in_run: r.start_idx_in_run,
                        n_records: r.n_records,
                        distance_m: r.distance_m,
                    });
                    let ms = r.time_ms as i32;
                    if best_run[si] < 0 || ms < best[si] {
                        best[si] = ms;
                        best_run[si] = ri as i16;
                    }
                }
                // 没跑进这个目标：全 0，服务端当「没有成绩」
                None => out.extend_from_slice(&[0u8; RUN_SUFFIX_PER_SETPOINT]),
            }
        }
    }
    // 记录流要求按位置递增；同一位置的多条（一跑同时命中几个目标）保持目标点顺序
    events.sort_by_key(|e| e.after);
    (out, best, best_run, events)
}

fn write_u32(buf: &mut [u8], at: usize, v: u32) {
    buf[at..at + 4].copy_from_slice(&v.to_le_bytes());
}

fn write_i16(buf: &mut [u8], at: usize, v: i16) {
    buf[at..at + 2].copy_from_slice(&v.to_le_bytes());
}

fn write_i32(buf: &mut [u8], at: usize, v: i32) {
    buf[at..at + 4].copy_from_slice(&v.to_le_bytes());
}

/// 编码一个完整的 Drag（直线加速）会话文件。
///
/// 返回的字节流可以直接 `POST /v1/session`。采样点为空时返回 None
/// （空会话服务端会拒，没必要发）。
pub fn encode_session(points: &[TrackPoint], meta: &SessionMeta) -> Option<Vec<u8>> {
    if points.is_empty() {
        return None;
    }
    // 服务端不自己分跑：跑次、成绩全靠文件里的 243 记录和尾部数据。
    // 不写这两样，云端就是 0 runs，webapp 的 Drag 列表里干脆看不到。
    let mut runs = detect_runs(points);
    runs.truncate(MAX_RUNS_PER_SESSION);
    let (suffix, best, best_run, events) = encode_run_suffix(points, &runs);
    let (body, n_sensor, n_sys) = encode_records(points, &runs, &events);

    // Drag 头部只有「极速」这一个统计字段，其余交给尾部。
    let max_speed = points
        .iter()
        .map(|p| p.speed_mmps as f64 / 1000.0 * 3.6)
        .fold(0.0f64, f64::max);

    let header_len = header_len();
    let mut h = vec![0u8; header_len];
    h[0..4].copy_from_slice(b"RB\x02\x01");
    write_u16(&mut h, 4, header_len as u16);
    h[6] = meta.recorder.0;
    h[7] = meta.recorder.1;
    write_u16(&mut h, 8, 0); // OtherBytesLength
    write_u16(&mut h, 10, suffix.len() as u16); // FileSuffixLength
    write_u32(&mut h, 12, body.len() as u32);
    h[16] = SESSION_TYPE_DRAG;
    h[17] = 1; // IndexInTheDay
    h[18] = 0; // SensorRecordsTimeDelta
    h[19] = 0;
    write_u32(&mut h, 20, n_sensor);
    // 240 系统记录 / 242 成绩记录都要精确报数：服务端解析循环拿它们当边界，
    // 报少了会提前收尾，报多了会读到记录流末尾之外。
    write_u32(&mut h, 24, n_sys);
    write_u32(&mut h, 28, events.len() as u32);
    write_i16(&mut h, 32, meta.tz_offset_min);

    let (y, mo, d, hh, mi, ss) = meta.utc;
    // 本地时间 = UTC + 时区偏移，由服务端反推时区展示用。
    let local = local_from_utc(y, mo, d, hh, mi, ss, meta.tz_offset_min as i64);
    write_datetime(&mut h, 34, local);
    write_datetime(&mut h, 40, meta.utc);
    h[46..58].copy_from_slice(&meta.session_id);
    h[60..68].copy_from_slice(&(meta.device_serial as i64).to_le_bytes());

    // ---------- Drag 专属段（68 起）----------
    let first = points[0];
    h[68] = DRAG_MODE_SPEED_METRIC;
    h[69] = 0; // FtRolloutEnabled
    h[70] = DEFAULT_SETPOINTS_KPH.len() as u8;
    h[71] = 0; // 圣诞树：(倒计时 << 4) | 模式
    write_i32(&mut h, 72, i32_of(first.lat * 1e7));
    write_i32(&mut h, 76, i32_of(first.lng * 1e7));
    write_i16(&mut h, 80, i16_of(max_speed * 100.0));
    write_u16(&mut h, 82, runs.len().min(u16::MAX as usize) as u16);

    let mut at = 68 + DRAG_FIXED_LEN;
    for (from, to) in DEFAULT_SETPOINTS_KPH {
        write_u16(&mut h, at, from);
        write_u16(&mut h, at + 2, to);
        // flags：bit0 = 已配置，bit1 = 距离模式（关），bit2 = 起始速度用英里（关），
        // bit4-7 = 单位（15 = km/h）
        h[at + 4] = 0x01 | (UNITS_KPH << 4);
        h[at + 5] = 0;
        at += 6;
    }
    for ms in &best {
        write_i32(&mut h, at, *ms); // BestResults：毫秒，0 = 没跑进
        at += 4;
    }
    for r in &best_run {
        write_i16(&mut h, at, *r); // BestResultsRuns：-1 = 没有对应跑次
        at += 2;
    }
    debug_assert_eq!(at, header_len - 12, "Drag 段长度与 header_len 对不上");

    // 传感器类型表 + 两个校验和，都贴在头部末尾
    let types_at = header_len - 12;
    h[types_at] = SENSOR_TYPE_COUNT as u8;
    h[types_at + 1] = 1; // SensorType::GPS
    h[types_at + 2] = 2; // SensorType::GForce
    h[types_at + 3] = 3; // SensorType::Gyroscope
    // DataChecksum 覆盖「记录流 + 尾部」，两段要一起算
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&body);
    hasher.update(&suffix);
    write_u32(&mut h, header_len - 8, hasher.finalize());
    // 头部校验和覆盖自身之前的所有字节（HeaderLength - 4）。
    // 先算完再写：同一句话里既可变借用 h 又只读借用会被借用检查挡下来。
    let header_crc = crc32(&h[..header_len - 4]);
    write_u32(&mut h, header_len - 4, header_crc);

    let mut out = Vec::with_capacity(header_len + body.len() + suffix.len());
    out.extend_from_slice(&h);
    out.extend_from_slice(&body);
    out.extend_from_slice(&suffix);
    Some(out)
}

/// 把 (年,月,日,时,分,秒) 按 6 字节紧凑格式写进去（年只存后两位）。
fn write_datetime(buf: &mut [u8], at: usize, dt: (i32, u32, u32, u32, u32, u32)) {
    let (y, mo, d, hh, mi, ss) = dt;
    buf[at] = (y - 2000) as u8;
    buf[at + 1] = mo as u8;
    buf[at + 2] = d as u8;
    buf[at + 3] = hh as u8;
    buf[at + 4] = mi as u8;
    buf[at + 5] = ss as u8;
}

/// UTC + 偏移分钟 → 本地时间。只用来填头部的「本地时间」字段，
/// 不涉及夏令时（设备本身也不处理）。
fn local_from_utc(
    y: i32,
    mo: u32,
    d: u32,
    hh: u32,
    mi: u32,
    ss: u32,
    offset_min: i64,
) -> (i32, u32, u32, u32, u32, u32) {
    // 用 chrono 做进位，避免自己写月份天数表。
    use chrono::{Datelike, Duration, NaiveDate, NaiveDateTime, Timelike};
    let base = NaiveDate::from_ymd_opt(y, mo, d).and_then(|date| {
        date.and_hms_opt(hh, mi, ss)
    });
    match base {
        Some(dt) => {
            let shifted: NaiveDateTime = dt + Duration::minutes(offset_min);
            (
                shifted.year(),
                shifted.month(),
                shifted.day(),
                shifted.hour(),
                shifted.minute(),
                shifted.second(),
            )
        }
        None => (y, mo, d, hh, mi, ss),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 导入的轨迹点补成记录后，坐标 / 速度 / 时间必须还读得回来
    /// （PostgreSQL 按年月日入库，MQTT 直接转发 raw）。
    #[test]
    fn records_from_points_keeps_values() {
        let points: Vec<TrackPoint> = (0..3).map(sample).collect();
        // 2026-07-25 17:31:05 UTC
        let start = 1_785_000_665_000i64;
        let records = records_from_points(&points, start);
        assert_eq!(records.len(), 3);
        assert_eq!(records[0].year, 2026);
        assert_eq!(records[0].month, 7);
        assert_eq!(records[0].day, 25);
        assert_eq!(records[0].hour, 17);
        // iTOW 增量 40ms -> 时间往后推
        assert!(records[1].nanoseconds >= records[0].nanoseconds);
        for (i, r) in records.iter().enumerate() {
            assert!((r.latitude - (36.4 + i as f64 * 0.00001)).abs() < 1e-5);
            assert!((r.longitude - (120.5 + i as f64 * 0.00001)).abs() < 1e-5);
            assert!((r.speed_kmh - (10_000 + i as u32) as f64 / 1000.0 * 3.6).abs() < 0.01);
            // raw 也要能被设备那套解码读出来（MQTT 原样转发它）
            let back = crate::protocol::Record::decode(&r.raw).expect("raw 记录可解码");
            assert_eq!(back.itow, r.itow);
            assert!((back.latitude - r.latitude).abs() < 1e-5);
        }
    }

    fn sample(i: usize) -> TrackPoint {
        TrackPoint {
            itow: 521_400_000 + (i as u32) * 40,
            lat: 36.4 + i as f64 * 0.00001,
            lng: 120.5 + i as f64 * 0.00001,
            alt: 42.0,
            speed_mmps: 10_000 + i as u32,
            heading: 90.0,
            gx: 0.01,
            gy: 0.0,
            gz: 1.0,
            ..Default::default()
        }
    }

    fn meta() -> SessionMeta {
        SessionMeta {
            session_id: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
            utc: (2026, 8, 18, 0, 12, 50),
            tz_offset_min: 480,
            device_serial: 3242704435,
            recorder: (1, 11),
        }
    }

    /// 头部自洽性：长度字段、两个 CRC、类型表都要对得上。
    #[test]
    fn header_is_consistent() {
        let pts: Vec<TrackPoint> = (0..300).map(sample).collect();
        let blob = encode_session(&pts, &meta()).expect("非空");
        let hl = header_len();
        assert_eq!(&blob[0..4], b"RB\x02\x01");
        assert_eq!(u16::from_le_bytes([blob[4], blob[5]]) as usize, hl);
        let data_len = u32::from_le_bytes([blob[12], blob[13], blob[14], blob[15]]) as usize;
        let suffix_len = u16::from_le_bytes([blob[10], blob[11]]) as usize;
        assert_eq!(blob.len(), hl + data_len + suffix_len);

        // 头部 CRC 覆盖 HeaderLength - 4 字节
        let want = u32::from_le_bytes([blob[hl - 4], blob[hl - 3], blob[hl - 2], blob[hl - 1]]);
        assert_eq!(crc32(&blob[..hl - 4]), want);
        // 记录流 CRC 覆盖「记录流 + 尾部」
        let want = u32::from_le_bytes([blob[hl - 8], blob[hl - 7], blob[hl - 6], blob[hl - 5]]);
        assert_eq!(crc32(&blob[hl..]), want);
        // 类型表
        assert_eq!(blob[hl - 12], 3);
        assert_eq!(&blob[hl - 11..hl - 8], &[1, 2, 3]);
    }

    /// 头部必须是 Drag 结构：类型 0、Drag 专属段字段齐全。
    ///
    /// 只改类型字节不够——服务端按 Drag 的布局读头部，读到 Track 那
    /// 88 + 128 字节赛道段就会错位，直接回 `Failed to parse session`。
    #[test]
    fn session_header_is_drag() {
        let pts: Vec<TrackPoint> = (0..10).map(sample).collect();
        let blob = encode_session(&pts, &meta()).expect("非空");
        let n = DEFAULT_SETPOINTS_KPH.len();
        let hl = header_len();
        assert_eq!(hl, 96 + n * 12, "Drag 头长应为 96 + 12N");
        assert_eq!(u16::from_le_bytes([blob[4], blob[5]]) as usize, hl);
        assert_eq!(blob[16], 0, "会话类型应为 Drag");
        assert_eq!(blob[17], 1, "IndexInTheDay");
        assert_eq!(blob[68], DRAG_MODE_SPEED_METRIC);
        assert_eq!(blob[70] as usize, n, "目标点数量");

        // 首点坐标进了头部
        let lat = i32::from_le_bytes([blob[72], blob[73], blob[74], blob[75]]) as f64 / 1e7;
        assert!((lat - pts[0].lat).abs() < 1e-6, "lat {lat}");

        // 每个目标点：已配置 + km/h
        for (i, (from, to)) in DEFAULT_SETPOINTS_KPH.iter().enumerate() {
            let at = 84 + i * 6;
            assert_eq!(blob[at + 4], 0xF1, "第 {i} 个目标点的 flags");
            assert_eq!(u16::from_le_bytes([blob[at], blob[at + 1]]), *from);
            assert_eq!(u16::from_le_bytes([blob[at + 2], blob[at + 3]]), *to);
        }
        // 成绩：这份样本一直是 36 km/h，跑不进 0-60，所以留 0 / -1
        let br = 84 + n * 6;
        for i in 0..n {
            let v = i32::from_le_bytes([
                blob[br + i * 4],
                blob[br + i * 4 + 1],
                blob[br + i * 4 + 2],
                blob[br + i * 4 + 3],
            ]);
            assert_eq!(v, 0, "BestResults[{i}] 应为 0");
        }
        let rr = br + n * 4;
        for i in 0..n {
            let v = i16::from_le_bytes([blob[rr + i * 2], blob[rr + i * 2 + 1]]);
            assert_eq!(v, -1, "BestResultsRuns[{i}] 应为 -1");
        }

        // 全程都在动 = 一趟跑，尾部就是这一趟的结果
        assert_eq!(u16::from_le_bytes([blob[82], blob[83]]), 1, "跑次应为 1");
        let suffix_len = u16::from_le_bytes([blob[10], blob[11]]) as usize;
        assert_eq!(suffix_len, RUN_SUFFIX_FIXED + n * RUN_SUFFIX_PER_SETPOINT);
        let n_event = u32::from_le_bytes([blob[28], blob[29], blob[30], blob[31]]);
        assert_eq!(n_event, 0, "不应有事件记录");
    }

    /// 记录流必须能被逐条走完，且条数与头部声明一致。
    #[test]
    fn records_walk_to_the_end() {
        let pts: Vec<TrackPoint> = (0..500).map(sample).collect();
        let blob = encode_session(&pts, &meta()).expect("非空");
        let hl = header_len();
        let data_len = u32::from_le_bytes([blob[12], blob[13], blob[14], blob[15]]) as usize;
        let body = &blob[hl..hl + data_len];
        let mut p = 0usize;
        let mut n = 0u32;
        let mut runs = 0u32;
        let mut events = 0u32;
        let mut systems = 0u32;
        while p < body.len() {
            let prefix = body[p];
            if prefix == RECORD_PREFIX_RUN_START {
                p += RUN_START_RECORD_LEN;
                runs += 1;
                continue;
            }
            if prefix == RECORD_PREFIX_EVENT {
                p += EVENT_RECORD_LEN;
                events += 1;
                continue;
            }
            if prefix == RECORD_PREFIX_SYSTEM {
                p += SYSTEM_RECORD_LEN;
                systems += 1;
                continue;
            }
            p += if prefix == RECORD_PREFIX_KEY {
                KEY_RECORD_LEN
            } else {
                DELTA_RECORD_LEN
            };
            n += 1;
        }
        assert_eq!(p, body.len(), "记录流长度没对齐");
        assert_eq!(n, 500);
        let declared = u32::from_le_bytes([blob[20], blob[21], blob[22], blob[23]]);
        assert_eq!(declared, 500);
        // 第 0 跑没有 243 标记：标记数 = 跑次数 - 1（与真机文件一致）
        let n_runs = u16::from_le_bytes([blob[82], blob[83]]) as u32;
        assert_eq!(runs + u32::from(n_runs > 0), n_runs);
        // 旁挂记录也要和头部报数一致，否则服务端解析循环会提前收尾
        assert_eq!(events, u32::from_le_bytes([blob[28], blob[29], blob[30], blob[31]]));
        assert_eq!(
            systems,
            u32::from_le_bytes([blob[24], blob[25], blob[26], blob[27]])
        );
    }

    /// 243 跑次开始标记要和真机文件一致：第 0 跑不写，后续跑编号 1、2、3…
    #[test]
    fn run_start_markers_start_at_one() {
        let mut pts = acceleration_points();
        pts.extend(acceleration_points());
        let blob = encode_session(&pts, &meta()).expect("非空");
        let n_runs = u16::from_le_bytes([blob[82], blob[83]]) as usize;
        assert!(n_runs >= 2, "测试数据要能分出至少两跑");

        let hl = header_len();
        let data_len = u32::from_le_bytes([blob[12], blob[13], blob[14], blob[15]]) as usize;
        let body = &blob[hl..hl + data_len];
        let (mut p, mut marks) = (0usize, Vec::new());
        while p < body.len() {
            match body[p] {
                RECORD_PREFIX_RUN_START => {
                    marks.push(u16::from_le_bytes([body[p + 1], body[p + 2]]));
                    p += RUN_START_RECORD_LEN;
                }
                RECORD_PREFIX_EVENT => p += EVENT_RECORD_LEN,
                RECORD_PREFIX_SYSTEM => p += SYSTEM_RECORD_LEN,
                RECORD_PREFIX_KEY => p += KEY_RECORD_LEN,
                _ => p += DELTA_RECORD_LEN,
            }
        }
        assert_eq!(p, body.len(), "记录流要能逐条走完");
        let want: Vec<u16> = (1..n_runs as u16).collect();
        assert_eq!(marks, want, "标记数 = 跑次数 - 1，且从 1 开始编号");
    }

    /// 240 系统记录（卫星数 / 定位精度）和 242 成绩记录都要写对，
    /// 详情页的「GPS Accuracy」「0-30 kph」这些行只认它们。
    #[test]
    fn system_and_event_records_are_written() {
        let mut pts = acceleration_points();
        for p in pts.iter_mut() {
            p.hacc_m = 0.51;
            p.sats = 15;
        }
        let blob = encode_session(&pts, &meta()).expect("非空");
        let n_sys = u32::from_le_bytes([blob[24], blob[25], blob[26], blob[27]]);
        let n_event = u32::from_le_bytes([blob[28], blob[29], blob[30], blob[31]]);
        // 每 50 条一条系统记录：下标 0、50、100（共 150 条采样）
        assert_eq!(n_sys, 3, "系统记录条数");
        // 这段数据跑到 100 km/h，0-30 / 0-60 / 0-100 各命中一次
        assert_eq!(n_event, 3, "成绩记录条数");

        // 按记录边界走一遍，找第一条 240 / 242（不能直接搜字节值：
        // 采样数据里也可能出现 240，会搜到假的位置）
        let hl = header_len();
        let data_len = u32::from_le_bytes([blob[12], blob[13], blob[14], blob[15]]) as usize;
        let body = &blob[hl..hl + data_len];
        let (mut p, mut sys, mut ev) = (0usize, None, None);
        while p < body.len() {
            match body[p] {
                RECORD_PREFIX_RUN_START => p += RUN_START_RECORD_LEN,
                RECORD_PREFIX_EVENT => {
                    ev.get_or_insert(p);
                    p += EVENT_RECORD_LEN;
                }
                RECORD_PREFIX_SYSTEM => {
                    sys.get_or_insert(p);
                    p += SYSTEM_RECORD_LEN;
                }
                RECORD_PREFIX_KEY => p += KEY_RECORD_LEN,
                _ => p += DELTA_RECORD_LEN,
            }
        }
        let at = sys.expect("应当有系统记录");
        assert_eq!((body[at + 3] >> 1) & 0x7F, 15, "卫星数");
        let hacc = u32::from_le_bytes([body[at + 4], body[at + 5], body[at + 6], body[at + 7]]);
        assert_eq!(hacc, 510, "水平精度 0.51 m");

        let at = ev.expect("应当有成绩记录");
        assert_eq!(body[at + 1], 0, "0-30 是第 0 个目标点");
        let ms = u32::from_le_bytes([body[at + 2], body[at + 3], body[at + 4], body[at + 5]]);
        assert!((ms as i32 - 1_160).abs() < 120, "0-30 约 1.16 秒，实际 {ms} ms");
    }

    /// 航向写 u16 ×100：328°-360° 用 i16 会被截成 327.67°（方向全错）。
    #[test]
    fn heading_is_u16_not_i16() {
        let mut p = sample(0);
        p.heading = 350.0;
        let (body, _, _) = encode_records(&[p], &[], &[]);
        // 键记录：前缀(1) + iTOW(4) + lat(4) + lng(4) + alt(4) + speed(4) 之后是航向
        assert_eq!(u16::from_le_bytes([body[21], body[22]]), 35_000);

        let mut q = sample(0);
        q.heading = 360.0;
        let (body, _, _) = encode_records(&[q], &[], &[]);
        assert_eq!(u16::from_le_bytes([body[21], body[22]]), 0, "360° 应归零");
    }

    /// 间隔超过 2 秒时必须重发键记录，否则服务端会算错时间轴。
    #[test]
    fn long_gap_forces_key_record() {
        let mut a = sample(0);
        let mut b = sample(1);
        b.itow = a.itow + 5_000;
        a.speed_mmps = 0;
        b.speed_mmps = 0;
        let (body, _, _) = encode_records(&[a, b], &[], &[]);
        assert_eq!(body.len(), KEY_RECORD_LEN * 2);
    }

    #[test]
    fn itow_matches_real_device_sample() {
        // 样本会话：UTC 2024-11-23 00:49:49，首条 iTOW = 521407800。
        // 差值 0.8 秒来自 UTC 字段只存整秒。
        let unix = 1_732_322_989i64 * 1000;
        let got = itow_from_unix(unix) as i64;
        assert!((got - 521_407_800).abs() < 1_000, "got {got}");
    }

    /// 造一段「静止 1 秒 -> 4 秒线性加到 100 km/h -> 静止 1 秒」的采样。
    fn acceleration_points() -> Vec<TrackPoint> {
        let mut pts = Vec::new();
        let mut t = 0u32;
        let push = |pts: &mut Vec<TrackPoint>, t: &mut u32, kph: f64| {
            let mut p = sample(0);
            p.itow = 500_000_000 + *t;
            p.speed_mmps = (kph * 1000.0 / 3.6).round() as u32;
            pts.push(p);
            *t += 40;
        };
        for _ in 0..25 {
            push(&mut pts, &mut t, 0.0);
        }
        for i in 1..=100 {
            push(&mut pts, &mut t, i as f64);
        }
        for _ in 0..25 {
            push(&mut pts, &mut t, 0.0);
        }
        pts
    }

    /// 一趟 0-100：要分出 1 跑，0-30 / 0-60 / 0-100 三档都要算出来。
    ///
    /// 服务端不自己分跑，全靠这些数据，所以这里必须算准。
    #[test]
    fn detects_run_and_scores_setpoints() {
        let pts = acceleration_points();
        let runs = detect_runs(&pts);
        assert_eq!(runs.len(), 1, "应当只有一趟");
        assert_eq!(runs[0].start, 25, "第 25 条开始动");

        let (suffix, best, best_run, events) = encode_run_suffix(&pts, &runs);
        assert_eq!(
            suffix.len(),
            RUN_SUFFIX_FIXED + DEFAULT_SETPOINTS_KPH.len() * RUN_SUFFIX_PER_SETPOINT
        );
        // 目标点是 0-30 / 0-60 / 0-100 / 0-150，容差 3 个采样（120ms）
        for (i, want) in [(0, 1_160), (1, 2_359), (2, 3_959)] {
            assert!(
                (best[i] - want).abs() < 120,
                "目标点 {i} 应约 {want} ms，实际 {} ms",
                best[i]
            );
            assert_eq!(best_run[i], 0, "目标点 {i} 应在第 0 跑");
        }
        assert_eq!(best[3], 0, "没到 150 km/h");
        assert_eq!(best_run[3], -1);

        // 成绩区间必须落在本跑内：服务端按
        // `跑的起点 + start_idx_in_run` 取 `n_records` 条来画曲线
        let at = RUN_SUFFIX_FIXED;
        let start_idx = u16::from_le_bytes([suffix[at + 6], suffix[at + 7]]) as usize;
        let n_records = u16::from_le_bytes([suffix[at + 8], suffix[at + 9]]) as usize;
        assert_eq!(start_idx, 0, "从静止起步，区间就是跑次起点");
        assert_eq!(n_records, 31, "1.16 秒 / 40ms ≈ 31 条");
        assert!(
            runs[0].start + start_idx + n_records <= runs[0].end + 1,
            "区间超出跑次范围"
        );

        // 每个命中都要有一条 242 事件，且位置落在本跑内
        assert_eq!(events.len(), 3);
        for e in &events {
            assert!(e.after >= runs[0].start && e.after <= runs[0].end);
        }
    }

    /// 全程静止不能分出一趟，也不能写出非法的跑次。
    #[test]
    fn stationary_session_has_no_runs() {
        let mut pts = acceleration_points();
        for p in pts.iter_mut() {
            p.speed_mmps = 0;
        }
        assert!(detect_runs(&pts).is_empty());

        let blob = encode_session(&pts, &meta()).expect("非空");
        assert_eq!(u16::from_le_bytes([blob[82], blob[83]]), 0, "跑次应为 0");
        assert_eq!(u16::from_le_bytes([blob[10], blob[11]]), 0, "尾部应为空");
    }

    #[test]
    fn empty_input_yields_none() {
        assert!(encode_session(&[], &meta()).is_none());
    }
}
