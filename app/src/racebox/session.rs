//! RaceBox 会话文件（RB V2 容器）编码器。
//!
//! 为什么需要自己写：RaceBox 官网只认设备原生的二进制会话文件，
//! 上传接口 `POST /v1/session` 收到的是裸字节，服务端解析后回填圈速、
//! 极速、最大 G。GPX / FIT 这些通用格式它不认识，必须先转成这个容器。
//!
//! 格式来自对 Android 版 2.3.0 的反编译（`RaceBox.Core.SessionData.V2`），
//! 完整字段表见 `racebox-re/README-cloud-api.md`。这里只实现「赛道会话 +
//! 无赛道关联」这一条路径——够用，且服务端实测接受。
//!
//! 布局：
//! ```text
//! [0 .. 316)                    头部（自带 CRC32）
//! [316 .. 316 + data_len)       传感器记录流（键记录 35 字节 / 增量记录 25 字节）
//! [.. + 0)                      尾部事件（本实现不产生圈事件，长度为 0）
//! ```

use crate::protocol::{Record, SPEED_KMH_PER_UNIT};
use chrono::{Datelike as _, Timelike as _};
use crc32fast::hash as crc32;

/// 头部固定长度：68 基础 + 88 赛道 + 128 配置 + 20 统计 + 1 类型数 + 3 类型 + 8 校验和。
pub const HEADER_LEN: usize = 316;

/// 会话类型：0 是直线加速（Drag），1 是赛道（Track）。
/// 这里一律按赛道写，服务端会算出「整段一圈」。
const SESSION_TYPE_TRACK: u8 = 1;

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

/// 无赛道关联时用的固定结构（88 字节）：前 12 字节是 id（全 0），
/// 偏移 12 起是 32 字节名称。
fn no_track() -> [u8; 88] {
    let mut b = [0u8; 88];
    b[12..20].copy_from_slice(b"No Track");
    b
}

/// 默认赛道配置（128 字节）：同样是 12 字节 id + 32 字节名称，
/// 偏移 45 处是 flags = 2（真实样本里的取值，表示「默认配置」）。
fn default_config() -> [u8; 128] {
    let mut b = [0u8; 128];
    b[12..19].copy_from_slice(b"Default");
    b[45] = 2;
    b
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

fn write_key(out: &mut Vec<u8>, p: &TrackPoint) {
    out.push(RECORD_PREFIX_KEY);
    out.extend_from_slice(&p.itow.to_le_bytes());
    out.extend_from_slice(&((p.lat * 1e7) as i32).to_le_bytes());
    out.extend_from_slice(&((p.lng * 1e7) as i32).to_le_bytes());
    out.extend_from_slice(&((p.alt * 1000.0) as i32).to_le_bytes());
    out.extend_from_slice(&p.speed_mmps.to_le_bytes());
    out.extend_from_slice(&i16_of(p.heading * 100.0).to_le_bytes());
    write_motion(out, p);
}

fn write_delta(out: &mut Vec<u8>, p: &TrackPoint, last: &TrackPoint, idx: u8) {
    out.push(idx);
    out.extend_from_slice(&((p.itow - last.itow) as u16).to_le_bytes());
    out.extend_from_slice(&i16_of((p.lat - last.lat) * 1e7).to_le_bytes());
    out.extend_from_slice(&i16_of((p.lng - last.lng) * 1e7).to_le_bytes());
    out.extend_from_slice(&i16_of((p.alt - last.alt) * 1000.0).to_le_bytes());
    out.extend_from_slice(&i16_of(p.speed_mmps as f64 - last.speed_mmps as f64).to_le_bytes());
    out.extend_from_slice(&i16_of(p.heading * 100.0).to_le_bytes());
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
    put_i32(&mut raw, 48, i32_of(p.speed_mmps as f64 / 1000.0 * 3.6 / SPEED_KMH_PER_UNIT));
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

/// 编码记录流，返回 (字节流, 键记录数 + 增量记录数)。
pub fn encode_records(points: &[TrackPoint]) -> (Vec<u8>, u32) {
    let mut out = Vec::with_capacity(points.len() * DELTA_RECORD_LEN + KEY_RECORD_LEN);
    let mut last: Option<TrackPoint> = None;
    let mut idx: u8 = 0;
    for p in points {
        if needs_key(last.as_ref(), p, idx) {
            write_key(&mut out, p);
            last = Some(*p);
            idx = 0;
        } else {
            idx += 1;
            let l = last.expect("增量记录一定有上一个键记录");
            write_delta(&mut out, p, &l, idx);
        }
    }
    (out, points.len() as u32)
}

fn write_u16(buf: &mut [u8], at: usize, v: u16) {
    buf[at..at + 2].copy_from_slice(&v.to_le_bytes());
}

fn write_u32(buf: &mut [u8], at: usize, v: u32) {
    buf[at..at + 4].copy_from_slice(&v.to_le_bytes());
}

fn write_i16(buf: &mut [u8], at: usize, v: i16) {
    buf[at..at + 2].copy_from_slice(&v.to_le_bytes());
}

/// 编码一个完整的赛道会话文件。
///
/// 返回的字节流可以直接 `POST /v1/session`。采样点为空时返回 None
/// （空会话服务端会拒，没必要发）。
pub fn encode_session(points: &[TrackPoint], meta: &SessionMeta) -> Option<Vec<u8>> {
    if points.is_empty() {
        return None;
    }
    let (body, n_sensor) = encode_records(points);

    // 统计量：服务端也会自己算一遍，但头部这些字段得填得说得过去，
    // 否则文件解析阶段就可能被判为损坏。
    let mut max_speed = f64::MIN;
    let mut min_speed = f64::MAX;
    let mut max_g = 0.0f64;
    for (i, p) in points.iter().enumerate() {
        let kph = p.speed_mmps as f64 / 1000.0 * 3.6;
        if kph > max_speed {
            max_speed = kph;
        }
        if i > 0 && kph < min_speed {
            min_speed = kph;
        }
        let g = (p.gx * p.gx + p.gy * p.gy + (p.gz - 1.0) * (p.gz - 1.0)).sqrt();
        if g > max_g {
            max_g = g;
        }
    }
    if min_speed == f64::MAX {
        min_speed = 0.0;
    }
    if max_speed == f64::MIN {
        max_speed = 0.0;
    }

    let mut h = [0u8; HEADER_LEN];
    h[0..4].copy_from_slice(b"RB\x02\x01");
    write_u16(&mut h, 4, HEADER_LEN as u16);
    h[6] = meta.recorder.0;
    h[7] = meta.recorder.1;
    write_u16(&mut h, 8, 0); // OtherBytesLength
    write_u16(&mut h, 10, 0); // FileSuffixLength（本实现不写圈事件）
    write_u32(&mut h, 12, body.len() as u32);
    h[16] = SESSION_TYPE_TRACK;
    h[17] = 1; // IndexInTheDay
    h[18] = 0; // SensorRecordsTimeDelta
    h[19] = 0;
    write_u32(&mut h, 20, n_sensor);
    write_u32(&mut h, 24, 0); // NumberOfSystemDataRecords
    write_u32(&mut h, 28, 0); // NumberOfEventRecords
    write_i16(&mut h, 32, meta.tz_offset_min);

    let (y, mo, d, hh, mi, ss) = meta.utc;
    // 本地时间 = UTC + 时区偏移，由服务端反推时区展示用。
    let local = local_from_utc(y, mo, d, hh, mi, ss, meta.tz_offset_min as i64);
    write_datetime(&mut h, 34, local);
    write_datetime(&mut h, 40, meta.utc);
    h[46..58].copy_from_slice(&meta.session_id);
    h[60..68].copy_from_slice(&(meta.device_serial as i64).to_le_bytes());

    h[68..156].copy_from_slice(&no_track());
    h[156..284].copy_from_slice(&default_config());

    // 时长按首尾时间戳算，而不是按点数 × 间隔——中间可能断过。
    let duration_ms = points
        .last()
        .map(|p| p.itow.wrapping_sub(points[0].itow))
        .unwrap_or(0);
    write_u32(&mut h, 284, duration_ms);
    h[288] = 0; // NumberOfLapEvents
    h[289] = 0; // NumberOfLaps
    write_u16(&mut h, 290, 0); // NumberOfSectorEvents
    write_u32(&mut h, 292, 0); // BestLapTime
    write_i16(&mut h, 296, i16_of(max_speed * 100.0));
    write_i16(&mut h, 298, i16_of(min_speed * 100.0));
    write_i16(&mut h, 300, i16_of(max_g * 1000.0));
    h[302] = 0; // BestLapNumber
    h[303] = 0; // AutoEnd: bit0 = 0 表示自动结束

    h[304] = 3;
    h[305] = 1; // SensorType::GPS
    h[306] = 2; // SensorType::GForce
    h[307] = 3; // SensorType::Gyro
    write_u32(&mut h, 308, crc32(&body));
    // 头部校验和覆盖前 312 字节，字段里放的是它自己算出来的值。
    // 先算完再写：同一句话里既可变借用 h 又只读借用 h 会被借用检查挡下来。
    let header_crc = crc32(&h[..312]);
    write_u32(&mut h, 312, header_crc);

    let mut out = Vec::with_capacity(HEADER_LEN + body.len());
    out.extend_from_slice(&h);
    out.extend_from_slice(&body);
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

    /// 头部自洽性：长度字段、CRC、类型表都要对得上。
    #[test]
    fn header_is_consistent() {
        let pts: Vec<TrackPoint> = (0..300).map(sample).collect();
        let blob = encode_session(&pts, &meta()).expect("非空");
        assert_eq!(&blob[0..4], b"RB\x02\x01");
        assert_eq!(u16::from_le_bytes([blob[4], blob[5]]) as usize, HEADER_LEN);
        let data_len = u32::from_le_bytes([blob[12], blob[13], blob[14], blob[15]]) as usize;
        assert_eq!(blob.len(), HEADER_LEN + data_len);

        // 头部 CRC 覆盖前 312 字节
        let want = u32::from_le_bytes([blob[312], blob[313], blob[314], blob[315]]);
        assert_eq!(crc32(&blob[..312]), want);
        // 记录流 CRC
        let want = u32::from_le_bytes([blob[308], blob[309], blob[310], blob[311]]);
        assert_eq!(crc32(&blob[HEADER_LEN..]), want);
        // 类型表
        assert_eq!(blob[304], 3);
        assert_eq!(&blob[305..308], &[1, 2, 3]);
    }

    /// 记录流必须能被逐条走完，且条数与头部声明一致。
    #[test]
    fn records_walk_to_the_end() {
        let pts: Vec<TrackPoint> = (0..500).map(sample).collect();
        let blob = encode_session(&pts, &meta()).expect("非空");
        let body = &blob[HEADER_LEN..];
        let mut p = 0usize;
        let mut n = 0u32;
        while p < body.len() {
            let prefix = body[p];
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
    }

    /// 间隔超过 2 秒时必须重发键记录，否则服务端会算错时间轴。
    #[test]
    fn long_gap_forces_key_record() {
        let mut a = sample(0);
        let mut b = sample(1);
        b.itow = a.itow + 5_000;
        a.speed_mmps = 0;
        b.speed_mmps = 0;
        let (body, _) = encode_records(&[a, b]);
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

    #[test]
    fn empty_input_yields_none() {
        assert!(encode_session(&[], &meta()).is_none());
    }
}
