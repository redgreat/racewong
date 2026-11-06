//! RaceBox 云端对接：登录、会话文件编码、本地文件导入。
//!
//! 模块划分：
//! - [`session`]  设备原生会话文件（RB V2 容器）的编码器
//! - [`import`]   GPX / Garmin FIT 解析，统一成采样点
//! - [`api`]      云端 HTTP 接口（登录 / 上传 / 列表）
//! - [`store`]    账号凭据的本地持久化

pub mod api;
pub mod import;
pub mod session;
pub mod store;

pub use api::CloudClient;
pub use session::{detect_runs, encode_session, records_from_points, SessionMeta, TrackPoint};

use crate::protocol::Record;

/// 把一个 BLE 记录换算成会话采样点。
///
/// 设备下发的 `Record` 里已经带了原始 iTOW 和全部传感器量，几乎是一一对应，
/// 只有三处要处理：
/// - 速度：记录里是 km/h，文件里要 mm/s
/// - 海拔：优先用海拔高（MSL），设备给的 WGS84 椭球高比它高几十米
/// - 航向：记录里已经是度
pub fn record_to_point(r: &Record) -> TrackPoint {
    let alt = if r.msl_altitude.abs() > 0.001 {
        r.msl_altitude
    } else {
        r.wgs_altitude
    };
    TrackPoint {
        itow: r.itow,
        lat: r.latitude,
        lng: r.longitude,
        alt,
        speed_mmps: ((r.speed_kmh.max(0.0)) / 3.6 * 1000.0) as u32,
        heading: r.heading.rem_euclid(360.0),
        gx: r.gforce_x,
        gy: r.gforce_y,
        gz: r.gforce_z,
        rx: r.rotation_rate_x,
        ry: r.rotation_rate_y,
        rz: r.rotation_rate_z,
        // 精度和卫星数记录流里没有，只能靠 240 系统记录带上去（详情页的 GPS Accuracy）
        hacc_m: r.horizontal_accuracy,
        sats: r.satellites,
    }
}

/// 从 BLE 设备名里截出设备序列号。
///
/// 设备名形如 `RaceBox Mini S 2254300997 (E8:D9:EE:6D:DC:57)`，序列号是中间
/// 那段 10 位数字。配置里 `cloud.device_serial` 没填（默认 0）时拿它兜底：
/// 序列号是云端补全天气 / 车辆信息的依据，留 0 的会话实测 99% 没有
/// `weather` 和 `vehicleId`，网页版详情页会缺一大片，手机 App 却照常显示。
///
/// 只认带 `RaceBox` 字样的名字、且数字至少 8 位：本地文件导入时
/// `Session.device` 是文件名（可能含 `20261106` 这样的日期），不能误当序列号。
pub fn serial_from_device_name(name: &str) -> Option<u64> {
    if !name.to_ascii_lowercase().contains("racebox") {
        return None;
    }
    let bytes = name.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i].is_ascii_digit() {
            let start = i;
            while i < bytes.len() && bytes[i].is_ascii_digit() {
                i += 1;
            }
            // 数字串全是 ASCII，切片边界一定落在字符边界上
            if i - start >= 8 {
                if let Ok(v) = name[start..i].parse::<u64>() {
                    if v != 0 {
                        return Some(v);
                    }
                }
            }
        } else {
            i += 1;
        }
    }
    None
}

/// 会话 id：12 字节，云端返回的 id 就是它的十六进制。
pub fn new_session_id() -> [u8; 12] {
    let u = uuid::Uuid::new_v4();
    let b = u.as_bytes();
    let mut id = [0u8; 12];
    id.copy_from_slice(&b[..12]);
    id
}

/// 一组记录 → 会话文件字节流。
/// 把一段设备记录编码成 Drag 会话文件，同时返回分出来的跑次数。
///
/// 跑次数要打日志：云端 Drag 列表只展示「有跑次」的会话，传上去却看不到时
/// 第一个要确认的就是这个数——0 跑等于没传。
pub fn session_bytes(
    records: &[Record],
    device_serial: u64,
    tz_offset_min: i16,
) -> Option<(Vec<u8>, usize)> {
    let first = records.first()?;
    let points: Vec<TrackPoint> = records.iter().map(record_to_point).collect();
    let meta = SessionMeta {
        session_id: new_session_id(),
        utc: (
            first.year as i32,
            first.month as u32,
            first.day as u32,
            first.hour as u32,
            first.minute as u32,
            first.second as u32,
        ),
        tz_offset_min,
        device_serial,
        recorder: (1, 11),
    };
    let runs = detect_runs(&points).len();
    encode_session(&points, &meta).map(|bytes| (bytes, runs))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn session_id_is_12_bytes() {
        let a = new_session_id();
        let b = new_session_id();
        assert_eq!(a.len(), 12);
        assert_ne!(a, b, "两次生成的 id 不应相同");
    }

    #[test]
    fn serial_from_device_name_works() {
        assert_eq!(
            serial_from_device_name("RaceBox Mini S 2254300997 (E8:D9:EE:6D:DC:57)"),
            Some(2254300997)
        );
        assert_eq!(
            serial_from_device_name("RaceBox Mini S 2254300997"),
            Some(2254300997)
        );
        assert_eq!(
            serial_from_device_name("RaceBox Micro 3242704435"),
            Some(3242704435)
        );
        assert_eq!(serial_from_device_name(""), None);
        assert_eq!(serial_from_device_name("未知设备"), None);
        // 本地导入时 Session.device 是文件名，日期数字不能被误认成序列号
        assert_eq!(serial_from_device_name("track_20261106.gpx"), None);
        assert_eq!(serial_from_device_name("RaceBox-123"), None);
        assert_eq!(serial_from_device_name("RaceBox Mini S"), None);
    }
}
