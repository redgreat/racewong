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
pub use session::{encode_session, records_from_points, SessionMeta, TrackPoint};

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
    }
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
pub fn session_bytes(records: &[Record], device_serial: u64, tz_offset_min: i16) -> Option<Vec<u8>> {
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
    encode_session(&points, &meta)
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
}
