//! RaceBox 云上传通道。
//!
//! 它是一条**附加**通道：和 PostgreSQL / MQTT 并列，不参与“二选一”。
//! 之所以做成 `DataSink`，是为了直接复用现有的一整套逻辑——按设备会话边界
//! 分段、段内进度回调、失败转本地缓存、取消时提前返回，全都不用重写。

use crate::config::CloudConfig;
use crate::racebox::api::rfc1123;
use crate::racebox::CloudClient;
use crate::sink::{DataSink, Session, SinkError, SinkResult};
use crate::t;
use async_trait::async_trait;
use chrono::{NaiveDate, TimeDelta, Utc};

fn recording_day(utc: chrono::DateTime<Utc>, offset_minutes: i16) -> Option<NaiveDate> {
    utc.naive_utc()
        .checked_add_signed(TimeDelta::minutes(i64::from(offset_minutes)))
        .map(|local| local.date())
}

fn highest_index(
    sessions: &[crate::racebox::api::CloudSessionIndex],
    serial: u64,
    day: NaiveDate,
    default_offset: i16,
) -> u8 {
    sessions.iter()
        .filter(|item| serial == 0 || item.device_serial_number == Some(serial))
        .filter_map(|item| {
            let utc = chrono::DateTime::from_timestamp_millis(item.started_at()?)?;
            let offset = item.tz_offset_minutes.unwrap_or(default_offset);
            (recording_day(utc, offset)? == day).then_some(item.index_in_the_day?)
        })
        .max()
        .unwrap_or(0)
}

#[test]
fn sequence_restarts_at_recording_timezone_midnight() {
    let sessions = serde_json::from_value::<Vec<crate::racebox::api::CloudSessionIndex>>(
        serde_json::json!([
            {"deviceSerialNumber": 1, "indexInTheDay": 19, "dateTimeStartedUTC": "2026-11-22T15:59:59Z"},
            {"deviceSerialNumber": 1, "indexInTheDay": 3, "dateTimeStartedUTC": "2026-11-22T16:00:00Z"},
            {"deviceSerialNumber": 1, "indexInTheDay": 2, "dateTimeStartedUTC": "2026-11-22T17:00:00Z"},
            {"deviceSerialNumber": 2, "indexInTheDay": 99, "dateTimeStartedUTC": "2026-11-22T16:00:00Z"}
        ]),
    ).unwrap();
    let day = |d| NaiveDate::from_ymd_opt(2026, 11, d).unwrap();
    assert_eq!(highest_index(&sessions, 1, day(22), 480) + 1, 20);
    assert_eq!(highest_index(&sessions, 1, day(23), 480) + 1, 4);
    assert_eq!(highest_index(&sessions, 1, day(24), 480) + 1, 1);
}

pub struct RaceBoxSink {
    client: CloudClient,
    device_serial: u64,
    /// 配置没填时的第二兜底：上次连接过的蓝牙设备名里截出来的序列号。
    /// 本地文件导入时 `Session.device` 是文件名，解析不出序列号就靠它。
    fallback_serial: u64,
    tz_offset_min: i16,
    indices: tokio::sync::Mutex<std::collections::HashMap<(u64, NaiveDate), u8>>,
}

impl RaceBoxSink {
    pub fn new(auth: String, cfg: &CloudConfig, last_device_name: Option<&str>) -> Self {
        Self {
            client: CloudClient::new(Some(auth)),
            device_serial: cfg.device_serial,
            fallback_serial: last_device_name
                .and_then(crate::racebox::serial_from_device_name)
                .unwrap_or(0),
            tz_offset_min: cfg.tz_offset_min,
            indices: Default::default(),
        }
    }

    /// 设备序列号取值顺序：配置 > 本次蓝牙设备名 > 上次记住的设备名 > 0。
    ///
    /// 配置里填了就以配置为准（用户可能用另一台旧设备归类）；没填才自动
    /// 从设备名截取。返回值连来源一起打进上传日志，方便排查「详情页没天气」。
    fn serial_for(&self, session: &Session) -> (u64, &'static str) {
        if self.device_serial != 0 {
            return (self.device_serial, "配置");
        }
        if let Some(v) = crate::racebox::serial_from_device_name(&session.device) {
            return (v, "蓝牙设备名");
        }
        if self.fallback_serial != 0 {
            return (self.fallback_serial, "上次连接的设备名");
        }
        (0, "未知")
    }
}

#[async_trait]
impl DataSink for RaceBoxSink {
    fn name(&self) -> &'static str {
        t!("RaceBox 云", "RaceBox Cloud")
    }

    async fn upload_session(
        &self,
        session: &Session,
        progress: &(dyn Fn(usize) + Send + Sync),
    ) -> SinkResult {
        let (device_serial, serial_source) = self.serial_for(session);
        let first = session.records.first().ok_or_else(|| SinkError::cloud(
            t!("这一段没有有效记录", "segment has no valid record"), false,
        ))?;
        let day = NaiveDate::from_ymd_opt(i32::from(first.year), first.month.into(), first.day.into())
            .and_then(|date| date.and_hms_opt(first.hour.into(), first.minute.into(), first.second.into()))
            .and_then(|utc| recording_day(utc.and_utc(), self.tz_offset_min))
            .ok_or_else(|| SinkError::cloud("会话起始时间无效，无法按日编号", false))?;
        let key = (device_serial, day);
        // 序列化云上传，避免两个并行的数据段拿到同一个序号。
        let mut indices = self.indices.lock().await;
        let index = if let Some(&last) = indices.get(&key) {
            last
        } else {
            let sessions = self.client.session_indices().await
                .map_err(|e| SinkError::cloud(e.msg, e.retry))?;
            highest_index(&sessions, device_serial, day, self.tz_offset_min)
        };
        let next_index = index.checked_add(1)
            .ok_or_else(|| SinkError::cloud("当天会话序号已达 255，无法分配新号", false))?;
        let (bytes, runs) =
            crate::racebox::session_bytes(
                &session.records,
                device_serial,
                self.tz_offset_min,
                u32::from(next_index),
            )
                .ok_or_else(|| {
                    SinkError::cloud(
                        t!("这一段没有有效记录", "segment has no valid record"),
                        false,
                    )
                })?;

        // 服务端用这个头做增量判断；用当前时间即可（我们没有本地会话文件的 mtime）。
        let stamp = rfc1123(std::time::SystemTime::now());
        let uploaded = self
            .client
            .upload_session(bytes, &stamp)
            .await
            .map_err(|e| SinkError::cloud(e.msg, e.retry))?;
        indices.insert(key, next_index);

        progress(session.records.len());
        // 明确写出类型和跑次：云端按类型分列表，Drag 列表只展示「有跑次」的会话，
        // 日志里带上这两个数才好排查「传了但看不到」这类问题。
        log::info!(
            "RaceBox 云: Drag 会话 {} 已上传，{} 条记录，{} 秒，{} 跑，设备序列号 {}（{}）",
            uploaded.id,
            uploaded.records,
            uploaded.duration,
            runs,
            device_serial,
            serial_source
        );
        if device_serial == 0 {
            log::warn!(
                "RaceBox 云: 设备序列号为空，官网可能不补全天气/车辆信息，网页版详情页会缺数据"
            );
        }
        Ok(())
    }
}
