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

pub struct RaceBoxSink {
    client: CloudClient,
    device_serial: u64,
    tz_offset_min: i16,
}

impl RaceBoxSink {
    pub fn new(auth: String, cfg: &CloudConfig) -> Self {
        Self {
            client: CloudClient::new(Some(auth)),
            device_serial: cfg.device_serial,
            tz_offset_min: cfg.tz_offset_min,
        }
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
        let (bytes, runs) = crate::racebox::session_bytes(
            &session.records,
            self.device_serial,
            self.tz_offset_min,
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
            .upload_session(&bytes, &stamp)
            .await
            .map_err(|e| SinkError::cloud(e.msg, e.retry))?;

        progress(session.records.len());
        // 明确写出类型和跑次：云端按类型分列表，Drag 列表只展示「有跑次」的会话，
        // 日志里带上这两个数才好排查「传了但看不到」这类问题。
        log::info!(
            "RaceBox 云: Drag 会话 {} 已上传，{} 条记录，{} 秒，{} 跑",
            uploaded.id,
            uploaded.records,
            uploaded.duration,
            runs
        );
        Ok(())
    }
}
