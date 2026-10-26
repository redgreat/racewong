use super::{DataSink, Session, SinkError, SinkResult};
use crate::config::MqttConfig;
use crate::protocol::{build_rbx2_frame, Rbx2Batch, RBX2_MAX_BATCH_RECORDS};
use crate::{t, tr};
use async_trait::async_trait;
use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

const ACK_TIMEOUT: Duration = Duration::from_secs(60);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

pub struct MqttSink {
    cfg: MqttConfig,
    client: AsyncClient,
    acks: Arc<tokio::sync::Mutex<tokio::sync::mpsc::UnboundedReceiver<()>>>,
    connected: Arc<AtomicBool>,
    stopping: Arc<AtomicBool>,
    /// 上传串行锁：ack 计数是共享通道，同时只有一个上传者在数
    upload_lock: tokio::sync::Mutex<()>,
}

impl MqttSink {
    pub fn is_connected(&self) -> bool {
        self.connected.load(Ordering::SeqCst)
    }

    pub async fn disconnect(&self) {
        self.stopping.store(true, Ordering::SeqCst);
        let _ = self.client.disconnect().await;
    }

    /// `cancel` 非空时，等待 CONNACK 的过程中被取消会立刻返回错误。
    pub async fn new(cfg: MqttConfig, cancel: Option<&AtomicBool>) -> Result<Self, SinkError> {
        if !cfg.ready() {
            return Err(SinkError::Config(
                t!("MQTT 配置不完整", "MQTT settings are incomplete").to_string(),
            ));
        }
        // 同一台机器上可能同时存在多个连接（例如设置页的探测连接还开着），
        // client_id 完全相同会被 broker 踢掉其中一个，所以每次连接都加随机后缀。
        let suffix = &uuid::Uuid::new_v4().simple().to_string()[..8];
        let client_id = format!("{}-{suffix}", cfg.client_id.trim());
        let mut options = MqttOptions::new(client_id, cfg.broker.clone(), cfg.port);
        if !cfg.user.trim().is_empty() {
            options.set_credentials(cfg.user.clone(), cfg.password.clone());
        }
        let keepalive = cfg.keepalive_secs.clamp(5, 600);
        options.set_keep_alive(Duration::from_secs(keepalive as u64));
        let (client, mut eventloop) = AsyncClient::new(options, 32);
        let connected = Arc::new(AtomicBool::new(false));
        let stopping = Arc::new(AtomicBool::new(false));
        let last_error = Arc::new(std::sync::Mutex::new(String::new()));
        let (ack_tx, ack_rx) = tokio::sync::mpsc::unbounded_channel::<()>();
        let connected_task = connected.clone();
        let stopping_task = stopping.clone();
        let last_error_task = last_error.clone();
        tokio::spawn(async move {
            // 这个任务一旦退出，request channel 就会断开，之后所有 publish 都会报
            // "Failed to send mqtt requests to eventloop"。rumqttc 的 eventloop
            // 只要继续 poll 就会在断线后自动重连，所以除“主动停止”外绝不退出。
            let mut announced = false;
            loop {
                match eventloop.poll().await {
                    Ok(Event::Incoming(Packet::ConnAck(_))) => {
                        connected_task.store(true, Ordering::SeqCst);
                        announced = false;
                    }
                    Ok(Event::Incoming(Packet::PubAck(_))) => {
                        let _ = ack_tx.send(());
                    }
                    Ok(Event::Incoming(Packet::Disconnect)) => {
                        connected_task.store(false, Ordering::SeqCst);
                    }
                    Ok(_) => {}
                    Err(e) => {
                        let was_connected = connected_task.swap(false, Ordering::SeqCst);
                        if stopping_task.load(Ordering::SeqCst) {
                            log::info!("MQTT 连接已按要求关闭");
                            break;
                        }
                        // 只在状态变化时记日志，避免 broker 长时间不在线时刷屏
                        if was_connected || !announced {
                            log::warn!("MQTT 连接中断: {e}，将自动重连");
                            announced = true;
                        }
                        if let Ok(mut slot) = last_error_task.lock() {
                            *slot = e.to_string();
                        }
                    }
                }
            }
        });

        let sink = Self {
            cfg,
            client,
            acks: Arc::new(tokio::sync::Mutex::new(ack_rx)),
            connected,
            stopping,
            upload_lock: tokio::sync::Mutex::new(()),
        };
        let deadline = Instant::now() + CONNECT_TIMEOUT;
        while Instant::now() < deadline {
            if sink.is_connected() {
                return Ok(sink);
            }
            if let Some(flag) = cancel {
                if flag.load(Ordering::SeqCst) {
                    return Err(SinkError::Mqtt(crate::worker::CANCELLED.to_string()));
                }
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        // 把 eventloop 里记录的真实错误带出来，比干巴巴的“超时”有用得多
        let detail = last_error.lock().map(|e| e.clone()).unwrap_or_default();
        let msg = if detail.is_empty() {
            tr!(
                "连接 broker {}:{} 超时",
                "timed out connecting to broker {}:{}",
                sink.cfg.broker,
                sink.cfg.port
            )
        } else {
            tr!(
                "连接 broker {}:{} 失败: {detail}",
                "failed to connect to broker {}:{}: {detail}",
                sink.cfg.broker,
                sink.cfg.port
            )
        };
        Err(SinkError::Mqtt(msg))
    }

    /// 等 n 个 PUBACK（流水线发布后批量计数）；连接断开时快速失败。
    async fn wait_acks(&self, n: usize) -> SinkResult {
        if n == 0 {
            return Ok(());
        }
        let deadline = Instant::now() + ACK_TIMEOUT.max(Duration::from_secs(30) * n as u32 / 64);
        let mut got = 0usize;
        while got < n {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(SinkError::Mqtt(tr!(
                    "等待 PUBACK 超时（已确认 {got}/{n}）",
                    "timed out waiting for PUBACK ({got}/{n} acknowledged)"
                )));
            }
            let mut rx = self.acks.lock().await;
            match tokio::time::timeout(remaining.min(Duration::from_millis(500)), rx.recv()).await
            {
                Ok(Some(())) => got += 1,
                Ok(None) => {
                    return Err(SinkError::Mqtt(
                        t!("MQTT 事件循环已退出", "MQTT event loop exited").to_string(),
                    ))
                }
                Err(_) => {
                    drop(rx);
                    if !self.is_connected() {
                        return Err(SinkError::Mqtt(
                            t!(
                                "MQTT 连接已断开，未收到发布确认",
                                "MQTT connection lost before publish was acknowledged"
                            )
                            .to_string(),
                        ));
                    }
                }
            }
        }
        Ok(())
    }
}

impl Drop for MqttSink {
    fn drop(&mut self) {
        self.stopping.store(true, Ordering::SeqCst);
    }
}

#[async_trait]
impl DataSink for MqttSink {
    fn name(&self) -> &'static str {
        "MQTT"
    }

    async fn upload_session(
        &self,
        session: &Session,
        progress: &(dyn Fn(usize) + Send + Sync),
    ) -> SinkResult {
        if session.records.is_empty() {
            return Ok(());
        }
        // 串行化：ack 计数通道是共享的
        let _guard = self.upload_lock.lock().await;
        let total_records = session.records.len();
        let batch_count = total_records.div_ceil(RBX2_MAX_BATCH_RECORDS);
        let sync_total = session.sync_total() as u32;

        // 先构建所有帧（每帧 ~9.7KB，一个 20 万条的段约 1600 帧 / 16MB，
        // 相比 records 本身占用可接受；真正限制来自 request channel 的背压）
        let mut frames: Vec<Vec<u8>> = Vec::with_capacity(batch_count);
        for batch_index in 0..batch_count {
            let start = batch_index * RBX2_MAX_BATCH_RECORDS;
            let end = (start + RBX2_MAX_BATCH_RECORDS).min(total_records);
            let records = &session.records[start..end];
            let mut records_flat = Vec::with_capacity(records.len() * 80);
            for r in records {
                records_flat.extend_from_slice(&r.raw);
            }
            let first = &records[0];
            let last = &records[records.len() - 1];
            let session_offset = start;
            let session_final = session_offset + records.len() == total_records;
            let batch = Rbx2Batch {
                records_flat,
                count: records.len(),
                sync_offset: session.sync_offset + start,
                sync_total,
                sync_final: session.sync_final && session_final,
                session_index: session.session_index,
                session_offset,
                session_total: total_records,
                session_start_utc: first.utc_numeric(),
                session_end_utc: if session_final { last.utc_numeric() } else { 0 },
                session_start_itow: first.itow,
                session_start_nano: first.nanoseconds,
                sync_id: session.sync_id,
                device: session.device.clone(),
                sync_date: first.local_date_numeric(),
            };
            frames.push(build_rbx2_frame(&batch));
        }

        // 流水线发布：连续发一批（CHUNK 个）再统一等 ACK，避免逐包往返
        const CHUNK: usize = 64;
        let mut sent = 0usize;
        while sent < batch_count {
            let end = (sent + CHUNK).min(batch_count);
            for frame in &frames[sent..end] {
                self.client
                    .publish(self.cfg.topic.clone(), QoS::AtLeastOnce, false, frame.clone())
                    .await
                    .map_err(|e| {
                        if self.is_connected() {
                            SinkError::Mqtt(tr!("MQTT 发布失败: {e}", "MQTT publish failed: {e}"))
                        } else {
                            SinkError::Mqtt(
                                t!(
                                    "MQTT 连接已断开，发布失败",
                                    "MQTT connection lost, publish failed"
                                )
                                .to_string(),
                            )
                        }
                    })?;
            }
            self.wait_acks(end - sent).await?;
            log::info!(
                "MQTT 已确认批次: 主题={}, 批次 {}-{}/{}, 记录={}",
                self.cfg.topic,
                sent + 1,
                end,
                batch_count,
                total_records
            );
            sent = end;
            // 已经确认的批次换算成记录数（最后一帧可能不满一批，用 min 收口）
            progress((sent * RBX2_MAX_BATCH_RECORDS).min(total_records));
        }
        Ok(())
    }
}
