use super::{DataSink, Session, SinkError, SinkResult};
use crate::config::MqttConfig;
use crate::protocol::{build_rbx2_frame, Rbx2Batch, RBX2_MAX_BATCH_RECORDS};
use async_trait::async_trait;
use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

const ACK_TIMEOUT: Duration = Duration::from_secs(60);

pub struct MqttSink {
    cfg: MqttConfig,
    client: AsyncClient,
    acks: Arc<tokio::sync::Mutex<tokio::sync::mpsc::UnboundedReceiver<()>>>,
    connected: Arc<AtomicBool>,
    stopping: Arc<AtomicBool>,
}

impl MqttSink {
    pub fn is_connected(&self) -> bool {
        self.connected.load(Ordering::SeqCst)
    }

    pub async fn disconnect(&self) {
        self.stopping.store(true, Ordering::SeqCst);
        let _ = self.client.disconnect().await;
    }

    pub async fn new(cfg: MqttConfig) -> Result<Self, SinkError> {
        if !cfg.ready() {
            return Err(SinkError::Config("MQTT 配置不完整".to_string()));
        }
        let mut options = MqttOptions::new(cfg.client_id.clone(), cfg.broker.clone(), cfg.port);
        if !cfg.user.trim().is_empty() {
            options.set_credentials(cfg.user.clone(), cfg.password.clone());
        }
        options.set_keep_alive(Duration::from_secs(30));
        let (client, mut eventloop) = AsyncClient::new(options, 32);
        let connected = Arc::new(AtomicBool::new(false));
        let stopping = Arc::new(AtomicBool::new(false));
        let (ack_tx, ack_rx) = tokio::sync::mpsc::unbounded_channel::<()>();
        let connected_task = connected.clone();
        let stopping_task = stopping.clone();
        tokio::spawn(async move {
            loop {
                match eventloop.poll().await {
                    Ok(Event::Incoming(Packet::ConnAck(_))) => {
                        connected_task.store(true, Ordering::SeqCst);
                    }
                    Ok(Event::Incoming(Packet::PubAck(_))) => {
                        let _ = ack_tx.send(());
                    }
                    Ok(Event::Incoming(Packet::Disconnect)) => {
                        connected_task.store(false, Ordering::SeqCst);
                    }
                    Ok(_) => {}
                    Err(e) => {
                        connected_task.store(false, Ordering::SeqCst);
                        if stopping_task.load(Ordering::SeqCst) {
                            log::info!("MQTT 连接已按要求关闭");
                        } else {
                            log::error!("MQTT 事件循环意外结束: {e}");
                        }
                        break;
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
        };
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while std::time::Instant::now() < deadline {
            if sink.is_connected() {
                return Ok(sink);
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        Err(SinkError::Mqtt(format!("连接 broker {}:{} 超时", sink.cfg.broker, sink.cfg.port)))
    }
    async fn wait_ack(&self) -> SinkResult {
        let result = tokio::time::timeout(ACK_TIMEOUT, async {
            let mut rx = self.acks.lock().await;
            rx.recv().await
        })
        .await;
        match result {
            Ok(Some(())) => Ok(()),
            Ok(None) => Err(SinkError::Mqtt("MQTT 事件循环已退出".to_string())),
            Err(_) => Err(SinkError::Mqtt("等待 PUBACK 超时".to_string())),
        }
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

    async fn upload_session(&self, session: &Session) -> SinkResult {
        if session.records.is_empty() {
            return Ok(());
        }
        let total_records = session.records.len();
        let batch_count = total_records.div_ceil(RBX2_MAX_BATCH_RECORDS);
        let sync_total = session.sync_total() as u32;

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
            let frame = build_rbx2_frame(&batch);
            self.client
                .publish(self.cfg.topic.clone(), QoS::AtLeastOnce, false, frame.clone())
                .await
                .map_err(|e| SinkError::Mqtt(e.to_string()))?;
            self.wait_ack().await?;
            log::info!(
                "MQTT 已确认批次: 主题={}, 记录={}/{}, 偏移={}",
                self.cfg.topic,
                records.len(),
                total_records,
                start
            );
        }
        Ok(())
    }
}
