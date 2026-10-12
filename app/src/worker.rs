use crate::ble;
use crate::config::Config;
use crate::protocol::{self, DeviceStatus, FrameReader, Record, CMD_DOWNLOAD, CMD_ERASE, CMD_STATUS};
use crate::sink::mqtt::MqttSink;
use crate::sink::pg::PgSink;
use crate::sink::{spool, DataSink, Session};
use btleplug::api::Characteristic;
use btleplug::platform::Peripheral;
use std::sync::mpsc::Sender;
use std::sync::Arc;
use std::time::{Duration, Instant};

const DEVICE_SCAN_TIMEOUT: Duration = Duration::from_secs(45);
const DOWNLOAD_TIMEOUT: Duration = Duration::from_secs(1800);
const DOWNLOAD_IDLE_TIMEOUT: Duration = Duration::from_secs(120);
const ERASE_TIMEOUT: Duration = Duration::from_secs(180);

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum LogLevel {
    Info,
    Warn,
    Error,
    Success,
}

#[derive(Clone, Debug)]
pub enum UiEvent {
    Phase(String),
    Log(LogLevel, String),
    /// 需要用户在同步页立刻看到的结果（测试连接、任务完成情况等）。
    Notice(LogLevel, String),
    DeviceFound { name: String, address: String },
    RememberDevice { address: String, name: String },
    Status(DeviceStatus),
    DownloadProgress { received: usize, total: usize },
    SessionInfo { index: u32, name: String, records: usize, ok: bool, detail: String },
    EraseProgress(u8),
    SpoolPending(usize),
    Finished { ok: bool, summary: String },
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum RunMode {
    Sync,
    Status,
    Erase,
}

fn u32le(payload: &[u8]) -> u32 {
    if payload.len() < 4 {
        return 0;
    }
    u32::from_le_bytes([payload[0], payload[1], payload[2], payload[3]])
}

struct SessionFlush {
    device: String,
    sync_id: [u8; 16],
    imp_stamp: String,
    records: Vec<Record>,
    session_index: u32,
    duration_secs: f64,
    sync_offset: usize,
    sync_total_upper: usize,
    sync_received_total: usize,
    sync_final: bool,
}

async fn flush_session(cfg: &Config, sinks: &[Box<dyn DataSink>], tx: &Sender<UiEvent>, flush: &SessionFlush) -> bool {
    if flush.records.is_empty() {
        return true;
    }
    let session = Session {
        device: flush.device.clone(),
        session_index: flush.session_index,
        records: flush.records.clone(),
        duration_secs: flush.duration_secs,
        sync_id: flush.sync_id,
        imp_stamp: flush.imp_stamp.clone(),
        sync_offset: flush.sync_offset,
        sync_total_upper: flush.sync_total_upper,
        sync_received_total: flush.sync_received_total,
        sync_final: flush.sync_final,
    };
    let mut ok = true;
    let mut detail = String::new();
    for sink in sinks {
        match sink.upload_session(&session).await {
            Ok(()) => {
                detail = format!("已上传至 {}", sink.name());
            }
            Err(e) => {
                ok = false;
                detail = format!("上传失败: {e}");
                let _ = tx.send(UiEvent::Log(LogLevel::Error, format!("第{}段 {} 上传失败: {e}", flush.session_index, sink.name(),)));
            }
        }
    }
    if !ok && cfg.spool_on_failure {
        match spool::write_session(&session) {
            Ok(path) => {
                let _ = tx.send(UiEvent::Log(LogLevel::Warn, format!("已写入本地缓存: {}", path.display())));
                detail.push_str("，已转本地缓存");
            }
            Err(e) => {
                let _ = tx.send(UiEvent::Log(LogLevel::Error, format!("本地缓存写入失败: {e}")));
            }
        }
    }
    let _ = tx.send(UiEvent::SpoolPending(spool::pending_count()));
    let _ = tx.send(UiEvent::SessionInfo {
        index: flush.session_index,
        name: session.display_name(),
        records: session.record_count(),
        ok,
        detail,
    });
    ok
}

async fn build_sinks(cfg: &Config, tx: &Sender<UiEvent>) -> Vec<Box<dyn DataSink>> {
    let mut sinks: Vec<Box<dyn DataSink>> = Vec::new();
    if cfg.uses_database() {
        sinks.push(Box::new(PgSink::new(cfg.pg.clone())));
    }
    if cfg.uses_mqtt() {
        match MqttSink::new(cfg.mqtt.clone()).await {
            Ok(sink) => sinks.push(Box::new(sink)),
            Err(e) => {
                let _ = tx.send(UiEvent::Log(
                    LogLevel::Error,
                    format!("MQTT 通道不可用: {e}"),
                ));
            }
        }
    }
    sinks
}

async fn query_status(
    peripheral: &Peripheral,
    rx_char: &Characteristic,
    tx_char: &Characteristic,
    tx: &Sender<UiEvent>,
) -> Option<DeviceStatus> {
    if ble::subscribe(peripheral, tx_char).await.is_err() {
        return None;
    }
    let mut stream = ble::notification_stream(peripheral).await.ok()?;
    ble::write_command(peripheral, rx_char, &CMD_STATUS).await.ok()?;
    let deadline = Instant::now() + Duration::from_secs(8);
    let mut reader = FrameReader::new();
    while Instant::now() < deadline {
        let data = ble::next_notification(&mut stream, Duration::from_secs(3)).await?;
        reader.push(&data);
        while let Some(packet) = reader.next_packet() {
            match packet.id {
                protocol::MSG_STATUS => {
                    if let Some(status) = protocol::parse_status(&packet.payload) {
                        let _ = tx.send(UiEvent::Log(
                            LogLevel::Info,
                            format!(
                                "设备状态: 录制={} 内存={}% 已存{}/{}条",
                                if status.recording { "是" } else { "否" },
                                status.memory_level,
                                status.stored_messages,
                                status.max_messages
                            ),
                        ));
                        return Some(status);
                    }
                }
                protocol::MSG_NACK => {
                    let _ = tx.send(UiEvent::Log(LogLevel::Warn, "设备不支持状态查询".to_string()));
                    return None;
                }
                _ => {}
            }
        }
    }
    None
}

async fn erase_memory(peripheral: &Peripheral, rx_char: &Characteristic, tx_char: &Characteristic, tx: &Sender<UiEvent>) -> Result<(), String> {
    ble::subscribe(peripheral, tx_char).await?;
    let mut stream = ble::notification_stream(peripheral).await?;
    ble::write_command(peripheral, rx_char, &CMD_ERASE).await?;
    let _ = tx.send(UiEvent::Log(LogLevel::Info, "开始清除设备内存...".to_string()));
    let deadline = Instant::now() + ERASE_TIMEOUT;
    let mut reader = FrameReader::new();
    let mut last_percent = 0u8;
    while Instant::now() < deadline {
        let data = ble::next_notification(&mut stream, Duration::from_secs(30))
            .await
            .ok_or_else(|| "清除过程中设备无响应".to_string())?;
        reader.push(&data);
        while let Some(packet) = reader.next_packet() {
            match packet.id {
                protocol::MSG_ERASE_PROGRESS => {
                    let percent = packet.payload.first().copied().unwrap_or(0);
                    if percent != last_percent {
                        last_percent = percent;
                        let _ = tx.send(UiEvent::EraseProgress(percent));
                    }
                }
                protocol::MSG_ACK => {
                    if packet.payload.starts_with(&[0xFF, protocol::MSG_ERASE_PROGRESS]) {
                        let _ = tx.send(UiEvent::Log(LogLevel::Success, "设备内存清除完成".to_string()));
                        return Ok(());
                    }
                }
                protocol::MSG_NACK => {
                    return Err("设备拒绝清除，内存可能已锁定或设备不支持".to_string());
                }
                _ => {}
            }
        }
    }
    Err("清除超时".to_string())
}

struct DownloadResult {
    sessions: u32,
    records: usize,
    skipped: usize,
    failures: usize,
}

async fn download_and_upload(
    cfg: &Config,
    sinks: &[Box<dyn DataSink>],
    device: &str,
    peripheral: &Peripheral,
    rx_char: &Characteristic,
    tx_char: &Characteristic,
    tx: &Sender<UiEvent>,
) -> Result<DownloadResult, String> {
    ble::subscribe(peripheral, tx_char).await?;
    let mut stream = ble::notification_stream(peripheral).await?;
    ble::write_command(peripheral, rx_char, &CMD_DOWNLOAD).await?;
    let _ = tx.send(UiEvent::Log(
        LogLevel::Info,
        format!("正在从设备 {device} 下载数据..."),
    ));

    let sync_id = *uuid::Uuid::new_v4().as_bytes();
    let imp_stamp = uuid::Uuid::new_v4().to_string();
    let mut reader = FrameReader::new();
    let mut current: Vec<Record> = Vec::new();
    let mut session_index = 0u32;
    let mut received_total = 0usize;
    let mut skipped = 0usize;
    let mut total_upper = 0usize;
    let mut records_total = 0usize;
    let mut failures = 0usize;
    let mut session_base = 0usize;
    let mut session_started = Instant::now();
    let mut download_done = false;

    let deadline = Instant::now() + DOWNLOAD_TIMEOUT;
    while !download_done {
        if Instant::now() >= deadline {
            return Err("下载超时（设备长时间无响应）".to_string());
        }
        let data = ble::next_notification(&mut stream, DOWNLOAD_IDLE_TIMEOUT)
            .await
            .ok_or_else(|| "下载过程中设备无响应".to_string())?;
        reader.push(&data);
        while let Some(packet) = reader.next_packet() {
            match packet.id {
                protocol::MSG_DOWNLOAD_START => {
                    total_upper = u32le(&packet.payload) as usize;
                    let _ = tx.send(UiEvent::Log(
                        LogLevel::Info,
                        format!("设备报告约 {total_upper} 条记录"),
                    ));
                    let _ = tx.send(UiEvent::DownloadProgress { received: 0, total: total_upper });
                }
                protocol::MSG_RECORD_HISTORY | protocol::MSG_RECORD_LIVE => {
                    received_total += 1;
                    match Record::decode(&packet.payload) {
                        Some(record) => current.push(record),
                        None => skipped += 1,
                    }
                    if received_total % 50 == 0 {
                        let _ = tx.send(UiEvent::DownloadProgress {
                            received: received_total,
                            total: if total_upper > 0 { total_upper } else { received_total },
                        });
                    }
                }
                protocol::MSG_SESSION_BOUNDARY => {
                    session_index += 1;
                    let flush = SessionFlush {
                        device: device.to_string(),
                        sync_id,
                        imp_stamp: imp_stamp.clone(),
                        records: std::mem::take(&mut current),
                        session_index,
                        duration_secs: session_started.elapsed().as_secs_f64(),
                        sync_offset: session_base,
                        sync_total_upper: total_upper,
                        sync_received_total: received_total,
                        sync_final: false,
                    };
                    records_total += flush.records.len();
                    if !flush_session(cfg, sinks, tx, &flush).await {
                        failures += 1;
                    }
                    session_base = received_total;
                    session_started = Instant::now();
                }
                protocol::MSG_ACK => {
                    if packet.payload.starts_with(&[0xFF, protocol::MSG_DOWNLOAD_START]) {
                        download_done = true;
                    }
                }
                protocol::MSG_NACK => {
                    return Err("设备拒绝下载".to_string());
                }
                _ => {}
            }
        }
    }

    session_index += 1;
    let flush = SessionFlush {
        device: device.to_string(),
        sync_id,
        imp_stamp,
        records: std::mem::take(&mut current),
        session_index,
        duration_secs: session_started.elapsed().as_secs_f64(),
        sync_offset: session_base,
        sync_total_upper: total_upper,
        sync_received_total: received_total,
        sync_final: true,
    };
    records_total += flush.records.len();
    if !flush_session(cfg, sinks, tx, &flush).await {
        failures += 1;
    }

    let _ = tx.send(UiEvent::DownloadProgress {
        received: received_total,
        total: received_total,
    });
    Ok(DownloadResult {
        sessions: session_index,
        records: records_total,
        skipped,
        failures,
    })
}

async fn run_inner(cfg: &Config, mode: RunMode, tx: &Sender<UiEvent>) -> Result<String, String> {
    let sinks = build_sinks(cfg, tx).await;
    if sinks.is_empty() && mode != RunMode::Status {
        return Err("没有可用的上传通道，请在设置中选择并配置 PostgreSQL 或 MQTT".to_string());
    }
    if !sinks.is_empty() {
        let refs: Vec<&dyn DataSink> = sinks.iter().map(|b| b.as_ref()).collect();
        for line in spool::replay(&refs).await {
            let _ = tx.send(UiEvent::Log(LogLevel::Info, line));
        }
        let _ = tx.send(UiEvent::SpoolPending(spool::pending_count()));
    }

    let _ = tx.send(UiEvent::Phase("扫描设备".to_string()));
    let adapter = ble::first_adapter().await?;
    let peripheral = ble::find_device(
        &adapter,
        &cfg.device_name_filter,
        cfg.last_device_address.as_deref(),
        DEVICE_SCAN_TIMEOUT,
        |name, address| {
            let _ = tx.send(UiEvent::DeviceFound {
                name: name.to_string(),
                address: address.to_string(),
            });
        },
    )
    .await?;

    let (device_name, device_address) = ble::device_info(&peripheral).await;
    let _ = tx.send(UiEvent::DeviceFound {
        name: device_name.clone(),
        address: device_address.clone(),
    });
    let _ = tx.send(UiEvent::RememberDevice {
        address: device_address.clone(),
        name: device_name.clone(),
    });
    let _ = tx.send(UiEvent::Log(
        LogLevel::Info,
        format!("已找到设备: {} ({device_address})", if device_name.is_empty() { "未知设备" } else { &device_name }),
    ));

    let _ = tx.send(UiEvent::Phase("连接设备".to_string()));
    let chars = ble::connect(&peripheral).await;
    let (rx_char, tx_char) = match chars {
        Ok(v) => v,
        Err(e) => {
            ble::disconnect(&peripheral).await;
            return Err(e);
        }
    };
    let _ = tx.send(UiEvent::Log(LogLevel::Success, "设备已连接".to_string()));

    let outcome = async {
        match mode {
            RunMode::Status => {
                let status = query_status(&peripheral, &rx_char, &tx_char, tx).await;
                match status {
                    Some(status) => {
                        let _ = tx.send(UiEvent::Status(status));
                        Ok("状态查询完成".to_string())
                    }
                    None => Err("状态查询失败或设备不支持".to_string()),
                }
            }
            RunMode::Erase => {
                erase_memory(&peripheral, &rx_char, &tx_char, tx).await?;
                Ok("设备内存已清理".to_string())
            }
            RunMode::Sync => {
                let result = download_and_upload(cfg, &sinks, &device_name, &peripheral, &rx_char, &tx_char, tx).await?;
                if result.records == 0 {
                    return Err("设备没有可取回的数据".to_string());
                }
                let summary = format!(
                    "下载完成: {} 个数据段, {} 条记录 (跳过无定位 {} 条)",
                    result.sessions, result.records, result.skipped
                );
                let _ = tx.send(UiEvent::Log(LogLevel::Success, summary.clone()));
                if cfg.auto_erase && result.failures == 0 {
                    let _ = tx.send(UiEvent::Phase("清理设备内存".to_string()));
                    match erase_memory(&peripheral, &rx_char, &tx_char, tx).await {
                        Ok(()) => Ok(format!("{summary}，设备内存已清理")),
                        Err(e) => Ok(format!("{summary}，但清理失败: {e} (设备数据未删除，可重新同步)")),
                    }
                } else if result.failures > 0 {
                    Ok(format!("{summary}，但存在上传失败，已跳过清理设备内存"))
                } else {
                    Ok(summary)
                }
            }
        }
    }
    .await;

    let _ = ble::unsubscribe(&peripheral, &tx_char).await;
    ble::disconnect(&peripheral).await;
    outcome
}

pub async fn run_sync(cfg: Arc<Config>, mode: RunMode, tx: Sender<UiEvent>) {
    let result = run_inner(&cfg, mode, &tx).await;
    match result {
        Ok(summary) => {
            let _ = tx.send(UiEvent::Log(LogLevel::Success, summary.clone()));
            let _ = tx.send(UiEvent::Finished { ok: true, summary });
        }
        Err(e) => {
            let _ = tx.send(UiEvent::Log(LogLevel::Error, e.clone()));
            let _ = tx.send(UiEvent::Finished { ok: false, summary: e });
        }
    }
}
