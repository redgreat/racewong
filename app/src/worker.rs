use crate::ble;
use crate::config::Config;
use crate::protocol::{self, DeviceStatus, FrameReader, Record, CMD_DOWNLOAD, CMD_ERASE, CMD_STATUS};
use crate::sink::mqtt::MqttSink;
use crate::sink::pg::PgSink;
use crate::sink::racebox::RaceBoxSink;
use crate::sink::{spool, DataSink, Session};
use crate::{t, tr};
use btleplug::api::Characteristic;
use btleplug::platform::Peripheral;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc::UnboundedSender;

const DOWNLOAD_TIMEOUT: Duration = Duration::from_secs(1800);
const DOWNLOAD_IDLE_TIMEOUT: Duration = Duration::from_secs(120);
const ERASE_TIMEOUT: Duration = Duration::from_secs(180);
/// 同步开始前检测上传通道的超时：主机不可达时 TCP 可能挂很久，不能无限等。
const PREFLIGHT_TIMEOUT: Duration = Duration::from_secs(20);

/// 用户取消任务时内部使用的错误标记。
///
/// 它只在本模块内部用来区分“取消”和“出错”，绝不会直接显示给用户
/// （界面上显示的是本地化过的“已取消”）。
pub const CANCELLED: &str = "__cancelled_by_user__";

fn cancelled(cancel: &AtomicBool) -> bool {
    cancel.load(Ordering::SeqCst)
}

#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub enum LogLevel {
    #[default]
    Info,
    Warn,
    Error,
    Success,
}

/// 任务阶段。
///
/// 以前阶段是个字符串，界面靠 `phase == "清理设备内存"` 这类比较来决定行为，
/// 一旦文案要翻译就全废了；现在用枚举承载语义，显示文字由 [`Phase::text`] 给。
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Phase {
    Idle,
    /// 同步前检测上传通道（PG 写入自检 / MQTT 等 CONNACK）。
    Preflight,
    Starting,
    Scanning,
    Connecting,
    Downloading,
    /// 本地文件导入：先解析 .gpx / .fit，再走上传通道。
    Parsing,
    Uploading,
    Erasing,
    Done,
    Failed,
    Cancelled,
}

impl Phase {
    pub fn text(self) -> &'static str {
        match self {
            Phase::Idle => t!("就绪", "Ready"),
            Phase::Preflight => t!("检测上传通道", "Checking upload channel"),
            Phase::Starting => t!("启动中", "Starting"),
            Phase::Scanning => t!("扫描设备", "Scanning for device"),
            Phase::Connecting => t!("连接设备", "Connecting to device"),
            Phase::Downloading => t!("下载中", "Downloading"),
            Phase::Parsing => t!("解析文件", "Parsing files"),
            Phase::Uploading => t!("上传中", "Uploading"),
            Phase::Erasing => t!("清理设备内存", "Erasing device memory"),
            Phase::Done => t!("完成", "Done"),
            Phase::Failed => t!("失败", "Failed"),
            Phase::Cancelled => t!("已取消", "Cancelled"),
        }
    }

    /// 任务是否已经结束（状态栏据此决定显示“空闲”还是终态）。
    pub fn is_final(self) -> bool {
        matches!(self, Phase::Done | Phase::Failed | Phase::Cancelled)
    }
}

/// 需要回写到设置页某个卡片里的提示。
///
/// 之前是靠 `msg.starts_with("PostgreSQL")` 猜归属的，界面一支持英文就失效了；
/// 现在由发送方直接标明目标。
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum NoticeTarget {
    Pg,
    Mqtt,
}

#[derive(Clone, Debug)]
pub enum UiEvent {
    Phase(Phase),
    Log(LogLevel, String),
    /// 需要用户在同步页立刻看到的结果（测试连接、任务完成情况等）。
    Notice {
        target: NoticeTarget,
        level: LogLevel,
        msg: String,
    },
    DeviceFound { name: String, address: String },
    RememberDevice { address: String, name: String },
    Status(DeviceStatus),
    /// 下载进度：`received`/`total` 是设备返回的消息数，`parsed` 是其中有效定位记录数。
    DownloadProgress {
        received: usize,
        total: usize,
        parsed: usize,
    },
    SessionInfo { index: u32, name: String, records: usize, ok: bool, detail: String },
    EraseProgress(u8),
    SpoolPending(usize),
    /// 上传进度（外圈环用）：已上传记录数 / 计划总数。
    UploadProgress { uploaded: usize, total: usize },
    /// 本次任务要跑哪几路上传通道：`ChannelProgress` 按路名更新进度。
    ///
    /// 每次同步开始都会先发一次，界面用它把上一次的通道残留清掉——
    /// 通道数量会随设置变化（比如关掉云上传就只剩一路）。
    ChannelList(Vec<String>),
    /// 某一路上传通道自己的进度。
    ///
    /// 下载完的数据会同时往几路送（自己的库 + RaceBox 云），各条路快慢不同，
    /// 混成一个数字看不出哪路卡住了，所以按路分开报。
    ChannelProgress {
        name: String,
        uploaded: usize,
        failed: usize,
        total: usize,
    },
    /// 一次任务的量化结果，用于界面上的“运行结果汇总”。
    RunStats(RunStats),
    Finished {
        ok: bool,
        summary: String,
        /// 是否为用户主动取消（用来在界面上显示“已取消”而不是“失败”）。
        cancelled: bool,
    },
}

/// 一次任务结束后的统计：下载条数、成功上传条数、内存清理结果。
#[derive(Clone, Copy, Debug)]
pub struct RunStats {
    /// 从设备取回并解析成功的记录条数。
    pub downloaded: usize,
    /// 实际上传成功的记录条数。
    pub uploaded: usize,
    /// 是否清理了设备内存：None=未执行，true=成功，false=失败。
    pub erased: Option<bool>,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum RunMode {
    Sync,
    Status,
    Erase,
    /// 本地文件导入：不走蓝牙，数据来自磁盘上的 .gpx / .fit，
    /// 但上传通道、进度与结果展示和 Sync 完全共用。
    Import,
}

fn u32le(payload: &[u8]) -> u32 {
    if payload.len() < 4 {
        return 0;
    }
    u32::from_le_bytes([payload[0], payload[1], payload[2], payload[3]])
}

/// 下载过程中攒起来的一段数据（等段边界到了再交给后台上传）。
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
    /// 界面上这一段的显示名（导入文件时是文件名）。
    display: Option<String>,
}

impl SessionFlush {
    fn into_session(self) -> Session {
        Session {
            device: self.device,
            session_index: self.session_index,
            records: self.records,
            duration_secs: self.duration_secs,
            sync_id: self.sync_id,
            imp_stamp: self.imp_stamp,
            sync_offset: self.sync_offset,
            sync_total_upper: self.sync_total_upper,
            sync_received_total: self.sync_received_total,
            sync_final: self.sync_final,
            display: self.display,
        }
    }
}

/// 一路上传通道自己累加的计数。
#[derive(Clone)]
struct ChannelCounter {
    /// 界面上显示的通道名（PostgreSQL / MQTT (RBX2) / RaceBox Cloud）。
    name: String,
    uploaded: Arc<AtomicUsize>,
    failed: Arc<AtomicUsize>,
}

/// 上传任务的共享计数器。
#[derive(Clone)]
struct FlushCounters {
    uploaded: Arc<AtomicUsize>,
    uploaded_ok: Arc<AtomicUsize>,
    /// 用户已取消：不再往界面上刷这一段的结果（数据照样落地）。
    silent: Arc<AtomicBool>,
    /// 每一路的计数，下标和 `build_sinks` 返回的 sinks 一一对应。
    channels: Vec<ChannelCounter>,
}

impl FlushCounters {
    /// 按本次要用的上传通道给每一路各起一组计数器。
    fn new(sinks: &[Arc<dyn DataSink>]) -> Self {
        let channels = sinks
            .iter()
            .map(|s| ChannelCounter {
                name: s.name().to_string(),
                uploaded: Arc::new(AtomicUsize::new(0)),
                failed: Arc::new(AtomicUsize::new(0)),
            })
            .collect();
        Self {
            uploaded: Arc::new(AtomicUsize::new(0)),
            uploaded_ok: Arc::new(AtomicUsize::new(0)),
            silent: Arc::new(AtomicBool::new(false)),
            channels,
        }
    }
}

/// 把一个数据段丢给后台上传任务（下载继续，不被上传阻塞）。
/// 返回 JoinHandle，await 得到该段是否上传成功。
fn spawn_flush(
    cfg: Arc<Config>,
    sinks: Vec<Arc<dyn DataSink>>,
    tx: UnboundedSender<UiEvent>,
    counters: FlushCounters,
    flush: SessionFlush,
) -> Option<tokio::task::JoinHandle<bool>> {
    if flush.records.is_empty() {
        return None;
    }
    let total_hint = if flush.sync_total_upper > 0 {
        flush.sync_total_upper
    } else {
        flush.sync_received_total
    };
    let session = flush.into_session();
    let index = session.session_index;
    let total = session.record_count();
    let session = Arc::new(session);
    Some(tokio::spawn(async move {
        let silent = counters.silent.clone();
        // 各路并行上传：它们互不相干（自己的库 / 官网云），串行会让慢的那一路
        // 拖住另一路的进度；并行起来每一路各报各的进度条也更直观。
        let mut tasks = Vec::with_capacity(sinks.len());
        for (sink, ch) in sinks.iter().zip(counters.channels.iter()) {
            let sink = sink.clone();
            let session = session.clone();
            let ch = ch.clone();
            let tx_progress = tx.clone();
            let silent = silent.clone();
            let ch_name = ch.name.clone();
            let base = ch.uploaded.load(Ordering::SeqCst);
            tasks.push(tokio::spawn(async move {
                // 回调要的是 &dyn Fn，会把 tx / silent 整个吞掉，所以先各留一份：
                // 这一段跑完还要用它们发结果。
                let tx_cb = tx_progress.clone();
                let silent_cb = silent.clone();
                let progress = move |done: usize| {
                    if silent_cb.load(Ordering::SeqCst) {
                        return;
                    }
                    // 段内进度：一段几万条要传好几秒，界面上能看见条数在涨才不会以为卡死
                    let _ = tx_cb.send(UiEvent::ChannelProgress {
                        name: ch_name.clone(),
                        uploaded: base + done,
                        failed: 0,
                        total: total_hint.max(base + done),
                    });
                };
                let result = sink.upload_session(&session, &progress).await;
                match &result {
                    Ok(()) => {
                        ch.uploaded.fetch_add(total, Ordering::SeqCst);
                    }
                    Err(e) => {
                        ch.failed.fetch_add(total, Ordering::SeqCst);
                        let _ = tx_progress.send(UiEvent::Log(
                            LogLevel::Error,
                            tr!(
                                "第{}段 {} 上传失败: {e}",
                                "segment {} -> {} failed: {e}",
                                index,
                                sink.name()
                            ),
                        ));
                    }
                }
                if !silent.load(Ordering::SeqCst) {
                    let _ = tx_progress.send(UiEvent::ChannelProgress {
                        name: ch.name.clone(),
                        uploaded: ch.uploaded.load(Ordering::SeqCst),
                        failed: ch.failed.load(Ordering::SeqCst),
                        total: total_hint.max(index as usize),
                    });
                }
                (sink.name(), result.is_ok(), result.err())
            }));
        }

        let mut ok = true;
        let mut uploaded_to: Vec<&'static str> = Vec::new();
        let mut first_error: Option<String> = None;
        for task in tasks {
            match task.await {
                Ok((name, true, None)) => uploaded_to.push(name),
                Ok((_, false, Some(e))) => {
                    ok = false;
                    if first_error.is_none() {
                        first_error = Some(e.to_string());
                    }
                }
                Ok((_, _, _)) => {}
                Err(e) => {
                    ok = false;
                    if first_error.is_none() {
                        first_error = Some(e.to_string());
                    }
                }
            }
        }
        let mut detail = if let Some(e) = first_error {
            tr!("上传失败: {}", "upload failed: {}", e)
        } else if !uploaded_to.is_empty() {
            let names = uploaded_to.join(" + ");
            tr!("已上传至 {}", "uploaded to {}", names)
        } else {
            String::new()
        };
        if !ok && cfg.spool_on_failure {
            match spool::write_session(session.as_ref()) {
                Ok(path) => {
                    let _ = tx.send(UiEvent::Log(
                        LogLevel::Warn,
                        tr!("已写入本地缓存: {}", "saved to local cache: {}", path.display()),
                    ));
                    detail.push_str(t!("，已转本地缓存", ", saved to local cache"));
                }
                Err(e) => {
                    let _ = tx.send(UiEvent::Log(
                        LogLevel::Error,
                        tr!("本地缓存写入失败: {e}", "failed to write local cache: {e}"),
                    ));
                }
            }
        }
        if ok {
            counters.uploaded_ok.fetch_add(total, Ordering::SeqCst);
        }
        let done = counters.uploaded.fetch_add(total, Ordering::SeqCst) + total;
        // 取消之后这些任务可能还在跑，但它们的结果已经没人关心了，别再往界面上冒
        if !counters.silent.load(Ordering::SeqCst) {
            let _ = tx.send(UiEvent::SpoolPending(spool::pending_count()));
            let _ = tx.send(UiEvent::UploadProgress {
                uploaded: done,
                total: total_hint.max(done),
            });
            let _ = tx.send(UiEvent::SessionInfo {
                index,
                name: session.display_name(),
                records: total,
                ok,
                detail,
            });
        }
        ok
    }))
}

/// 同步前检测上传通道。不通就直接返回错误，让任务在“还没下载任何数据”时停下。
///
/// 之前没有这一步：连不上库也要先把设备数据全下载完（几分钟），
/// 然后在写库时才报错、转本地缓存。用户看到的是一趟白跑的同步。
async fn preflight(
    cfg: &Config,
    tx: &UnboundedSender<UiEvent>,
    cancel: &AtomicBool,
) -> Result<(), String> {
    if !cfg.uses_database() {
        // MQTT 的连通性在 build_sinks 里靠等 CONNACK 保证，这里不重复
        return Ok(());
    }
    let _ = tx.send(UiEvent::Phase(Phase::Preflight));
    let _ = tx.send(UiEvent::Log(
        LogLevel::Info,
        tr!(
            "开始同步前先检测 PostgreSQL 连通性...",
            "checking PostgreSQL connectivity before syncing..."
        ),
    ));
    let probe = tokio::time::timeout(PREFLIGHT_TIMEOUT, PgSink::probe(&cfg.pg)).await;
    if cancelled(cancel) {
        return Err(CANCELLED.to_string());
    }
    match probe {
        Err(_) => Err(tr!(
            "PostgreSQL 检测超时（{} 秒无响应），已停止本次同步。请检查主机 / 端口 / 网络后重试",
            "PostgreSQL check timed out after {}s, sync aborted. Check host/port/network and retry",
            PREFLIGHT_TIMEOUT.as_secs()
        )),
        Ok(Ok(detail)) => {
            let _ = tx.send(UiEvent::Log(
                LogLevel::Success,
                tr!("PostgreSQL 可用（{detail}）", "PostgreSQL is ready ({detail})"),
            ));
            Ok(())
        }
        Ok(Err(e)) => Err(tr!(
            "PostgreSQL 不可用，已停止本次同步（设备数据没有动）: {e}",
            "PostgreSQL is not usable, sync aborted (device data untouched): {e}"
        )),
    }
}

/// 准备上传通道。通道不可用直接返回错误——宁可不下载，也不要把数据白下一遍。
async fn build_sinks(
    cfg: &Config,
    tx: &UnboundedSender<UiEvent>,
    cancel: &AtomicBool,
) -> Result<Vec<Arc<dyn DataSink>>, String> {
    let mut sinks: Vec<Arc<dyn DataSink>> = Vec::new();
    if cfg.uses_database() {
        if cancelled(cancel) {
            return Err(CANCELLED.to_string());
        }
        sinks.push(Arc::new(PgSink::new(cfg.pg.clone())));
    }
    if cfg.uses_mqtt() {
        match MqttSink::new(cfg.mqtt.clone(), Some(cancel)).await {
            Ok(sink) => sinks.push(Arc::new(sink)),
            Err(e) => {
                if cancelled(cancel) {
                    return Err(CANCELLED.to_string());
                }
                let msg = tr!(
                    "MQTT 通道不可用，已停止本次同步: {e}",
                    "MQTT channel unavailable, sync aborted: {e}"
                );
                let _ = tx.send(UiEvent::Log(LogLevel::Error, msg.clone()));
                return Err(msg);
            }
        }
    }
    // RaceBox 云是**附加**通道：开了就多传一份到官网，不开不影响主流程。
    // 没登录时只提示一句，不阻断同步——官网那份属于锦上添花，不该让本地
    // 数据下不下来。
    if cfg.cloud.enabled {
        let account = crate::racebox::store::load();
        if account.logged_in() {
            sinks.push(Arc::new(RaceBoxSink::new(account.auth, &cfg.cloud)));
        } else {
            let _ = tx.send(UiEvent::Log(
                LogLevel::Warn,
                t!(
                    "已开启 RaceBox 云上传，但账号还没登录，本次跳过",
                    "RaceBox Cloud is on but no account is signed in; skipping"
                )
                .to_string(),
            ));
        }
    }
    if sinks.is_empty() {
        return Err(t!(
            "没有可用的上传通道，请在设置中选择并配置 PostgreSQL 或 MQTT",
            "no upload channel configured; choose and configure PostgreSQL or MQTT in Settings"
        )
        .to_string());
    }
    Ok(sinks)
}

async fn query_status(
    peripheral: &Peripheral,
    rx_char: &Characteristic,
    tx_char: &Characteristic,
    tx: &UnboundedSender<UiEvent>,
    cancel: &AtomicBool,
) -> Result<Option<DeviceStatus>, String> {
    if ble::subscribe(peripheral, tx_char).await.is_err() {
        return Ok(None);
    }
    let mut stream = ble::notification_stream(peripheral)
        .await
        .ok()
        .ok_or_else(|| t!("设备通知通道打开失败", "failed to open device notification channel").to_string())?;
    ble::write_command(peripheral, rx_char, &CMD_STATUS)
        .await
        .ok()
        .ok_or_else(|| t!("状态查询命令发送失败", "failed to send status query").to_string())?;
    let deadline = Instant::now() + Duration::from_secs(8);
    let mut reader = FrameReader::new();
    while Instant::now() < deadline {
        if cancelled(cancel) {
            return Err(CANCELLED.to_string());
        }
        let data = ble::next_notification(&mut stream, Duration::from_secs(3)).await;
        let data = match data {
            Some(d) => d,
            None => return Ok(None),
        };
        reader.push(&data);
        while let Some(packet) = reader.next_packet() {
            match packet.id {
                protocol::MSG_STATUS => {
                    if let Some(status) = protocol::parse_status(&packet.payload) {
                        let _ = tx.send(UiEvent::Log(
                            LogLevel::Info,
                            tr!(
                                "设备状态: 录制={} 内存={}% 已存{}/{}条",
                                "device status: recording={} memory={}% stored={}/{}",
                                if status.recording {
                                    t!("是", "yes")
                                } else {
                                    t!("否", "no")
                                },
                                status.memory_level,
                                status.stored_messages,
                                status.max_messages
                            ),
                        ));
                        return Ok(Some(status));
                    }
                }
                protocol::MSG_NACK => {
                    let _ = tx.send(UiEvent::Log(
                        LogLevel::Warn,
                        t!("设备不支持状态查询", "device does not support status query").to_string(),
                    ));
                    return Ok(None);
                }
                _ => {}
            }
        }
    }
    Ok(None)
}

async fn erase_memory(
    peripheral: &Peripheral,
    rx_char: &Characteristic,
    tx_char: &Characteristic,
    tx: &UnboundedSender<UiEvent>,
    cancel: &AtomicBool,
) -> Result<(), String> {
    ble::subscribe(peripheral, tx_char).await?;
    let mut stream = ble::notification_stream(peripheral).await?;
    ble::write_command(peripheral, rx_char, &CMD_ERASE).await?;
    let _ = tx.send(UiEvent::Log(
        LogLevel::Info,
        t!("开始清除设备内存...", "erasing device memory...").to_string(),
    ));
    let deadline = Instant::now() + ERASE_TIMEOUT;
    let mut reader = FrameReader::new();
    let mut last_percent = 0u8;
    while Instant::now() < deadline {
        if cancelled(cancel) {
            return Err(CANCELLED.to_string());
        }
        let data = ble::next_notification(&mut stream, Duration::from_secs(30))
            .await
            .ok_or_else(|| {
                t!(
                    "清除过程中设备无响应",
                    "device stopped responding while erasing"
                )
                .to_string()
            })?;
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
                        let _ = tx.send(UiEvent::Log(
                            LogLevel::Success,
                            t!("设备内存清除完成", "device memory erased").to_string(),
                        ));
                        return Ok(());
                    }
                }
                protocol::MSG_NACK => {
                    return Err(t!(
                        "设备拒绝清除，内存可能已锁定或设备不支持",
                        "device refused to erase; memory may be locked or unsupported"
                    )
                    .to_string());
                }
                _ => {}
            }
        }
    }
    Err(t!("清除超时", "erase timed out").to_string())
}

struct DownloadResult {
    sessions: u32,
    records: usize,
    skipped: usize,
    failures: usize,
    /// 上传成功的记录条数。
    uploaded_ok: usize,
}

/// 用户取消时的收尾：已下载但还没上传的数据落本地缓存（写盘是毫秒级的），
/// 已经发出去的上传任务标成“静默”——它们会自己跑完，但不再往界面上刷记录，
/// 免得取消之后还一条条往外冒，看着像没取消掉。
fn abort_and_spool(
    cfg: &Config,
    tx: &UnboundedSender<UiEvent>,
    counters: &FlushCounters,
    partial: Session,
) -> String {
    counters.silent.store(true, Ordering::SeqCst);
    let count = partial.record_count();
    if count == 0 {
        return CANCELLED.to_string();
    }
    if cfg.spool_on_failure {
        match spool::write_session(&partial) {
            Ok(_) => {
                let _ = tx.send(UiEvent::Log(
                    LogLevel::Warn,
                    tr!(
                        "已取消：第{}段 {} 条还没上传完，已写入本地缓存，下次启动会自动补传",
                        "cancelled: segment {} with {} record(s) not uploaded yet was saved to local cache and will be replayed next time",
                        partial.session_index,
                        count
                    ),
                ));
                let _ = tx.send(UiEvent::SpoolPending(spool::pending_count()));
            }
            Err(e) => {
                let _ = tx.send(UiEvent::Log(
                    LogLevel::Error,
                    tr!(
                        "已取消，且本地缓存写入失败（这部分数据丢失）: {e}",
                        "cancelled, and writing local cache failed (this data is lost): {e}"
                    ),
                ));
            }
        }
    } else {
        let _ = tx.send(UiEvent::Log(
            LogLevel::Warn,
            tr!(
                "已取消：第{}段 {} 条还没上传，且未开启“失败写本地缓存”，这部分数据已丢弃",
                "cancelled: segment {} with {} record(s) was dropped because local cache on failure is disabled",
                partial.session_index,
                count
            ),
        ));
    }
    CANCELLED.to_string()
}

async fn download_and_upload(
    cfg: &Arc<Config>,
    sinks: &[Arc<dyn DataSink>],
    device: &str,
    peripheral: &Peripheral,
    rx_char: &Characteristic,
    tx_char: &Characteristic,
    tx: &UnboundedSender<UiEvent>,
    cancel: &AtomicBool,
) -> Result<DownloadResult, String> {
    ble::subscribe(peripheral, tx_char).await?;
    let mut stream = ble::notification_stream(peripheral).await?;
    ble::write_command(peripheral, rx_char, &CMD_DOWNLOAD).await?;
    let _ = tx.send(UiEvent::Phase(Phase::Downloading));
    let _ = tx.send(UiEvent::Log(
        LogLevel::Info,
        tr!("正在从设备 {device} 下载数据...", "downloading from device {device}..."),
    ));

    let sync_id = *uuid::Uuid::new_v4().as_bytes();
    let imp_stamp = uuid::Uuid::new_v4().to_string();
    let mut reader = FrameReader::new();
    let mut current: Vec<Record> = Vec::new();
    let mut session_index = 0u32;
    let mut received_total = 0usize;
    let mut parsed_total = 0usize;
    let mut skipped = 0usize;
    let mut total_upper = 0usize;
    let mut records_total = 0usize;
    let mut failures = 0usize;
    let mut session_base = 0usize;
    let mut session_started = Instant::now();
    let mut download_done = false;
    // 后台上传任务句柄 + 计数器（下载和上传并行）
    let mut upload_handles: Vec<tokio::task::JoinHandle<bool>> = Vec::new();
    let counters = FlushCounters::new(sinks);

    let deadline = Instant::now() + DOWNLOAD_TIMEOUT;
    while !download_done {
        if cancelled(cancel) {
            // 取消要立刻生效：不等上传，把手头这段直接落盘
            let partial = SessionFlush {
                device: device.to_string(),
                sync_id,
                imp_stamp: imp_stamp.clone(),
                records: std::mem::take(&mut current),
                session_index: session_index + 1,
                duration_secs: session_started.elapsed().as_secs_f64(),
                sync_offset: session_base,
                sync_total_upper: total_upper,
                sync_received_total: received_total,
                sync_final: false,
                display: None,
            }
            .into_session();
            return Err(abort_and_spool(cfg, tx, &counters, partial));
        }
        if Instant::now() >= deadline {
            return Err(t!(
                "下载超时（设备长时间无响应）",
                "download timed out (device stopped responding)"
            )
            .to_string());
        }
        let data = ble::next_notification(&mut stream, DOWNLOAD_IDLE_TIMEOUT)
            .await
            .ok_or_else(|| {
                t!(
                    "下载过程中设备无响应",
                    "device stopped responding during download"
                )
                .to_string()
            })?;
        reader.push(&data);
        while let Some(packet) = reader.next_packet() {
            match packet.id {
                protocol::MSG_DOWNLOAD_START => {
                    total_upper = u32le(&packet.payload) as usize;
                    let _ = tx.send(UiEvent::Log(
                        LogLevel::Info,
                        tr!(
                            "设备报告约 {total_upper} 条记录",
                            "device reports about {total_upper} record(s)"
                        ),
                    ));
                    let _ = tx.send(UiEvent::DownloadProgress {
                        received: 0,
                        total: total_upper,
                        parsed: 0,
                    });
                }
                protocol::MSG_RECORD_HISTORY | protocol::MSG_RECORD_LIVE => {
                    received_total += 1;
                    match Record::decode(&packet.payload) {
                        Some(record) => {
                            current.push(record);
                            parsed_total += 1;
                        }
                        None => skipped += 1,
                    }
                    if received_total % 50 == 0 {
                        let _ = tx.send(UiEvent::DownloadProgress {
                            received: received_total,
                            total: if total_upper > 0 { total_upper } else { received_total },
                            parsed: parsed_total,
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
                        display: None,
                    };
                    records_total += flush.records.len();
                    let _ = tx.send(UiEvent::Phase(Phase::Uploading));
                    if let Some(h) = spawn_flush(
                        cfg.clone(),
                        sinks.to_vec(),
                        tx.clone(),
                        counters.clone(),
                        flush,
                    ) {
                        upload_handles.push(h);
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
                    return Err(t!("设备拒绝下载", "device refused to download").to_string());
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
                display: None,
    };
    records_total += flush.records.len();
    let _ = tx.send(UiEvent::Phase(Phase::Uploading));
    if let Some(h) = spawn_flush(
        cfg.clone(),
        sinks.to_vec(),
        tx.clone(),
        counters.clone(),
        flush,
    ) {
        upload_handles.push(h);
    }

    let _ = tx.send(UiEvent::DownloadProgress {
        received: received_total,
        total: if total_upper > 0 { total_upper } else { received_total },
        parsed: parsed_total,
    });
    // 等所有后台上传任务收尾（取消时已经提前 return，走不到这里）
    for h in upload_handles {
        match h.await {
            Ok(true) => {}
            Ok(false) => failures += 1,
            Err(e) => {
                failures += 1;
                let _ = tx.send(UiEvent::Log(
                    LogLevel::Error,
                    tr!("上传任务执行失败: {e}", "upload task failed: {e}"),
                ));
            }
        }
    }
    Ok(DownloadResult {
        sessions: session_index,
        records: records_total,
        skipped,
        failures,
        uploaded_ok: counters.uploaded_ok.load(Ordering::SeqCst),
    })
}

async fn run_inner(
    cfg: &Arc<Config>,
    mode: RunMode,
    tx: &UnboundedSender<UiEvent>,
    cancel: &Arc<AtomicBool>,
) -> Result<String, String> {
    // 先留一份 Arc 给后台任务用，再把名字收窄成 &AtomicBool 方便传参
    let cancel_arc = cancel.clone();
    let cancel: &AtomicBool = cancel_arc.as_ref();
    // 只有完整同步才需要上传通道；查询状态 / 清除内存不碰网络，启动更快
    let sinks = if mode == RunMode::Sync {
        // 先确认通道能用，再动设备：PG 不通就别下载，省得白跑一趟
        preflight(cfg, tx, cancel).await?;
        build_sinks(cfg, tx, cancel).await?
    } else {
        Vec::new()
    };
    // 本地缓存重放放到后台任务：以前它在启动时同步跑，积压文件多时
    // 会把“启动中”卡住几分钟（每个文件等 PUBACK 最长 60 秒）。
    // 现在它会边跑边报进度，也能被取消按钮打断。
    if !sinks.is_empty() {
        // 告诉界面这次要跑哪几路：通道数量随设置变化（关掉云上传就只剩一路），
        // 界面收到就重画进度条，上一次的残留正好一并清掉。
        let names: Vec<String> = sinks.iter().map(|s| s.name().to_string()).collect();
        let _ = tx.send(UiEvent::ChannelList(names));
        let refs = sinks.clone();
        let tx_replay = tx.clone();
        let cancel_replay = cancel_arc.clone();
        tokio::spawn(async move {
            let sink_refs: Vec<&dyn DataSink> = refs.iter().map(|a| a.as_ref()).collect();
            spool::replay(&sink_refs, &tx_replay, &cancel_replay).await;
            let _ = tx_replay.send(UiEvent::SpoolPending(spool::pending_count()));
        });
        let _ = tx.send(UiEvent::SpoolPending(spool::pending_count()));
    }
    if cancelled(cancel) {
        return Err(CANCELLED.to_string());
    }

    let _ = tx.send(UiEvent::Phase(Phase::Scanning));
    let adapter = ble::first_adapter().await?;
    let scan_result = ble::find_device(
        &adapter,
        &cfg.device_name_filter,
        cfg.last_device_address.as_deref(),
        Duration::from_secs(cfg.scan_timeout_secs.clamp(10, 300) as u64),
        cancel,
        |name, address| {
            let _ = tx.send(UiEvent::DeviceFound {
                name: name.to_string(),
                address: address.to_string(),
            });
        },
    )
    .await;
    let peripheral = match scan_result {
        Ok(p) => p,
        Err(e) => {
            if cancelled(cancel) {
                return Err(CANCELLED.to_string());
            }
            return Err(e);
        }
    };

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
        tr!(
            "已找到设备: {} ({device_address})",
            "device found: {} ({device_address})",
            if device_name.is_empty() {
                t!("未知设备", "unknown device")
            } else {
                device_name.as_str()
            }
        ),
    ));

    let _ = tx.send(UiEvent::Phase(Phase::Connecting));
    let chars = ble::connect(&peripheral).await;
    let (rx_char, tx_char) = match chars {
        Ok(v) => v,
        Err(e) => {
            ble::disconnect(&peripheral).await;
            return Err(e);
        }
    };
    if cancelled(cancel) {
        ble::disconnect(&peripheral).await;
        return Err(CANCELLED.to_string());
    }
    let _ = tx.send(UiEvent::Log(
        LogLevel::Success,
        t!("设备已连接", "device connected").to_string(),
    ));

    let outcome = async {
        match mode {
            RunMode::Status => {
                let status = query_status(&peripheral, &rx_char, &tx_char, tx, cancel).await?;
                match status {
                    Some(status) => {
                        let _ = tx.send(UiEvent::Status(status));
                        Ok(t!("状态查询完成", "status query finished").to_string())
                    }
                    None => Err(t!(
                        "状态查询失败或设备不支持",
                        "status query failed or unsupported by this device"
                    )
                    .to_string()),
                }
            }
            RunMode::Erase => {
                let _ = tx.send(UiEvent::Phase(Phase::Erasing));
                erase_memory(&peripheral, &rx_char, &tx_char, tx, cancel).await?;
                let _ = tx.send(UiEvent::RunStats(RunStats {
                    downloaded: 0,
                    uploaded: 0,
                    erased: Some(true),
                }));
                Ok(t!("设备内存已清理", "device memory erased").to_string())
            }
            RunMode::Sync => {
                let result = download_and_upload(
                    cfg, &sinks, &device_name, &peripheral, &rx_char, &tx_char, tx, cancel,
                )
                .await?;
                let summary = tr!(
                    "下载完成: {} 个数据段, {} 条记录 (跳过无定位 {} 条)",
                    "download finished: {} segment(s), {} record(s) ({} skipped without a fix)",
                    result.sessions,
                    result.records,
                    result.skipped
                );
                let _ = tx.send(UiEvent::Log(LogLevel::Success, summary.clone()));
                let mut erased = None;
                let out = if cfg.auto_erase && result.failures == 0 {
                    let _ = tx.send(UiEvent::Phase(Phase::Erasing));
                    match erase_memory(&peripheral, &rx_char, &tx_char, tx, cancel).await {
                        Ok(()) => {
                            erased = Some(true);
                            tr!(
                                "{}，设备内存已清理",
                                "{}, device memory erased",
                                summary
                            )
                        }
                        Err(e) => {
                            erased = Some(false);
                            tr!(
                                "{}，但清理失败: {} (设备数据未删除，可重新同步)",
                                "{}, but erasing failed: {} (device data kept, you can sync again)",
                                summary,
                                e
                            )
                        }
                    }
                } else if result.failures > 0 {
                    tr!(
                        "{}，但存在上传失败，已跳过清理设备内存",
                        "{}, but some uploads failed so device memory was kept",
                        summary
                    )
                } else {
                    summary
                };
                let _ = tx.send(UiEvent::RunStats(RunStats {
                    downloaded: result.records,
                    uploaded: result.uploaded_ok,
                    erased,
                }));
                Ok(out)
            }
            // 本地文件导入不走蓝牙，由 `run_import` 自己处理；
            // 这里只是让 match 穷尽（调用方不会把 Import 传进来）。
            RunMode::Import => Err(t!(
                "本地文件导入不能用同步流程跑",
                "local file import cannot run through the sync flow"
            )
            .to_string()),
        }
    }
    .await;

    let _ = ble::unsubscribe(&peripheral, &tx_char).await;
    ble::disconnect(&peripheral).await;
    outcome
}

/// 本地文件导入：解析 `.gpx` / `.fit`，然后走和蓝牙同步**完全相同**的上传通道。
///
/// 通道（PostgreSQL / MQTT / RaceBox 云）由设置里的开关决定，导入不做特殊处理：
/// 用户选了什么就传到哪，段内进度、失败转本地缓存、结果列表也和同步一致——
/// 界面上那套「哪一段成功、多少条、耗时多久」的展示因此可以直接复用。
pub async fn run_import(
    cfg: Arc<Config>,
    paths: Vec<PathBuf>,
    tx: UnboundedSender<UiEvent>,
    cancel: Arc<AtomicBool>,
) {
    let result = import_inner(&cfg, paths, &tx, &cancel).await;
    match result {
        Ok(summary) => {
            let _ = tx.send(UiEvent::Log(LogLevel::Success, summary.clone()));
            let _ = tx.send(UiEvent::Finished { ok: true, summary, cancelled: false });
        }
        Err(e) if e == CANCELLED => {
            let _ = tx.send(UiEvent::Log(
                LogLevel::Warn,
                t!("任务已被用户取消", "task cancelled by user").to_string(),
            ));
            let _ = tx.send(UiEvent::Finished {
                ok: false,
                summary: t!("已取消", "Cancelled").to_string(),
                cancelled: true,
            });
        }
        Err(e) => {
            let _ = tx.send(UiEvent::Log(LogLevel::Error, e.clone()));
            let _ = tx.send(UiEvent::Finished { ok: false, summary: e, cancelled: false });
        }
    }
}

async fn import_inner(
    cfg: &Arc<Config>,
    paths: Vec<PathBuf>,
    tx: &UnboundedSender<UiEvent>,
    cancel: &AtomicBool,
) -> Result<String, String> {
    if paths.is_empty() {
        return Err(t!("还没有勾选任何文件", "no file selected").to_string());
    }
    // ---------- 先解析 ----------
    // 总条数要提前知道：进度条和圆环的分母是「所有文件的记录总数」，
    // 边传边算的话分母会一直变，进度条会往回跳。
    let _ = tx.send(UiEvent::Phase(Phase::Parsing));
    let _ = tx.send(UiEvent::Log(
        LogLevel::Info,
        tr!("解析 {} 个文件...", "parsing {} files...", paths.len()),
    ));
    let mut parsed: Vec<(String, Vec<Record>, f64)> = Vec::new();
    let mut failed = 0usize;
    for path in &paths {
        if cancelled(cancel) {
            return Err(CANCELLED.to_string());
        }
        let name = path
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or("?")
            .to_string();
        let p = path.clone();
        let out = tokio::task::spawn_blocking(move || crate::racebox::import::load(&p)).await;
        match out {
            Ok(Ok(track)) => {
                let start_ms = unix_ms_of(track.start_utc);
                let records = crate::racebox::records_from_points(&track.points, start_ms);
                if records.is_empty() {
                    failed += 1;
                    let _ = tx.send(UiEvent::SessionInfo {
                        index: parsed.len() as u32 + 1,
                        name,
                        records: 0,
                        ok: false,
                        detail: t!("没有有效轨迹点", "no usable track point").to_string(),
                    });
                } else {
                    parsed.push((name, records, track.duration_secs));
                }
            }
            Ok(Err(e)) => {
                failed += 1;
                let _ = tx.send(UiEvent::Log(
                    LogLevel::Error,
                    tr!("解析 {name} 失败: {e}", "failed to parse {name}: {e}"),
                ));
                let _ = tx.send(UiEvent::SessionInfo {
                    index: parsed.len() as u32 + 1,
                    name,
                    records: 0,
                    ok: false,
                    detail: e,
                });
            }
            Err(e) => {
                failed += 1;
                let _ = tx.send(UiEvent::SessionInfo {
                    index: parsed.len() as u32 + 1,
                    name,
                    records: 0,
                    ok: false,
                    detail: format!("{e}"),
                });
            }
        }
    }
    if parsed.is_empty() {
        return Err(t!("没有解析出任何可上传的轨迹", "no track could be parsed").to_string());
    }

    // ---------- 再上传（和蓝牙同步同一套通道） ----------
    let total: usize = parsed.iter().map(|(_, r, _)| r.len()).sum();
    let _ = tx.send(UiEvent::Phase(Phase::Preflight));
    let sinks = build_sinks(cfg, tx, cancel).await?;
    let _ = tx.send(UiEvent::ChannelList(
        sinks.iter().map(|s| s.name().to_string()).collect(),
    ));
    let _ = tx.send(UiEvent::UploadProgress { uploaded: 0, total });
    let counters = FlushCounters::new(&sinks);
    let sync_id = *uuid::Uuid::new_v4().as_bytes();
    let imp_stamp = uuid::Uuid::new_v4().to_string();
    let _ = tx.send(UiEvent::Phase(Phase::Uploading));

    let mut handles = Vec::new();
    for (i, (name, records, duration)) in parsed.into_iter().enumerate() {
        if cancelled(cancel) {
            break;
        }
        let flush = SessionFlush {
            device: name.clone(),
            sync_id,
            imp_stamp: imp_stamp.clone(),
            records,
            session_index: i as u32 + 1,
            duration_secs: duration,
            sync_offset: 0,
            sync_total_upper: total,
            sync_received_total: total,
            sync_final: true,
            display: Some(name),
        };
        if let Some(h) = spawn_flush(cfg.clone(), sinks.clone(), tx.clone(), counters.clone(), flush)
        {
            handles.push(h);
        }
    }
    let mut ok = 0usize;
    let mut bad = failed;
    for h in handles {
        match h.await {
            Ok(true) => ok += 1,
            _ => bad += 1,
        }
    }
    let _ = tx.send(UiEvent::SpoolPending(spool::pending_count()));
    let uploaded_ok = counters.uploaded_ok.load(Ordering::SeqCst);
    let _ = tx.send(UiEvent::UploadProgress { uploaded: total, total });
    let _ = tx.send(UiEvent::RunStats(RunStats {
        downloaded: total,
        uploaded: uploaded_ok,
        erased: None,
    }));
    if cancelled(cancel) {
        return Err(CANCELLED.to_string());
    }
    Ok(tr!(
        "导入完成：成功 {} 个，失败 {} 个，共 {} 条记录",
        "import finished: {} ok, {} failed, {} records in total",
        ok,
        bad,
        total
    ))
}

/// (年, 月, 日, 时, 分, 秒) -> UTC 毫秒。解析结果只给了拆分好的字段。
fn unix_ms_of(utc: (i32, u32, u32, u32, u32, u32)) -> i64 {
    use chrono::TimeZone as _;
    let (y, mo, d, h, mi, s) = utc;
    chrono::Utc
        .with_ymd_and_hms(y, mo, d, h, mi, s)
        .single()
        .map(|dt| dt.timestamp_millis())
        .unwrap_or(0)
}

pub async fn run_sync(
    cfg: Arc<Config>,
    mode: RunMode,
    tx: UnboundedSender<UiEvent>,
    cancel: Arc<AtomicBool>,
) {
    let result = run_inner(&cfg, mode, &tx, &cancel).await;
    match result {
        Ok(summary) => {
            let _ = tx.send(UiEvent::Log(LogLevel::Success, summary.clone()));
            let _ = tx.send(UiEvent::Finished { ok: true, summary, cancelled: false });
        }
        Err(e) if e == CANCELLED => {
            let _ = tx.send(UiEvent::Log(
                LogLevel::Warn,
                t!("任务已被用户取消", "task cancelled by user").to_string(),
            ));
            let _ = tx.send(UiEvent::Finished {
                ok: false,
                summary: t!("已取消", "Cancelled").to_string(),
                cancelled: true,
            });
        }
        Err(e) => {
            let _ = tx.send(UiEvent::Log(LogLevel::Error, e.clone()));
            let _ = tx.send(UiEvent::Finished { ok: false, summary: e, cancelled: false });
        }
    }
}
