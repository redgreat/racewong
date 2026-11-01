use eframe::egui;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::runtime::Runtime;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

use crate::config::{self, CloseAction, Config, MqttConfig, PgConfig, SinkMode, Theme};
use crate::i18n::Lang;
use crate::protocol::DeviceStatus;
use crate::style::{self, dialog_field, Palette};
use crate::worker::{LogLevel, NoticeTarget, Phase, RunMode, RunStats, UiEvent};
use crate::{t, tr};

#[derive(Clone, Copy, PartialEq, Eq)]
enum Tab {
    Sync,
    Settings,
}

/// 同步页的两种数据来源。
///
/// 设备同步（BLE 下载后上传）和本地文件导入是两件不同的事，硬挤在一屏里
/// 又长又乱，顶栏用一条分段控件二选一：每一屏都正好放得下。
#[derive(Clone, Copy, PartialEq, Eq)]
enum SyncSource {
    /// 从 RaceBox 设备下载。
    Device,
    /// 从本地 .gpx / .fit 文件导入。
    Files,
}

/// 一路上传通道的运行进度（下载完成后数据会同时往几路送）。
#[derive(Clone)]
struct ChannelRow {
    name: String,
    uploaded: usize,
    failed: usize,
    total: usize,
    /// 上一次速度采样时的（条数, 时刻）。
    ///
    /// 各路的进度回调密度不一样（PG 每批一次、RaceBox 云一整段才回调一次），
    /// 直接用「这次回调的增量 / 这次回调的间隔」算出来全是噪声，所以按时间窗口采样。
    sample: Option<(usize, Instant)>,
    /// 平滑后的实时速度（条/秒）。
    rate: Option<f64>,
    /// 这一路开始上传的时刻：回调太稀、攒不出采样窗口时用它算平均速度。
    started: Instant,
}

impl ChannelRow {
    fn new(name: String) -> Self {
        Self {
            name,
            uploaded: 0,
            failed: 0,
            total: 0,
            sample: None,
            rate: None,
            started: Instant::now(),
        }
    }

    /// 计数变了之后重新采样一次速度。
    ///
    /// 窗口不到 0.4 秒就先攒着——回调可能几十毫秒一次，每次都算会把速度抖成一片。
    /// 算出来的瞬时值再和上一次做指数平滑，界面上那串数字才不至于乱跳。
    fn resample(&mut self, now: Instant) {
        if let Some((last_n, last_t)) = self.sample {
            let dt = now.duration_since(last_t).as_secs_f64();
            if dt >= 0.4 {
                let inst = self.uploaded.saturating_sub(last_n) as f64 / dt;
                self.rate = Some(match self.rate {
                    Some(prev) => prev * 0.6 + inst * 0.4,
                    None => inst,
                });
                self.sample = Some((self.uploaded, now));
            }
        } else {
            self.sample = Some((self.uploaded, now));
        }
        // 兜底：RaceBox 云是把一整段一次性 POST 出去的，服务端不报进度，
        // 一路只有开头和结尾两次回调，采样窗口永远攒不出来。等这一路传完，
        // 用「从开始到传完」的平均值补一个，别让速度那一格永远是空的。
        if self.rate.is_none() && self.done() {
            let secs = now.duration_since(self.started).as_secs_f64();
            if secs >= 0.2 {
                self.rate = Some(self.total as f64 / secs);
            }
        }
    }

    /// 这一路跑完了没（有总数且已经追平）。
    fn done(&self) -> bool {
        self.total > 0 && self.uploaded >= self.total
    }
}

/// 连接测试的状态（进行中 / 结果文字 / 结果等级）。
///
/// 等级不再靠“文字里有没有‘失败’”去猜——那样一翻译就失效了。
#[derive(Default)]
struct TestState {
    busy: bool,
    result: String,
    level: LogLevel,
}

/// 一个轨迹数据段的上传结果行。
struct SessionRow {
    index: u32,
    name: String,
    records: usize,
    ok: bool,
    detail: String,
}

enum UpdateMsg {
    Current,
    Downloaded(crate::updater::UpdateInfo, std::path::PathBuf),
    Failed(String),
}

/// 弹窗里在填的东西。
///
/// 账号密码这类敏感字段不在页面上直接摆输入框：设置页一眼扫过去全是密文字段
/// 既占地方又容易误改，收进弹窗按需编辑，页面只显示「已设置 · 修改」。
enum Dialog {
    /// RaceBox 云账号登录。
    CloudLogin {
        email: String,
        password: String,
        revealed: bool,
    },
    /// PostgreSQL / MQTT 的完整连接参数（主机、端口、账号密码……）。
    Connection {
        target: ConnTarget,
        draft: ConnDraft,
        revealed: bool,
    },
    /// 一句话提示（比如点上传时才发现没登录）。
    ///
    /// 这类话不常驻在页面上占一行——真走到那一步才弹一次。
    Notice {
        title: String,
        body: String,
    },
}

/// 哪一路上传通道的连接参数。
#[derive(Clone, Copy, PartialEq, Eq)]
enum ConnTarget {
    Pg,
    Mqtt,
}

impl ConnTarget {
    fn title(self) -> &'static str {
        match self {
            ConnTarget::Pg => "PostgreSQL",
            ConnTarget::Mqtt => "MQTT",
        }
    }
}

/// 连接参数弹窗里正在编辑的草稿。
///
/// PostgreSQL 和 MQTT 要填的字段不完全一样，这里取并集，渲染时按 target 挑着显示，
/// 好处是开关弹窗、写入配置都只有一份代码。
#[derive(Clone)]
struct ConnDraft {
    /// PostgreSQL 的主机 / MQTT 的 broker 地址。
    host: String,
    port: String,
    /// PostgreSQL 库名（MQTT 不用）。
    database: String,
    user: String,
    password: String,
    /// MQTT 主题（PostgreSQL 不用）。
    topic: String,
    /// MQTT 客户端 ID（PostgreSQL 不用）。
    client_id: String,
    /// MQTT 保活间隔，字符串进弹窗、落盘时才解析（MQTT 不用时忽略）。
    keepalive: String,
}

impl ConnDraft {
    /// 草稿 → PostgreSQL 配置。
    ///
    /// 端口留空或写错都记成 0，连接时由 [`PgConfig::port_or_default`] 兜成 5432；
    /// 这样「不填端口」和「填了默认端口」是一回事，不用在弹窗里塞默认值。
    fn pg_config(&self) -> PgConfig {
        PgConfig {
            host: self.host.trim().to_string(),
            port: self.port.trim().parse::<u16>().unwrap_or(0),
            database: self.database.trim().to_string(),
            user: self.user.clone(),
            password: self.password.clone(),
        }
    }

    /// 草稿 → MQTT 配置。主题 / Client ID 留空同样是「用默认值」，见
    /// [`MqttConfig::topic_or_default`] / [`MqttConfig::client_id_or_default`]。
    fn mqtt_config(&self) -> MqttConfig {
        MqttConfig {
            broker: self.host.trim().to_string(),
            port: self.port.trim().parse::<u16>().unwrap_or(0),
            user: self.user.clone(),
            password: self.password.clone(),
            topic: self.topic.trim().to_string(),
            client_id: self.client_id.trim().to_string(),
            keepalive_secs: self
                .keepalive
                .trim()
                .parse::<u32>()
                .unwrap_or(30)
                .clamp(10, 300),
        }
    }
}

pub struct App {
    cfg: Config,
    runtime: Arc<Runtime>,
    tx: UnboundedSender<UiEvent>,
    rx: UnboundedReceiver<UiEvent>,
    probe_tx: std::sync::mpsc::Sender<crate::sink::mqtt::MqttSink>,
    probe_rx: std::sync::mpsc::Receiver<crate::sink::mqtt::MqttSink>,
    ctx: Option<egui::Context>,
    applied_theme: Theme,
    tab: Tab,
    /// 同步页当前看的是设备同步还是本地文件导入。
    source: SyncSource,
    /// 「本地文件」卡是否要排在「运行结果」卡前面。
    ///
    /// 用户主动点开「本地文件」时置真——那一刻他在挑文件，这张卡得在眼前；
    /// 一旦开始上传就置假——那时该看的是进度和结果，传完也保持假，
    /// 直到他再点一次「本地文件」。
    files_card_first: bool,
    /// RaceBox 云页面的状态（登录、导入、云端列表）。
    cloud: crate::cloud_ui::CloudState,
    /// 日志抽屉是否展开（日志不再是独立标签页）。
    show_logs: bool,
    about_open: bool,
    /// 进入设置页时的配置快照，用于判断有没有未保存的更改。
    settings_snapshot: Config,
    /// 关闭设置页时的“有未保存更改”确认弹窗。
    confirm_close: bool,
    /// 用户是否已点击过取消（避免重复提示）。
    cancel_requested: bool,
    /// 关于弹窗里的应用图标纹理（懒加载）。
    about_icon: Option<egui::TextureHandle>,
    logs: Vec<(LogLevel, String)>,
    sessions: Vec<SessionRow>,
    phase: Phase,
    device_name: String,
    device_address: String,
    status: Option<DeviceStatus>,
    received: usize,
    total: usize,
    /// 其中有效定位记录数（设备消息里可能有无定位的，不计入）。
    downloaded_parsed: usize,
    /// 已上传记录数 / 计划总数（外圈环）。
    uploaded: usize,
    upload_total: usize,
    /// 各上传通道各自的进度（PostgreSQL / MQTT / RaceBox 云）。
    channels: Vec<ChannelRow>,
    erase_percent: Option<u8>,
    spool_pending: usize,
    running: bool,
    /// 当前任务类型（取消按钮的文案随它变化）。
    current_mode: Option<RunMode>,
    /// 本次任务的量化结果（下载 / 上传条数、清理是否成功）。
    run_stats: Option<RunStats>,
    /// 当前阶段的开始时刻，用于统计各阶段耗时。
    phase_start: Option<Instant>,
    /// 各阶段累计耗时（阶段 -> 秒）。
    phase_times: Vec<(Phase, Duration)>,
    last_summary: String,
    mqtt_probe: Option<crate::sink::mqtt::MqttSink>,
    cancel_flag: Arc<AtomicBool>,
    pg_test: TestState,
    mqtt_test: TestState,
    /// 保存 / 放弃等操作的结果：(文字, 等级)。
    action_result: Option<(String, LogLevel)>,
    scan_timeout_buf: String,
    /// 系统托盘图标：设置成“隐藏到托盘”且创建成功时才有。
    tray: Option<crate::tray::Tray>,
    /// 托盘创建失败的原因（设置页里提示用）。
    tray_error: Option<String>,
    /// 托盘菜单点了“退出”：下一次关闭请求直接放行，不再拦。
    quit_requested: bool,
    /// 窗口当前是否被收进了托盘。
    hidden_to_tray: bool,
    /// 主窗口的原生句柄（Windows 的 HWND；其它平台为 0）。
    ///
    /// 托盘菜单点击时直接用它把窗口拉回来 / 关掉：窗口藏进托盘后，
    /// egui 侧的 `Visible(true)` / `Close` 命令经常落不了地。
    window_handle: isize,
    update_tx: std::sync::mpsc::Sender<UpdateMsg>,
    update_rx: std::sync::mpsc::Receiver<UpdateMsg>,
    update_busy: bool,
    update_result: Option<(String, LogLevel)>,
    /// 当前打开的编辑弹窗（凭据 / 云登录）。
    dialog: Option<Dialog>,
    /// 因为「连接信息还没填」被拦下来的那次任务：弹窗填完就接着跑，
    /// 不用让用户回去再点一次开始。
    pending_run: Option<RunMode>,
    /// 关于窗口上一次的矩形：判断点击是不是落在窗口外要靠它。
    about_rect: Option<egui::Rect>,
}

impl App {
    pub fn new(
        runtime: Arc<Runtime>,
        cfg: Config,
        tx: UnboundedSender<UiEvent>,
        rx: UnboundedReceiver<UiEvent>,
        window_handle: isize,
    ) -> Self {
        let (probe_tx, probe_rx) = std::sync::mpsc::channel::<crate::sink::mqtt::MqttSink>();
        let (update_tx, update_rx) = std::sync::mpsc::channel::<UpdateMsg>();
        let applied_theme = cfg.theme;
        let settings_snapshot = cfg.clone();
        let scan_timeout_buf = cfg.scan_timeout_secs.to_string();
        let cloud = crate::cloud_ui::CloudState::new(&cfg, window_handle);
        Self {
            cfg,
            runtime,
            tx,
            rx,
            probe_tx,
            probe_rx,
            ctx: None,
            applied_theme,
            tab: Tab::Sync,
            source: SyncSource::Device,
            files_card_first: false,
            cloud,
            show_logs: false,
            about_open: false,
            settings_snapshot,
            confirm_close: false,
            cancel_requested: false,
            about_icon: None,
            logs: Vec::new(),
            sessions: Vec::new(),
            phase: Phase::Idle,
            device_name: String::new(),
            device_address: String::new(),
            status: None,
            received: 0,
            total: 0,
            downloaded_parsed: 0,
            uploaded: 0,
            upload_total: 0,
            channels: Vec::new(),
            erase_percent: None,
            spool_pending: 0,
            running: false,
            current_mode: None,
            run_stats: None,
            phase_start: None,
            phase_times: Vec::new(),
            last_summary: String::new(),
            mqtt_probe: None,
            cancel_flag: Arc::new(AtomicBool::new(false)),
            pg_test: TestState::default(),
            mqtt_test: TestState::default(),
            action_result: None,
            scan_timeout_buf,
            tray: None,
            tray_error: None,
            quit_requested: false,
            hidden_to_tray: false,
            window_handle,
            update_tx,
            update_rx,
            update_busy: false,
            update_result: None,
            dialog: None,
            pending_run: None,
            about_rect: None,
        }
    }

    /// 按当前设置准备 / 收起托盘图标。
    ///
    /// **图标常驻**，不再跟着「关闭行为」开关——以前选「直接退出」就把图标销毁，
    /// 结果用户点 × 前先看到图标没了，以为程序坏了；切回「隐藏到托盘」还要等
    /// Windows 重新往通知区域塞一次，经常塞进折叠区里看不见。
    /// 「关闭行为」只决定点 × 是退出还是藏起来，跟图标在不在没关系。
    ///
    /// 创建失败会记住原因、不再重试（`tray_error`），避免每帧都去撞一次系统接口。
    fn sync_tray(&mut self) {
        if self.tray.is_some() || self.tray_error.is_some() {
            return;
        }
        // 托盘必须在事件循环起来之后、主线程上创建，所以第一次 update 拿到 ctx 才建
        let Some(ctx) = self.ctx.clone() else {
            return;
        };
        match crate::tray::Tray::new(&ctx, self.window_handle) {
            Ok(tray) => {
                self.tray = Some(tray);
                self.tray_error = None;
            }
            Err(e) => {
                self.tray_error = Some(e);
            }
        }
    }

    /// 处理托盘菜单与窗口关闭请求。
    fn handle_close_and_tray(&mut self, ctx: &egui::Context) {        // 托盘里点了「退出」：标志位在点击时就立好了（窗口藏起来时不一定还有帧，
        // 队列里的命令可能来不及消费），这里必须先认下来，否则下面会把这次
        // 关闭请求当成「收进托盘」再 CancelClose 一次，窗口就再也关不掉了。
        if crate::tray::quit_requested() {
            self.quit_requested = true;
        }

        // Windows 在主窗口不可见且没有其它界面动画时，可能不会因为托盘菜单回调里的
        // request_repaint() 及时进入下一次 update。保持一个很低频的心跳，让隐藏窗口后
        // “显示主窗口 / 退出”两条命令都一定会被消费；5 Hz 对空闲占用可以忽略。
        if self.tray.is_some() {
            ctx.request_repaint_after(Duration::from_millis(200));
        }

        // 托盘菜单：显示主窗口 / 退出
        loop {
            let Some(cmd) = self.tray.as_ref().and_then(|t| t.poll()) else {
                break;
            };
            match cmd {
                crate::tray::TrayCommand::Show => {
                    self.hidden_to_tray = false;
                    ctx.send_viewport_cmd(egui::ViewportCommand::Visible(true));
                    ctx.send_viewport_cmd(egui::ViewportCommand::Minimized(false));
                    ctx.send_viewport_cmd(egui::ViewportCommand::Focus);
                }
                crate::tray::TrayCommand::Quit => {
                    self.quit_requested = true;
                    ctx.send_viewport_cmd(egui::ViewportCommand::Close);
                }
            }
        }

        // 点右上角 ×：按设置决定退出还是收进托盘
        if ctx.input(|i| i.viewport().close_requested()) && !self.quit_requested {
            if self.cfg.close_action == CloseAction::Tray && self.tray.is_some() {
                ctx.send_viewport_cmd(egui::ViewportCommand::CancelClose);
                ctx.send_viewport_cmd(egui::ViewportCommand::Visible(false));
                if !self.hidden_to_tray {
                    self.push_log(
                        LogLevel::Info,
                        t!(
                            "已隐藏到系统托盘，点托盘图标可重新打开或退出",
                            "hidden to the system tray; click the tray icon to reopen or quit"
                        )
                        .to_string(),
                    );
                }
                self.hidden_to_tray = true;
            }
        }
    }

    fn palette(&self) -> Palette {
        style::palette(self.cfg.theme)
    }

    fn set_theme(&mut self, ctx: &egui::Context, theme: Theme) {
        if self.cfg.theme == theme {
            return;
        }
        self.cfg.theme = theme;
        style::apply(ctx, theme);
        self.applied_theme = theme;
        let _ = config::save(&self.cfg);
        // 主题是即时保存的，不算未保存更改
        self.settings_snapshot = self.cfg.clone();
    }

    /// 打开设置页（记录快照用于脏检查）。
    fn open_settings(&mut self) {
        self.commit_edits();
        self.settings_snapshot = self.cfg.clone();
        self.tab = Tab::Settings;
        self.confirm_close = false;
    }

    /// 请求关闭设置页：有未保存更改先弹窗确认。
    fn request_close_settings(&mut self) {
        self.commit_edits();
        if self.cfg != self.settings_snapshot {
            self.confirm_close = true;
        } else {
            self.tab = Tab::Sync;
        }
    }

    fn commit_edits(&mut self) {
        // 连接参数（端口 / 保活）只在弹窗里改，这里只剩页面上那个扫描超时
        if let Ok(secs) = self.scan_timeout_buf.trim().parse::<u32>() {
            self.cfg.scan_timeout_secs = secs.clamp(10, 120);
        }
        self.scan_timeout_buf = self.cfg.scan_timeout_secs.to_string();
    }

    fn push_log(&mut self, level: LogLevel, message: String) {
        crate::write_ui_log(level_name(level), &message);
        self.logs.push((level, message));
        if self.logs.len() > 2000 {
            self.logs.drain(..500);
        }
    }

    /// 结算当前阶段的耗时，累加到 `phase_times`（同名阶段合并）。
    fn close_phase(&mut self) {
        if let Some(start) = self.phase_start.take() {
            let elapsed = start.elapsed();
            if let Some(entry) = self.phase_times.iter_mut().find(|(n, _)| *n == self.phase) {
                entry.1 += elapsed;
            } else {
                self.phase_times.push((self.phase, elapsed));
            }
        }
    }

    /// 是否处于“清理设备内存”阶段：该阶段不允许中断。
    fn erasing(&self) -> bool {
        self.running && (self.erase_percent.is_some() || self.phase == Phase::Erasing)
    }

    /// 上传中的实时速度（条/秒）。
    ///
    /// 只在“上传中”阶段且已经跑了一会儿才有意义：刚进阶段那几百毫秒里
    /// 计数还是 0，算出来会是 0 条/秒，看着像卡住了。
    fn upload_rate(&self) -> Option<f64> {
        if self.phase != Phase::Uploading || !self.running {
            return None;
        }
        let secs = self.phase_start?.elapsed().as_secs_f64();
        if secs < 0.4 || self.uploaded == 0 {
            return None;
        }
        Some(self.uploaded as f64 / secs)
    }

    /// 结算后的平均上传速度（条/秒）：用“上传中”这一段的耗时算，
    /// 不把扫描/连接/下载的时间摊进去。
    fn avg_upload_rate(&self, records: usize) -> Option<f64> {
        let secs: f64 = self
            .phase_times
            .iter()
            .filter(|(ph, _)| *ph == Phase::Uploading)
            .map(|(_, d)| d.as_secs_f64())
            .sum();
        if secs < 0.05 || records == 0 {
            return None;
        }
        Some(records as f64 / secs)
    }

    fn drain_events(&mut self, ctx: &egui::Context) {
        while let Ok(event) = self.rx.try_recv() {
            match event {
                UiEvent::Phase(p) => {
                    // 阶段切换时结算上一个阶段的耗时（同名阶段会累计，如“上传中”）
                    if p != self.phase {
                        self.close_phase();
                        self.phase = p;
                        self.phase_start = Some(Instant::now());
                    }
                }
                UiEvent::Log(level, msg) => self.push_log(level, msg),
                UiEvent::Notice {
                    target,
                    level,
                    msg,
                } => {
                    // 连接测试的结果只回写到设置页对应的卡片里，不往首页冒
                    match target {
                        NoticeTarget::Pg => {
                            self.pg_test.busy = false;
                            self.pg_test.level = level;
                            self.pg_test.result = msg.clone();
                        }
                        NoticeTarget::Mqtt => {
                            self.mqtt_test.busy = false;
                            self.mqtt_test.level = level;
                            self.mqtt_test.result = msg.clone();
                        }
                    }
                    self.push_log(level, msg);
                }
                UiEvent::DeviceFound { name, address } => {
                    self.device_name = name;
                    self.device_address = address;
                }
                UiEvent::RememberDevice { address, name } => {
                    self.cfg.last_device_address = Some(address);
                    self.cfg.last_device_name = Some(name);
                    let _ = config::save(&self.cfg);
                    if self.tab == Tab::Settings {
                        self.settings_snapshot = self.cfg.clone();
                    }
                }
                UiEvent::Status(status) => self.status = Some(status),
                UiEvent::DownloadProgress {
                    received,
                    total,
                    parsed,
                } => {
                    self.received = received;
                    self.total = total;
                    self.downloaded_parsed = parsed;
                }
                UiEvent::UploadProgress { uploaded, total } => {
                    self.uploaded = uploaded;
                    self.upload_total = total;
                }
                UiEvent::ChannelList(names) => {
                    // 新任务先重建：通道数量随设置变化，上一次的残留要丢掉
                    self.channels = names.into_iter().map(ChannelRow::new).collect();
                    self.phase_times.retain(|(p, _)| *p != Phase::Uploading);
                }
                UiEvent::ChannelProgress {
                    name,
                    uploaded,
                    failed,
                    total,
                } => {
                    let now = Instant::now();
                    match self.channels.iter_mut().find(|c| c.name == name) {
                        Some(row) => {
                            // 各段并发上传时计数可能有交错，取最大值避免进度条回退
                            row.uploaded = row.uploaded.max(uploaded);
                            row.failed = row.failed.max(failed);
                            row.total = row.total.max(total);
                            row.resample(now);
                        }
                        None => {
                            let mut row = ChannelRow::new(name);
                            row.uploaded = uploaded;
                            row.failed = failed;
                            row.total = total;
                            row.resample(now);
                            self.channels.push(row);
                        }
                    }
                }
                UiEvent::SessionInfo { index, name, records, ok, detail } => {
                    self.sessions.push(SessionRow {
                        index,
                        name,
                        records,
                        ok,
                        detail,
                    });
                }
                UiEvent::EraseProgress(p) => self.erase_percent = Some(p),
                UiEvent::SpoolPending(n) => self.spool_pending = n,
                UiEvent::RunStats(stats) => self.run_stats = Some(stats),
                UiEvent::Finished { ok, summary, cancelled } => {
                    self.close_phase();
                    self.running = false;
                    self.cancel_requested = false;
                    self.phase = if cancelled {
                        Phase::Cancelled
                    } else if ok {
                        Phase::Done
                    } else {
                        Phase::Failed
                    };
                    self.last_summary = summary;
                    self.erase_percent = None;
                    if !ok {
                        ctx.request_repaint();
                    }
                }
            }
        }
        while let Ok(sink) = self.probe_rx.try_recv() {
            self.mqtt_probe = Some(sink);
        }
        while let Ok(message) = self.update_rx.try_recv() {
            self.update_busy = false;
            match message {
                UpdateMsg::Current => {
                    self.update_result = Some((
                        tr!("当前已是最新版（{}）", "Already up to date ({})", env!("CARGO_PKG_VERSION")),
                        LogLevel::Success,
                    ));
                }
                UpdateMsg::Downloaded(info, path) => match crate::updater::launch_installer(&path) {
                    Ok(()) => {
                        self.update_result = Some((
                            tr!("v{} 已下载，正在后台静默安装…", "v{} downloaded; installing silently…", info.version),
                            LogLevel::Success,
                        ));
                        #[cfg(target_os = "windows")]
                        {
                            // 更新安装器需要替换当前 exe；即使用户配置了“关闭到托盘”，
                            // 此处也必须真正退出，不能让旧进程继续占用文件。
                            self.quit_requested = true;
                            ctx.send_viewport_cmd(egui::ViewportCommand::Close);
                        }
                    }
                    Err(e) => self.update_result = Some((e, LogLevel::Error)),
                },
                UpdateMsg::Failed(e) => self.update_result = Some((e, LogLevel::Error)),
            }
        }
        if self.running {
            ctx.request_repaint_after(Duration::from_millis(200));
        }
        if self.pg_test.busy || self.mqtt_test.busy || self.update_busy {
            ctx.request_repaint_after(Duration::from_millis(120));
        }
    }

    fn check_update(&mut self) {
        if self.update_busy {
            return;
        }
        self.update_busy = true;
        self.update_result = Some((
            t!("正在检查 GitHub Release…", "Checking GitHub Release…").to_string(),
            LogLevel::Info,
        ));
        let tx = self.update_tx.clone();
        self.runtime.spawn(async move {
            let message = match crate::updater::check().await {
                Ok(None) => UpdateMsg::Current,
                Ok(Some(info)) => match crate::updater::download(&info).await {
                    Ok(path) => UpdateMsg::Downloaded(info, path),
                    Err(e) => UpdateMsg::Failed(e),
                },
                Err(e) => UpdateMsg::Failed(e),
            };
            let _ = tx.send(message);
        });
    }

    fn start(&mut self, mode: RunMode) {
        if self.running {
            return;
        }
        // 连接信息还没填就直接把填写窗口弹出来：放它跑下去的话，要等设备数据
        // 全下完、到写入那一步才报错，白等一场。
        if matches!(mode, RunMode::Sync) && !self.channel_ready() {
            self.pending_run = Some(mode);
            self.open_conn_dialog(self.active_target());
            return;
        }
        self.commit_edits();
        self.cancel_flag.store(false, Ordering::SeqCst);
        self.cancel_requested = false;
        self.current_mode = Some(mode);
        self.running = true;
        self.phase = Phase::Starting;
        self.phase_times.clear();
        self.phase_start = Some(Instant::now());
        self.run_stats = None;
        self.last_summary.clear();
        self.received = 0;
        self.total = 0;
        self.downloaded_parsed = 0;
        self.uploaded = 0;
        self.upload_total = 0;
        self.erase_percent = None;
        self.status = None;
        self.sessions.clear();
        // 上一次任务的通道进度不能留着：查询状态 / 清除内存根本不走上传，
        // 不清的话结果卡里会挂着上一轮「已完成」的通道，看着像这次也传了
        self.channels.clear();
        self.push_log(
            LogLevel::Info,
            tr!("任务开始: {}", "task started: {}", mode_text(mode)),
        );
        let cfg = Arc::new(self.cfg.clone());
        let tx = self.tx.clone();
        let runtime = self.runtime.clone();
        let cancel = self.cancel_flag.clone();
        runtime.spawn(async move {
            crate::worker::run_sync(cfg, mode, tx, cancel).await;
        });
        if let Some(ctx) = self.ctx.clone() {
            ctx.request_repaint();
        }
    }

    /// 上传勾选的本地文件。
    ///
    /// 和 [`Self::start`] 基本一样，只是后台跑的是 `worker::run_import`：
    /// 不碰蓝牙，数据来源换成磁盘上的文件，上传通道、进度、结果展示全共用。
    fn start_import(&mut self) {
        // 同上：先确认上传通道填好了，再让用户挑文件
        if !self.channel_ready() {
            self.pending_run = Some(RunMode::Import);
            self.open_conn_dialog(self.active_target());
            return;
        }
        let mut paths = self.cloud.selected_paths();
        if paths.is_empty() {
            // 一个都没勾就点「上传文件」：先把文件夹选择框弹出来，选完继续上传。
            // 只弹一句「还没选文件」等于把用户顶回去再点一遍，白白多一步。
            self.cloud.pick_dir(&mut self.cfg);
            paths = self.cloud.selected_paths();
        }
        if paths.is_empty() {
            self.dialog = Some(Dialog::Notice {
                title: t!("还没选文件", "No file selected").to_string(),
                body: t!(
                    "先在下面的「本地文件」里勾选要上传的文件。",
                    "Select the files to upload in the Local files card below first."
                )
                .to_string(),
            });
            return;
        }
        if self.running {
            return;
        }
        self.commit_edits();
        self.cancel_flag.store(false, Ordering::SeqCst);
        self.cancel_requested = false;
        self.current_mode = Some(RunMode::Import);
        self.running = true;
        // 开始传了：结果卡回到第一位，本地文件卡收起来
        self.files_card_first = false;
        self.phase = Phase::Starting;
        self.phase_times.clear();
        self.phase_start = Some(Instant::now());
        self.run_stats = None;
        self.last_summary.clear();
        self.received = 0;
        self.total = 0;
        self.downloaded_parsed = 0;
        self.uploaded = 0;
        self.upload_total = 0;
        self.erase_percent = None;
        self.status = None;
        self.sessions.clear();
        // 上一次任务的通道进度不能留着：查询状态 / 清除内存根本不走上传，
        // 不清的话结果卡里会挂着上一轮「已完成」的通道，看着像这次也传了
        self.channels.clear();
        self.push_log(
            LogLevel::Info,
            tr!("任务开始: {}", "task started: {}", mode_text(RunMode::Import)),
        );
        let cfg = Arc::new(self.cfg.clone());
        let tx = self.tx.clone();
        let runtime = self.runtime.clone();
        let cancel = self.cancel_flag.clone();
        runtime.spawn(async move {
            crate::worker::run_import(cfg, paths, tx, cancel).await;
        });
        if let Some(ctx) = self.ctx.clone() {
            ctx.request_repaint();
        }
    }

    /// 用户点击“取消同步”：worker 会在下一个检查点中断任务。
    fn cancel_run(&mut self) {
        // 清理设备内存阶段不允许中断，避免设备存储处于中间状态
        if self.erasing() {
            return;
        }
        if self.running && !self.cancel_requested {
            self.cancel_requested = true;
            self.cancel_flag.store(true, Ordering::SeqCst);
            self.push_log(
                LogLevel::Warn,
                t!("正在取消任务...", "cancelling task...").to_string(),
            );
        }
    }

    fn save_config(&mut self) {
        self.commit_edits();
        match config::save(&self.cfg) {
            Ok(()) => {
                let msg = t!("配置已保存", "settings saved").to_string();
                self.action_result = Some((msg.clone(), LogLevel::Success));
                self.settings_snapshot = self.cfg.clone();
                self.push_log(LogLevel::Success, msg);
                // 保存成功直接回首页
                self.tab = Tab::Sync;
            }
            Err(e) => {
                let msg = tr!("保存失败: {e}", "failed to save: {e}");
                self.action_result = Some((msg, LogLevel::Error));
            }
        }
    }

    fn discard_edits(&mut self) {
        self.cfg = config::load();
        self.scan_timeout_buf = self.cfg.scan_timeout_secs.to_string();
        // 关闭行为可能被改过，托盘图标要跟着回退
        self.sync_tray();
        self.action_result = Some((
            t!("已重新载入配置", "settings reloaded").to_string(),
            LogLevel::Info,
        ));
        self.settings_snapshot = self.cfg.clone();
    }

    /// 切换界面语言：立即生效并落盘（和主题一样，不算“未保存的更改”）。
    fn set_lang(&mut self, lang: Lang) {
        if self.cfg.lang == lang {
            return;
        }
        self.cfg.lang = lang;
        crate::i18n::set(lang);
        let _ = config::save(&self.cfg);
        self.settings_snapshot = self.cfg.clone();
        // 这些文字是“生成时”就定型的，切换语言后不会再变，清掉免得中英混排
        self.last_summary.clear();
        self.pg_test.result.clear();
        self.mqtt_test.result.clear();
        self.action_result = None;
        // 托盘菜单的文字是创建时定型的，语言变了就重建一次
        self.tray = None;
        self.tray_error = None;
        self.sync_tray();
    }

    /// 取消按钮文案：跟着当前任务类型走。
    fn cancel_label(&self) -> &'static str {
        match self.current_mode {
            Some(RunMode::Import) => t!("取消上传", "Cancel upload"),
            Some(RunMode::Status) => t!("取消查询", "Cancel query"),
            Some(RunMode::Erase) => t!("取消清除内存", "Cancel erase"),
            _ => t!("取消同步", "Cancel sync"),
        }
    }

    /// 弹窗里点了「测试连接」：把草稿翻成配置去测。
    fn start_conn_test(&mut self, target: ConnTarget, draft: &ConnDraft) {
        match target {
            ConnTarget::Pg => self.test_pg(draft.pg_config()),
            ConnTarget::Mqtt => self.test_mqtt(draft.mqtt_config()),
        }
    }

    /// 起一次 PostgreSQL 自检。`cfg` 是**要测的那组参数**：弹窗里测的是正在填的
    /// 草稿，不是已保存的配置，所以参数从外面传进来。
    fn test_pg(&mut self, cfg: PgConfig) {
        if self.running || self.pg_test.busy {
            return;
        }
        let tx = self.tx.clone();
        let runtime = self.runtime.clone();
        self.pg_test.busy = true;
        self.pg_test.level = LogLevel::Info;
        self.pg_test.result = t!(
            "正在测试 PostgreSQL 连接与写入...",
            "testing PostgreSQL connection and write path..."
        )
        .to_string();
        runtime.spawn(async move {
            // 整体加超时：主机不可达时 TCP 可能要挂很久，不能一直转圈。
            // 走的是真正的写入链路（事务内试写两条再回滚），
            // 表结构 / 列类型 / 写权限有问题当场就能暴露，不用等同步到一半才报错。
            let probe = tokio::time::timeout(
                Duration::from_secs(20),
                crate::sink::pg::PgSink::probe(&cfg),
            )
            .await;
            let (msg, level) = match probe {
                Err(_) => (
                    t!(
                        "PostgreSQL 连接超时（20 秒无响应），请检查主机、端口或网络",
                        "PostgreSQL timed out after 20s, check host, port or network"
                    )
                    .to_string(),
                    LogLevel::Error,
                ),
                Ok(Ok(detail)) => (
                    tr!(
                        "PostgreSQL 连接成功: {}:{}/{}（{detail}）",
                        "PostgreSQL is ready: {}:{}/{} ({detail})",
                        cfg.host,
                        cfg.port_or_default(),
                        cfg.database
                    ),
                    LogLevel::Success,
                ),
                Ok(Err(e)) => {
                    let mut m = tr!(
                        "PostgreSQL 自检失败: {e}",
                        "PostgreSQL self-test failed: {e}"
                    );
                    if m.contains("password authentication failed") {
                        m.push_str(t!(
                            " → 密码不对，请检查密码输入",
                            " -> wrong password, please check it"
                        ));
                    } else if m.contains("Connection refused") {
                        m.push_str(t!(
                            " → 端口未监听，请检查 host/port",
                            " -> nothing is listening, check host/port"
                        ));
                    } else if m.contains("does not exist") {
                        m.push_str(t!(
                            " → 库或表不存在，请检查 database 名称与建表脚本",
                            " -> database or table missing, check the database name and schema script"
                        ));
                    } else if m.contains("serializing parameter") {
                        m.push_str(t!(
                            " → 客户端与表结构类型不匹配，请核对建表脚本",
                            " -> client/table type mismatch, please check the schema script"
                        ));
                    }
                    (m, LogLevel::Error)
                }
            };
            let _ = tx.send(UiEvent::Notice {
                target: NoticeTarget::Pg,
                level,
                msg,
            });
        });
    }

    /// 起一次 MQTT 连接自检（参数同 [`Self::test_pg`]，测的是草稿）。
    fn test_mqtt(&mut self, cfg: MqttConfig) {
        if self.running || self.mqtt_test.busy {
            return;
        }
        let tx = self.tx.clone();
        let runtime = self.runtime.clone();
        let probe_tx = self.probe_tx.clone();
        // 上一次测试成功后留着的连接先断掉，避免堆积
        if let Some(old) = self.mqtt_probe.take() {
            runtime.spawn(async move {
                old.disconnect().await;
            });
        }
        self.mqtt_test.busy = true;
        self.mqtt_test.level = LogLevel::Info;
        self.mqtt_test.result =
            t!("正在测试 MQTT 连接...", "testing MQTT connection...").to_string();
        runtime.spawn(async move {
            match crate::sink::mqtt::MqttSink::new(cfg.clone(), None).await {
                Ok(sink) => {
                    let _ = tx.send(UiEvent::Notice {
                        target: NoticeTarget::Mqtt,
                        level: LogLevel::Success,
                        msg: tr!(
                            "MQTT 连接成功: {}:{} (主题 {})",
                            "MQTT is ready: {}:{} (topic {})",
                            cfg.broker,
                            cfg.port_or_default(),
                            cfg.topic_or_default()
                        ),
                    });
                    // 连接留着不要立刻断，否则会打一条误导性的断开错误
                    let _ = probe_tx.send(sink);
                }
                Err(e) => {
                    let _ = tx.send(UiEvent::Notice {
                        target: NoticeTarget::Mqtt,
                        level: LogLevel::Error,
                        msg: tr!("MQTT 连接失败: {e}", "MQTT connection failed: {e}"),
                    });
                }
            }
        });
    }


    /// 通道状态要不要显示：有通道数据、且任务跑过（或正在跑）才展开。
    fn show_channels(&self) -> bool {
        !self.channels.is_empty() && (self.running || self.phase.is_final())
    }

    /// 各上传通道挤成一行：`MQTT 2,071 条/秒 · RaceBox 云 1,842 条/秒`。
    ///
    /// 原来每路一条进度条，和大圆环的总体进度是重复的（数据是同一份、同时往几路送），
    /// 屏幕上多出两根条只是把结果卡顶下去。这里只留文字：谁在跑、跑到多少、多快，
    /// 圆环负责「还剩多少」，两边不重叠。
    fn channel_line(&self, ui: &mut egui::Ui, p: &Palette) {
        // 通道名之间用 `·` 分隔，和「各阶段耗时」那行保持同一种节奏
        ui.horizontal_wrapped(|ui| {
            for (i, row) in self.channels.iter().enumerate() {
                if i > 0 {
                    ui.add_space(6.0);
                    ui.colored_label(p.muted, "·");
                    ui.add_space(6.0);
                }
                ui.label(egui::RichText::new(row.name.clone()).strong().color(p.text));
                ui.add_space(6.0);
                let (text, color) = if row.failed > 0 {
                    (
                        tr!(
                            "{} 条 · 失败 {} 条",
                            "{} rec · {} failed",
                            fmt_count(row.uploaded),
                            fmt_count(row.failed)
                        ),
                        p.error,
                    )
                } else if row.done() {
                    (
                        tr!("{} 条 · 已完成", "{} rec · done", fmt_count(row.total)),
                        p.success,
                    )
                } else if row.total > 0 {
                    (
                        tr!(
                            "{} / {} 条",
                            "{} / {} rec",
                            fmt_count(row.uploaded),
                            fmt_count(row.total)
                        ),
                        p.text,
                    )
                } else if self.running && self.phase == Phase::Uploading {
                    // 这一路还一条都没报上来（RaceBox 云是整段一次性发出去的），
                    // 摆个「0 条」看着像卡死，不如直说正在传
                    (format!("{}…", t!("上传中", "Uploading")), p.muted)
                } else {
                    (tr!("{} 条", "{} rec", fmt_count(row.uploaded)), p.text)
                };
                ui.label(egui::RichText::new(text).strong().color(color));
                // 速度：跑的时候是实时值，跑完是这一路最后算出来的平均值——
                // 两边留着，正好能横向比出哪一路慢
                if row.failed == 0 {
                    if let Some(rate) = row.rate.filter(|r| *r >= 1.0) {
                        ui.add_space(6.0);
                        ui.colored_label(p.muted, "·");
                        ui.add_space(6.0);
                        ui.colored_label(
                            p.muted,
                            tr!("{} 条/秒", "{}/s", fmt_count(rate.round() as usize)),
                        );
                    }
                }
            }
        });
    }

    fn show_sync_tab(&mut self, ui: &mut egui::Ui, p: &Palette) {
        // 预先取快照，避免闭包里同时借用 self
        let connected = !(self.device_name.is_empty() && self.device_address.is_empty());
        let device_line = if connected {
            format!("{} ({})", self.device_name, self.device_address)
        } else {
            t!("未连接", "not connected").to_string()
        };
        // 数据源：设备（蓝牙）还是本地文件。开关在下面「本地文件」卡里。
        let files_mode = self.source == SyncSource::Files;
        let import_task = self.running && self.current_mode == Some(RunMode::Import);
        let picked_files = self.cloud.selected_paths().len();
        let (received, total, parsed) = (self.received, self.total, self.downloaded_parsed);
        let downloading = total > 0 || parsed > 0;
        let erase_percent = self.erase_percent;
        let spool_pending = self.spool_pending;
        // 清理设备内存阶段：不可中断，圆环只保留外圈显示清理百分比
        let erasing = self.erasing();
        // “清除内存”任务从头到尾都只用单圈
        let erase_task = self.running && self.current_mode == Some(RunMode::Erase);
        let sync_task = self.running && self.current_mode == Some(RunMode::Sync);
        // 未开始 / 已结束不画任何环，只有运行中才显示
        // 单圈（Single）只画进度弧本身，不带占位轨道；只有同步有进度后才是带轨道的双层环
        let ring: style::RingMode = if !self.running {
            style::RingMode::Hidden
        } else if import_task {
            // 本地文件导入没有下载环节，只有一条上传进度
            style::RingMode::Single {
                progress: if self.upload_total > 0 {
                    Some((self.uploaded as f32 / self.upload_total as f32).min(1.0))
                } else {
                    None
                },
                color: p.accent,
            }
        } else if erasing || erase_task {
            style::RingMode::Single {
                progress: erase_percent.map(|v| v as f32 / 100.0),
                color: p.info,
            }
        } else if !sync_task {
            // 查询状态：只有单圈转圈
            style::RingMode::Single {
                progress: None,
                color: p.info,
            }
        } else if total > 0 || self.upload_total > 0 {
            // 同步：有进度数据后是带轨道的双层环（内圈下载、外圈上传）
            style::RingMode::Dual {
                download: if total > 0 {
                    Some((parsed as f32 / total as f32).min(1.0))
                } else {
                    None
                },
                upload: if self.upload_total > 0 {
                    Some((self.uploaded as f32 / self.upload_total as f32).min(1.0))
                } else {
                    None
                },
            }
        } else {
            // 同步的启动/扫描/连接阶段：还没有进度，单圈转圈
            style::RingMode::Single {
                progress: None,
                color: p.info,
            }
        };
        let ring_text: Option<String> = match ring {
            // 导入任务也只有一圈，但那是上传进度，不是清理进度
            style::RingMode::Single { progress: Some(f), .. } if import_task => Some(tr!(
                "上传 {}% · {}/{} 条",
                "upload {}% · {}/{}",
                (f * 100.0).round() as u32,
                self.uploaded,
                self.upload_total
            )),
            style::RingMode::Single {
                progress: Some(f), ..
            } => Some(tr!(
                "清理内存 {}%",
                "erasing {}%",
                (f * 100.0).round() as u32
            )),
            style::RingMode::Single { progress: None, .. } => None,
            style::RingMode::Dual {
                download: Some(f), ..
            } => {
                if self.upload_total > 0 {
                    Some(tr!(
                        "下载 {}% · 上传 {}/{} 条",
                        "download {}% · upload {}/{}",
                        (f * 100.0).round() as u32,
                        self.uploaded,
                        self.upload_total
                    ))
                } else {
                    Some(tr!(
                        "下载 {}%",
                        "download {}%",
                        (f * 100.0).round() as u32
                    ))
                }
            }
            style::RingMode::Dual { download: None, .. } if self.upload_total > 0 => Some(tr!(
                "上传 {}/{} 条",
                "upload {}/{}",
                self.uploaded,
                self.upload_total
            )),
            _ => None,
        };

        // ---------- 主卡：大圆钮 + 两个描边按键（紧凑，一屏放得下） ----------
        style::card(p).show(ui, |ui| {
            style::fill_card_width(ui);

            // 圆形主按钮 + 进度说明（阶段状态看左下角状态栏，这里不重复）
            ui.vertical_centered(|ui| {
                ui.add_space(2.0);
                let circle_label = if erasing {
                    t!("清理内存中", "Erasing").to_string()
                } else if self.running {
                    self.cancel_label().to_string()
                } else if files_mode {
                    // 一个主按钮承担两种来源：文案说清楚这次按下去传的是什么
                    if picked_files > 0 {
                        tr!("上传 {} 个文件", "upload {} files", picked_files)
                    } else {
                        t!("上传文件", "Upload files").to_string()
                    }
                } else {
                    t!("开始同步", "Start sync").to_string()
                };
                if style::round_action_button(
                    ui,
                    p,
                    &circle_label,
                    self.running,
                    !erasing,
                    ring,
                    // 120 是默认 980×720 窗口下一屏放得下的上限（原来 168 会把结果卡顶出屏幕）
                    120.0,
                )
                .on_hover_text(if erasing {
                    t!(
                        "正在清理设备内存，此阶段不可中断",
                        "device memory is being erased; this step cannot be interrupted"
                    )
                } else if self.running {
                    t!("点击取消当前任务", "click to cancel the current task")
                } else if files_mode {
                    t!(
                        "把勾选的本地文件上传到设置里配置的去处",
                        "upload the selected local files to the configured destinations"
                    )
                } else {
                    t!(
                        "下载数据并按设置上传，成功后自动清理设备内存",
                        "download data and upload it as configured, then erase device memory"
                    )
                })
                .clicked()
                {
                    if self.running {
                        self.cancel_run();
                    } else if files_mode {
                        self.start_import();
                    } else {
                        self.start(RunMode::Sync);
                    }
                }
                if let Some(text) = &ring_text {
                    ui.add_space(2.0);
                    ui.colored_label(p.muted, text.clone());
                }
            });
            ui.add_space(8.0);

            // 查询状态 / 清除内存 / 本地文件：三个等宽描边框居中。
            // 第三个是「切换开关」：点一下打开下面的本地文件卡，再点一下（或点卡里的
            // 「取消本地上传」）收起来，所以它选中时是实心红，平时是描边。
            // （不用 horizontal_centered：它会把整行撑满剩余高度）
            //
            // 三个键等宽、按卡片宽度分摊（150~200px 之间），整行真正居中：
            // 宽度要按最长的那条译文算（西/葡/俄比中文长一截），并且用 item_spacing
            // 而不是 add_space 做间距，否则每个缝里会多叠 8px、整行被顶偏。
            let row_labels = [
                t!("查询状态", "Device status"),
                t!("清除内存", "Erase memory"),
                t!("本地文件", "Local files"),
            ];
            ui.horizontal(|ui| {
                let gap = 12.0;
                let bw = style::row_button_width(ui, &row_labels, gap, 150.0, 200.0);
                let total = bw * row_labels.len() as f32 + gap * (row_labels.len() - 1) as f32;
                let pad = ((ui.available_width() - total) / 2.0).max(0.0);
                // 按钮之间的间距交给 item_spacing：`add_space` 只是推进光标，
                // 下一个控件前面还会再叠一次 item_spacing，两处都给会比设计的
                // 间距多出 8px（原来那版三个键之间就是这么变成 20px 的）
                ui.spacing_mut().item_spacing.x = gap;
                ui.add_space(pad);
                if style::outline_button(
                    ui,
                    p,
                    row_labels[0],
                    p.text,
                    egui::vec2(bw, 34.0),
                    !self.running,
                )
                .on_hover_text(t!(
                    "读取设备录制状态与存储用量",
                    "read the recording state and storage usage from the device"
                ))
                .clicked()
                {
                    self.start(RunMode::Status);
                }
                if style::outline_button(
                    ui,
                    p,
                    row_labels[1],
                    if self.running { p.muted } else { p.error },
                    egui::vec2(bw, 34.0),
                    !self.running,
                )
                .on_hover_text(t!(
                    "擦除设备存储的所有数据，不可恢复",
                    "erase all data stored on the device; this cannot be undone"
                ))
                .clicked()
                {
                    self.start(RunMode::Erase);
                }
                if style::toggle_outline_button(
                    ui,
                    p,
                    row_labels[2],
                    files_mode,
                    egui::vec2(bw, 34.0),
                    !self.running,
                )
                .on_hover_text(t!(
                    "打开本地文件卡，选一个文件夹把里面的 .gpx / .fit 全传上去",
                    "open the local file card and pick a folder whose .gpx / .fit files you want to upload"
                ))
                .clicked()
                {
                    if files_mode {
                        self.source = SyncSource::Device;
                        self.files_card_first = false;
                    } else {
                        self.source = SyncSource::Files;
                        // 刚点开：这张卡是用户此刻的操作对象，排到结果卡前面去
                        self.files_card_first = true;
                        // 打开卡片的同时就弹文件夹选择框：用户点「本地文件」时
                        // 想的就是「让我挑一个目录」，先给一张空卡片还得再点一次是多余的。
                        // 已经有文件（上次记住的目录里扫出来的）就只切模式，不再弹。
                        if self.cloud.selected_paths().is_empty() {
                            self.cloud.pick_dir(&mut self.cfg);
                        }
                    }
                }
            });
            ui.add_space(6.0);
        });

        // ---------- 本地文件卡 ----------
        // 上传本身不在这张卡里（整页只有上面那一个大圆），它只回答「这次传什么」。
        // 位置按状态走：
        //   · 刚点开「本地文件」→ 排在运行结果卡**上面**（此刻在挑文件，
        //     上一次的结果再长也不能把选文件的地方顶到屏幕外）；
        //   · 点了上传 / 传完之后 → 排到结果卡下面（这时该看的是进度和结论）。
        // 任务跑起来时整张卡收起，结果卡自然回到第一位。
        // 拖文件进窗口随时都能用，所以 accept_dropped 不跟着这张卡一起藏。
        self.cloud.accept_dropped(ui);
        let files_card_on_top = files_mode && !self.running && self.files_card_first;
        if files_card_on_top {
            self.local_files_card(ui, p);
        }

        // ---------- 运行结果卡：汇总 + 设备状态 + 数据段（有内容才出现） ----------
        // 汇总 / 阶段耗时只针对完整同步；清除内存显示清理结果；查询状态只看设备状态
        // 导入和同步共用同一套结果展示：哪些段（文件）成功、多少条、耗时多久
        let is_sync_task = matches!(
            self.current_mode,
            Some(RunMode::Sync) | Some(RunMode::Import)
        );
        let is_erase_task = self.current_mode == Some(RunMode::Erase);
        // 跑的过程中也要出这张卡：实时速度、各通道状态都摆在这里，
        // 等任务结束才显示的话，「实时」两个字就没意义了。
        let live = self.running
            && (downloading
                || self.uploaded > 0
                || !self.channels.is_empty()
                || erase_percent.is_some());
        let has_result = self.status.is_some()
            || live
            || (is_sync_task
                && (!self.last_summary.is_empty()
                    || !self.sessions.is_empty()
                    || self.run_stats.is_some()))
            || (is_erase_task && !self.last_summary.is_empty());
        // 任务结果横幅颜色：完成=成功，失败=错误，取消=警告
        let result_color = match self.phase {
            Phase::Done => p.success,
            Phase::Cancelled => p.warn,
            _ => p.error,
        };
        if has_result {
            ui.add_space(10.0);
            style::card(p).show(ui, |ui| {
                style::fill_card_width(ui);
                ui.horizontal(|ui| {
                    style::card_title(ui, p, t!("运行结果", "Result"));
                    ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                        ui.colored_label(p.muted, &device_line);
                    });
                });

                // ---------- ① 结论横幅：放最上面 ----------
                // 跑完第一眼要看的是「成没成」，条数、耗时这些明细排在它后面。
                // 同步 / 导入 / 清除内存都有结论，查询状态没有（那是一次读数）。
                if (is_sync_task || is_erase_task) && !self.last_summary.is_empty() {
                    ui.add_space(12.0);
                    style::status_banner(ui, p, result_color, &self.last_summary.clone());
                }

                // ---------- ② 待重传告警：要用户处理的事，紧跟着结论 ----------
                if spool_pending > 0 {
                    ui.add_space(10.0);
                    ui.colored_label(
                        p.warn,
                        tr!(
                            "本地缓存待重传 {} 个",
                            "{} cached segment(s) waiting to be replayed",
                            spool_pending
                        ),
                    );
                }

                // ---------- ③ 本次任务汇总：下载 / 已同步 / 清理内存 / 速度 / 总耗时 ----------
                let stats = self.run_stats;
                let synced = stats.map(|s| s.uploaded).unwrap_or(self.uploaded);
                let downloaded = stats.map(|s| s.downloaded).unwrap_or(parsed);
                let (erase_text, erase_color) = match stats.and_then(|s| s.erased) {
                    Some(true) => (t!("成功", "done"), p.success),
                    Some(false) => (t!("失败", "failed"), p.error),
                    None => (t!("未执行", "skipped"), p.muted),
                };
                let mut total_secs: f64 =
                    self.phase_times.iter().map(|(_, d)| d.as_secs_f64()).sum();
                // 跑的时候把「当前这一段」也算进去，否则总耗时看着一直不涨
                if self.running {
                    if let Some(start) = self.phase_start {
                        total_secs += start.elapsed().as_secs_f64();
                    }
                }
                // 速度优先用实时值（上传中这一段），跑完才回落到结算后的平均值：
                // 以前跑的过程中这里一直是「—」，要等任务结束才知道快慢。
                let speed_text = match self.upload_rate().or_else(|| self.avg_upload_rate(synced)) {
                    Some(r) => tr!("{} 条/秒", "{} rec/s", fmt_count(r.round() as usize)),
                    None => "—".to_string(),
                };
                // 汇总统计只在完整同步时展示
                if is_sync_task
                    && (stats.is_some() || downloading || self.uploaded > 0 || erase_percent.is_some())
                {
                    ui.add_space(12.0);
                    // 导入没有「下载」和「清理内存」这两格：数据是本地读的，设备也没动过
                    let is_import = self.current_mode == Some(RunMode::Import);
                    let mut cells: Vec<(&str, String, egui::Color32)> = Vec::new();
                    if is_import {
                        cells.push((
                            t!("解析", "Parsed"),
                            tr!("{} 条", "{} rec", downloaded),
                            p.text,
                        ));
                        cells.push((
                            t!("已上传", "Uploaded"),
                            tr!("{} 条", "{} rec", synced),
                            p.success,
                        ));
                    } else {
                        cells.push((
                            t!("下载", "Downloaded"),
                            tr!("{} 条", "{} rec", downloaded),
                            p.text,
                        ));
                        cells.push((
                            t!("已同步", "Uploaded"),
                            tr!("{} 条", "{} rec", synced),
                            p.success,
                        ));
                        cells.push((t!("清理内存", "Erased"), erase_text.to_string(), erase_color));
                    }
                    cells.push((t!("上传速度", "Upload speed"), speed_text, p.text));
                    cells.push((
                        t!("总耗时", "Total time"),
                        fmt_dur(Duration::from_secs_f64(total_secs)),
                        p.text,
                    ));
                    let cols = cells.len();
                    let grid_w = (ui.available_width() - (cols as f32 - 1.0) * 16.0) / cols as f32;
                    egui::Grid::new("run_summary_grid")
                        .num_columns(cols)
                        .min_col_width(grid_w.max(90.0))
                        .spacing([16.0, 6.0])
                        .show(ui, |ui| {
                            for (label, value, color) in cells {
                                ui.vertical(|ui| {
                                    ui.colored_label(p.muted, label);
                                    ui.label(
                                        egui::RichText::new(value)
                                            .strong()
                                            .size(16.0)
                                            .color(color),
                                    );
                                });
                            }
                        });
                }

                // ---------- ④ 各通道状态：一行文字，不带进度条 ----------
                // 大圆环已经表达总体进度了，再摆两根条只是重复（数据是同一份、
                // 同时往几路送）；这里只回答「哪一路慢」，跑的时候带实时速度。
                if self.show_channels() {
                    ui.add_space(10.0);
                    self.channel_line(ui, p);
                }

                // ---------- ⑤ 运行中的实时计数 ----------
                // 跑完就不重复「已下载 x 条」了——上面汇总网格里已经有这一格
                if self.running && downloading {
                    ui.add_space(8.0);
                    let mut text = if total > 0 {
                        tr!(
                            "已下载 {} / {} 条",
                            "downloaded {} / {}",
                            parsed,
                            total
                        )
                    } else {
                        tr!("已下载 {} 条", "downloaded {}", parsed)
                    };
                    if received > parsed {
                        text.push_str(&tr!(
                            "（跳过无定位 {} 条）",
                            " ({} skipped without a fix)",
                            received - parsed
                        ));
                    }
                    ui.colored_label(p.muted, text);
                }
                if let Some(percent) = erase_percent {
                    ui.add_space(8.0);
                    ui.colored_label(p.muted, tr!("清理内存 {}%", "erasing {}%", percent));
                }

                // 各阶段耗时（只针对完整同步）
                if is_sync_task && !self.phase_times.is_empty() {
                    ui.add_space(10.0);
                    ui.colored_label(p.muted, t!("各阶段耗时", "Time per phase"));
                    ui.add_space(4.0);
                    ui.horizontal_wrapped(|ui| {
                        for (i, (name, d)) in self.phase_times.iter().enumerate() {
                            if i > 0 {
                                ui.add_space(6.0);
                                ui.colored_label(p.muted, "·");
                                ui.add_space(6.0);
                            }
                            ui.colored_label(p.muted, name.text());
                            ui.add_space(4.0);
                            ui.label(
                                egui::RichText::new(fmt_dur(*d))
                                    .strong()
                                    .color(p.text),
                            );
                        }
                    });
                }

                // 设备状态网格（查询成功后出现）
                if let Some(s) = &self.status {
                    ui.add_space(12.0);
                    let grid_w = (ui.available_width() - 3.0 * 16.0) / 4.0;
                    let rec_text = if s.recording {
                        t!("是", "yes")
                    } else {
                        t!("否", "no")
                    };
                    let mem_text = format!("{}%", s.memory_level);
                    let stored_text = tr!("{}/{} 条", "{}/{} rec", s.stored_messages, s.max_messages);
                    let lock_text = if s.security_enabled {
                        t!("已启用", "enabled")
                    } else {
                        t!("未启用", "disabled")
                    };
                    egui::Grid::new("status_grid")
                        .num_columns(4)
                        .min_col_width(grid_w.max(90.0))
                        .spacing([16.0, 10.0])
                        .show(ui, |ui| {
                            for (label, value) in [
                                (t!("录制", "Recording"), rec_text),
                                (t!("内存", "Memory"), mem_text.as_str()),
                                (t!("已存", "Stored"), stored_text.as_str()),
                                (t!("安全锁", "Lock"), lock_text),
                            ] {
                                ui.vertical(|ui| {
                                    ui.colored_label(p.muted, label);
                                    ui.label(
                                        egui::RichText::new(value)
                                            .strong()
                                            .size(16.0)
                                            .color(p.text),
                                    );
                                });
                            }
                        });
                }

                // 本次数据段（最新的在最上面）
                if !self.sessions.is_empty() {
                    ui.add_space(12.0);
                    ui.horizontal(|ui| {
                        ui.colored_label(p.muted, t!("本次数据段", "Segments in this run"));
                    });
                    ui.add_space(2.0);
                    egui::ScrollArea::vertical()
                        .id_salt("sessions")
                        .auto_shrink([false, false])
                        .max_height(110.0)
                        .show(ui, |ui| {
                            for s in self.sessions.iter().rev() {
                                ui.horizontal(|ui| {
                                    style::mini_badge(ui, p, &format!("#{}", s.index));
                                    ui.add_space(8.0);
                                    ui.label(egui::RichText::new(&s.name).strong());
                                    ui.with_layout(
                                        egui::Layout::right_to_left(egui::Align::Center),
                                        |ui| {
                                            // 图标用线条画（不依赖字体里的 ✓/✗，缺字会变成空心方框）；
                                            // 失败原因放在悬停提示里，避免长文本把这一行挤爆
                                            if s.ok {
                                                ui.colored_label(
                                                    p.success,
                                                    egui::RichText::new(t!("已上传", "Uploaded"))
                                                        .strong(),
                                                );
                                                style::status_icon(ui, p.success, true);
                                            } else {
                                                ui.colored_label(
                                                    p.error,
                                                    egui::RichText::new(t!(
                                                        "上传失败",
                                                        "Upload failed"
                                                    ))
                                                    .strong(),
                                                )
                                                .on_hover_text(s.detail.clone());
                                                style::status_icon(ui, p.error, false);
                                            }
                                            ui.add_space(10.0);
                                            ui.colored_label(
                                                p.muted,
                                                tr!("{} 条", "{} rec", s.records),
                                            );
                                        },
                                    );
                                });
                            }
                        });
                }
            });
        }

        // 传过一轮之后（或刚关掉又打开）这张卡排到结果卡下面
        if files_mode && !self.running && !files_card_on_top {
            self.local_files_card(ui, p);
        }
    }

    /// 画「本地文件」卡。
    fn local_files_card(&mut self, ui: &mut egui::Ui, p: &Palette) {
        ui.add_space(10.0);
        if self.cloud.import_card(ui, p, &mut self.cfg) {
            // 点了「取消本地上传」：主页回到设备同步，这张卡消失
            self.source = SyncSource::Device;
            self.files_card_first = false;
        }
    }

    /// 当前上传方式对应的那一路连接参数。
    fn active_target(&self) -> ConnTarget {
        match self.cfg.sink_mode {
            SinkMode::Database => ConnTarget::Pg,
            SinkMode::Mqtt => ConnTarget::Mqtt,
        }
    }

    /// 当前上传通道的连接信息填全了没。
    fn channel_ready(&self) -> bool {
        match self.active_target() {
            ConnTarget::Pg => self.cfg.pg.ready(),
            ConnTarget::Mqtt => self.cfg.mqtt.ready(),
        }
    }

    /// 打开某一路上传通道的填写弹窗。
    fn open_conn_dialog(&mut self, target: ConnTarget) {
        let draft = self.conn_draft(target);
        // 上次测试的结论是给旧参数看的，换一份参数就清掉
        match target {
            ConnTarget::Pg => self.pg_test.result.clear(),
            ConnTarget::Mqtt => self.mqtt_test.result.clear(),
        }
        self.dialog = Some(Dialog::Connection {
            target,
            draft,
            revealed: false,
        });
    }

    /// 「上传方式」卡下半截：当前通道配好了没 + 唯一的填写入口。
    ///
    /// 页面上**不显示任何连接信息**（主机、账号、密码一个都不露），只留一个状态
    /// 和一个按钮——那些东西全在弹窗里，改完才落到配置里。
    fn channel_row(&mut self, ui: &mut egui::Ui, p: &Palette) {
        let target = self.active_target();
        let (name, ready) = match target {
            ConnTarget::Pg => ("PostgreSQL", self.cfg.pg.ready()),
            ConnTarget::Mqtt => ("MQTT", self.cfg.mqtt.ready()),
        };
        ui.horizontal(|ui| {
            ui.colored_label(p.text, name);
            ui.add_space(10.0);
            if ready {
                ui.colored_label(p.success, t!("已填写", "configured"));
            } else {
                ui.colored_label(p.warn, t!("还没填写连接信息", "not configured yet"));
            }
            ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                // 按钮文字跟着状态走：还没填是「填写连接」，填过就是「修改连接」。
                // 不带省略号——这不是「打开更多」的意思，就是一个直接的动作。
                let label = if ready {
                    t!("修改连接", "Edit connection")
                } else {
                    t!("填写连接", "Set up connection")
                };
                if ui
                    .add(style::secondary_button(p, label))
                    .on_hover_text(match target {
                        ConnTarget::Pg => t!(
                            "同步前会先探一次连通性，连不上就直接停下，不会先把设备数据白下一遍。",
                            "Connectivity is probed before syncing; if it fails, the run stops without downloading anything."
                        ),
                        ConnTarget::Mqtt => t!(
                            "连接时会等 CONNACK，拿不到就当作通道不可用，直接停止本次同步。",
                            "Connecting waits for CONNACK; if it never arrives the channel counts as unusable and the sync stops."
                        ),
                    })
                    .clicked()
                {
                    self.open_conn_dialog(target);
                }
            });
        });
    }

    /// 打开连接参数弹窗时的初始值。
    ///
    /// 端口为 0（= 没填）时给空串，弹窗里就不会出现一个「0」；
    /// 主题 / Client ID 没填时也是空串，靠输入框的灰色提示给出建议值。
    fn conn_draft(&self, target: ConnTarget) -> ConnDraft {
        fn port_text(port: u16) -> String {
            if port == 0 {
                String::new()
            } else {
                port.to_string()
            }
        }
        match target {
            ConnTarget::Pg => {
                let c = &self.cfg.pg;
                ConnDraft {
                    host: c.host.clone(),
                    port: port_text(c.port),
                    database: c.database.clone(),
                    user: c.user.clone(),
                    password: c.password.clone(),
                    topic: String::new(),
                    client_id: String::new(),
                    keepalive: String::new(),
                }
            }
            ConnTarget::Mqtt => {
                let c = &self.cfg.mqtt;
                ConnDraft {
                    host: c.broker.clone(),
                    port: port_text(c.port),
                    database: String::new(),
                    user: c.user.clone(),
                    password: c.password.clone(),
                    topic: c.topic.clone(),
                    client_id: c.client_id.clone(),
                    keepalive: c.keepalive_secs.to_string(),
                }
            }
        }
    }

    /// 弹窗里点「确定」后把草稿写回配置。
    fn apply_conn_draft(&mut self, target: ConnTarget, draft: &ConnDraft) {
        match target {
            ConnTarget::Pg => self.cfg.pg = draft.pg_config(),
            ConnTarget::Mqtt => self.cfg.mqtt = draft.mqtt_config(),
        }
    }

    /// 凭据 / 云登录 / 连接参数弹窗。返回 true 表示弹窗还开着。
    fn show_dialog(&mut self, ctx: &egui::Context, p: &Palette) -> bool {
        let Some(dialog) = self.dialog.as_mut() else {
            return false;
        };
        let mut keep_open = true;
        let mut finished = false;
        let mut login_request: Option<(String, String)> = None;
        // 弹窗里点「测试连接」时不能直接调 self 的方法（`dialog` 已经把 self 借走了），
        // 先把草稿攒下来，等窗口画完再统一发起测试。
        let mut test_request: Option<(ConnTarget, ConnDraft)> = None;
        // 用户在弹窗里改过字段：上一次的测试结论不再作数
        let mut stale_test = false;
        let conn_target: Option<ConnTarget> = match &*dialog {
            Dialog::Connection { target, .. } => Some(*target),
            _ => None,
        };
        // 测试进度和结果同理：先把要显示的快照拷出来（pg_test / mqtt_test 是
        // 另外两个字段，和 `dialog` 不冲突）
        let test_state: Option<(bool, String, LogLevel)> = match &*dialog {
            Dialog::Connection { target, .. } => Some(match target {
                ConnTarget::Pg => (
                    self.pg_test.busy,
                    self.pg_test.result.clone(),
                    self.pg_test.level,
                ),
                ConnTarget::Mqtt => (
                    self.mqtt_test.busy,
                    self.mqtt_test.result.clone(),
                    self.mqtt_test.level,
                ),
            }),
            _ => None,
        };
        let title = match dialog {
            Dialog::CloudLogin { .. } => t!("登录 RaceBox 账号", "Sign in to RaceBox").to_string(),
            Dialog::Connection { target, .. } => {
                tr!("{} 连接参数", "{} connection", target.title())
            }
            Dialog::Notice { title, .. } => title.clone(),
        };
        // 模态：先铺一层遮罩压暗背景并吃掉背景上的点击，弹窗才真的「必须先关掉」。
        style::modal_shield(ctx, p);
        // 标题栏右上角带一个 ×：光靠「取消」按钮关窗不顺手
        let mut window_open = true;
        // 首次出现时大致居中。高度只能估——egui 没法在画之前量出窗口多高，
        // 估偏了也就偏几十像素，反正窗口能拖。
        let half_h = match dialog {
            Dialog::Notice { .. } => 90.0,
            Dialog::CloudLogin { .. } => 170.0,
            Dialog::Connection { target, .. } => match target {
                ConnTarget::Pg => 215.0,
                ConnTarget::Mqtt => 265.0,
            },
        };
        let center = ctx.screen_rect().center();
        egui::Window::new(title)
            .open(&mut window_open)
            .collapsible(false)
            .resizable(false)
            // 用 default_pos 而不是 anchor：anchor 会把窗口钉死在屏幕中间，想挪开
            // 看看被挡住的内容都挪不动。第一次出现在中间，之后记住用户拖到的位置。
            .default_pos(egui::pos2(center.x - 186.0, center.y - half_h))
            // 必须比遮罩高一层：遮罩在 Middle，弹窗放 Foreground。
            // 两者同层时，点一下背景遮罩就会被提到最上面，弹窗整窗变灰、点不动也关不掉。
            .order(egui::Order::Foreground)
            .frame(style::window(p))
            .show(ctx, |ui| {
                ui.set_width(match dialog {
                    Dialog::Notice { .. } => 300.0,
                    _ => 340.0,
                });
                match dialog {
                    Dialog::CloudLogin {
                        email,
                        password,
                        revealed,
                    } => {
                        ui.colored_label(p.muted, t!("邮箱", "E-mail"));
                        ui.add(
                            egui::TextEdit::singleline(email)
                                .hint_text("you@example.com")
                                .desired_width(f32::INFINITY),
                        );
                        ui.add_space(8.0);
                        ui.colored_label(p.muted, t!("密码", "Password"));
                        ui.add(
                            egui::TextEdit::singleline(password)
                                .password(!*revealed)
                                .desired_width(f32::INFINITY),
                        );
                        ui.add_space(6.0);
                        ui.checkbox(revealed, t!("显示密码", "Show password"));
                        ui.add_space(8.0);
                        // 说明不再整段铺在弹窗里，收进右上角的「?」（和设置页卡片一致）
                        ui.horizontal(|ui| {
                            style::help_icon(
                                ui,
                                p,
                                "cloud_login_cred",
                                t!(
                                    "凭据只保存在本机的应用数据目录（cloud.json），不进 config.json。",
                                    "Credentials stay in the app data folder on this machine (cloud.json); they are not written into config.json."
                                ),
                            );
                            ui.add_space(6.0);
                            style::hint(ui, p, t!("凭据只保存在本机", "credentials stay on this machine"));
                        });
                        ui.add_space(14.0);
                        let ready = !email.trim().is_empty() && !password.is_empty();
                        let (ok, cancel) = style::dialog_buttons(
                            ui,
                            p,
                            t!("登录", "Sign in"),
                            t!("取消", "Cancel"),
                            ready,
                        );
                        if ok {
                            login_request = Some((email.clone(), password.clone()));
                            keep_open = false;
                        }
                        if cancel {
                            keep_open = false;
                        }
                    }
                    Dialog::Connection {
                        target,
                        draft,
                        revealed,
                    } => {
                        let pg = *target == ConnTarget::Pg;
                        // 任何字段一动，上一次的测试结论就过期了，画完统一清掉
                        let mut dirty = false;
                        // 主机 / Broker
                        dirty |= dialog_field(
                            ui,
                            p,
                            if pg { t!("主机", "Host") } else { "Broker" },
                            &mut draft.host,
                            "",
                            false,
                            false,
                        )
                        .changed();
                        // 端口 +（PG 的库名 / MQTT 的保活）
                        ui.horizontal(|ui| {
                            let half = (ui.available_width() - 12.0) / 2.0;
                            ui.vertical(|ui| {
                                ui.colored_label(p.muted, t!("端口", "Port"));
                                dirty |= ui
                                    .add(
                                        egui::TextEdit::singleline(&mut draft.port)
                                            .desired_width(half),
                                    )
                                    .changed();
                            });
                            ui.add_space(12.0);
                            ui.vertical(|ui| {
                                if pg {
                                    ui.colored_label(p.muted, t!("数据库", "Database"));
                                    dirty |= ui
                                        .add(
                                            egui::TextEdit::singleline(&mut draft.database)
                                                .desired_width(half),
                                        )
                                        .changed();
                                } else {
                                    ui.colored_label(p.muted, t!("保活间隔", "Keep-alive"));
                                    dirty |= ui
                                        .add(
                                            egui::TextEdit::singleline(&mut draft.keepalive)
                                                .desired_width(half),
                                        )
                                        .changed();
                                }
                            });
                        });
                        ui.add_space(8.0);
                        // 账号
                        dirty |= dialog_field(
                            ui,
                            p,
                            t!("用户", "User"),
                            &mut draft.user,
                            "",
                            false,
                            false,
                        )
                        .changed();
                        // 密码：默认打码，勾上「显示密码」才明文
                        ui.colored_label(p.muted, t!("密码", "Password"));
                        ui.add_space(4.0);
                        dirty |= ui
                            .add(
                                egui::TextEdit::singleline(&mut draft.password)
                                    .password(!*revealed)
                                    .desired_width(f32::INFINITY),
                            )
                            .changed();
                        ui.add_space(6.0);
                        ui.checkbox(revealed, t!("显示密码", "Show password"));
                        if !pg {
                            ui.add_space(8.0);
                            // 这两个留空就是「用内置默认值」，所以给灰色提示、不预填：
                            // 预填进去等于替用户做了决定，还得先删掉才能写自己的
                            dirty |= dialog_field(
                                ui,
                                p,
                                t!("主题", "Topic"),
                                &mut draft.topic,
                                crate::config::DEFAULT_MQTT_TOPIC,
                                false,
                                false,
                            )
                            .changed();
                            dirty |= dialog_field(
                                ui,
                                p,
                                "Client ID",
                                &mut draft.client_id,
                                crate::config::DEFAULT_MQTT_CLIENT_ID,
                                false,
                                false,
                            )
                            .changed();
                        }
                        ui.add_space(12.0);

                        // 测试连接：测的是**弹窗里正在填的这组参数**，不是已保存的配置，
                        // 所以不用先点「确定」就能验证能不能连上。
                        let busy = test_state.as_ref().map_or(false, |(busy, _, _)| *busy);
                        ui.horizontal(|ui| {
                            if ui
                                .add_enabled(
                                    !busy,
                                    style::secondary_button(
                                        p,
                                        t!("测试连接", "Test connection"),
                                    ),
                                )
                                .clicked()
                            {
                                test_request = Some((*target, draft.clone()));
                            }
                            if busy {
                                ui.spinner();
                            }
                        });
                        if let Some((_, result, level)) = &test_state {
                            if !result.is_empty() {
                                ui.add_space(8.0);
                                style::status_banner(ui, p, level_color(p, *level), result);
                            }
                        }

                        ui.add_space(14.0);
                        // 端口留空 = 用默认端口，所以只要求主机填了
                        let ready = if pg {
                            !draft.host.trim().is_empty()
                                && !draft.database.trim().is_empty()
                                && !draft.user.trim().is_empty()
                        } else {
                            !draft.host.trim().is_empty()
                        };
                        let (ok, cancel) = style::dialog_buttons(
                            ui,
                            p,
                            t!("确定", "OK"),
                            t!("取消", "Cancel"),
                            ready,
                        );
                        if ok {
                            keep_open = false;
                            finished = true;
                        }
                        if cancel {
                            keep_open = false;
                        }
                        if dirty {
                            stale_test = true;
                        }
                    }
                    Dialog::Notice { body, .. } => {
                        ui.style_mut().wrap_mode = Some(egui::TextWrapMode::Wrap);
                        ui.colored_label(p.text, body.clone());
                        ui.add_space(16.0);
                        ui.vertical_centered(|ui| {
                            if ui
                                .add(
                                    style::primary_button(p, t!("确定", "OK"))
                                        .min_size(egui::vec2(120.0, 32.0)),
                                )
                                .clicked()
                            {
                                keep_open = false;
                            }
                        });
                    }
                }
            });
        if !window_open || ctx.input(|i| i.key_pressed(egui::Key::Escape)) {
            keep_open = false;
        }
        // 改过参数就把上一次的测试结论清掉：那条结论是给旧参数看的，
        // 留着会让人以为「刚填的这组」已经验证过了
        if stale_test {
            match conn_target {
                Some(ConnTarget::Pg) => self.pg_test.result.clear(),
                Some(ConnTarget::Mqtt) => self.mqtt_test.result.clear(),
                None => {}
            }
        }
        if finished {
            if let Some(Dialog::Connection { target, draft, .. }) = self.dialog.take() {
                self.apply_conn_draft(target, &draft);
                // 不在设置页时没有「保存」按钮可点，这里直接落盘，
                // 否则填完的连接参数关掉程序就没了
                if self.tab != Tab::Settings {
                    let _ = config::save(&self.cfg);
                    self.settings_snapshot = self.cfg.clone();
                }
                // 这次弹窗是被「开始同步」拦下来的：填完接着跑，别让用户再点一次
                if let Some(mode) = self.pending_run.take() {
                    match mode {
                        RunMode::Import => self.start_import(),
                        other => self.start(other),
                    }
                }
            }
        }
        if let Some((target, draft)) = test_request {
            self.start_conn_test(target, &draft);
        }
        if let Some((email, password)) = login_request {
            self.dialog = None;
            let rt = self.runtime.clone();
            self.cloud.login(&rt, email, password);
        }
        if !keep_open {
            self.dialog = None;
            // 弹窗被取消 / 关掉：那次被拦下来的任务就不该再自己跑起来
            self.pending_run = None;
        }
        self.dialog.is_some()
    }

    /// ------------------------------------------------------------------
    /// 设置页
    /// ------------------------------------------------------------------
    fn show_settings_tab(&mut self, ui: &mut egui::Ui, p: &Palette) {
        // ---------- 上传方式 ----------
        style::card(p).show(ui, |ui| {
            style::fill_card_width(ui);
            style::card_title_with_help(
                ui,
                p,
                t!("上传方式", "Upload channel"),
                "settings_channel",
                t!(
                    "设备数据取回来之后往哪儿送。这一路是主通道，RaceBox 云是叠加在上面的可选通道。",
                    "Where the downloaded data goes. This is the primary channel; the RaceBox cloud is an optional extra on top."
                ),
            );
            ui.add_space(10.0);
            // 两个互斥选项做成整块可点的卡片：比两个圆点更好读，副标题能把区别说清。
            let choices = [
                (
                    SinkMode::Database,
                    t!("直接写自己的库", "write straight into your own database"),
                ),
                (
                    SinkMode::Mqtt,
                    t!("按 RBX2 协议发消息", "publish RBX2 messages"),
                ),
            ];
            let w = ((ui.available_width() - 12.0) / 2.0).max(160.0);
            ui.horizontal(|ui| {
                // 卡片之间的缝直接设成 item_spacing：如果再额外 add_space(12)，
                // egui 会在它前后各插一次默认间距，整行就比可用宽度宽出一截。
                ui.spacing_mut().item_spacing.x = 12.0;
                for (mode, sub) in choices.iter() {
                    if style::choice_card(
                        ui,
                        p,
                        self.cfg.sink_mode == *mode,
                        mode.label(),
                        sub,
                        egui::vec2(w, 58.0),
                    )
                    .clicked()
                        && self.cfg.sink_mode != *mode
                    {
                        self.cfg.sink_mode = *mode;
                    }
                }
            });
            ui.add_space(14.0);
            // 连接参数不再是单独两张卡：页面上一个地址都不露，只有「填好了没」
            // 和唯一的入口，真正的主机/账号/密码都在弹窗里
            self.channel_row(ui, p);
        });

        ui.add_space(10.0);
        // ---------- RaceBox 云（账号 + 上传配置，原「云页面」搬到这里） ----------
        style::card(p).show(ui, |ui| {
            style::fill_card_width(ui);
            let rt = self.runtime.clone();
            self.cloud.account_card(ui, p, &rt);
            ui.add_space(12.0);
            let dirty = self.cloud.upload_rows(ui, p, &mut self.cfg);
            if dirty {
                self.cloud.save_cloud(&self.cfg);
                // 改的是云这一段，设置页其余部分是草稿，不能被覆盖写掉了
                self.settings_snapshot.cloud = self.cfg.cloud.clone();
            }
        });

        ui.add_space(10.0);
        // ---------- 行为 ----------
        style::card(p).show(ui, |ui| {
            style::fill_card_width(ui);
            style::card_title_with_help(
                ui,
                p,
                t!("行为", "Behaviour"),
                "settings_behaviour",
                t!(
                    "清除设备内存会擦掉设备上存的所有数据；本地缓存用来兜住上传失败的部分。",
                    "Erasing device memory wipes everything stored on the device; the local cache catches segments that failed to upload."
                ),
            );
            ui.add_space(12.0);
            style::toggle_row(
                ui,
                p,
                t!(
                    "上传成功后自动清除设备内存",
                    "Erase device memory after a successful upload"
                ),
                &mut self.cfg.auto_erase,
            );
            ui.add_space(12.0);
            style::toggle_row(
                ui,
                p,
                t!(
                    "上传失败时写入本地缓存，下次启动重传",
                    "Cache failed uploads on disk and replay them on the next launch"
                ),
                &mut self.cfg.spool_on_failure,
            );
            ui.add_space(14.0);
            ui.horizontal(|ui| {
                ui.colored_label(p.muted, t!("设备扫描超时", "Scan timeout"));
                ui.add_space(10.0);
                ui.add(
                    egui::TextEdit::singleline(&mut self.scan_timeout_buf)
                        .desired_width(80.0)
                        .horizontal_align(egui::Align::Center),
                );
                ui.colored_label(p.muted, t!("秒", "s"));
            });
            ui.add_space(14.0);
            ui.horizontal(|ui| {
                ui.colored_label(p.muted, t!("设备名过滤", "Device name filter"));
                ui.add_space(10.0);
                ui.add(
                    egui::TextEdit::singleline(&mut self.cfg.device_name_filter).desired_width(200.0),
                );
            });
            ui.add_space(16.0);
            // 关闭窗口时的行为：二选一，和「上传方式」同一套卡片控件
            ui.colored_label(p.muted, t!("点关闭按钮时", "When the window is closed"));
            ui.add_space(6.0);
            let choices = [
                (CloseAction::Quit, t!("点 × 直接退出", "× quits the app")),
                (
                    CloseAction::Tray,
                    t!("收进托盘后台继续跑", "keep running in the tray"),
                ),
            ];
            let w = ((ui.available_width() - 12.0) / 2.0).max(160.0);
            ui.horizontal(|ui| {
                // 同上：间距交给 item_spacing，别再 add_space，否则整行会超出可用宽度
                ui.spacing_mut().item_spacing.x = 12.0;
                for (action, sub) in choices.iter() {
                    if style::choice_card(
                        ui,
                        p,
                        self.cfg.close_action == *action,
                        action.label(),
                        sub,
                        egui::vec2(w, 58.0),
                    )
                    .clicked()
                        && self.cfg.close_action != *action
                    {
                        self.cfg.close_action = *action;
                        // 图标常驻，这里不用再管它；留着调用是为了万一以后
                        // 托盘创建失败过（tray_error），切换设置时能再试一次
                        self.sync_tray();
                    }
                }
            });
            if let Some(err) = self.tray_error.clone() {
                ui.add_space(6.0);
                ui.colored_label(
                    p.warn,
                    tr!(
                        "托盘不可用（{err}），点关闭仍会直接退出",
                        "tray unavailable ({err}); closing the window will quit instead"
                    ),
                );
            }
        });

        ui.add_space(10.0);
        // ---------- 语言 ----------
        style::card(p).show(ui, |ui| {
            style::fill_card_width(ui);
            style::card_title(ui, p, t!("语言 / Language", "Language / 语言"));
            ui.add_space(10.0);
            ui.horizontal(|ui| {
                // 语言多了用下拉列表：10 个单选按钮横着排会挤成一坨
                let mut picked = self.cfg.lang;
                egui::ComboBox::from_id_salt("lang_combo")
                    .selected_text(picked.native())
                    .width(200.0)
                    .show_ui(ui, |ui| {
                        for lang in Lang::ALL {
                            ui.selectable_value(&mut picked, lang, lang.native());
                        }
                    });
                if picked != self.cfg.lang {
                    // 立即生效并落盘，不用再点“保存设置”
                    self.set_lang(picked);
                }
            });
        });

        ui.add_space(10.0);
        // ---------- 软件更新 ----------
        style::card(p).show(ui, |ui| {
            style::fill_card_width(ui);
            style::card_title(ui, p, t!("软件更新", "Software update"));
            ui.add_space(10.0);
            ui.horizontal(|ui| {
                ui.colored_label(
                    p.muted,
                    tr!("当前版本 {}", "Current version {}", env!("CARGO_PKG_VERSION")),
                );
                ui.add_space(12.0);
                if ui.add_enabled(
                    !self.update_busy,
                    style::secondary_button(p, t!("检查并更新", "Check and update")),
                ).clicked() {
                    self.check_update();
                }
                if self.update_busy { ui.spinner(); }
            });
            if let Some((text, level)) = &self.update_result {
                style::status_banner(ui, p, level_color(p, *level), text);
            }
        });

        ui.add_space(10.0);
        // ---------- 外观 ----------
        style::card(p).show(ui, |ui| {
            style::fill_card_width(ui);
            style::card_title(ui, p, t!("外观", "Appearance"));
            ui.add_space(12.0);
            ui.horizontal(|ui| {
                ui.colored_label(p.muted, t!("主题", "Theme"));
                ui.add_space(16.0);
                let dark = self.cfg.theme == Theme::Dark;
                if style::theme_card(ui, p, dark, t!("深色", "Dark"), true).clicked() && !dark {
                    self.set_theme(ui.ctx(), Theme::Dark);
                }
                ui.add_space(14.0);
                if style::theme_card(ui, p, !dark, t!("浅色", "Light"), false).clicked() && dark {
                    self.set_theme(ui.ctx(), Theme::Light);
                }
            });
        });

        ui.add_space(16.0);
        // 保存 / 放弃靠右下角（右侧留点空隙，不贴边）。
        // 状态提示跟按钮**同一行**放在左边：原来它单独占一行、还靠左贴边，
        // 点完保存视线要往下跳一大截才看得到结果。先拷一份，闭包里要改 self。
        let action_result = self.action_result.clone();
        ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
            ui.add_space(56.0);
            if ui
                .add_enabled(
                    !self.running,
                    style::secondary_button(p, t!("放弃修改", "Discard changes")),
                )
                .clicked()
            {
                self.discard_edits();
            }
            ui.add_space(8.0);
            if ui
                .add_enabled(
                    !self.running,
                    style::primary_button(p, t!("保存设置", "Save settings")),
                )
                .clicked()
            {
                self.save_config();
            }
            if let Some((text, level)) = &action_result {
                ui.add_space(12.0);
                // 剩下的宽度全给提示，超了截断，绝不把按钮挤出去
                let max_w = (ui.available_width() - 4.0).max(60.0);
                style::status_pill(ui, p, level_color(p, *level), text, max_w);
            }
        });
    }

    /// 日志抽屉内容（底部可折叠面板，不再是独立标签页）。
    fn show_logs_panel(&mut self, ui: &mut egui::Ui, p: &Palette) {
        ui.horizontal(|ui| {
            ui.label(
                egui::RichText::new(t!("日志", "Logs"))
                    .strong()
                    .size(14.0)
                    .color(p.text),
            );
            ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                if ui
                    .add(style::small_button(p, t!("收起", "Hide")))
                    .clicked()
                {
                    self.show_logs = false;
                }
                ui.add_space(6.0);
                if ui
                    .add_enabled(!self.running, style::small_button(p, t!("清空", "Clear")))
                    .clicked()
                {
                    self.logs.clear();
                }
            });
        });
        ui.add_space(4.0);
        egui::ScrollArea::vertical()
            .id_salt("logs")
            .auto_shrink([false, false])
            .stick_to_bottom(true)
            .show(ui, |ui| {
                if self.logs.is_empty() {
                    style::hint(ui, p, t!("暂无日志", "No log entries yet"));
                }
                for (level, message) in &self.logs {
                    ui.horizontal(|ui| {
                        let color = p.level_color(*level).unwrap_or(p.muted);
                        style::level_badge(ui, p, Palette::level_tag(*level), color);
                        ui.add_space(8.0);
                        match p.level_color(*level) {
                            Some(c) => ui.colored_label(c, message.as_str()),
                            None => ui.label(message.as_str()),
                        }
                    });
                }
            });
    }
}

/// 结果提示文字的颜色。
///
/// 以前是靠“文字里有没有‘失败/成功’”去猜颜色，界面一支持英文就失效了；
/// 现在结果等级跟着消息一起传过来，颜色直接查表。
fn level_color(p: &Palette, level: LogLevel) -> egui::Color32 {
    match level {
        LogLevel::Success => p.success,
        LogLevel::Warn => p.warn,
        LogLevel::Error => p.error,
        LogLevel::Info => p.info,
    }
}

fn level_name(level: LogLevel) -> &'static str {
    match level {
        LogLevel::Info => "INFO",
        LogLevel::Success => "SUCCESS",
        LogLevel::Warn => "WARN",
        LogLevel::Error => "ERROR",
    }
}

/// 千分位计数：18 万条写成 180,000 才一眼看出量级。
fn fmt_count(n: usize) -> String {
    let s = n.to_string();
    let mut out = String::with_capacity(s.len() + s.len() / 3);
    for (i, c) in s.chars().enumerate() {
        if i > 0 && (s.len() - i) % 3 == 0 {
            out.push(',');
        }
        out.push(c);
    }
    out
}

/// 耗时的人类可读格式：不足 1 分钟显示秒，否则显示“X 分 YY 秒”。
fn fmt_dur(d: Duration) -> String {
    let secs = d.as_secs_f64();
    if secs < 60.0 {
        tr!("{} 秒", "{}s", format!("{secs:.1}"))
    } else {
        let m = (secs / 60.0) as u64;
        let s = d.as_secs() % 60;
        tr!("{} 分 {} 秒", "{}m {}s", m, format!("{s:02}"))
    }
}

fn mode_text(mode: RunMode) -> &'static str {
    match mode {
        RunMode::Sync => t!("完整同步", "Full sync"),
        RunMode::Import => t!("本地文件导入", "Local file import"),
        RunMode::Status => t!("状态查询", "Status query"),
        RunMode::Erase => t!("清除内存", "Erase memory"),
    }
}

impl eframe::App for App {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        if self.ctx.is_none() {
            self.ctx = Some(ctx.clone());
            // 启动时 main.rs 已经 apply 过一次，但那时还没进入真正的帧循环，
            // 「让系统标题栏跟着主题走」这条视口命令可能没被派发出去。
            // 第一帧再补一次：apply 是幂等的，多调一次没有副作用。
            style::apply(ctx, self.cfg.theme);
            self.applied_theme = self.cfg.theme;
        }
        // 托盘图标要在事件循环起来之后才能建，所以放在这里（每帧检查一次，没需求就直接返回）
        self.sync_tray();
        self.handle_close_and_tray(ctx);
        if self.cfg.theme != self.applied_theme {
            style::apply(ctx, self.cfg.theme);
            self.applied_theme = self.cfg.theme;
        }
        self.drain_events(ctx);
        // 云的后台任务（登录、文件导入）不管在哪个页面都要按时收结果
        self.cloud.poll();

        let p = self.palette();

        // 顶部栏：主页右上角是齿轮（设置）和信息（关于）图标；设置页左上角是“返回”
        egui::TopBottomPanel::top("top_bar")
            .frame(
                egui::Frame::side_top_panel(&ctx.style())
                    .fill(p.header)
                    .inner_margin(egui::Margin::symmetric(10, 5)),
            )
            .show(ctx, |ui| {
                ui.horizontal(|ui| {
                    if self.tab == Tab::Settings {
                        if style::back_button(ui, &p)
                            .on_hover_text(t!("返回同步页", "Back to sync"))
                            .clicked()
                        {
                            self.request_close_settings();
                        }
                    }
                    ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                        if style::info_button(ui, &p)
                            .on_hover_text(t!("关于", "About"))
                            .clicked() {
                            self.about_open = true;
                        }
                        if self.tab != Tab::Settings {
                            if style::gear_button(ui, &p)
                                .on_hover_text(t!("设置", "Settings"))
                                .clicked()
                            {
                                self.open_settings();
                            }
                        }
                    });
                });
                ui.add_space(4.0);
            });

        // 底部状态栏（最外层底部）：固定高度，整条内容垂直居中、小号字体
        egui::TopBottomPanel::bottom("status_bar")
            .exact_height(30.0)
            .frame(
                egui::Frame::side_top_panel(&ctx.style())
                    .fill(p.header)
                    .inner_margin(egui::Margin::symmetric(12, 0)),
            )
            .show(ctx, |ui| {
                let full = ui.available_rect_before_wrap();
                let mid = full.center().y;
                let font = egui::FontId::proportional(12.0);
                let (dot, text, color) = if self.running {
                    (
                        p.info,
                        tr!("运行中 · {}", "Running · {}", self.phase.text()),
                        p.text,
                    )
                } else if self.phase.is_final() {
                    (p.muted, self.phase.text().to_string(), p.muted)
                } else {
                    (p.muted, t!("空闲", "Idle").to_string(), p.muted)
                };
                let painter = ui.painter();
                // 状态点（运行中呼吸闪烁）
                let dot_color = if self.running {
                    let t = ui.input(|i| i.time) as f32;
                    let alpha = (150.0 + 70.0 * (t * 2.5).sin()) as u8;
                    egui::Color32::from_rgba_unmultiplied(dot.r(), dot.g(), dot.b(), alpha)
                } else {
                    dot
                };
                painter.circle_filled(egui::pos2(full.left() + 4.0, mid), 3.0, dot_color);
                // 状态文字
                painter.text(
                    egui::pos2(full.left() + 13.0, mid),
                    egui::Align2::LEFT_CENTER,
                    &text,
                    font,
                    color,
                );
                // 右侧：待重传 + 日志开关（覆盖整条右对齐，保证垂直居中）
                ui.allocate_new_ui(
                    egui::UiBuilder::new()
                        .max_rect(full)
                        .layout(egui::Layout::right_to_left(egui::Align::Center)),
                    |ui| {
                        if self.tab == Tab::Sync {
                            if style::tab_button_sized(
                                ui,
                                &p,
                                self.show_logs,
                                t!("日志", "Logs"),
                                egui::vec2(48.0, 20.0),
                            )
                            .clicked()
                            {
                                self.show_logs = !self.show_logs;
                            }
                            ui.add_space(12.0);
                        }
                        ui.colored_label(
                            p.muted,
                            egui::RichText::new(tr!(
                                "待重传 {}",
                                "{} pending",
                                self.spool_pending
                            ))
                            .size(12.0),
                        );
                    },
                );
            });

        // 日志抽屉（状态栏上方，可拖高；只在同步页显示）
        if self.show_logs && self.tab == Tab::Sync {
            egui::TopBottomPanel::bottom("logs")
                .resizable(true)
                .default_height(200.0)
                .frame(
                    egui::Frame::side_top_panel(&ctx.style())
                        .fill(p.card)
                        .inner_margin(egui::Margin::symmetric(10, 8)),
                )
                .show(ctx, |ui| {
                    self.show_logs_panel(ui, &p);
                });
        }

        egui::CentralPanel::default()
            .frame(
                egui::Frame::central_panel(&ctx.style())
                    .fill(p.bg)
                    .inner_margin(egui::Margin::symmetric(16, 14)),
            )
            .show(ctx, |ui| {
                egui::ScrollArea::vertical()
                    .auto_shrink([false, false])
                    .show(ui, |ui| match self.tab {
                        Tab::Sync => self.show_sync_tab(ui, &p),
                        Tab::Settings => self.show_settings_tab(ui, &p),
                    });
            });

        // 关于窗口：带标题栏（可以用标题栏拖动），点窗口外任意处关闭，
        // 点窗体内部不关（里面还有「检查更新」要按）。
        if self.about_open {
            // 懒加载应用图标纹理（和 exe/窗口图标同款 icon256.png）
            if self.about_icon.is_none() {
                let bytes = include_bytes!("../assets/icon256.png");
                if let Ok(icon) = eframe::icon_data::from_png_bytes(bytes) {
                    let image = egui::ColorImage::from_rgba_unmultiplied(
                        [icon.width as usize, icon.height as usize],
                        &icon.rgba,
                    );
                    self.about_icon =
                        Some(ctx.load_texture("about_icon", image, Default::default()));
                }
            }
            // 标题栏可拖动：拖动后 egui 会记住位置，下次打开还在那儿；
            // 首次没记录时才用这个居中坐标兜一下。
            let center = ctx.screen_rect().center();
            let win = egui::Window::new(t!("关于", "About"))
                .default_pos(egui::pos2(center.x - 170.0, center.y - 180.0))
                .collapsible(false)
                .resizable(false)
                .default_size([320.0, 360.0])
                .default_open(true)
                .frame(style::window(&p));
            let mut open = true;
            let resp = win
                .open(&mut open)
                .show(ctx, |ui| {
                    ui.set_width(300.0);
                    ui.style_mut().wrap_mode = Some(egui::TextWrapMode::Wrap);
                    ui.vertical_centered(|ui| {
                        // 应用图标
                        if let Some(tex) = &self.about_icon {
                            ui.add(egui::Image::new((
                                tex.id(),
                                egui::vec2(52.0, 52.0),
                            )));
                        } else {
                            ui.add(egui::Label::new(
                                egui::RichText::new("RaceBox").strong().size(20.0),
                            ));
                        }
                        ui.add_space(8.0);
                        ui.label(
                            egui::RichText::new(t!("RaceBox同步", "RaceBox Sync"))
                                .strong()
                                .size(18.0)
                                .color(p.text),
                        );
                        ui.add_space(4.0);
                        // 版本号本身就是「检查更新」的入口：点一下就开始检查
                        let busy = self.update_busy;
                        let version_label = if busy {
                            tr!(
                                "版本 {} · 检查中…",
                                "version {} · checking…",
                                env!("CARGO_PKG_VERSION")
                            )
                        } else {
                            tr!("版本 {} · 检查更新", "version {} · check", env!("CARGO_PKG_VERSION"))
                        };
                        let version_btn = egui::Button::new(
                            egui::RichText::new(version_label).size(12.5).color(p.info),
                        )
                        .frame(false);
                        let version_resp = ui
                            .add_enabled(!busy, version_btn)
                            .on_hover_text(t!(
                                "点这里检查 GitHub Release 上的新版本",
                                "click to check GitHub Releases for a newer version"
                            ));
                        if version_resp.clicked() {
                            self.check_update();
                        }
                    });
                    if let Some((text, level)) = &self.update_result {
                        style::status_banner(ui, &p, level_color(&p, *level), text);
                        ui.add_space(8.0);
                    }
                    ui.add_space(6.0);
                    about_row(ui, &p, t!("作者", "Author"), "wangcw");
                    about_row(ui, &p, t!("邮箱", "Email"), "rubygreat@msn.com");
                    about_row(
                        ui,
                        &p,
                        t!("协议", "Protocol"),
                        "ublox BLE · PostgreSQL / MQTT (RBX2)",
                    );
                    ui.add_space(6.0);
                });
            self.about_rect = resp.and_then(|r| Some(r.response.rect));
            if !open {
                self.about_open = false;
            } else {
                // 点（或 Esc）在窗体之外就关掉：窗体内部要留给按钮和文本选择
                let outside = ctx.pointer_interact_pos().map_or(true, |pos| {
                    self.about_rect.map_or(true, |rect| !rect.contains(pos))
                });
                let pressed_outside = outside && ctx.input(|i| i.pointer.any_pressed());
                if pressed_outside || ctx.input(|i| i.key_pressed(egui::Key::Escape)) {
                    self.about_open = false;
                }
            }
        } else {
            self.about_rect = None;
        }

        // 关闭设置页前的“有未保存更改”确认弹窗
        if self.confirm_close {
            let mut keep_open = true;
            style::modal_shield(ctx, &p);
            let center = ctx.screen_rect().center();
            egui::Window::new(t!("未保存的更改", "Unsaved changes"))
                .collapsible(false)
                .resizable(false)
                // 和连接参数弹窗同一套：可拖动，并且压在遮罩之上
                .default_pos(egui::pos2(center.x - 160.0, center.y - 90.0))
                .order(egui::Order::Foreground)
                .frame(style::window(&p))
                .show(ctx, |ui| {
                    ui.set_width(320.0);
                    ui.label(t!(
                        "设置页面有未保存的更改，是否保存？",
                        "You have unsaved changes. Save them?"
                    ));
                    ui.add_space(14.0);
                    ui.horizontal(|ui| {
                        if ui
                            .add(style::primary_button(&p, t!("保存并退出", "Save and close")))
                            .clicked()
                        {
                            self.save_config();
                            self.confirm_close = false;
                            keep_open = false;
                            self.tab = Tab::Sync;
                        }
                        if ui
                            .add(style::secondary_button(&p, t!("不保存退出", "Close without saving")))
                            .clicked()
                        {
                            self.discard_edits();
                            self.confirm_close = false;
                            keep_open = false;
                            self.tab = Tab::Sync;
                        }
                        if ui
                            .add(style::secondary_button(&p, t!("取消", "Cancel")))
                            .clicked()
                        {
                            self.confirm_close = false;
                            keep_open = false;
                        }
                    });
                });
            if !keep_open || ctx.input(|i| i.key_pressed(egui::Key::Escape)) {
                self.confirm_close = false;
            }
        }

        // 设置页里点了云账号的「登录」：弹窗统一在这里渲染（放最后，盖在其它内容之上）
        if self.cloud.take_login_request() && self.dialog.is_none() {
            self.dialog = Some(Dialog::CloudLogin {
                email: String::new(),
                password: String::new(),
                revealed: false,
            });
        }
        self.show_dialog(ctx, &p);
    }
}

/// 关于弹窗里的“标签: 值”信息行。
fn about_row(ui: &mut egui::Ui, p: &Palette, key: &str, value: &str) {
    ui.horizontal(|ui| {
        ui.add_space(24.0);
        ui.colored_label(p.muted, format!("{key}:"));
        ui.add_space(8.0);
        if value.contains('@') {
            // 邮箱可选中复制
            ui.add(
                egui::Label::new(egui::RichText::new(value.to_owned()).color(p.info)).selectable(true),
            );
        } else {
            ui.label(value);
        }
    });
}
