use eframe::egui;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::runtime::Runtime;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

use crate::config::{self, CloseAction, Config, SinkMode, Theme};
use crate::i18n::Lang;
use crate::protocol::DeviceStatus;
use crate::style::{self, Palette};
use crate::worker::{LogLevel, NoticeTarget, Phase, RunMode, RunStats, UiEvent};
use crate::{t, tr};

#[derive(Clone, Copy, PartialEq, Eq)]
enum Tab {
    Sync,
    Settings,
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
    /// 当前正在上传的那一段：(段号, 已写入, 总数)。
    session_progress: Option<(u32, usize, usize)>,
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
    pg_port_buf: String,
    mqtt_port_buf: String,
    scan_timeout_buf: String,
    keepalive_buf: String,
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
}

impl App {
    pub fn new(
        runtime: Arc<Runtime>,
        cfg: Config,
        tx: UnboundedSender<UiEvent>,
        rx: UnboundedReceiver<UiEvent>,
        window_handle: isize,
    ) -> Self {
        let pg_port_buf = cfg.pg.port.to_string();
        let mqtt_port_buf = cfg.mqtt.port.to_string();
        let (probe_tx, probe_rx) = std::sync::mpsc::channel::<crate::sink::mqtt::MqttSink>();
        let (update_tx, update_rx) = std::sync::mpsc::channel::<UpdateMsg>();
        let applied_theme = cfg.theme;
        let settings_snapshot = cfg.clone();
        let scan_timeout_buf = cfg.scan_timeout_secs.to_string();
        let keepalive_buf = cfg.mqtt.keepalive_secs.to_string();
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
            session_progress: None,
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
            pg_port_buf,
            mqtt_port_buf,
            scan_timeout_buf,
            keepalive_buf,
            tray: None,
            tray_error: None,
            quit_requested: false,
            hidden_to_tray: false,
            window_handle,
            update_tx,
            update_rx,
            update_busy: false,
            update_result: None,
        }
    }

    /// 按当前设置准备 / 收起托盘图标。
    ///
    /// 托盘图标只在“隐藏到托盘”模式下常驻——选“直接退出”时不该在通知区域多出一个图标。
    /// 创建失败会记住原因、不再重试（`tray_error`），避免每帧都去撞一次系统接口。
    fn sync_tray(&mut self) {
        if self.cfg.close_action != CloseAction::Tray {
            self.tray = None;
            self.tray_error = None;
            return;
        }
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
    fn handle_close_and_tray(&mut self, ctx: &egui::Context) {
        // 托盘里点了「退出」：标志位在点击时就立好了（窗口藏起来时不一定还有帧，
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
        if let Ok(port) = self.pg_port_buf.trim().parse::<u16>() {
            self.cfg.pg.port = port;
        }
        if let Ok(port) = self.mqtt_port_buf.trim().parse::<u16>() {
            self.cfg.mqtt.port = port;
        }
        // 数字项：解析失败时保留原值，并把缓冲区改回原值，避免显示和实际不一致
        if let Ok(secs) = self.scan_timeout_buf.trim().parse::<u32>() {
            self.cfg.scan_timeout_secs = secs.clamp(10, 120);
        }
        self.scan_timeout_buf = self.cfg.scan_timeout_secs.to_string();
        if let Ok(secs) = self.keepalive_buf.trim().parse::<u32>() {
            self.cfg.mqtt.keepalive_secs = secs.clamp(10, 300);
        }
        self.keepalive_buf = self.cfg.mqtt.keepalive_secs.to_string();
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
                UiEvent::SessionProgress { index, done, total } => {
                    self.session_progress = Some((index, done, total));
                }
                UiEvent::SessionInfo { index, name, records, ok, detail } => {
                    // 这一段已经有结论了，段内进度条该收起来了
                    if matches!(self.session_progress, Some((i, _, _)) if i == index) {
                        self.session_progress = None;
                    }
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
                    self.session_progress = None;
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
        self.session_progress = None;
        self.erase_percent = None;
        self.status = None;
        self.sessions.clear();
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
        self.pg_port_buf = self.cfg.pg.port.to_string();
        self.mqtt_port_buf = self.cfg.mqtt.port.to_string();
        self.scan_timeout_buf = self.cfg.scan_timeout_secs.to_string();
        self.keepalive_buf = self.cfg.mqtt.keepalive_secs.to_string();
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
            Some(RunMode::Status) => t!("取消查询", "Cancel query"),
            Some(RunMode::Erase) => t!("取消清除内存", "Cancel erase"),
            _ => t!("取消同步", "Cancel sync"),
        }
    }

    fn test_pg(&mut self) {
        if self.running || self.pg_test.busy {
            return;
        }
        self.commit_edits();
        let cfg = self.cfg.pg.clone();
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
                        cfg.port,
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

    fn test_mqtt(&mut self) {
        if self.running || self.mqtt_test.busy {
            return;
        }
        self.commit_edits();
        let cfg = self.cfg.mqtt.clone();
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
                            cfg.port,
                            cfg.topic
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

    fn show_sync_tab(&mut self, ui: &mut egui::Ui, p: &Palette) {
        // 预先取快照，避免闭包里同时借用 self
        let connected = !(self.device_name.is_empty() && self.device_address.is_empty());
        let device_line = if connected {
            format!("{} ({})", self.device_name, self.device_address)
        } else {
            t!("未连接", "not connected").to_string()
        };
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
            ui.set_width(ui.available_width());

            // 圆形主按钮 + 进度说明（阶段状态看左下角状态栏，这里不重复）
            ui.vertical_centered(|ui| {
                ui.add_space(2.0);
                let circle_label = if erasing {
                    t!("清理内存中", "Erasing")
                } else if self.running {
                    self.cancel_label()
                } else {
                    t!("开始同步", "Start sync")
                };
                if style::round_action_button(
                    ui,
                    p,
                    circle_label,
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

            // 查询状态 / 清除内存：描边样式，平铺居中
            // （不用 horizontal_centered：它会把整行撑满剩余高度）
            ui.horizontal(|ui| {
                let pad = ((ui.available_width() - 312.0) / 2.0).max(0.0);
                ui.add_space(pad);
                if style::outline_button(
                    ui,
                    p,
                    t!("查询状态", "Device status"),
                    p.text,
                    egui::vec2(150.0, 34.0),
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
                ui.add_space(12.0);
                if style::outline_button(
                    ui,
                    p,
                    t!("清除内存", "Erase memory"),
                    if self.running { p.muted } else { p.error },
                    egui::vec2(150.0, 34.0),
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
            });
            ui.add_space(6.0);
        });

        // ---------- 运行结果卡：汇总 + 设备状态 + 数据段（有内容才出现） ----------
        // 汇总 / 阶段耗时只针对完整同步；清除内存显示清理结果；查询状态只看设备状态
        let is_sync_task = self.current_mode == Some(RunMode::Sync);
        let is_erase_task = self.current_mode == Some(RunMode::Erase);
        let has_result = self.status.is_some()
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
                ui.set_width(ui.available_width());
                ui.horizontal(|ui| {
                    style::card_title(ui, p, t!("运行结果", "Result"));
                    ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                        ui.colored_label(p.muted, &device_line);
                    });
                });

                // ---------- 清除内存任务：只显示清理结果 ----------
                if is_erase_task && !self.last_summary.is_empty() {
                    ui.add_space(12.0);
                    style::status_banner(ui, p, result_color, &self.last_summary.clone());
                }

                // ---------- 本次任务汇总：下载 / 已同步 / 清理内存 / 总耗时 ----------
                let stats = self.run_stats;
                let synced = stats.map(|s| s.uploaded).unwrap_or(self.uploaded);
                let downloaded = stats.map(|s| s.downloaded).unwrap_or(parsed);
                let (erase_text, erase_color) = match stats.and_then(|s| s.erased) {
                    Some(true) => (t!("成功", "done"), p.success),
                    Some(false) => (t!("失败", "failed"), p.error),
                    None => (t!("未执行", "skipped"), p.muted),
                };
                let total_secs: f64 = self.phase_times.iter().map(|(_, d)| d.as_secs_f64()).sum();
                // 汇总统计只在完整同步时展示
                if is_sync_task
                    && (stats.is_some() || downloading || self.uploaded > 0 || erase_percent.is_some())
                {
                    ui.add_space(12.0);
                    let grid_w = (ui.available_width() - 3.0 * 16.0) / 4.0;
                    egui::Grid::new("run_summary_grid")
                        .num_columns(4)
                        .min_col_width(grid_w.max(90.0))
                        .spacing([16.0, 6.0])
                        .show(ui, |ui| {
                            for (label, value, color) in [
                                (
                                    t!("下载", "Downloaded"),
                                    tr!("{} 条", "{} rec", downloaded),
                                    p.text,
                                ),
                                (
                                    t!("已同步", "Uploaded"),
                                    tr!("{} 条", "{} rec", synced),
                                    p.success,
                                ),
                                (
                                    t!("清理内存", "Erased"),
                                    erase_text.to_string(),
                                    erase_color,
                                ),
                                (
                                    t!("总耗时", "Total time"),
                                    fmt_dur(Duration::from_secs_f64(total_secs)),
                                    p.text,
                                ),
                            ] {
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

                // 待重传告警
                if spool_pending > 0 {
                    ui.add_space(8.0);
                    ui.colored_label(
                        p.warn,
                        tr!(
                            "本地缓存待重传 {} 个",
                            "{} cached segment(s) waiting to be replayed",
                            spool_pending
                        ),
                    );
                }
                // 下载 / 清理的实时计数（已下载显示的是有效定位记录数）
                if downloading {
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
                // 段内上传进度：几万条一段要传好几秒，光看整段完成会以为卡住了
                if let Some((index, done, seg_total)) = self.session_progress {
                    ui.add_space(8.0);
                    ui.colored_label(
                        p.muted,
                        tr!(
                            "第 {} 段上传中 {} / {} 条",
                            "uploading segment {}: {} / {}",
                            index,
                            done,
                            seg_total
                        ),
                    );
                }
                if let Some(percent) = erase_percent {
                    ui.add_space(8.0);
                    ui.colored_label(p.muted, tr!("清理内存 {}%", "erasing {}%", percent));
                }
                // 任务结果文字（只针对完整同步）
                if is_sync_task && !self.last_summary.is_empty() {
                    ui.add_space(10.0);
                    style::status_banner(ui, p, result_color, &self.last_summary.clone());
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
    }

    fn show_settings_tab(&mut self, ui: &mut egui::Ui, p: &Palette) {
        // ---------- 上传方式 ----------
        style::card(p).show(ui, |ui| {
            ui.set_width(ui.available_width());
            style::card_title(ui, p, t!("上传方式", "Upload channel"));
            ui.add_space(10.0);
            // 单选：两个选项互斥，点哪个就只留哪个
            ui.horizontal(|ui| {
                for mode in [SinkMode::Database, SinkMode::Mqtt] {
                    if style::radio_button(ui, p, self.cfg.sink_mode == mode, mode.label()).clicked() {
                        self.cfg.sink_mode = mode;
                    }
                    ui.add_space(6.0);
                }
            });
        });

        ui.add_space(10.0);
        // ---------- PostgreSQL ----------
        style::card(p).show(ui, |ui| {
            ui.set_width(ui.available_width());
            style::card_title(ui, p, "PostgreSQL");
            ui.add_space(10.0);
            egui::Grid::new("pg_grid")
                .num_columns(2)
                .min_col_width(90.0)
                .spacing([12.0, 10.0])
                .show(ui, |ui| {
                    ui.colored_label(p.muted, t!("主机", "Host"));
                    ui.add(egui::TextEdit::singleline(&mut self.cfg.pg.host).desired_width(300.0));
                    ui.end_row();
                    ui.colored_label(p.muted, t!("端口", "Port"));
                    ui.add(egui::TextEdit::singleline(&mut self.pg_port_buf).desired_width(120.0));
                    ui.end_row();
                    ui.colored_label(p.muted, t!("数据库", "Database"));
                    ui.add(egui::TextEdit::singleline(&mut self.cfg.pg.database).desired_width(300.0));
                    ui.end_row();
                    ui.colored_label(p.muted, t!("用户", "User"));
                    ui.add(egui::TextEdit::singleline(&mut self.cfg.pg.user).desired_width(300.0));
                    ui.end_row();
                    ui.colored_label(p.muted, t!("密码", "Password"));
                    ui.add(
                        egui::TextEdit::singleline(&mut self.cfg.pg.password)
                            .password(true)
                            .desired_width(300.0),
                    );
                    ui.end_row();
                });
            ui.add_space(12.0);
            ui.horizontal(|ui| {
                let busy = self.pg_test.busy;
                if ui
                    .add_enabled(
                        !self.running && !busy,
                        style::secondary_button(p, t!("测试连接", "Test connection")),
                    )
                    .clicked()
                {
                    self.test_pg();
                }
                if busy {
                    ui.spinner();
                }
            });
            if !self.pg_test.result.is_empty() {
                style::status_banner(
                    ui,
                    p,
                    level_color(p, self.pg_test.level),
                    &self.pg_test.result,
                );
            }
        });

        ui.add_space(10.0);
        // ---------- MQTT ----------
        style::card(p).show(ui, |ui| {
            ui.set_width(ui.available_width());
            style::card_title(ui, p, "MQTT");
            ui.add_space(10.0);
            egui::Grid::new("mqtt_grid")
                .num_columns(2)
                .min_col_width(90.0)
                .spacing([12.0, 10.0])
                .show(ui, |ui| {
                    ui.colored_label(p.muted, "Broker");
                    ui.add(egui::TextEdit::singleline(&mut self.cfg.mqtt.broker).desired_width(300.0));
                    ui.end_row();
                    ui.colored_label(p.muted, t!("端口", "Port"));
                    ui.add(egui::TextEdit::singleline(&mut self.mqtt_port_buf).desired_width(120.0));
                    ui.end_row();
                    ui.colored_label(p.muted, t!("用户名", "Username"));
                    ui.add(egui::TextEdit::singleline(&mut self.cfg.mqtt.user).desired_width(300.0));
                    ui.end_row();
                    ui.colored_label(p.muted, t!("密码", "Password"));
                    ui.add(
                        egui::TextEdit::singleline(&mut self.cfg.mqtt.password)
                            .password(true)
                            .desired_width(300.0),
                    );
                    ui.end_row();
                    ui.colored_label(p.muted, t!("主题", "Topic"));
                    ui.add(egui::TextEdit::singleline(&mut self.cfg.mqtt.topic).desired_width(300.0));
                    ui.end_row();
                    ui.colored_label(p.muted, "Client ID");
                    ui.add(egui::TextEdit::singleline(&mut self.cfg.mqtt.client_id).desired_width(300.0));
                    ui.end_row();
                    ui.colored_label(p.muted, t!("保活间隔", "Keep-alive"));
                    ui.horizontal(|ui| {
                        ui.add(
                            egui::TextEdit::singleline(&mut self.keepalive_buf)
                                .desired_width(80.0)
                                .horizontal_align(egui::Align::Center),
                        );
                        ui.colored_label(p.muted, t!("秒", "s"));
                    });
                    ui.end_row();
                });
            ui.add_space(12.0);
            ui.horizontal(|ui| {
                let busy = self.mqtt_test.busy;
                if ui
                    .add_enabled(
                        !self.running && !busy,
                        style::secondary_button(p, t!("测试连接", "Test connection")),
                    )
                    .clicked()
                {
                    self.test_mqtt();
                }
                if busy {
                    ui.spinner();
                }
            });
            if !self.mqtt_test.result.is_empty() {
                style::status_banner(
                    ui,
                    p,
                    level_color(p, self.mqtt_test.level),
                    &self.mqtt_test.result,
                );
            }
        });

        ui.add_space(10.0);
        // ---------- 行为 ----------
        style::card(p).show(ui, |ui| {
            ui.set_width(ui.available_width());
            style::card_title(ui, p, t!("行为", "Behaviour"));
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
            // 关闭窗口时的行为：二选一（和“上传方式”同一套单选控件）
            ui.colored_label(p.muted, t!("点关闭按钮时", "When the window is closed"));
            ui.add_space(6.0);
            ui.horizontal(|ui| {
                for action in [CloseAction::Quit, CloseAction::Tray] {
                    if style::radio_button(ui, p, self.cfg.close_action == action, action.label())
                        .clicked()
                        && self.cfg.close_action != action
                    {
                        self.cfg.close_action = action;
                        // 立即生效：切到“直接退出”就把托盘图标摘掉，反之马上挂上
                        self.sync_tray();
                    }
                    ui.add_space(6.0);
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
            ui.set_width(ui.available_width());
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
            ui.set_width(ui.available_width());
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
            ui.set_width(ui.available_width());
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
        // 保存 / 放弃靠右下角（右侧留点空隙，不贴边）
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
        });
        if let Some((text, level)) = &self.action_result {
            style::status_banner(ui, p, level_color(p, *level), text);
        }
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
        RunMode::Status => t!("状态查询", "Status query"),
        RunMode::Erase => t!("清除内存", "Erase memory"),
    }
}

impl eframe::App for App {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        if self.ctx.is_none() {
            self.ctx = Some(ctx.clone());
        }
        // 托盘图标要在事件循环起来之后才能建，所以放在这里（每帧检查一次，没需求就直接返回）
        self.sync_tray();
        self.handle_close_and_tray(ctx);
        if self.cfg.theme != self.applied_theme {
            style::apply(ctx, self.cfg.theme);
            self.applied_theme = self.cfg.theme;
        }
        self.drain_events(ctx);

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
                                .clicked() {
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

        // 关于窗口：无标题栏、无按钮，点任意位置或按 Esc 关闭
        if self.about_open {
            let dismiss = ctx.input(|i| i.pointer.any_pressed() || i.key_pressed(egui::Key::Escape));
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
            egui::Window::new(t!("关于", "About"))
                .title_bar(false)
                .collapsible(false)
                .resizable(false)
                .anchor(egui::Align2::CENTER_CENTER, [0.0, 0.0])
                .frame(
                    egui::Frame::NONE
                        .fill(p.card)
                        .stroke(egui::Stroke::new(1.0_f32, p.card_stroke))
                        .corner_radius(egui::CornerRadius::same(12))
                        .inner_margin(egui::Margin::symmetric(22, 20)),
                )
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
                        ui.add_space(2.0);
                        ui.colored_label(
                            p.muted,
                            tr!("版本 {}", "version {}", env!("CARGO_PKG_VERSION")),
                        );
                    });
                    ui.add_space(14.0);
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
            if dismiss {
                self.about_open = false;
            }
        }

        // 关闭设置页前的“有未保存更改”确认弹窗
        if self.confirm_close {
            let mut keep_open = true;
            egui::Window::new(t!("未保存的更改", "Unsaved changes"))
                .collapsible(false)
                .resizable(false)
                .anchor(egui::Align2::CENTER_CENTER, [0.0, 0.0])
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
            if !keep_open {
                self.confirm_close = false;
            }
        }
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
