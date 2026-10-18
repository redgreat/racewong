use eframe::egui;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, Sender};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::runtime::Runtime;

use crate::config::{self, Config, SinkMode, Theme};
use crate::protocol::DeviceStatus;
use crate::style::{self, Palette};
use crate::worker::{LogLevel, RunMode, RunStats, UiEvent};

#[derive(Clone, Copy, PartialEq, Eq)]
enum Tab {
    Sync,
    Settings,
}

/// 连接测试的状态（进行中 / 结果文字）。
#[derive(Default)]
struct TestState {
    busy: bool,
    result: String,
}

/// 一个轨迹数据段的上传结果行。
struct SessionRow {
    index: u32,
    name: String,
    records: usize,
    ok: bool,
    detail: String,
}

pub struct App {
    cfg: Config,
    runtime: Arc<Runtime>,
    tx: Sender<UiEvent>,
    rx: Receiver<UiEvent>,
    probe_tx: Sender<crate::sink::mqtt::MqttSink>,
    probe_rx: Receiver<crate::sink::mqtt::MqttSink>,
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
    phase: String,
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
    erase_percent: Option<u8>,
    spool_pending: usize,
    running: bool,
    /// 当前任务类型（取消按钮的文案随它变化）。
    current_mode: Option<RunMode>,
    /// 本次任务的量化结果（下载 / 上传条数、清理是否成功）。
    run_stats: Option<RunStats>,
    /// 当前阶段的开始时刻，用于统计各阶段耗时。
    phase_start: Option<Instant>,
    /// 各阶段累计耗时（阶段名 -> 秒）。
    phase_times: Vec<(String, Duration)>,
    last_summary: String,
    mqtt_probe: Option<crate::sink::mqtt::MqttSink>,
    cancel_flag: Arc<AtomicBool>,
    pg_test: TestState,
    mqtt_test: TestState,
    action_result: String,
    pg_port_buf: String,
    mqtt_port_buf: String,
    scan_timeout_buf: String,
    keepalive_buf: String,
}

impl App {
    pub fn new(runtime: Arc<Runtime>, cfg: Config, tx: Sender<UiEvent>, rx: Receiver<UiEvent>) -> Self {
        let pg_port_buf = cfg.pg.port.to_string();
        let mqtt_port_buf = cfg.mqtt.port.to_string();
        let (probe_tx, probe_rx) = std::sync::mpsc::channel::<crate::sink::mqtt::MqttSink>();
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
            phase: "就绪".to_string(),
            device_name: String::new(),
            device_address: String::new(),
            status: None,
            received: 0,
            total: 0,
            downloaded_parsed: 0,
            uploaded: 0,
            upload_total: 0,
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
            action_result: String::new(),
            pg_port_buf,
            mqtt_port_buf,
            scan_timeout_buf,
            keepalive_buf,
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
                self.phase_times.push((self.phase.clone(), elapsed));
            }
        }
    }

    /// 是否处于“清理设备内存”阶段：该阶段不允许中断。
    fn erasing(&self) -> bool {
        self.running && (self.erase_percent.is_some() || self.phase == "清理设备内存")
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
                UiEvent::Notice(level, msg) => {
                    // 连接测试的结果只回写到设置页对应的卡片里，不往首页冒
                    if msg.starts_with("PostgreSQL") {
                        self.pg_test.busy = false;
                        self.pg_test.result = msg.clone();
                    } else if msg.starts_with("MQTT") {
                        self.mqtt_test.busy = false;
                        self.mqtt_test.result = msg.clone();
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
                        "已取消".to_string()
                    } else if ok {
                        "完成".to_string()
                    } else {
                        "失败".to_string()
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
        if self.running {
            ctx.request_repaint_after(Duration::from_millis(200));
        }
        if self.pg_test.busy || self.mqtt_test.busy {
            ctx.request_repaint_after(Duration::from_millis(120));
        }
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
        self.phase = "启动中".to_string();
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
        self.push_log(LogLevel::Info, format!("任务开始: {}", mode_text(mode)));
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
            self.push_log(LogLevel::Warn, "正在取消任务...".to_string());
        }
    }

    fn save_config(&mut self) {
        self.commit_edits();
        match config::save(&self.cfg) {
            Ok(()) => {
                self.action_result = "配置已保存".to_string();
                self.settings_snapshot = self.cfg.clone();
                self.push_log(LogLevel::Success, "配置已保存".to_string());
                // 保存成功直接回首页
                self.tab = Tab::Sync;
            }
            Err(e) => {
                self.action_result = format!("保存失败: {e}");
            }
        }
    }

    fn discard_edits(&mut self) {
        self.cfg = config::load();
        self.pg_port_buf = self.cfg.pg.port.to_string();
        self.mqtt_port_buf = self.cfg.mqtt.port.to_string();
        self.scan_timeout_buf = self.cfg.scan_timeout_secs.to_string();
        self.keepalive_buf = self.cfg.mqtt.keepalive_secs.to_string();
        self.action_result = "已重新载入配置".to_string();
        self.settings_snapshot = self.cfg.clone();
    }

    /// 取消按钮文案：跟着当前任务类型走。
    fn cancel_label(&self) -> &'static str {
        match self.current_mode {
            Some(RunMode::Status) => "取消查询",
            Some(RunMode::Erase) => "取消清除内存",
            _ => "取消同步",
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
        self.pg_test.result = "正在测试 PostgreSQL 连接...".to_string();
        runtime.spawn(async move {
            // 整体加超时：主机不可达时 TCP 可能要挂很久，不能一直转圈。
            let probe = tokio::time::timeout(Duration::from_secs(15), async {
                let mut pg = tokio_postgres::Config::new();
                pg.host(&cfg.host)
                    .port(cfg.port)
                    .dbname(&cfg.database)
                    .user(&cfg.user)
                    .password(&cfg.password);
                let (client, connection) = pg.connect(tokio_postgres::NoTls).await?;
                tokio::spawn(async move {
                    let _ = connection.await;
                });
                client.query_one("select 1", &[]).await
            })
            .await;
            match probe {
                Err(_) => {
                    let _ = tx.send(UiEvent::Notice(
                        LogLevel::Error,
                        "PostgreSQL 连接超时（15 秒无响应），请检查主机、端口或网络".to_string(),
                    ));
                }
                Ok(Err(e)) => {
                    let detail = crate::sink::pg::describe_db_error(&e);
                    let msg = if e.as_db_error().is_some() {
                        // 服务端有应答，只是查询没过
                        format!("PostgreSQL 已连接但查询失败: {detail}")
                    } else {
                        let mut m = format!("PostgreSQL 连接失败: {detail}");
                        if m.contains("password authentication failed") {
                            m.push_str(" → 密码不对，请检查密码输入");
                        } else if m.contains("Connection refused") {
                            m.push_str(" → 端口未监听，请检查 host/port");
                        } else if m.contains("database") && m.contains("does not exist") {
                            m.push_str(" → 数据库不存在，请检查 database 名称");
                        }
                        m
                    };
                    let _ = tx.send(UiEvent::Notice(LogLevel::Error, msg));
                }
                Ok(Ok(_)) => {
                    let _ = tx.send(UiEvent::Notice(
                        LogLevel::Success,
                        format!("PostgreSQL 连接成功: {}:{}/{}", cfg.host, cfg.port, cfg.database),
                    ));
                }
            }
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
        self.mqtt_test.result = "正在测试 MQTT 连接...".to_string();
        runtime.spawn(async move {
            match crate::sink::mqtt::MqttSink::new(cfg.clone(), None).await {
                Ok(sink) => {
                    let _ = tx.send(UiEvent::Notice(
                        LogLevel::Success,
                        format!("MQTT 连接成功: {}:{} (主题 {})", cfg.broker, cfg.port, cfg.topic),
                    ));
                    // 连接留着不要立刻断，否则会打一条误导性的断开错误
                    let _ = probe_tx.send(sink);
                }
                Err(e) => {
                    let _ = tx.send(UiEvent::Notice(
                        LogLevel::Error,
                        format!("MQTT 连接失败: {e}"),
                    ));
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
            "未连接".to_string()
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
            } => Some(format!("清理内存 {}%", (f * 100.0).round() as u32)),
            style::RingMode::Single { progress: None, .. } => None,
            style::RingMode::Dual {
                download: Some(f), ..
            } => {
                if self.upload_total > 0 {
                    Some(format!(
                        "下载 {}% · 上传 {}/{} 条",
                        (f * 100.0).round() as u32,
                        self.uploaded,
                        self.upload_total
                    ))
                } else {
                    Some(format!("下载 {}%", (f * 100.0).round() as u32))
                }
            }
            style::RingMode::Dual { download: None, .. } if self.upload_total > 0 => Some(format!(
                "上传 {}/{} 条",
                self.uploaded, self.upload_total
            )),
            _ => None,
        };

        // ---------- 主卡：大圆钮 + 两个描边按键（紧凑，一屏放得下） ----------
        style::card(p).show(ui, |ui| {
            ui.set_width(ui.available_width());

            // 圆形主按钮 + 进度说明（阶段状态看左下角状态栏，这里不重复）
            ui.vertical_centered(|ui| {
                ui.add_space(6.0);
                let circle_label = if erasing {
                    "清理内存中"
                } else if self.running {
                    self.cancel_label()
                } else {
                    "开始同步"
                };
                if style::round_action_button(
                    ui,
                    p,
                    circle_label,
                    self.running,
                    !erasing,
                    ring,
                    168.0,
                )
                .on_hover_text(if erasing {
                    "正在清理设备内存，此阶段不可中断"
                } else if self.running {
                    "点击取消当前任务"
                } else {
                    "下载数据并按设置上传，成功后自动清理设备内存"
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
            ui.add_space(14.0);

            // 查询状态 / 清除内存：描边样式，平铺居中
            // （不用 horizontal_centered：它会把整行撑满剩余高度）
            ui.horizontal(|ui| {
                let pad = ((ui.available_width() - 312.0) / 2.0).max(0.0);
                ui.add_space(pad);
                if style::outline_button(
                    ui,
                    p,
                    "查询状态",
                    p.text,
                    egui::vec2(150.0, 34.0),
                    !self.running,
                )
                .on_hover_text("读取设备录制状态与存储用量")
                .clicked()
                {
                    self.start(RunMode::Status);
                }
                ui.add_space(12.0);
                if style::outline_button(
                    ui,
                    p,
                    "清除内存",
                    if self.running { p.muted } else { p.error },
                    egui::vec2(150.0, 34.0),
                    !self.running,
                )
                .on_hover_text("擦除设备存储的所有数据，不可恢复")
                .clicked()
                {
                    self.start(RunMode::Erase);
                }
            });
            ui.add_space(10.0);
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
        let result_color = match self.phase.as_str() {
            "完成" => p.success,
            "已取消" => p.warn,
            _ => p.error,
        };
        if has_result {
            ui.add_space(12.0);
            style::card(p).show(ui, |ui| {
                ui.set_width(ui.available_width());
                ui.horizontal(|ui| {
                    style::card_title(ui, p, "运行结果");
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
                    Some(true) => ("成功", p.success),
                    Some(false) => ("失败", p.error),
                    None => ("未执行", p.muted),
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
                        .spacing([16.0, 10.0])
                        .show(ui, |ui| {
                            for (label, value, color) in [
                                ("下载", format!("{downloaded} 条"), p.text),
                                ("已同步", format!("{synced} 条"), p.success),
                                ("清理内存", erase_text.to_string(), erase_color),
                                (
                                    "总耗时",
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
                    ui.colored_label(p.muted, "各阶段耗时");
                    ui.add_space(4.0);
                    ui.horizontal_wrapped(|ui| {
                        for (i, (name, d)) in self.phase_times.iter().enumerate() {
                            if i > 0 {
                                ui.add_space(6.0);
                                ui.colored_label(p.muted, "·");
                                ui.add_space(6.0);
                            }
                            ui.colored_label(p.muted, name.clone());
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
                    let rec_text = if s.recording { "是" } else { "否" };
                    let mem_text = format!("{}%", s.memory_level);
                    let stored_text = format!("{}/{} 条", s.stored_messages, s.max_messages);
                    let lock_text = if s.security_enabled { "已启用" } else { "未启用" };
                    egui::Grid::new("status_grid")
                        .num_columns(4)
                        .min_col_width(grid_w.max(90.0))
                        .spacing([16.0, 10.0])
                        .show(ui, |ui| {
                            for (label, value) in [
                                ("录制", rec_text),
                                ("内存", mem_text.as_str()),
                                ("已存", stored_text.as_str()),
                                ("安全锁", lock_text),
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
                    ui.colored_label(p.warn, format!("本地缓存待重传 {spool_pending} 个"));
                }
                // 下载 / 清理的实时计数（已下载显示的是有效定位记录数）
                if downloading {
                    ui.add_space(8.0);
                    let mut text = if total > 0 {
                        format!("已下载 {parsed} / {total} 条")
                    } else {
                        format!("已下载 {parsed} 条")
                    };
                    if received > parsed {
                        text.push_str(&format!("（跳过无定位 {} 条）", received - parsed));
                    }
                    ui.colored_label(p.muted, text);
                }
                if let Some(percent) = erase_percent {
                    ui.add_space(8.0);
                    ui.colored_label(p.muted, format!("设备内存清理进度 {percent}%"));
                }
                // 任务结果文字（只针对完整同步）
                if is_sync_task && !self.last_summary.is_empty() {
                    ui.add_space(10.0);
                    style::status_banner(ui, p, result_color, &self.last_summary.clone());
                }

                // 本次数据段（最新的在最上面）
                if !self.sessions.is_empty() {
                    ui.add_space(14.0);
                    ui.horizontal(|ui| {
                        ui.colored_label(p.muted, "本次数据段");
                    });
                    ui.add_space(4.0);
                    egui::ScrollArea::vertical()
                        .id_salt("sessions")
                        .auto_shrink([false, false])
                        .max_height(150.0)
                        .show(ui, |ui| {
                            for s in self.sessions.iter().rev() {
                                ui.horizontal(|ui| {
                                    style::mini_badge(ui, p, &format!("#{}", s.index));
                                    ui.add_space(8.0);
                                    ui.label(egui::RichText::new(&s.name).strong());
                                    ui.with_layout(
                                        egui::Layout::right_to_left(egui::Align::Center),
                                        |ui| {
                                            if s.ok {
                                                ui.colored_label(
                                                    p.success,
                                                    egui::RichText::new("✓ 已上传").strong(),
                                                );
                                            } else {
                                                ui.colored_label(
                                                    p.error,
                                                    egui::RichText::new(
                                                        format!("✗ {}", s.detail),
                                                    )
                                                    .strong(),
                                                );
                                            }
                                            ui.add_space(10.0);
                                            ui.colored_label(
                                                p.muted,
                                                format!("{} 条", s.records),
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
            style::card_title(ui, p, "上传方式");
            ui.add_space(10.0);
            ui.horizontal(|ui| {
                for mode in [SinkMode::Database, SinkMode::Mqtt, SinkMode::Both] {
                    if style::segment_button(ui, p, self.cfg.sink_mode == mode, mode.label())
                        .clicked()
                    {
                        self.cfg.sink_mode = mode;
                    }
                    ui.add_space(4.0);
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
                    ui.colored_label(p.muted, "主机");
                    ui.add(egui::TextEdit::singleline(&mut self.cfg.pg.host).desired_width(300.0));
                    ui.end_row();
                    ui.colored_label(p.muted, "端口");
                    ui.add(egui::TextEdit::singleline(&mut self.pg_port_buf).desired_width(120.0));
                    ui.end_row();
                    ui.colored_label(p.muted, "数据库");
                    ui.add(egui::TextEdit::singleline(&mut self.cfg.pg.database).desired_width(300.0));
                    ui.end_row();
                    ui.colored_label(p.muted, "用户");
                    ui.add(egui::TextEdit::singleline(&mut self.cfg.pg.user).desired_width(300.0));
                    ui.end_row();
                    ui.colored_label(p.muted, "密码");
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
                    .add_enabled(!self.running && !busy, style::secondary_button(p, "测试连接"))
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
                    test_result_color(p, &self.pg_test.result),
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
                    ui.colored_label(p.muted, "端口");
                    ui.add(egui::TextEdit::singleline(&mut self.mqtt_port_buf).desired_width(120.0));
                    ui.end_row();
                    ui.colored_label(p.muted, "用户名");
                    ui.add(egui::TextEdit::singleline(&mut self.cfg.mqtt.user).desired_width(300.0));
                    ui.end_row();
                    ui.colored_label(p.muted, "密码");
                    ui.add(
                        egui::TextEdit::singleline(&mut self.cfg.mqtt.password)
                            .password(true)
                            .desired_width(300.0),
                    );
                    ui.end_row();
                    ui.colored_label(p.muted, "主题");
                    ui.add(egui::TextEdit::singleline(&mut self.cfg.mqtt.topic).desired_width(300.0));
                    ui.end_row();
                    ui.colored_label(p.muted, "Client ID");
                    ui.add(egui::TextEdit::singleline(&mut self.cfg.mqtt.client_id).desired_width(300.0));
                    ui.end_row();
                    ui.colored_label(p.muted, "保活间隔");
                    ui.horizontal(|ui| {
                        ui.add(
                            egui::TextEdit::singleline(&mut self.keepalive_buf)
                                .desired_width(80.0)
                                .horizontal_align(egui::Align::Center),
                        );
                        ui.colored_label(p.muted, "秒");
                    });
                    ui.end_row();
                });
            ui.add_space(12.0);
            ui.horizontal(|ui| {
                let busy = self.mqtt_test.busy;
                if ui
                    .add_enabled(!self.running && !busy, style::secondary_button(p, "测试连接"))
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
                    test_result_color(p, &self.mqtt_test.result),
                    &self.mqtt_test.result,
                );
            }
        });

        ui.add_space(10.0);
        // ---------- 行为 ----------
        style::card(p).show(ui, |ui| {
            ui.set_width(ui.available_width());
            style::card_title(ui, p, "行为");
            ui.add_space(12.0);
            style::toggle_row(
                ui,
                p,
                "上传成功后自动清除设备内存",
                &mut self.cfg.auto_erase,
            );
            ui.add_space(12.0);
            style::toggle_row(
                ui,
                p,
                "上传失败时写入本地缓存，下次启动重传",
                &mut self.cfg.spool_on_failure,
            );
            ui.add_space(14.0);
            ui.horizontal(|ui| {
                ui.colored_label(p.muted, "设备扫描超时");
                ui.add_space(10.0);
                ui.add(
                    egui::TextEdit::singleline(&mut self.scan_timeout_buf)
                        .desired_width(80.0)
                        .horizontal_align(egui::Align::Center),
                );
                ui.colored_label(p.muted, "秒");
            });
            ui.add_space(14.0);
            ui.horizontal(|ui| {
                ui.colored_label(p.muted, "设备名过滤");
                ui.add_space(10.0);
                ui.add(
                    egui::TextEdit::singleline(&mut self.cfg.device_name_filter).desired_width(200.0),
                );
            });
        });

        ui.add_space(10.0);
        // ---------- 外观 ----------
        style::card(p).show(ui, |ui| {
            ui.set_width(ui.available_width());
            style::card_title(ui, p, "外观");
            ui.add_space(12.0);
            ui.horizontal(|ui| {
                ui.colored_label(p.muted, "主题");
                ui.add_space(16.0);
                let dark = self.cfg.theme == Theme::Dark;
                if style::theme_card(ui, p, dark, "深色", true).clicked() && !dark {
                    self.set_theme(ui.ctx(), Theme::Dark);
                }
                ui.add_space(14.0);
                if style::theme_card(ui, p, !dark, "浅色", false).clicked() && dark {
                    self.set_theme(ui.ctx(), Theme::Light);
                }
            });
        });

        ui.add_space(16.0);
        // 保存 / 放弃靠右下角（右侧留点空隙，不贴边）
        ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
            ui.add_space(56.0);
            if ui
                .add_enabled(!self.running, style::secondary_button(p, "放弃修改"))
                .clicked()
            {
                self.discard_edits();
            }
            ui.add_space(8.0);
            if ui
                .add_enabled(!self.running, style::primary_button(p, "保存设置"))
                .clicked()
            {
                self.save_config();
            }
        });
        if !self.action_result.is_empty() {
            style::status_banner(
                ui,
                p,
                test_result_color(p, &self.action_result),
                &self.action_result,
            );
        }
    }

    /// 日志抽屉内容（底部可折叠面板，不再是独立标签页）。
    fn show_logs_panel(&mut self, ui: &mut egui::Ui, p: &Palette) {
        ui.horizontal(|ui| {
            ui.label(egui::RichText::new("日志").strong().size(14.0).color(p.text));
            ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                if ui.add(style::small_button(p, "收起")).clicked() {
                    self.show_logs = false;
                }
                ui.add_space(6.0);
                if ui
                    .add_enabled(!self.running, style::small_button(p, "清空"))
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
                    style::hint(ui, p, "暂无日志");
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

/// 测试 / 操作结果文字的颜色。
fn test_result_color(p: &Palette, msg: &str) -> egui::Color32 {
    if msg.starts_with("正在") {
        p.info
    } else if msg.contains("失败") || msg.contains("超时") || msg.contains("密码不对") {
        p.error
    } else if msg.contains("成功") || msg.contains("已保存") || msg.contains("已重新载入") {
        p.success
    } else {
        p.muted
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
        format!("{secs:.1} 秒")
    } else {
        let m = (secs / 60.0) as u64;
        let s = d.as_secs() % 60;
        format!("{m} 分 {s:02} 秒")
    }
}

fn mode_text(mode: RunMode) -> &'static str {
    match mode {
        RunMode::Sync => "完整同步",
        RunMode::Status => "状态查询",
        RunMode::Erase => "清除内存",
    }
}

impl eframe::App for App {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        if self.ctx.is_none() {
            self.ctx = Some(ctx.clone());
        }
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
                            .on_hover_text("返回同步页")
                            .clicked()
                        {
                            self.request_close_settings();
                        }
                    }
                    ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                        if style::info_button(ui, &p).on_hover_text("关于").clicked() {
                            self.about_open = true;
                        }
                        if self.tab != Tab::Settings {
                            if style::gear_button(ui, &p).on_hover_text("设置").clicked() {
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
                    (p.info, format!("运行中 · {}", self.phase), p.text)
                } else if matches!(self.phase.as_str(), "完成" | "失败" | "已取消") {
                    (p.muted, self.phase.clone(), p.muted)
                } else {
                    (p.muted, "空闲".to_string(), p.muted)
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
                                "日志",
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
                            egui::RichText::new(format!("待重传 {}", self.spool_pending)).size(12.0),
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
            egui::Window::new("关于")
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
                            egui::RichText::new("RaceBox 同步")
                                .strong()
                                .size(18.0)
                                .color(p.text),
                        );
                        ui.add_space(2.0);
                        ui.colored_label(
                            p.muted,
                            format!("版本 {}", env!("CARGO_PKG_VERSION")),
                        );
                    });
                    ui.add_space(14.0);
                    about_row(ui, &p, "作者", "wangcw");
                    about_row(ui, &p, "邮箱", "rubygreat@msn.com");
                    about_row(ui, &p, "协议", "ublox BLE · PostgreSQL / MQTT (RBX2)");
                    ui.add_space(6.0);
                });
            if dismiss {
                self.about_open = false;
            }
        }

        // 关闭设置页前的“有未保存更改”确认弹窗
        if self.confirm_close {
            let mut keep_open = true;
            egui::Window::new("未保存的更改")
                .collapsible(false)
                .resizable(false)
                .anchor(egui::Align2::CENTER_CENTER, [0.0, 0.0])
                .show(ctx, |ui| {
                    ui.set_width(320.0);
                    ui.label("设置页面有未保存的更改，是否保存？");
                    ui.add_space(14.0);
                    ui.horizontal(|ui| {
                        if ui
                            .add(style::primary_button(&p, "保存并退出"))
                            .clicked()
                        {
                            self.save_config();
                            self.confirm_close = false;
                            keep_open = false;
                            self.tab = Tab::Sync;
                        }
                        if ui
                            .add(style::secondary_button(&p, "不保存退出"))
                            .clicked()
                        {
                            self.discard_edits();
                            self.confirm_close = false;
                            keep_open = false;
                            self.tab = Tab::Sync;
                        }
                        if ui.add(style::secondary_button(&p, "取消")).clicked() {
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
