use eframe::egui;
use std::sync::mpsc::{Receiver, Sender};
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Runtime;

use crate::config::{self, Config, SinkMode, Theme};
use crate::protocol::DeviceStatus;
use crate::style::{self, Palette};
use crate::worker::{LogLevel, RunMode, UiEvent};

#[derive(Clone, Copy, PartialEq, Eq)]
enum Tab {
    Sync,
    Settings,
    Logs,
}

/// 连接测试的状态（进行中 / 结果文字）。
#[derive(Default)]
struct TestState {
    busy: bool,
    result: String,
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
    logs: Vec<(LogLevel, String)>,
    sessions: Vec<String>,
    phase: String,
    device_name: String,
    device_address: String,
    status: Option<DeviceStatus>,
    received: usize,
    total: usize,
    erase_percent: Option<u8>,
    spool_pending: usize,
    running: bool,
    last_summary: String,
    notice: Option<(LogLevel, String)>,
    mqtt_probe: Option<crate::sink::mqtt::MqttSink>,
    pg_test: TestState,
    mqtt_test: TestState,
    action_result: String,
    pg_port_buf: String,
    mqtt_port_buf: String,
}

impl App {
    pub fn new(runtime: Arc<Runtime>, cfg: Config, tx: Sender<UiEvent>, rx: Receiver<UiEvent>) -> Self {
        let pg_port_buf = cfg.pg.port.to_string();
        let mqtt_port_buf = cfg.mqtt.port.to_string();
        let (probe_tx, probe_rx) = std::sync::mpsc::channel::<crate::sink::mqtt::MqttSink>();
        Self {
            cfg,
            runtime,
            tx,
            rx,
            probe_tx,
            probe_rx,
            ctx: None,
            applied_theme: Theme::Dark,
            tab: Tab::Sync,
            logs: Vec::new(),
            sessions: Vec::new(),
            phase: "就绪".to_string(),
            device_name: String::new(),
            device_address: String::new(),
            status: None,
            received: 0,
            total: 0,
            erase_percent: None,
            spool_pending: 0,
            running: false,
            last_summary: String::new(),
            notice: None,
            mqtt_probe: None,
            pg_test: TestState::default(),
            mqtt_test: TestState::default(),
            action_result: String::new(),
            pg_port_buf,
            mqtt_port_buf,
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
    }

    fn commit_edits(&mut self) {
        if let Ok(port) = self.pg_port_buf.trim().parse::<u16>() {
            self.cfg.pg.port = port;
        }
        if let Ok(port) = self.mqtt_port_buf.trim().parse::<u16>() {
            self.cfg.mqtt.port = port;
        }
    }

    fn push_log(&mut self, level: LogLevel, message: String) {
        crate::write_ui_log(level_name(level), &message);
        self.logs.push((level, message));
        if self.logs.len() > 2000 {
            self.logs.drain(..500);
        }
    }

    fn drain_events(&mut self, ctx: &egui::Context) {
        while let Ok(event) = self.rx.try_recv() {
            match event {
                UiEvent::Phase(p) => self.phase = p,
                UiEvent::Log(level, msg) => self.push_log(level, msg),
                UiEvent::Notice(level, msg) => {
                    // 连接测试的结果要回写到设置页对应的卡片里，
                    // 否则“正在测试 …”会一直挂着，像卡死一样。
                    if msg.starts_with("PostgreSQL") {
                        self.pg_test.busy = false;
                        self.pg_test.result = msg.clone();
                    } else if msg.starts_with("MQTT") {
                        self.mqtt_test.busy = false;
                        self.mqtt_test.result = msg.clone();
                    }
                    self.notice = Some((level, msg.clone()));
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
                }
                UiEvent::Status(status) => self.status = Some(status),
                UiEvent::DownloadProgress { received, total } => {
                    self.received = received;
                    self.total = total;
                }
                UiEvent::SessionInfo { index, name, records, ok, detail } => {
                    self.sessions.push(format!(
                        "#{index} {name}  {records} 条  {}",
                        if ok { "✓".to_string() } else { format!("✗ {detail}") }
                    ));
                }
                UiEvent::EraseProgress(p) => self.erase_percent = Some(p),
                UiEvent::SpoolPending(n) => self.spool_pending = n,
                UiEvent::Finished { ok, summary } => {
                    self.running = false;
                    self.phase = if ok { "完成".to_string() } else { "失败".to_string() };
                    self.last_summary = summary.clone();
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
        self.running = true;
        self.phase = "启动中".to_string();
        self.received = 0;
        self.total = 0;
        self.erase_percent = None;
        self.status = None;
        self.sessions.clear();
        self.push_log(LogLevel::Info, format!("任务开始: {}", mode_text(mode)));
        let cfg = Arc::new(self.cfg.clone());
        let tx = self.tx.clone();
        let runtime = self.runtime.clone();
        runtime.spawn(async move {
            crate::worker::run_sync(cfg, mode, tx).await;
        });
        if let Some(ctx) = self.ctx.clone() {
            ctx.request_repaint();
        }
    }

    fn save_config(&mut self) {
        self.commit_edits();
        match config::save(&self.cfg) {
            Ok(()) => {
                self.action_result = format!("配置已保存: {}", config::config_path().display());
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
        self.action_result = "已重新载入配置".to_string();
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
            match crate::sink::mqtt::MqttSink::new(cfg.clone()).await {
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
        ui.horizontal(|ui| {
            if ui
                .add_enabled(!self.running, style::secondary_button(p, "查询状态"))
                .clicked()
            {
                self.start(RunMode::Status);
            }
            if ui
                .add_enabled(!self.running, style::secondary_button(p, "清除内存"))
                .clicked()
            {
                self.start(RunMode::Erase);
            }
            if ui
                .add_enabled(
                    !self.running,
                    style::primary_button(p, "开始同步"),
                )
                .on_hover_text("下载数据并按设置上传，成功后自动清理设备内存")
                .clicked()
            {
                self.start(RunMode::Sync);
            }
        });
        ui.add_space(10.0);

        style::card(p).show(ui, |ui| {
            ui.set_width(ui.available_width());
            ui.horizontal(|ui| {
                style::card_title(ui, p, "设备");
                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    let color = if self.running { p.info } else { p.muted };
                    ui.colored_label(color, format!("阶段: {}", self.phase));
                });
            });
            ui.add_space(8.0);

            ui.horizontal(|ui| {
                ui.label(egui::RichText::new("设备").color(p.muted));
                ui.add_space(4.0);
                if self.device_name.is_empty() && self.device_address.is_empty() {
                    ui.colored_label(p.muted, "未连接");
                } else {
                    ui.label(format!("{} ({})", self.device_name, self.device_address));
                }
            });
            ui.horizontal(|ui| {
                ui.label(egui::RichText::new("状态").color(p.muted));
                ui.add_space(4.0);
                match &self.status {
                    Some(s) => {
                        ui.label(format!(
                            "录制={}  内存={}%  已存 {}/{} 条  安全锁={}",
                            if s.recording { "是" } else { "否" },
                            s.memory_level,
                            s.stored_messages,
                            s.max_messages,
                            if s.security_enabled { "已启用" } else { "未启用" }
                        ));
                    }
                    None => {
                        ui.colored_label(p.muted, "未知");
                    }
                }
            });
            ui.horizontal(|ui| {
                ui.label(egui::RichText::new("下载进度").color(p.muted));
                ui.add_space(4.0);
                if self.total > 0 {
                    let frac = (self.received as f32 / self.total as f32).min(1.0);
                    ui.add(
                        egui::ProgressBar::new(frac)
                            .desired_width(240.0)
                            .show_percentage(),
                    );
                    ui.label(format!("{} / {}", self.received, self.total));
                } else if self.received > 0 {
                    ui.add(egui::ProgressBar::new(0.0).desired_width(240.0));
                    ui.label(format!("{} 条", self.received));
                } else {
                    ui.colored_label(p.muted, "等待下载");
                }
            });
            if let Some(percent) = self.erase_percent {
                ui.horizontal(|ui| {
                    ui.label(egui::RichText::new("清理进度").color(p.muted));
                    ui.add_space(4.0);
                    ui.add(
                        egui::ProgressBar::new(percent as f32 / 100.0)
                            .desired_width(240.0)
                            .show_percentage(),
                    );
                    ui.label(format!("{percent}%"));
                });
            }
            ui.horizontal(|ui| {
                ui.label(
                    egui::RichText::new(format!("本地缓存待重传: {} 个", self.spool_pending))
                        .color(p.muted),
                );
                if !self.last_summary.is_empty() {
                    ui.add_space(8.0);
                    ui.label(format!("上次结果: {}", self.last_summary));
                }
            });
            if let Some((level, message)) = &self.notice {
                ui.add_space(4.0);
                let color = match level {
                    LogLevel::Error => p.error,
                    LogLevel::Warn => p.warn,
                    LogLevel::Success => p.success,
                    LogLevel::Info => p.info,
                };
                ui.colored_label(color, format!("提示: {message}"));
            }
        });

        ui.add_space(10.0);
        style::card(p).show(ui, |ui| {
            ui.set_width(ui.available_width());
            style::card_title(ui, p, "本次数据段");
            ui.add_space(6.0);
            egui::ScrollArea::vertical()
                .id_salt("sessions")
                .auto_shrink([false, false])
                .max_height(200.0)
                .show(ui, |ui| {
                    if self.sessions.is_empty() {
                        style::hint(ui, p, "暂无");
                    }
                    for line in &self.sessions {
                        ui.label(line.as_str());
                    }
                });
        });
    }

    fn show_settings_tab(&mut self, ui: &mut egui::Ui, p: &Palette) {
        style::card(p).show(ui, |ui| {
            ui.set_width(ui.available_width());
            style::card_title(ui, p, "上传方式");
            ui.add_space(6.0);
            ui.horizontal(|ui| {
                ui.radio_value(&mut self.cfg.sink_mode, SinkMode::Database, "PostgreSQL 直连");
                ui.radio_value(&mut self.cfg.sink_mode, SinkMode::Mqtt, "MQTT (RBX2)");
                ui.radio_value(&mut self.cfg.sink_mode, SinkMode::Both, "两者都传");
            });
        });

        ui.add_space(8.0);
        style::card(p).show(ui, |ui| {
            ui.set_width(ui.available_width());
            style::card_title(ui, p, "PostgreSQL");
            ui.add_space(6.0);
            egui::Grid::new("pg_grid")
                .num_columns(2)
                .spacing([12.0, 8.0])
                .show(ui, |ui| {
                    ui.label("主机");
                    ui.add(egui::TextEdit::singleline(&mut self.cfg.pg.host).desired_width(280.0));
                    ui.end_row();
                    ui.label("端口");
                    ui.add(egui::TextEdit::singleline(&mut self.pg_port_buf).desired_width(100.0));
                    ui.end_row();
                    ui.label("数据库");
                    ui.add(egui::TextEdit::singleline(&mut self.cfg.pg.database).desired_width(280.0));
                    ui.end_row();
                    ui.label("用户");
                    ui.add(egui::TextEdit::singleline(&mut self.cfg.pg.user).desired_width(280.0));
                    ui.end_row();
                    ui.label("密码");
                    ui.add(
                        egui::TextEdit::singleline(&mut self.cfg.pg.password)
                            .password(true)
                            .desired_width(280.0),
                    );
                    ui.end_row();
                });
            ui.add_space(8.0);
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
                if !self.pg_test.result.is_empty() {
                    ui.colored_label(test_result_color(p, &self.pg_test.result), &self.pg_test.result);
                }
            });
        });

        ui.add_space(8.0);
        style::card(p).show(ui, |ui| {
            ui.set_width(ui.available_width());
            style::card_title(ui, p, "MQTT");
            ui.add_space(6.0);
            egui::Grid::new("mqtt_grid")
                .num_columns(2)
                .spacing([12.0, 8.0])
                .show(ui, |ui| {
                    ui.label("Broker");
                    ui.add(egui::TextEdit::singleline(&mut self.cfg.mqtt.broker).desired_width(280.0));
                    ui.end_row();
                    ui.label("端口");
                    ui.add(egui::TextEdit::singleline(&mut self.mqtt_port_buf).desired_width(100.0));
                    ui.end_row();
                    ui.label("用户名");
                    ui.add(egui::TextEdit::singleline(&mut self.cfg.mqtt.user).desired_width(280.0));
                    ui.end_row();
                    ui.label("密码");
                    ui.add(
                        egui::TextEdit::singleline(&mut self.cfg.mqtt.password)
                            .password(true)
                            .desired_width(280.0),
                    );
                    ui.end_row();
                    ui.label("主题");
                    ui.add(egui::TextEdit::singleline(&mut self.cfg.mqtt.topic).desired_width(280.0));
                    ui.end_row();
                    ui.label("Client ID");
                    ui.add(egui::TextEdit::singleline(&mut self.cfg.mqtt.client_id).desired_width(280.0));
                    ui.end_row();
                });
            ui.add_space(8.0);
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
                if !self.mqtt_test.result.is_empty() {
                    ui.colored_label(test_result_color(p, &self.mqtt_test.result), &self.mqtt_test.result);
                }
            });
        });

        ui.add_space(8.0);
        style::card(p).show(ui, |ui| {
            ui.set_width(ui.available_width());
            style::card_title(ui, p, "行为");
            ui.add_space(4.0);
            ui.checkbox(&mut self.cfg.auto_erase, "上传成功后自动清除设备内存");
            ui.checkbox(&mut self.cfg.spool_on_failure, "上传失败时写入本地缓存，下次启动重传");
            ui.horizontal(|ui| {
                ui.label("设备名过滤");
                ui.add(
                    egui::TextEdit::singleline(&mut self.cfg.device_name_filter).desired_width(160.0),
                );
            });
        });

        ui.add_space(8.0);
        style::card(p).show(ui, |ui| {
            ui.set_width(ui.available_width());
            style::card_title(ui, p, "外观");
            ui.add_space(6.0);
            ui.horizontal(|ui| {
                ui.label(egui::RichText::new("主题").color(p.muted));
                ui.add_space(4.0);
                let dark = self.cfg.theme == Theme::Dark;
                if style::tab_button(ui, p, dark, "深色").clicked() && !dark {
                    self.set_theme(ui.ctx(), Theme::Dark);
                }
                if style::tab_button(ui, p, !dark, "浅色").clicked() && dark {
                    self.set_theme(ui.ctx(), Theme::Light);
                }
                ui.add_space(8.0);
                style::hint(ui, p, "切换后自动保存");
            });
        });

        ui.add_space(12.0);
        ui.horizontal(|ui| {
            if ui
                .add_enabled(!self.running, style::primary_button(p, "保存设置"))
                .clicked()
            {
                self.save_config();
            }
            if ui
                .add_enabled(!self.running, style::secondary_button(p, "放弃修改"))
                .clicked()
            {
                self.discard_edits();
            }
            if !self.action_result.is_empty() {
                ui.add_space(8.0);
                ui.colored_label(test_result_color(p, &self.action_result), &self.action_result);
            }
        });
        ui.add_space(6.0);
        style::hint(ui, p, &format!("配置文件: {}", config::config_path().display()));
    }

    fn show_logs_tab(&mut self, ui: &mut egui::Ui, p: &Palette) {
        ui.horizontal(|ui| {
            if ui
                .add_enabled(!self.running, style::secondary_button(p, "清空日志"))
                .clicked()
            {
                self.logs.clear();
            }
            ui.add_space(8.0);
            style::hint(ui, p, &format!("日志目录: {}", config::log_path().display()));
        });
        ui.add_space(8.0);
        style::card(p).show(ui, |ui| {
            ui.set_width(ui.available_width());
            ui.set_height(ui.available_height());
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
                            let tag = Palette::level_tag(*level);
                            match p.level_color(*level) {
                                Some(c) => {
                                    ui.colored_label(c, egui::RichText::new(tag).strong());
                                    ui.colored_label(c, message.as_str());
                                }
                                None => {
                                    ui.colored_label(p.muted, egui::RichText::new(tag).strong());
                                    ui.label(message.as_str());
                                }
                            }
                        });
                    }
                });
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
        let header_frame = egui::Frame::side_top_panel(&ctx.style())
            .fill(p.header)
            .inner_margin(egui::Margin {
                left: 16,
                right: 16,
                top: 10,
                bottom: 0,
            });
        egui::TopBottomPanel::top("header")
            .frame(header_frame)
            .show(ctx, |ui| {
                ui.horizontal(|ui| {
                    // 品牌色块 + 标题
                    let (dot, _) =
                        ui.allocate_exact_size(egui::vec2(10.0, 10.0), egui::Sense::hover());
                    ui.painter()
                        .rect_filled(dot, egui::CornerRadius::same(3), p.accent);
                    ui.add_space(8.0);
                    ui.label(
                        egui::RichText::new("RaceBox 同步")
                            .strong()
                            .size(17.0)
                            .color(p.text),
                    );
                    ui.add_space(14.0);
                    for (tab, label) in [
                        (Tab::Sync, "同步"),
                        (Tab::Settings, "设置"),
                        (Tab::Logs, "日志"),
                    ] {
                        if style::tab_button(ui, &p, self.tab == tab, label).clicked() {
                            self.tab = tab;
                        }
                        ui.add_space(2.0);
                    }
                    ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                        let next = if p.is_dark() { Theme::Light } else { Theme::Dark };
                        let label = if p.is_dark() { "浅色" } else { "深色" };
                        if ui.add(style::small_button(&p, label)).clicked() {
                            self.set_theme(ui.ctx(), next);
                        }
                        ui.add_space(10.0);
                        if self.running {
                            ui.colored_label(p.info, format!("运行中 · {}", self.phase));
                        } else {
                            ui.colored_label(p.muted, "空闲");
                        }
                    });
                });
                ui.add_space(9.0);
                ui.separator();
            });

        egui::CentralPanel::default()
            .frame(
                egui::Frame::central_panel(&ctx.style())
                    .fill(p.bg)
                    .inner_margin(egui::Margin::symmetric(16, 14)),
            )
            .show(ctx, |ui| {
                if matches!(self.tab, Tab::Logs) {
                    self.show_logs_tab(ui, &p);
                } else {
                    egui::ScrollArea::vertical()
                        .auto_shrink([false, false])
                        .show(ui, |ui| match self.tab {
                            Tab::Sync => self.show_sync_tab(ui, &p),
                            Tab::Settings => self.show_settings_tab(ui, &p),
                            Tab::Logs => unreachable!(),
                        });
                }
            });
    }
}
