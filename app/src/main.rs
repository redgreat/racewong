#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

mod ble;
mod config;
mod protocol;
mod sink;
mod style;
mod ui;
mod worker;

use std::sync::Arc;
use tokio::runtime::Runtime;

fn setup_fonts(ctx: &eframe::egui::Context) {
    let mut fonts = eframe::egui::FontDefinitions::default();
    let candidates: &[(&str, &[&str])] = &[
        (
            "windows",
            &[
                "C:\\Windows\\Fonts\\msyh.ttc",
                "C:\\Windows\\Fonts\\simhei.ttf",
                "C:\\Windows\\Fonts\\simsun.ttc",
            ],
        ),
        (
            "macos",
            &[
                "/System/Library/Fonts/PingFang.ttc",
                "/System/Library/Fonts/STHeiti Light.ttc",
            ],
        ),
        (
            "linux",
            &[
                "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
                "/usr/share/fonts/truetype/wqy/wqy-zenhei.ttc",
                "/usr/share/fonts/truetype/arphic/uming.ttc",
            ],
        ),
    ];
    for (_os, paths) in candidates {
        if let Some(path) = paths.iter().find(|p| std::path::Path::new(p).exists()) {
            if let Ok(data) = std::fs::read(path) {
                fonts.font_data.insert(
                    "cjk".to_owned(),
                    std::sync::Arc::new(eframe::egui::FontData::from_owned(data)),
                );
                fonts
                    .families
                    .entry(eframe::egui::FontFamily::Proportional)
                    .or_default()
                    .insert(0, "cjk".to_owned());
                fonts
                    .families
                    .entry(eframe::egui::FontFamily::Monospace)
                    .or_default()
                    .push("cjk".to_owned());
                break;
            }
        }
    }
    ctx.set_fonts(fonts);
}

fn init_file_logger() {
    struct FileLogger;
    impl log::Log for FileLogger {
        fn enabled(&self, _metadata: &log::Metadata) -> bool {
            true
        }

        fn log(&self, record: &log::Record) {
            if !self.enabled(record.metadata()) {
                return;
            }
            let line = format!(
                "{} [{}] {}\n",
                chrono::Local::now().format("%Y-%m-%d %H:%M:%S"),
                record.level(),
                record.args()
            );
            use std::io::Write;
            if let Ok(mut file) = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(config::log_path())
            {
                let _ = file.write_all(line.as_bytes());
            }
        }

        fn flush(&self) {}
    }
    let _ = log::set_boxed_logger(Box::new(FileLogger));
    log::set_max_level(log::LevelFilter::Info);
}

fn window_icon() -> Option<std::sync::Arc<eframe::egui::IconData>> {
    let bytes = include_bytes!("../assets/icon256.png");
    match eframe::icon_data::from_png_bytes(bytes) {
        Ok(icon) => Some(std::sync::Arc::new(icon)),
        Err(e) => {
            eprintln!("窗口图标加载失败: {e}");
            None
        }
    }
}

/// 把界面上的日志同步写一份到日志文件，方便 GUI 里看不到时报错时排查。
pub fn write_ui_log(level: &str, message: &str) {
    use std::io::Write;
    let line = format!(
        "{} [UI/{level}] {message}\n",
        chrono::Local::now().format("%Y-%m-%d %H:%M:%S")
    );
    if let Ok(mut file) = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(config::log_path())
    {
        let _ = file.write_all(line.as_bytes());
    }
}

/// `racewong-app --check`：不开窗口，直接按当前配置测 PG/MQTT 连通性。
async fn run_check() {
    let cfg = config::load();
    println!("配置文件: {}", config::config_path().display());
    println!(
        "上传方式: {}",
        match cfg.sink_mode {
            config::SinkMode::Database => "PostgreSQL",
            config::SinkMode::Mqtt => "MQTT",
            config::SinkMode::Both => "PostgreSQL + MQTT",
        }
    );

    if cfg.uses_database() {
        print!("PostgreSQL {}:{}/{} ... ", cfg.pg.host, cfg.pg.port, cfg.pg.database);
        let mut pg = tokio_postgres::Config::new();
        pg.host(&cfg.pg.host)
            .port(cfg.pg.port)
            .dbname(&cfg.pg.database)
            .user(&cfg.pg.user)
            .password(&cfg.pg.password);
        match pg.connect(tokio_postgres::NoTls).await {
            Ok((client, connection)) => {
                tokio::spawn(async move {
                    let _ = connection.await;
                });
                match client.query_one("select 1", &[]).await {
                    Ok(_) => println!("OK"),
                    Err(e) => println!("连接成功但查询失败: {e}"),
                }
            }
            Err(e) => println!("失败: {}", crate::sink::pg::describe_db_error(&e)),
        }
    }

    if cfg.uses_mqtt() {
        print!("MQTT {}:{}/{} ... ", cfg.mqtt.broker, cfg.mqtt.port, cfg.mqtt.topic);
        match crate::sink::mqtt::MqttSink::new(cfg.mqtt.clone()).await {
            Ok(sink) => {
                println!("OK (已收到 CONNACK)");
                sink.disconnect().await;
            }
            Err(e) => println!("失败: {e}"),
        }
    }
}

fn main() -> eframe::Result<()> {
    init_file_logger();
    if std::env::args().any(|a| a == "--check" || a == "-c") {
        let runtime = Runtime::new().expect("创建 tokio 运行时失败");
        runtime.block_on(run_check());
        return Ok(());
    }
    let runtime = Runtime::new().expect("创建 tokio 运行时失败");
    let runtime = Arc::new(runtime);
    let cfg = config::load();
    let (tx, rx) = std::sync::mpsc::channel();
    let mut viewport = eframe::egui::ViewportBuilder::default()
        .with_inner_size([980.0, 720.0])
        .with_min_inner_size([760.0, 560.0]);
    if let Some(icon) = window_icon() {
        viewport = viewport.with_icon(icon);
    }
    let native_options = eframe::NativeOptions {
        viewport,
        ..Default::default()
    };
    let app_runtime = runtime.clone();
    eframe::run_native(
        "RaceBox 同步",
        native_options,
        Box::new(move |cc| {
            setup_fonts(&cc.egui_ctx);
            style::apply(&cc.egui_ctx, cfg.theme);
            Ok(Box::new(ui::App::new(app_runtime, cfg, tx, rx)))
        }),
    )
}
