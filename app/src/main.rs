#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

mod ble;
mod cloud_ui;
mod config;
mod i18n;
mod protocol;
mod racebox;
mod single_instance;
mod sink;
mod style;
mod tray;
mod ui;
mod updater;
mod worker;

use std::sync::Arc;
use tokio::runtime::Runtime;


fn setup_fonts(ctx: &eframe::egui::Context) {
    let mut fonts = eframe::egui::FontDefinitions::default();

    // 每个槽位是一组候选路径，取第一个存在的。
    //
    // 为什么要三个槽位：微软雅黑（msyh）有中日韩共用汉字和西里尔字母，
    // 但**没有韩文谚文**，只装它的话切到한국어就是一片豆腐块；日文虽然能用
    // 汉字凑合，字形是中文写法，所以日文界面优先用日文字体。
    let slots: &[(&str, &[&str])] = &[
        (
            "cjk",
            &[
                "C:\\Windows\\Fonts\\msyh.ttc",
                "C:\\Windows\\Fonts\\simhei.ttf",
                "C:\\Windows\\Fonts\\simsun.ttc",
                "/System/Library/Fonts/PingFang.ttc",
                "/System/Library/Fonts/STHeiti Light.ttc",
                "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
                "/usr/share/fonts/truetype/wqy/wqy-zenhei.ttc",
                "/usr/share/fonts/truetype/arphic/uming.ttc",
            ],
        ),
        (
            "kr",
            &[
                "C:\\Windows\\Fonts\\malgun.ttf",
                "/System/Library/Fonts/AppleSDGothicNeo.ttc",
                "/usr/share/fonts/truetype/nanum/NanumGothic.ttf",
                "/usr/share/fonts/opentype/noto/NotoSansCJKkr-Regular.otf",
            ],
        ),
        (
            "jp",
            &[
                "C:\\Windows\\Fonts\\YuGothM.ttc",
                "C:\\Windows\\Fonts\\msgothic.ttc",
                "/System/Library/Fonts/ヒラギノ角ゴシック W3.ttc",
                "/usr/share/fonts/opentype/noto/NotoSansCJKjp-Regular.otf",
            ],
        ),
    ];

    for (key, paths) in slots {
        if let Some(path) = paths.iter().find(|p| std::path::Path::new(p).exists()) {
            if let Ok(data) = std::fs::read(path) {
                fonts.font_data.insert(
                    (*key).to_owned(),
                    std::sync::Arc::new(eframe::egui::FontData::from_owned(data)),
                );
            }
        }
    }

    // 排版顺序：当前语言专属字体 → 通用 CJK → 韩文 → 日文。
    // egui 是按家族列表逐字回退的，排在前面的优先。
    let mut order: Vec<&str> = Vec::new();
    match crate::i18n::current() {
        crate::i18n::Lang::Ko => order.push("kr"),
        crate::i18n::Lang::Ja => order.push("jp"),
        _ => {}
    }
    for key in ["cjk", "kr", "jp"] {
        if !order.contains(&key) {
            order.push(key);
        }
    }
    let present: Vec<String> = order
        .into_iter()
        .filter(|k| fonts.font_data.contains_key(*k))
        .map(str::to_owned)
        .collect();

    let proportional = fonts
        .families
        .entry(eframe::egui::FontFamily::Proportional)
        .or_default();
    // 逆序插到最前面，最终顺序就是 present 的顺序
    for key in present.iter().rev() {
        proportional.insert(0, (*key).clone());
    }
    let monospace = fonts
        .families
        .entry(eframe::egui::FontFamily::Monospace)
        .or_default();
    for key in present.iter() {
        monospace.push((*key).clone());
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
            eprintln!("{}: {e}", crate::t!("窗口图标加载失败", "failed to load window icon"));
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
    println!(
        "{}: {}",
        crate::t!("配置文件", "config file"),
        config::config_path().display()
    );
    println!(
        "{}: {}",
        crate::t!("上传方式", "upload channel"),
        match cfg.sink_mode {
            config::SinkMode::Database => "PostgreSQL",
            config::SinkMode::Mqtt => "MQTT",
        }
    );

    if cfg.uses_database() {
        print!("PostgreSQL {}:{}/{} ... ", cfg.pg.host, cfg.pg.port, cfg.pg.database);
        // 和界面上的“测试连接”一致：走真实写入链路（事务内试写后回滚）
        match crate::sink::pg::PgSink::probe(&cfg.pg).await {
            Ok(detail) => println!("OK ({detail})"),
            Err(e) => println!("{}: {e}", crate::t!("失败", "failed")),
        }
    }

    if cfg.uses_mqtt() {
        print!("MQTT {}:{}/{} ... ", cfg.mqtt.broker, cfg.mqtt.port, cfg.mqtt.topic);
        match crate::sink::mqtt::MqttSink::new(cfg.mqtt.clone(), None).await {
            Ok(sink) => {
                println!("OK ({})", crate::t!("已收到 CONNACK", "CONNACK received"));
                sink.disconnect().await;
            }
            Err(e) => println!("{}: {e}", crate::t!("失败", "failed")),
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
    // 单实例：托盘常驻时窗口只是藏起来，用户再点一次快捷方式就会起第二个进程，
    // 通知区域于是多出一个同名图标。这里抢不到互斥体就把已有窗口拉到前台再退出。
    // `--check` 是短命的一次性探测，不参与这个限制。
    if !single_instance::claim() {
        return Ok(());
    }
    let runtime = Runtime::new().expect("创建 tokio 运行时失败");
    let runtime = Arc::new(runtime);
    let cfg = config::load();
    // 用 tokio 的无界通道而不是 std 的：上传进度回调要在后台任务里
    // 被 `&(dyn Fn(usize) + Send + Sync)` 捕获，std 的 Sender 满足不了 Sync 约束。
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
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
        // 标题栏只写品牌名：它由窗口系统绘制，不跟着界面语言变，
        // 带上“同步 / Sync”就得为 10 种语言各维护一份，不值得。
        "RaceBox",
        native_options,
        Box::new(move |cc| {
            setup_fonts(&cc.egui_ctx);
            style::apply(&cc.egui_ctx, cfg.theme);
            // 托盘菜单要用原生句柄直接操作窗口（藏进托盘后 egui 的命令不一定生效）
            let window_handle = native_window_handle(cc);
            Ok(Box::new(ui::App::new(
                app_runtime,
                cfg,
                tx,
                rx,
                window_handle,
            )))
        }),
    )
}

/// 取主窗口的原生句柄：Windows 上是 HWND，其它平台返回 0（用不到）。
fn native_window_handle(cc: &eframe::CreationContext<'_>) -> isize {
    #[cfg(target_os = "windows")]
    {
        use raw_window_handle::{HasWindowHandle, RawWindowHandle};
        if let Ok(handle) = cc.window_handle() {
            if let RawWindowHandle::Win32(w) = handle.as_raw() {
                return w.hwnd.get();
            }
        }
    }
    let _ = cc;
    0
}
