//! 非阻塞文件日志。
//!
//! UI 和异步任务只往有界队列投递；单独线程长期持有文件句柄，避免每条日志都
//! `OpenOptions::open` 阻塞界面。队列满时丢日志也好过拖住 BLE/上传主流程。

use std::io::Write;
use std::sync::mpsc::{sync_channel, SyncSender, TrySendError};
use std::sync::OnceLock;

static TX: OnceLock<SyncSender<String>> = OnceLock::new();

struct AsyncLogger {
    tx: SyncSender<String>,
}

impl log::Log for AsyncLogger {
    fn enabled(&self, metadata: &log::Metadata) -> bool {
        metadata.level() <= log::Level::Info
    }

    fn log(&self, record: &log::Record) {
        if self.enabled(record.metadata()) {
            send(
                &self.tx,
                format!(
                    "{} [{}] {}\n",
                    chrono::Local::now().format("%Y-%m-%d %H:%M:%S"),
                    record.level(),
                    record.args()
                ),
            );
        }
    }

    fn flush(&self) {}
}

pub fn init() {
    let (tx, rx) = sync_channel::<String>(4096);
    let _ = TX.set(tx.clone());
    std::thread::Builder::new()
        .name("racewong-log".to_string())
        .spawn(move || {
            let mut file = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(crate::config::log_path())
                .ok();
            while let Ok(line) = rx.recv() {
                if file.is_none() {
                    file = std::fs::OpenOptions::new()
                        .create(true)
                        .append(true)
                        .open(crate::config::log_path())
                        .ok();
                }
                if let Some(f) = file.as_mut() {
                    if f.write_all(line.as_bytes()).is_err() {
                        file = None;
                    }
                }
            }
            if let Some(mut f) = file {
                let _ = f.flush();
            }
        })
        .ok();
    let _ = log::set_boxed_logger(Box::new(AsyncLogger { tx }));
    log::set_max_level(log::LevelFilter::Info);
}

pub fn ui(level: &str, message: &str) {
    let Some(tx) = TX.get() else { return };
    send(
        tx,
        format!(
            "{} [UI/{level}] {message}\n",
            chrono::Local::now().format("%Y-%m-%d %H:%M:%S")
        ),
    );
}

fn send(tx: &SyncSender<String>, line: String) {
    match tx.try_send(line) {
        Ok(()) | Err(TrySendError::Full(_)) | Err(TrySendError::Disconnected(_)) => {}
    }
}
