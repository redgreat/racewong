use super::{Session, SpoolMeta};
use crate::config;
use crate::protocol::{Record, RECORD_SIZE};
use crate::sink::DataSink;
use crate::tr;
use crate::worker::{LogLevel, UiEvent};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::mpsc::UnboundedSender;

fn pair_paths(stem: &str) -> (PathBuf, PathBuf) {
    let dir = config::spool_dir();
    (dir.join(format!("{stem}.bin")), dir.join(format!("{stem}.json")))
}

pub fn write_session(session: &Session) -> std::io::Result<PathBuf> {
    let stem = format!("{}_{}", session.imp_stamp, session.session_index);
    let (bin_path, json_path) = pair_paths(&stem);

    let mut flat: Vec<u8> = Vec::with_capacity(session.records.len() * RECORD_SIZE);
    for r in &session.records {
        flat.extend_from_slice(&r.raw);
    }
    std::fs::write(&bin_path, flat)?;

    let meta = SpoolMeta {
        device: session.device.clone(),
        session_index: session.session_index,
        duration_secs: session.duration_secs,
        sync_id: uuid::Uuid::from_bytes(session.sync_id).to_string(),
        imp_stamp: session.imp_stamp.clone(),
        sync_offset: session.sync_offset,
        sync_total_upper: session.sync_total_upper,
        sync_received_total: session.sync_received_total,
        sync_final: session.sync_final,
        file_name: session.display_name(),
        record_count: session.records.len(),
    };
    std::fs::write(&json_path, serde_json::to_string_pretty(&meta).unwrap_or_else(|_| "{}".to_string()))?;
    Ok(bin_path)
}

fn read_session(json_path: &Path) -> Option<Session> {
    let meta: SpoolMeta = serde_json::from_str(&std::fs::read_to_string(json_path).ok()?).ok()?;
    let bin_path = json_path.with_extension("bin");
    let flat = std::fs::read(&bin_path).ok()?;
    let mut records = Vec::with_capacity(flat.len() / RECORD_SIZE);
    for chunk in flat.chunks(RECORD_SIZE) {
        if let Some(record) = Record::decode(chunk) {
            records.push(record);
        }
    }
    let sync_id = uuid::Uuid::parse_str(&meta.sync_id).ok()?.into_bytes();
    Some(Session {
        device: meta.device,
        session_index: meta.session_index,
        records,
        duration_secs: meta.duration_secs,
        sync_id,
        imp_stamp: meta.imp_stamp,
        sync_offset: meta.sync_offset,
        sync_total_upper: meta.sync_total_upper,
        sync_received_total: meta.sync_received_total,
        sync_final: meta.sync_final,
    })
}

pub fn pending_count() -> usize {
    let dir = config::spool_dir();
    match std::fs::read_dir(&dir) {
        Ok(entries) => entries
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().map(|x| x == "json").unwrap_or(false))
            .count(),
        Err(_) => 0,
    }
}

/// 把本地缓存里的数据补传上去。
///
/// 以前这个函数把所有日志攒成一个 Vec，跑完才一次性交给界面——积压几十个文件时
/// 界面上几分钟没有任何动静，看起来就像卡死了。现在改成边跑边发日志，
/// 并且能被用户的“取消”中断（`cancel` 是界面上那个取消按钮的同一个标志）。
pub async fn replay(
    sinks: &[&dyn DataSink],
    tx: &UnboundedSender<UiEvent>,
    cancel: &AtomicBool,
) {
    let dir = config::spool_dir();
    let entries = match std::fs::read_dir(&dir) {
        Ok(entries) => entries,
        Err(_) => return,
    };
    let mut json_files: Vec<PathBuf> = entries
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().map(|x| x == "json").unwrap_or(false))
        .collect();
    json_files.sort();
    if json_files.is_empty() {
        return;
    }
    let total = json_files.len();
    let _ = tx.send(UiEvent::Log(
        LogLevel::Info,
        tr!(
            "发现 {total} 个待重传的本地缓存文件，开始后台补传",
            "found {total} cached segment(s) on disk, replaying in background"
        ),
    ));
    let mut done = 0usize;
    for json_path in json_files {
        if cancel.load(Ordering::SeqCst) {
            let _ = tx.send(UiEvent::Log(
                LogLevel::Warn,
                tr!(
                    "已取消，剩余 {} 个缓存文件留到下次启动再传",
                    "cancelled, {} cached file(s) left for the next run",
                    total - done
                ),
            ));
            return;
        }
        let Some(session) = read_session(&json_path) else {
            let _ = tx.send(UiEvent::Log(
                LogLevel::Error,
                tr!(
                    "缓存文件损坏，已跳过: {}",
                    "corrupted cache file skipped: {}",
                    json_path.display()
                ),
            ));
            done += 1;
            continue;
        };
        let bin_path = json_path.with_extension("bin");
        let mut failure: Option<String> = None;
        for sink in sinks {
            if let Err(e) = sink.upload_session(&session, &|_: usize| {}).await {
                failure = Some(format!("{e}"));
                break;
            }
        }
        done += 1;
        match failure {
            None => {
                let _ = std::fs::remove_file(&json_path);
                let _ = std::fs::remove_file(&bin_path);
                let _ = tx.send(UiEvent::Log(
                    LogLevel::Success,
                    tr!(
                        "补传成功 {done}/{total}: {} ({} 条)",
                        "replayed {done}/{total}: {} ({} records)",
                        session.display_name(),
                        session.records.len()
                    ),
                ));
            }
            Some(e) => {
                let _ = tx.send(UiEvent::Log(
                    LogLevel::Error,
                    tr!(
                        "补传失败 {}: {e}",
                        "replay failed {}: {e}",
                        session.display_name()
                    ),
                ));
            }
        }
        let _ = tx.send(UiEvent::SpoolPending(pending_count()));
    }
}
