use super::{Session, SpoolMeta};
use crate::config;
use crate::protocol::{Record, RECORD_SIZE};
use crate::sink::DataSink;
use std::path::{Path, PathBuf};

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

pub async fn replay(sinks: &[&dyn DataSink]) -> Vec<String> {
    let dir = config::spool_dir();
    let mut logs = Vec::new();
    let entries = match std::fs::read_dir(&dir) {
        Ok(entries) => entries,
        Err(_) => return logs,
    };
    let mut json_files: Vec<PathBuf> = entries
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().map(|x| x == "json").unwrap_or(false))
        .collect();
    json_files.sort();
    if json_files.is_empty() {
        return logs;
    }
    logs.push(format!("发现 {} 个待重放的本地缓存文件", json_files.len()));
    for json_path in json_files {
        let Some(session) = read_session(&json_path) else {
            logs.push(format!("缓存文件损坏，已跳过: {}", json_path.display()));
            continue;
        };
        let bin_path = json_path.with_extension("bin");
        let mut failure: Option<String> = None;
        for sink in sinks {
            if let Err(e) = sink.upload_session(&session).await {
                failure = Some(format!("{}", e));
                break;
            }
        }
        match failure {
            None => {
                let _ = std::fs::remove_file(&json_path);
                let _ = std::fs::remove_file(&bin_path);
                logs.push(format!("重放成功: {} ({} 条)", session.display_name(), session.records.len()));
            }
            Some(e) => logs.push(format!("重放失败 {}: {e}", session.display_name())),
        }
    }
    logs
}
