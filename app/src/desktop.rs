//! 桌面系统集成：通知与诊断包。

/// 后台发送系统通知，平台接口慢或失败都不能卡住 UI。
pub fn notify(title: String, body: String, window_handle: isize) {
    std::thread::spawn(move || {
        let mut notification = notify_rust::Notification::new();
        notification
            .appname("RaceBox Sync")
            .summary(&title)
            .body(&body)
            .timeout(notify_rust::Timeout::Milliseconds(8_000));
        #[cfg(target_os = "windows")]
        notification.action("open", "打开");
        let Ok(handle) = notification.show() else {
            return;
        };
        #[cfg(target_os = "windows")]
        handle.wait_for_action(|action| {
            if action == "open" || action == "default" {
                crate::tray::show_window(window_handle);
            }
        });
        #[cfg(not(target_os = "windows"))]
        let _ = (handle, window_handle);
    });
}

/// 导出一个不含密码、token、账号的 ZIP 诊断包。
pub fn export_diagnostics(cfg: &crate::config::Config) -> Result<std::path::PathBuf, String> {
    let Some(path) = rfd::FileDialog::new()
        .set_title("导出 RaceBox 诊断包")
        .set_file_name("racebox-diagnostics.zip")
        .add_filter("ZIP", &["zip"])
        .save_file()
    else {
        return Err("已取消".to_string());
    };

    let report = serde_json::json!({
        "app_version": env!("CARGO_PKG_VERSION"),
        "os": std::env::consts::OS,
        "arch": std::env::consts::ARCH,
        "upload_channel": match cfg.sink_mode {
            crate::config::SinkMode::Database => "PostgreSQL",
            crate::config::SinkMode::Mqtt => "MQTT",
        },
        "postgresql_configured": cfg.pg.ready(),
        "mqtt_configured": cfg.mqtt.ready(),
        "cloud_enabled": cfg.cloud.enabled,
        "auto_start": cfg.auto_start,
        "auto_sync": cfg.auto_sync,
        "auto_replay": cfg.auto_replay,
        "notifications": cfg.notifications,
        "spool_pending": crate::sink::spool::pending_count(),
        "generated_at": chrono::Utc::now().to_rfc3339(),
    });
    let report = serde_json::to_vec_pretty(&report).map_err(|e| e.to_string())?;
    let mut log =
        String::from_utf8_lossy(&std::fs::read(crate::config::log_path()).unwrap_or_default())
            .into_owned();
    let cloud = crate::racebox::store::load();
    for secret in [
        cfg.pg.password.as_str(),
        cfg.mqtt.password.as_str(),
        cloud.password.as_str(),
        cloud.auth.as_str(),
    ] {
        if !secret.is_empty() {
            log = log.replace(secret, "***");
        }
    }
    let log = log.into_bytes();
    // 日志最多带最后 512 KiB，避免长期运行后诊断包无限膨胀。
    let log = if log.len() > 512 * 1024 {
        log[log.len() - 512 * 1024..].to_vec()
    } else {
        log
    };
    let files = [
        ("diagnostics.json", report.as_slice()),
        ("racewong.log", log.as_slice()),
    ];
    write_zip(&path, &files).map_err(|e| format!("写入诊断包失败: {e}"))?;
    Ok(path)
}

/// 只用 ZIP 的 Store 模式，避免为两个小文件再引一套压缩依赖。
fn write_zip(path: &std::path::Path, files: &[(&str, &[u8])]) -> std::io::Result<()> {
    use std::io::Write;
    let mut out = std::fs::File::create(path)?;
    let mut central = Vec::new();
    let mut offset = 0u32;
    for (name, data) in files {
        let name = name.as_bytes();
        let crc = crc32fast::hash(data);
        let size = data.len() as u32;
        out.write_all(&0x0403_4b50u32.to_le_bytes())?;
        out.write_all(&20u16.to_le_bytes())?;
        out.write_all(&0u16.to_le_bytes())?;
        out.write_all(&0u16.to_le_bytes())?;
        out.write_all(&0u16.to_le_bytes())?;
        out.write_all(&0u16.to_le_bytes())?;
        out.write_all(&crc.to_le_bytes())?;
        out.write_all(&size.to_le_bytes())?;
        out.write_all(&size.to_le_bytes())?;
        out.write_all(&(name.len() as u16).to_le_bytes())?;
        out.write_all(&0u16.to_le_bytes())?;
        out.write_all(name)?;
        out.write_all(data)?;

        central.extend_from_slice(&0x0201_4b50u32.to_le_bytes());
        central.extend_from_slice(&20u16.to_le_bytes());
        central.extend_from_slice(&20u16.to_le_bytes());
        central.extend_from_slice(&0u16.to_le_bytes());
        central.extend_from_slice(&0u16.to_le_bytes());
        central.extend_from_slice(&0u16.to_le_bytes());
        central.extend_from_slice(&0u16.to_le_bytes());
        central.extend_from_slice(&crc.to_le_bytes());
        central.extend_from_slice(&size.to_le_bytes());
        central.extend_from_slice(&size.to_le_bytes());
        central.extend_from_slice(&(name.len() as u16).to_le_bytes());
        central.extend_from_slice(&0u16.to_le_bytes());
        central.extend_from_slice(&0u16.to_le_bytes());
        central.extend_from_slice(&0u16.to_le_bytes());
        central.extend_from_slice(&0u16.to_le_bytes());
        central.extend_from_slice(&0u32.to_le_bytes());
        central.extend_from_slice(&offset.to_le_bytes());
        central.extend_from_slice(name);
        offset += 30 + name.len() as u32 + size;
    }
    let central_offset = offset;
    out.write_all(&central)?;
    out.write_all(&0x0605_4b50u32.to_le_bytes())?;
    out.write_all(&0u16.to_le_bytes())?;
    out.write_all(&0u16.to_le_bytes())?;
    out.write_all(&(files.len() as u16).to_le_bytes())?;
    out.write_all(&(files.len() as u16).to_le_bytes())?;
    out.write_all(&(central.len() as u32).to_le_bytes())?;
    out.write_all(&central_offset.to_le_bytes())?;
    out.write_all(&0u16.to_le_bytes())?;
    out.flush()
}

#[cfg(test)]
mod tests {
    #[test]
    fn zip_has_required_signatures() {
        let path = std::env::temp_dir().join("racewong-diagnostics-test.zip");
        super::write_zip(&path, &[("a.txt", b"abc")]).unwrap();
        let data = std::fs::read(&path).unwrap();
        assert_eq!(&data[..4], &0x0403_4b50u32.to_le_bytes());
        assert!(data.windows(4).any(|w| w == 0x0605_4b50u32.to_le_bytes()));
        let _ = std::fs::remove_file(path);
    }
}
