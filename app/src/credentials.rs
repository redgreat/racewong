//! 系统凭据库封装。
//!
//! Windows 使用 Credential Manager，macOS 使用 Keychain。Linux 构建暂时保留
//! 配置文件存储，避免强行依赖桌面环境的 Secret Service。

const SERVICE: &str = "cn.wongcw.racewong";

#[cfg(any(target_os = "windows", target_os = "macos"))]
fn entry(name: &str) -> Result<keyring::Entry, String> {
    keyring::Entry::new(SERVICE, name).map_err(|e| e.to_string())
}

#[cfg(any(target_os = "windows", target_os = "macos"))]
pub fn get(name: &str) -> Option<String> {
    entry(name).ok()?.get_password().ok()
}

#[cfg(not(any(target_os = "windows", target_os = "macos")))]
pub fn get(_name: &str) -> Option<String> {
    None
}

#[cfg(any(target_os = "windows", target_os = "macos"))]
pub fn set(name: &str, value: &str) -> Result<(), String> {
    let entry = entry(name)?;
    if value.is_empty() {
        match entry.delete_credential() {
            Ok(()) | Err(keyring::Error::NoEntry) => Ok(()),
            Err(e) => Err(e.to_string()),
        }
    } else {
        entry.set_password(value).map_err(|e| e.to_string())
    }
}

#[cfg(not(any(target_os = "windows", target_os = "macos")))]
pub fn set(_name: &str, _value: &str) -> Result<(), String> {
    Ok(())
}

pub fn load_config(cfg: &mut crate::config::Config) {
    if let Some(value) = get("postgresql-password") {
        cfg.pg.password = value;
    }
    if let Some(value) = get("mqtt-password") {
        cfg.mqtt.password = value;
    }
}

/// 保存密码并返回适合写进 config.json 的副本。
pub fn config_for_disk(cfg: &crate::config::Config) -> Result<crate::config::Config, String> {
    let mut disk = cfg.clone();
    if cfg!(any(target_os = "windows", target_os = "macos")) {
        set("postgresql-password", &cfg.pg.password)?;
        set("mqtt-password", &cfg.mqtt.password)?;
        disk.pg.password.clear();
        disk.mqtt.password.clear();
    }
    Ok(disk)
}

/// 把旧配置里的明文密码迁入系统凭据库。失败时不清除原值，避免丢凭据。
pub fn migrate_plaintext(cfg: &crate::config::Config) -> bool {
    if !cfg!(any(target_os = "windows", target_os = "macos")) {
        return false;
    }
    let pg_ok = cfg.pg.password.is_empty() || set("postgresql-password", &cfg.pg.password).is_ok();
    let mqtt_ok = cfg.mqtt.password.is_empty() || set("mqtt-password", &cfg.mqtt.password).is_ok();
    pg_ok && mqtt_ok && (!cfg.pg.password.is_empty() || !cfg.mqtt.password.is_empty())
}
