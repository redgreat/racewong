use crate::i18n::Lang;
use crate::t;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

pub const DEFAULT_MQTT_TOPIC: &str = "deskwong/racebox/data";

#[derive(Clone, PartialEq, Serialize, Deserialize)]
pub struct PgConfig {
    pub host: String,
    pub port: u16,
    pub database: String,
    pub user: String,
    pub password: String,
}

impl Default for PgConfig {
    fn default() -> Self {
        Self {
            host: "fn.wongcw.cn".to_string(),
            port: 8432,
            database: "eadm".to_string(),
            user: "user_eadm".to_string(),
            password: String::new(),
        }
    }
}

impl PgConfig {
    pub fn ready(&self) -> bool {
        !self.host.trim().is_empty() && !self.database.trim().is_empty() && !self.user.trim().is_empty()
    }
}

#[derive(Clone, PartialEq, Serialize, Deserialize)]
pub struct MqttConfig {
    pub broker: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub topic: String,
    pub client_id: String,
    /// 保活间隔（秒）。
    #[serde(default = "default_keepalive")]
    pub keepalive_secs: u32,
}

fn default_keepalive() -> u32 {
    30
}

impl Default for MqttConfig {
    fn default() -> Self {
        Self {
            broker: "127.0.0.1".to_string(),
            port: 1883,
            user: String::new(),
            password: String::new(),
            topic: DEFAULT_MQTT_TOPIC.to_string(),
            client_id: "racewong-app".to_string(),
            keepalive_secs: default_keepalive(),
        }
    }
}

impl MqttConfig {
    pub fn ready(&self) -> bool {
        !self.broker.trim().is_empty() && self.port > 0 && !self.topic.trim().is_empty()
    }
}

/// 界面主题。
#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Debug)]
#[serde(rename_all = "lowercase")]
pub enum Theme {
    Dark,
    Light,
}

impl Default for Theme {
    fn default() -> Self {
        Theme::Dark
    }
}

/// 上传通道：二选一，不做“同时上传”。
///
/// 老配置里可能存着 `"Both"`（早期版本有“两者都传”），所以这里手工反序列化：
/// 认不出来的值一律按直连处理，避免整个 config.json 解析失败被重置成默认配置。
#[derive(Clone, Copy, PartialEq, Eq, Serialize, Debug)]
pub enum SinkMode {
    Database,
    Mqtt,
}

impl<'de> Deserialize<'de> for SinkMode {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let raw = String::deserialize(deserializer)?;
        Ok(match raw.as_str() {
            "Mqtt" => SinkMode::Mqtt,
            _ => SinkMode::Database,
        })
    }
}

impl SinkMode {
    pub fn label(self) -> &'static str {
        match self {
            SinkMode::Database => t!("PostgreSQL 直连", "PostgreSQL direct"),
            SinkMode::Mqtt => "MQTT (RBX2)",
        }
    }
}

impl Default for SinkMode {
    fn default() -> Self {
        SinkMode::Database
    }
}

/// 点窗口关闭按钮（右上角 ×）时的行为，二选一。
#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Debug, Default)]
pub enum CloseAction {
    /// 直接退出应用。
    #[default]
    Quit,
    /// 隐藏到系统托盘（Windows 通知区域 / macOS 菜单栏），后台继续运行。
    Tray,
}

impl CloseAction {
    pub fn label(self) -> &'static str {
        match self {
            CloseAction::Quit => t!("直接退出应用", "Quit the app"),
            // macOS 上常驻的位置是菜单栏，不叫“托盘”
            CloseAction::Tray => {
                if cfg!(target_os = "macos") {
                    t!("隐藏到菜单栏，后台继续运行", "Hide to menu bar and keep running")
                } else {
                    t!("隐藏到托盘，后台继续运行", "Hide to tray and keep running")
                }
            }
        }
    }
}

#[derive(Clone, PartialEq, Serialize, Deserialize)]
pub struct Config {
    pub pg: PgConfig,
    pub mqtt: MqttConfig,
    pub sink_mode: SinkMode,
    /// 点关闭按钮时的行为（老配置里没有这一项，默认直接退出）。
    #[serde(default)]
    pub close_action: CloseAction,
    #[serde(default)]
    pub theme: Theme,
    /// 界面语言（老配置里没有这一项，默认中文）。
    #[serde(default)]
    pub lang: Lang,
    pub auto_erase: bool,
    pub spool_on_failure: bool,
    pub device_name_filter: String,
    /// 蓝牙扫描超时（秒）。
    #[serde(default = "default_scan_timeout")]
    pub scan_timeout_secs: u32,
    pub last_device_address: Option<String>,
    pub last_device_name: Option<String>,
}

fn default_scan_timeout() -> u32 {
    45
}

impl Default for Config {
    fn default() -> Self {
        Self {
            pg: PgConfig::default(),
            mqtt: MqttConfig::default(),
            sink_mode: SinkMode::default(),
            close_action: CloseAction::default(),
            theme: Theme::default(),
            lang: Lang::default(),
            auto_erase: true,
            spool_on_failure: true,
            device_name_filter: "RaceBox".to_string(),
            scan_timeout_secs: default_scan_timeout(),
            last_device_address: None,
            last_device_name: None,
        }
    }
}

impl Config {
    pub fn uses_database(&self) -> bool {
        matches!(self.sink_mode, SinkMode::Database)
    }

    pub fn uses_mqtt(&self) -> bool {
        matches!(self.sink_mode, SinkMode::Mqtt)
    }
}

pub fn project_dirs() -> Option<directories::ProjectDirs> {
    directories::ProjectDirs::from("cn", "wongcw", "racewong")
}

pub fn base_dir() -> PathBuf {
    if let Some(pd) = project_dirs() {
        let dir = pd.config_dir().to_path_buf();
        let _ = std::fs::create_dir_all(&dir);
        return dir;
    }
    let fallback = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
    fallback
}

pub fn config_path() -> PathBuf {
    base_dir().join("config.json")
}

pub fn spool_dir() -> PathBuf {
    let dir = base_dir().join("spool");
    let _ = std::fs::create_dir_all(&dir);
    dir
}

pub fn log_path() -> PathBuf {
    base_dir().join("racewong.log")
}

pub fn load() -> Config {
    let path = config_path();
    let cfg = match std::fs::read_to_string(&path) {
        Ok(text) => match serde_json::from_str::<Config>(&text) {
            Ok(cfg) => cfg,
            Err(e) => {
                eprintln!("配置文件解析失败 {}, 使用默认配置: {e}", path.display());
                Config::default()
            }
        },
        Err(_) => Config::default(),
    };
    // 语言是全局状态，读配置的地方就是唯一入口，在这里同步最省事
    // （否则每个调用点都得记着自己 set 一次）
    crate::i18n::set(cfg.lang);
    cfg
}

pub fn save(cfg: &Config) -> std::io::Result<()> {
    let path = config_path();
    let text = serde_json::to_string_pretty(cfg).unwrap_or_else(|_| "{}".to_string());
    std::fs::write(&path, text)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sink_mode_roundtrip() {
        for mode in [SinkMode::Database, SinkMode::Mqtt] {
            let text = serde_json::to_string(&mode).unwrap();
            assert_eq!(serde_json::from_str::<SinkMode>(&text).unwrap(), mode);
        }
    }

    #[test]
    fn legacy_both_falls_back_to_database() {
        // 老配置里可能还是 "Both"：不能让它把整个 config.json 解析失败（那样会重置成默认配置）
        assert_eq!(
            serde_json::from_str::<SinkMode>("\"Both\"").unwrap(),
            SinkMode::Database
        );
        assert_eq!(
            serde_json::from_str::<SinkMode>("\"Mqtt\"").unwrap(),
            SinkMode::Mqtt
        );
    }
}


