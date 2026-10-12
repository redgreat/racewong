use serde::{Deserialize, Serialize};
use std::path::PathBuf;

pub const DEFAULT_MQTT_TOPIC: &str = "deskwong/racebox/data";

#[derive(Clone, Serialize, Deserialize)]
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

#[derive(Clone, Serialize, Deserialize)]
pub struct MqttConfig {
    pub broker: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub topic: String,
    pub client_id: String,
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

#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Debug)]
pub enum SinkMode {
    Database,
    Mqtt,
    Both,
}

impl Default for SinkMode {
    fn default() -> Self {
        SinkMode::Database
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Config {
    pub pg: PgConfig,
    pub mqtt: MqttConfig,
    pub sink_mode: SinkMode,
    #[serde(default)]
    pub theme: Theme,
    pub auto_erase: bool,
    pub spool_on_failure: bool,
    pub device_name_filter: String,
    pub last_device_address: Option<String>,
    pub last_device_name: Option<String>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            pg: PgConfig::default(),
            mqtt: MqttConfig::default(),
            sink_mode: SinkMode::default(),
            theme: Theme::default(),
            auto_erase: true,
            spool_on_failure: true,
            device_name_filter: "RaceBox".to_string(),
            last_device_address: None,
            last_device_name: None,
        }
    }
}

impl Config {
    pub fn uses_database(&self) -> bool {
        matches!(self.sink_mode, SinkMode::Database | SinkMode::Both)
    }

    pub fn uses_mqtt(&self) -> bool {
        matches!(self.sink_mode, SinkMode::Mqtt | SinkMode::Both)
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
    match std::fs::read_to_string(&path) {
        Ok(text) => match serde_json::from_str::<Config>(&text) {
            Ok(cfg) => cfg,
            Err(e) => {
                eprintln!("配置文件解析失败 {}, 使用默认配置: {e}", path.display());
                Config::default()
            }
        },
        Err(_) => Config::default(),
    }
}

pub fn save(cfg: &Config) -> std::io::Result<()> {
    let path = config_path();
    let text = serde_json::to_string_pretty(cfg).unwrap_or_else(|_| "{}".to_string());
    std::fs::write(&path, text)
}


