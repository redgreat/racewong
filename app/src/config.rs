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

/// RaceBox 云上传（官网 racebox.pro）。
///
/// 和 `sink_mode` 里的 PostgreSQL / MQTT 不是互斥关系：那两条是「把数据送到
/// 自己的库里」，这条是「同时往官网传一份」，所以单独一个开关。
#[derive(Clone, PartialEq, Serialize, Deserialize)]
pub struct CloudConfig {
    /// 同步时是否顺带往官网传一份。
    #[serde(default)]
    pub enabled: bool,
    /// 上传时写进会话文件的设备序列号。0 表示不指定（官网会显示成未知设备）。
    #[serde(default)]
    pub device_serial: u64,
    /// 会话头里的时区偏移（分钟）。+480 = UTC+8。
    #[serde(default = "default_tz_offset")]
    pub tz_offset_min: i16,
    /// 上次导入本地文件的目录，下次打开直接用。
    #[serde(default)]
    pub import_dir: String,
}

fn default_tz_offset() -> i16 {
    480
}

impl Default for CloudConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            device_serial: 0,
            tz_offset_min: default_tz_offset(),
            import_dir: String::new(),
        }
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
    /// RaceBox 云上传。老配置里没有这一项，默认关闭。
    #[serde(default)]
    pub cloud: CloudConfig,
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
            cloud: CloudConfig::default(),
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
        // UTF-8 BOM 会让 serde_json 直接报 "expected value at line 1 column 1"，
        // 然后整份配置被丢掉换成默认值。Windows 上的编辑器/PowerShell 很容易写出
        // 带 BOM 的 JSON，所以读进来先剥掉。
        Ok(text) => match serde_json::from_str::<Config>(text.trim_start_matches('\u{feff}')) {
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

/// 只把云上传那一段写回 `config.json`。
///
/// 云页面是「改动立即生效」，直接 [`save`] 整份配置会把设置页里用户还没点
/// 「保存设置」的改动一起落盘——那样设置页的「放弃修改」就失效了。
/// 这里读回磁盘上的原文，只替换 `cloud` 这个键，其余原样保留。
pub fn save_cloud(cloud: &CloudConfig) -> std::io::Result<()> {
    let path = config_path();
    // 原文读不回来就别写了：宁可这次云配置存不下，也不能把磁盘上那份配置清掉。
    let text = std::fs::read_to_string(&path)?;
    let mut value: serde_json::Value =
        serde_json::from_str(text.trim_start_matches('\u{feff}')).map_err(std::io::Error::other)?;
    if !value.is_object() {
        return Err(std::io::Error::other(format!(
            "配置文件格式异常: {}",
            path.display()
        )));
    }
    if let Some(obj) = value.as_object_mut() {
        obj.insert(
            "cloud".to_string(),
            serde_json::to_value(cloud).unwrap_or(serde_json::Value::Null),
        );
    }
    let text = serde_json::to_string_pretty(&value).unwrap_or_else(|_| "{}".to_string());
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
    fn bom_prefixed_json_still_parses() {
        // PowerShell 的 Set-Content -Encoding utf8 会在开头写 EF BB BF，
        // 不剥掉的话整份配置会被当成解析失败、静默退回默认值。
        let body = serde_json::to_string(&Config::default()).unwrap();
        let with_bom = format!("\u{feff}{body}");
        let parsed = serde_json::from_str::<Config>(with_bom.trim_start_matches('\u{feff}')).unwrap();
        assert_eq!(parsed.lang, Config::default().lang);
        // 反过来确认：不剥 BOM 确实会失败，这个测试才有意义
        assert!(serde_json::from_str::<Config>(&with_bom).is_err());
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


