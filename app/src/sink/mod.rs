pub mod mqtt;
pub mod pg;
pub mod racebox;
pub mod spool;

use crate::protocol::Record;
use crate::t;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

/// 上传进度回调：参数是**本段**已写入的记录数（从 0 开始累加，最后等于段内总数）。
///
/// 之所以按“段内条数”而不是全局条数：一段动辄几万条，上传要花好几秒，
/// 界面上能看见条数在涨，才不会让人以为卡死了。
///
/// 注意：trait 方法里不用这个别名写参数，而是把 `&(dyn Fn(..) + Send + Sync)`
/// 展开写——`async_trait` 展开时要自己给每个引用参数起生命周期名，
/// 类型别名里的 `'_` 会让它不好判断。
pub type ProgressFn<'a> = &'a (dyn Fn(usize) + Send + Sync);

#[async_trait]
pub trait DataSink: Send + Sync {
    fn name(&self) -> &'static str;

    async fn upload_session(
        &self,
        session: &Session,
        progress: &(dyn Fn(usize) + Send + Sync),
    ) -> SinkResult;
}

#[derive(Debug, Clone)]
pub struct Session {
    pub device: String,
    pub session_index: u32,
    pub records: Vec<Record>,
    pub duration_secs: f64,
    pub sync_id: [u8; 16],
    pub imp_stamp: String,
    pub sync_offset: usize,
    pub sync_total_upper: usize,
    pub sync_received_total: usize,
    pub sync_final: bool,
    /// 界面上显示的名字：`None` 时用记录时间拼出的默认名。
    ///
    /// 导入本地文件时填文件名——「哪一段上传成功了」看文件名比看时间戳直观。
    pub display: Option<String>,
}

impl Session {
    pub fn record_count(&self) -> usize {
        self.records.len()
    }

    pub fn sync_total(&self) -> usize {
        if self.sync_final {
            self.sync_received_total
        } else {
            self.sync_total_upper
        }
    }

    pub fn display_name(&self) -> String {
        if let Some(name) = &self.display {
            return name.clone();
        }
        if self.records.is_empty() {
            return t!("空数据段", "empty segment").to_string();
        }
        crate::protocol::session_file_name(&self.records[0], &self.records[self.records.len() - 1])
    }
}

#[derive(Debug)]
pub enum SinkError {
    Db(String),
    Mqtt(String),
    Cloud(String),
    Config(String),
}

impl std::fmt::Display for SinkError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SinkError::Db(e) => write!(f, "PostgreSQL: {e}"),
            SinkError::Mqtt(e) => write!(f, "MQTT: {e}"),
            SinkError::Cloud(e) => write!(f, "RaceBox: {e}"),
            SinkError::Config(e) => write!(f, "{}: {e}", t!("配置", "config")),
        }
    }
}

impl std::error::Error for SinkError {}

pub type SinkResult = Result<(), SinkError>;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SpoolMeta {
    pub device: String,
    pub session_index: u32,
    pub duration_secs: f64,
    pub sync_id: String,
    pub imp_stamp: String,
    pub sync_offset: usize,
    pub sync_total_upper: usize,
    pub sync_received_total: usize,
    pub sync_final: bool,
    pub file_name: String,
    pub record_count: usize,
}
