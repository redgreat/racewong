pub mod mqtt;
pub mod pg;
pub mod racebox;
pub mod spool;

use crate::protocol::Record;
use crate::t;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

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
    /// 云通道的错误。`retry` 由 [`crate::racebox::api::CloudError`] 按 HTTP 状态码判断：
    /// 5xx / 429 / 连接超时值得再试，401 掉登录、400 参数不对再试也没用。
    Cloud { msg: String, retry: bool },
    Config(String),
}

impl SinkError {
    pub fn cloud(msg: impl Into<String>, retry: bool) -> Self {
        SinkError::Cloud {
            msg: msg.into(),
            retry,
        }
    }

    /// 这条错误值不值得再试一次？
    ///
    /// 只对「过一会儿再试可能就好了」的错误点头。认证失败、SQL 语法错、
    /// 参数不对这类，重试一百次结果一样——让它们立刻失败，别让用户对着
    /// 一个注定失败的转圈等下去。
    pub fn retryable(&self) -> bool {
        match self {
            SinkError::Config(_) => false,
            SinkError::Cloud { retry, .. } => *retry,
            // 取消不是「失败」：重试它等于把用户按下的取消当成没发生
            SinkError::Mqtt(msg) => msg != crate::worker::CANCELLED,
            SinkError::Db(msg) => db_error_retryable(msg),
        }
    }
}

impl std::fmt::Display for SinkError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SinkError::Db(e) => write!(f, "PostgreSQL: {e}"),
            SinkError::Mqtt(e) => write!(f, "MQTT: {e}"),
            SinkError::Cloud { msg, .. } => write!(f, "RaceBox: {msg}"),
            SinkError::Config(e) => write!(f, "{}: {e}", t!("配置", "config")),
        }
    }
}

impl std::error::Error for SinkError {}

/// PostgreSQL 的错误值不值得重试。
///
/// `describe_db_error` 会在描述末尾附上 SQLSTATE（形如 `[23505]`），前两位是
/// 类别：08 连接异常、40 事务冲突、53 资源不足、57 服务端主动断开、XX 内部错误，
/// 都是「过一会儿再试可能就好了」；23 约束冲突、42 语法错误、28 认证失败，
/// 再试一百次也一样。
fn db_error_retryable(msg: &str) -> bool {
    match sqlstate(msg) {
        Some(code) => matches!(&code[..2], "08" | "40" | "53" | "57" | "XX"),
        // 连 SQLSTATE 都没有，说明请求根本没走到服务端（DNS / 连接被拒 / 超时）
        None => true,
    }
}

/// 从错误描述末尾取出 SQLSTATE：`... [23505]`。
fn sqlstate(msg: &str) -> Option<&str> {
    let body = msg.strip_suffix(']')?;
    let start = body.rfind('[')?;
    let code = &body[start + 1..];
    (code.len() == 5 && code.chars().all(|c| c.is_ascii_alphanumeric())).then_some(code)
}

pub type SinkResult = Result<(), SinkError>;

/// 一段数据最多上传几次（含第一次）。
///
/// 三次是折中：Cloudflare 520 / 网关抖动这类错误通常第二次就过了，
/// 再多就是在网络真的断掉时白白拖着用户等。
pub const UPLOAD_ATTEMPTS: usize = 3;

/// 本地缓存补传时每个文件最多试几次。
///
/// 补传是挨个过积压的文件，每个都退避两次会把整个启动拖住，
/// 所以比正常上传少一次——这一轮不行，下次启动还会再试。
pub const REPLAY_ATTEMPTS: usize = 2;

/// 第 `attempt` 次失败后等多久再试。
fn retry_delay(attempt: usize) -> Duration {
    Duration::from_secs(if attempt <= 1 { 2 } else { 6 })
}

/// 上传一段数据，遇到「可能只是抖了一下」的错误就退避重试。
///
/// 以前是一次失败就把整段丢进本地缓存：RaceBox 云偶尔回一个 Cloudflare 520，
/// 九万条数据立刻被标成「失败」，还得等下次启动才补传。现在先自己重试两次，
/// 520 这类错误往往第二次就过了。
///
/// 只重试 [`SinkError::retryable`] 点头的错误；用户取消时立即放弃，
/// 不再睡退避的那几秒。
pub async fn upload_with_retry(
    sink: &dyn DataSink,
    session: &Session,
    progress: &(dyn Fn(usize) + Send + Sync),
    cancel: &AtomicBool,
    attempts: usize,
    mut on_retry: impl FnMut(usize, &SinkError, Duration),
) -> SinkResult {
    let mut attempt = 1;
    loop {
        match sink.upload_session(session, progress).await {
            Ok(()) => return Ok(()),
            Err(e) => {
                if attempt >= attempts || !e.retryable() || cancel.load(Ordering::SeqCst) {
                    return Err(e);
                }
                let delay = retry_delay(attempt);
                on_retry(attempt, &e, delay);
                if !sleep_cancellable(delay, cancel).await {
                    return Err(e);
                }
                attempt += 1;
            }
        }
    }
}

/// 可被取消打断的等待：用户按了取消，不该还在这儿睡满 6 秒。
async fn sleep_cancellable(delay: Duration, cancel: &AtomicBool) -> bool {
    let deadline = Instant::now() + delay;
    while Instant::now() < deadline {
        if cancel.load(Ordering::SeqCst) {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    !cancel.load(Ordering::SeqCst)
}

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
