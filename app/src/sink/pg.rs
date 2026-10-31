use super::{DataSink, ProgressFn, Session, SinkError, SinkResult};
use crate::config::PgConfig;
use crate::protocol::{session_file_name, Record, RECORD_SIZE};
use crate::{t, tr};
use async_trait::async_trait;
use std::collections::HashMap;
use std::time::Instant;
use tokio_postgres::NoTls;

const PAGE_SIZE: usize = 1000;

const IMP_SQL: &str =
    "insert into imp_racebox(imp_stamp, file_name, duration) values ($1::text::uuid, $2, $3::int4)";

/// lc_racebox 的列顺序，以及每列参数在 SQL 里的**显式类型转换**。
///
/// 这些转换不是可有可无的装饰：tokio-postgres 不会替你做类型提升，
/// Postgres 又会按目标列去推断参数类型（imp_stamp → uuid、经纬度/速度等 → numeric），
/// 而 Rust 侧的值是 String / f64，两边对不上时请求还没发出去就在“编码参数”阶段失败，
/// 报错就是 `error serializing parameter N`（参数从 0 开始编号）。
/// pg 直连每次都是“连接成功、上传全失败、转本地缓存”，根因就在这里。
///
/// - int 列     → Rust i32    → `$n::int4`
/// - uuid 列    → Rust String → `$n::text::uuid`
/// - numeric 列 → Rust f64    → `$n::float8::numeric`
const LC_COLUMNS: [(&str, &str); 29] = [
    ("itow", "::int4"),
    ("imp_stamp", "::text::uuid"),
    ("year", "::int4"),
    ("month", "::int4"),
    ("day", "::int4"),
    ("hour", "::int4"),
    ("minute", "::int4"),
    ("second", "::int4"),
    ("time_accuracy", "::int4"),
    ("nanoseconds", "::int4"),
    ("fix_status", "::int4"),
    ("numberof_svs", "::int4"),
    ("longitude", "::float8::numeric"),
    ("latitude", "::float8::numeric"),
    ("wgs_altitude", "::float8::numeric"),
    ("msl_altitude", "::float8::numeric"),
    ("horizontal_accuracy", "::float8::numeric"),
    ("vertical_accuracy", "::float8::numeric"),
    ("speed", "::float8::numeric"),
    ("heading", "::float8::numeric"),
    ("speed_accuracy", "::int4"),
    // 表里 heading_accuracy 是 int 列（精度 1e-5 的角秒值在这里被截成整数，
    // 和原先 psycopg2 直接写 float 时的行为一致），所以按 int 送。
    ("heading_accuracy", "::int4"),
    ("pdop", "::int4"),
    ("gforce_x", "::float8::numeric"),
    ("gforce_y", "::float8::numeric"),
    ("gforce_z", "::float8::numeric"),
    ("rotation_rate_x", "::float8::numeric"),
    ("rotation_rate_y", "::float8::numeric"),
    ("rotation_rate_z", "::float8::numeric"),
];

/// 一条已建立的连接，外加它上面预处理过的语句。
///
/// 为什么要缓存预处理语句：一段数据动辄几万条，按 1000 行一批算，
/// 每批是 1000 × 29 = 29000 个占位符。如果每批都直接把 SQL 字符串丢给
/// `execute`，tokio-postgres 会先做一次 prepare（解析 + 规划），
/// 等于把这条两万多参数的巨型语句反复解析几十遍。实测一段 5 万条的记录
/// 要跑三四分钟，用户看到的就是“点取消半天之后才冒出上传记录”。
/// 按批大小把 Statement 缓存下来后，解析和规划只发生一次。
struct Conn {
    client: tokio_postgres::Client,
    stmts: HashMap<usize, tokio_postgres::Statement>,
}

impl Conn {
    fn new(client: tokio_postgres::Client) -> Self {
        Self {
            client,
            stmts: HashMap::new(),
        }
    }
}

pub struct PgSink {
    cfg: PgConfig,
    client: tokio::sync::Mutex<Option<Conn>>,
}

/// tokio-postgres 的 Db 错误 Display 只有 "db error"，这里把服务端的真实信息翻出来。
pub fn describe_db_error(e: &tokio_postgres::Error) -> String {
    if let Some(db) = e.as_db_error() {
        let mut s = db.message().to_string();
        if let Some(detail) = db.detail() {
            s.push_str(&format!("（{detail}）"));
        }
        if let Some(hint) = db.hint() {
            s.push_str(&format!(" 提示: {hint}"));
        }
        return format!("{s} [{}]", db.code().code());
    }
    e.to_string()
}

fn db_err(e: tokio_postgres::Error) -> SinkError {
    SinkError::Db(describe_db_error(&e))
}

/// 建立连接，并把 connection 任务挂到后台（不然连接会一直不推进）。
async fn connect(cfg: &PgConfig) -> Result<tokio_postgres::Client, SinkError> {
    if !cfg.ready() {
        return Err(SinkError::Config(
            t!("PostgreSQL 配置不完整", "PostgreSQL settings are incomplete").to_string(),
        ));
    }
    let mut pg_cfg = tokio_postgres::Config::new();
    pg_cfg
        .host(&cfg.host)
        .port(cfg.port)
        .dbname(&cfg.database)
        .user(&cfg.user)
        .password(&cfg.password);
    let (client, connection) = pg_cfg.connect(NoTls).await.map_err(db_err)?;
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            log::error!("PostgreSQL 连接任务结束: {e}");
        }
    });
    Ok(client)
}

fn lc_insert_sql(rows: usize) -> String {
    let columns: Vec<&str> = LC_COLUMNS.iter().map(|(name, _)| *name).collect();
    let mut sql = format!("insert into lc_racebox({}) values ", columns.join(", "));
    for row in 0..rows {
        if row > 0 {
            sql.push(',');
        }
        let base = row * LC_COLUMNS.len() + 1;
        sql.push('(');
        for (i, (_, cast)) in LC_COLUMNS.iter().enumerate() {
            if i > 0 {
                sql.push(',');
            }
            sql.push_str(&format!("${}{}", base + i, cast));
        }
        sql.push(')');
    }
    sql
}

fn push_param(
    owned: &mut Vec<Box<dyn tokio_postgres::types::ToSql + Sync + Send>>,
    value: impl tokio_postgres::types::ToSql + Sync + Send + 'static,
) {
    owned.push(Box::new(value));
}

async fn session_exists(
    client: &tokio_postgres::Client,
    file_name: &str,
) -> Result<bool, SinkError> {
    let file_name = file_name.to_string();
    let row = client
        .query_one(
            "select count(1) from imp_racebox where file_name = $1",
            &[&file_name],
        )
        .await
        .map_err(db_err)?;
    let count: i64 = row.get(0);
    Ok(count > 0)
}

/// 写入一段数据：先记导入标记，再按 1000 条一批写明细。
/// 事务由调用方控制（正式上传用 begin/commit，自检用 begin/rollback）。
async fn insert_session(
    conn: &mut Conn,
    session: &Session,
    file_name: &str,
    progress: ProgressFn<'_>,
) -> Result<usize, SinkError> {
    let file_name = file_name.to_string();
    let duration = session.duration_secs as i32;
    conn.client
        .execute(IMP_SQL, &[&session.imp_stamp, &file_name, &duration])
        .await
        .map_err(db_err)?;

    let total = session.records.len();
    let mut inserted = 0usize;
    for chunk in session.records.chunks(PAGE_SIZE) {
        let rows_in_chunk = chunk.len();
        // 按批大小缓存预处理语句：正常段每批都是 PAGE_SIZE，整段只 prepare 一次
        if !conn.stmts.contains_key(&rows_in_chunk) {
            let stmt = conn
                .client
                .prepare(&lc_insert_sql(rows_in_chunk))
                .await
                .map_err(db_err)?;
            conn.stmts.insert(rows_in_chunk, stmt);
        }
        let mut owned: Vec<Box<dyn tokio_postgres::types::ToSql + Sync + Send>> =
            Vec::with_capacity(rows_in_chunk * LC_COLUMNS.len());
        for r in chunk {
            let record: &Record = r;
            // 顺序必须与 LC_COLUMNS 完全一致
            push_param(&mut owned, record.itow as i32); // itow
            push_param(&mut owned, session.imp_stamp.clone()); // imp_stamp
            push_param(&mut owned, record.year as i32); // year
            push_param(&mut owned, record.month as i32); // month
            push_param(&mut owned, record.day as i32); // day
            push_param(&mut owned, record.hour as i32); // hour
            push_param(&mut owned, record.minute as i32); // minute
            push_param(&mut owned, record.second as i32); // second
            push_param(&mut owned, record.time_accuracy as i32); // time_accuracy
            push_param(&mut owned, record.nanoseconds); // nanoseconds
            push_param(&mut owned, record.fix_status as i32); // fix_status
            push_param(&mut owned, record.satellites as i32); // numberof_svs
            push_param(&mut owned, record.longitude); // longitude
            push_param(&mut owned, record.latitude); // latitude
            push_param(&mut owned, record.wgs_altitude); // wgs_altitude
            push_param(&mut owned, record.msl_altitude); // msl_altitude
            push_param(&mut owned, record.horizontal_accuracy); // horizontal_accuracy
            push_param(&mut owned, record.vertical_accuracy); // vertical_accuracy
            push_param(&mut owned, record.speed_kmh); // speed
            push_param(&mut owned, record.heading); // heading
            push_param(&mut owned, record.speed_accuracy as i32); // speed_accuracy
            push_param(&mut owned, record.heading_accuracy.round() as i32); // heading_accuracy
            push_param(&mut owned, record.pdop as i32); // pdop
            push_param(&mut owned, record.gforce_x); // gforce_x
            push_param(&mut owned, record.gforce_y); // gforce_y
            push_param(&mut owned, record.gforce_z); // gforce_z
            push_param(&mut owned, record.rotation_rate_x); // rotation_rate_x
            push_param(&mut owned, record.rotation_rate_y); // rotation_rate_y
            push_param(&mut owned, record.rotation_rate_z); // rotation_rate_z
        }
        let mut params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> =
            Vec::with_capacity(owned.len());
        for b in &owned {
            let p: &(dyn tokio_postgres::types::ToSql + Sync) = b.as_ref();
            params.push(p);
        }
        // 这里只借用 conn 的两个不同字段，互不冲突
        let stmt = match conn.stmts.get(&rows_in_chunk) {
            Some(s) => s,
            None => {
                return Err(SinkError::Db(
                    t!("预处理语句丢失", "prepared statement missing").to_string(),
                ))
            }
        };
        let rows = conn.client.execute(stmt, &params).await.map_err(db_err)?;
        inserted += rows as usize;
        progress(inserted.min(total));
    }
    Ok(inserted)
}

/// 自检用的假记录（不会留在库里，整个事务会回滚）。
fn probe_record(itow: u32) -> Record {
    Record {
        raw: [0u8; RECORD_SIZE],
        itow,
        year: 2026,
        month: 1,
        day: 1,
        hour: 0,
        minute: 0,
        second: 0,
        time_accuracy: 0,
        nanoseconds: 0,
        fix_status: 3,
        satellites: 8,
        longitude: 0.0,
        latitude: 0.0,
        wgs_altitude: 0.0,
        msl_altitude: 0.0,
        horizontal_accuracy: 0.0,
        vertical_accuracy: 0.0,
        speed_kmh: 0.0,
        heading: 0.0,
        speed_accuracy: 0,
        heading_accuracy: 0.0,
        pdop: 0,
        gforce_x: 0.0,
        gforce_y: 0.0,
        gforce_z: 0.0,
        rotation_rate_x: 0.0,
        rotation_rate_y: 0.0,
        rotation_rate_z: 0.0,
    }
}

fn probe_session() -> Session {
    Session {
        device: "probe".to_string(),
        session_index: 0,
        records: vec![probe_record(1), probe_record(2)],
        duration_secs: 0.0,
        sync_id: [0u8; 16],
        imp_stamp: uuid::Uuid::new_v4().to_string(),
        sync_offset: 0,
        sync_total_upper: 0,
        sync_received_total: 0,
        sync_final: true,
        display: None,
    }
}

impl PgSink {
    pub fn new(cfg: PgConfig) -> Self {
        Self {
            cfg,
            client: tokio::sync::Mutex::new(None),
        }
    }

    async fn connect_locked(&self, guard: &mut Option<Conn>) -> Result<(), SinkError> {
        if !self.cfg.ready() {
            return Err(SinkError::Config(
                t!("PostgreSQL 配置不完整", "PostgreSQL settings are incomplete").to_string(),
            ));
        }
        if guard.is_some() {
            return Ok(());
        }
        *guard = Some(Conn::new(connect(&self.cfg).await?));
        Ok(())
    }

    /// 连接 + 写入自检：连库 → 开事务 → 用两条假记录走一遍真实写入 → 回滚。
    ///
    /// 以前“测试连接”只跑 `select 1`，只能证明网络和账号通了；
    /// 表缺列、列类型对不上、没有写权限这些都要等到真正上传时才炸，
    /// 而那时候数据已经从设备下载完、只能转本地缓存。这里把整条写入链路提前跑一遍。
    ///
    /// 现在它还被当成“同步前的连通性闸门”用：PG 不通就直接不让开始下载。
    pub async fn probe(cfg: &PgConfig) -> Result<String, SinkError> {
        let client = connect(cfg).await?;
        let mut conn = Conn::new(client);
        conn.client.batch_execute("begin").await.map_err(db_err)?;
        let session = probe_session();
        let file_name = session_file_name(
            &session.records[0],
            &session.records[session.records.len() - 1],
        );
        let outcome = insert_session(&mut conn, &session, &file_name, &|_: usize| {}).await;
        let _ = conn.client.batch_execute("rollback").await;
        let rows = outcome?;
        Ok(tr!(
            "写入自检通过，试写 {} 条后已回滚",
            "write self-test passed, {} rows written then rolled back",
            rows
        ))
    }
}

/// 真正写一段（含事务），单独拆出来是为了出错时能把整条连接丢掉重连。
async fn upload_inner(
    conn: &mut Conn,
    session: &Session,
    progress: ProgressFn<'_>,
) -> SinkResult {
    let first = &session.records[0];
    let last = &session.records[session.records.len() - 1];
    let file_name = session_file_name(first, last);

    if session_exists(&conn.client, &file_name).await? {
        log::info!("文件 {file_name} 数据库中已存在，跳过");
        return Ok(());
    }

    // 导入标记和明细放在同一个事务里：中途失败整段回滚。
    // 否则会留下“导入标记已写、明细只写了一半”的状态，
    // 下次同步看到 file_name 已存在就直接跳过，这段数据就永久丢了。
    conn.client.batch_execute("begin").await.map_err(db_err)?;
    let started = Instant::now();
    match insert_session(conn, session, &file_name, progress).await {
        Ok(inserted) => {
            if let Err(e) = conn.client.batch_execute("commit").await {
                let _ = conn.client.batch_execute("rollback").await;
                return Err(db_err(e));
            }
            let secs = started.elapsed().as_secs_f64().max(0.001);
            // 把吞吐打进日志文件：下次再觉得慢，可以直接看这条定位
            // （是网络慢、还是服务端慢、还是批太小）
            log::info!(
                "PostgreSQL 写入完成: {file_name}, {inserted} 条, 耗时 {secs:.1}s ({:.0} 条/秒)",
                inserted as f64 / secs
            );
            Ok(())
        }
        Err(e) => {
            let _ = conn.client.batch_execute("rollback").await;
            Err(e)
        }
    }
}

#[async_trait]
impl DataSink for PgSink {
    fn name(&self) -> &'static str {
        "PostgreSQL"
    }

    async fn upload_session(
        &self,
        session: &Session,
        progress: &(dyn Fn(usize) + Send + Sync),
    ) -> SinkResult {
        if session.records.is_empty() {
            return Ok(());
        }
        // 串行化：一个 PgSink 只有一条连接，预处理语句也挂在它上面
        let mut guard = self.client.lock().await;
        self.connect_locked(&mut guard).await?;
        let outcome = match guard.as_mut() {
            Some(conn) => upload_inner(conn, session, progress).await,
            None => {
                return Err(SinkError::Db(
                    t!("连接不可用", "connection unavailable").to_string(),
                ))
            }
        };
        if outcome.is_err() {
            // 出错就把连接丢掉，下一段重新连。
            // 否则一条已经半死的连接会让后面所有段全部失败（然后全部转本地缓存）。
            *guard = None;
        }
        outcome
    }
}
