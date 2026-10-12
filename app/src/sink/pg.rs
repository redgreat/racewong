use super::{DataSink, Session, SinkError, SinkResult};
use crate::config::PgConfig;
use crate::protocol::{session_file_name, Record};
use async_trait::async_trait;
use tokio_postgres::NoTls;

const LC_COLUMNS: usize = 29;
const PAGE_SIZE: usize = 1000;

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

pub struct PgSink {
    cfg: PgConfig,
    client: tokio::sync::Mutex<Option<tokio_postgres::Client>>,
}

fn lc_insert_sql(rows: usize) -> String {
    let mut sql = String::from(
        "insert into lc_racebox(itow, imp_stamp, year, month, day, hour, minute, second, \
         time_accuracy, nanoseconds, fix_status, numberof_svs, longitude, latitude, wgs_altitude, \
         msl_altitude, horizontal_accuracy, vertical_accuracy, speed, heading, speed_accuracy, \
         heading_accuracy, pdop, gforce_x, gforce_y, gforce_z, rotation_rate_x, rotation_rate_y, \
         rotation_rate_z) values ",
    );
    for row in 0..rows {
        if row > 0 {
            sql.push(',');
        }
        let base = row * LC_COLUMNS + 1;
        let placeholders: Vec<String> = (0..LC_COLUMNS).map(|i| format!("${}", base + i)).collect();
        sql.push('(');
        sql.push_str(&placeholders.join(","));
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

impl PgSink {
    pub fn new(cfg: PgConfig) -> Self {
        Self {
            cfg,
            client: tokio::sync::Mutex::new(None),
        }
    }



    async fn connect_locked(&self, guard: &mut Option<tokio_postgres::Client>) -> Result<(), SinkError> {
        if !self.cfg.ready() {
            return Err(SinkError::Config("PostgreSQL 配置不完整".to_string()));
        }
        if guard.is_some() {
            return Ok(());
        }
        let mut pg_cfg = tokio_postgres::Config::new();
        pg_cfg
            .host(&self.cfg.host)
            .port(self.cfg.port)
            .dbname(&self.cfg.database)
            .user(&self.cfg.user)
            .password(&self.cfg.password);
        let (client, connection) = pg_cfg
            .connect(NoTls)
            .await
            .map_err(db_err)?;
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                log::error!("PostgreSQL 连接任务结束: {e}");
            }
        });
        *guard = Some(client);
        Ok(())
    }
}

async fn session_exists(client: &tokio_postgres::Client, file_name: &str) -> Result<bool, SinkError> {
    let row = client
        .query_one("select count(1) from imp_racebox where file_name = $1", &[&file_name])
        .await
        .map_err(db_err)?;
    let count: i64 = row.get(0);
    Ok(count > 0)
}

#[async_trait]
impl DataSink for PgSink {
    fn name(&self) -> &'static str {
        "PostgreSQL"
    }

    async fn upload_session(&self, session: &Session) -> SinkResult {
        if session.records.is_empty() {
            return Ok(());
        }
        let mut guard = self.client.lock().await;
        self.connect_locked(&mut guard).await?;
        let client = match guard.as_ref() {
            Some(c) => c,
            None => return Err(SinkError::Db("连接不可用".to_string())),
        };
        let first = &session.records[0];
        let last = &session.records[session.records.len() - 1];
        let file_name = session_file_name(first, last);

        if session_exists(client, &file_name).await? {
            log::info!("文件 {file_name} 数据库中已存在，跳过");
            return Ok(());
        }

        client
            .execute(
                "insert into imp_racebox(imp_stamp, file_name, duration) values ($1::uuid, $2, $3)",
                &[&session.imp_stamp, &file_name, &(session.duration_secs as i32)],
            )
            .await
            .map_err(db_err)?;

        let mut inserted = 0usize;
        for chunk in session.records.chunks(PAGE_SIZE) {
            let sql = lc_insert_sql(chunk.len());
            let mut owned: Vec<Box<dyn tokio_postgres::types::ToSql + Sync + Send>> =
                Vec::with_capacity(chunk.len() * LC_COLUMNS);
            for r in chunk {
                let record: &Record = r;
                push_param(&mut owned, record.itow as i32);
                push_param(&mut owned, session.imp_stamp.clone());
                push_param(&mut owned, record.year as i16);
                push_param(&mut owned, record.month as i16);
                push_param(&mut owned, record.day as i16);
                push_param(&mut owned, record.hour as i16);
                push_param(&mut owned, record.minute as i16);
                push_param(&mut owned, record.second as i16);
                push_param(&mut owned, record.time_accuracy as i32);
                push_param(&mut owned, record.nanoseconds);
                push_param(&mut owned, record.fix_status as i16);
                push_param(&mut owned, record.satellites as i16);
                push_param(&mut owned, record.longitude);
                push_param(&mut owned, record.latitude);
                push_param(&mut owned, record.wgs_altitude);
                push_param(&mut owned, record.msl_altitude);
                push_param(&mut owned, record.horizontal_accuracy);
                push_param(&mut owned, record.vertical_accuracy);
                push_param(&mut owned, record.speed_kmh);
                push_param(&mut owned, record.heading);
                push_param(&mut owned, record.speed_accuracy as i32);
                push_param(&mut owned, record.heading_accuracy);
                push_param(&mut owned, record.pdop as i32);
                push_param(&mut owned, record.gforce_x);
                push_param(&mut owned, record.gforce_y);
                push_param(&mut owned, record.gforce_z);
                push_param(&mut owned, record.rotation_rate_x);
                push_param(&mut owned, record.rotation_rate_y);
                push_param(&mut owned, record.rotation_rate_z);
            }
            let mut params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> =
                Vec::with_capacity(owned.len());
            for b in &owned {
                let p: &(dyn tokio_postgres::types::ToSql + Sync) = b.as_ref();
                params.push(p);
            }
            let rows = client
                .execute(&sql, &params)
                .await
                .map_err(db_err)?;
            inserted += rows as usize;
        }
        log::info!("PostgreSQL 写入完成: {file_name}, {inserted} 条");
        Ok(())
    }
}
fn db_err(e: tokio_postgres::Error) -> SinkError {
    SinkError::Db(describe_db_error(&e))
}
