//! RaceBox 云 API 客户端。
//!
//! 只有两件事：登录、上传会话。
//!
//! 鉴权是 **Cookie** 而不是 Bearer：登录接口返回的 `auth` 要放进
//! `Cookie: racebox-api-auth=<auth>`，服务端只认这个（反编译 `BaseApiClient`
//! 确认过，它往 `CookieContainer` 里塞的就是这个名字）。reqwest 的 cookie
//! 功能需要额外开 feature，这里手工读写响应头，省一个依赖。

use serde::Deserialize;

const BASE: &str = "https://api.racebox.pro/v1/";
const COOKIE_NAME: &str = "racebox-api-auth";
/// 冒充官方 App 2.3.0：请求头、UA、登录上报的平台信息都用这一套。
/// 详见 `CloudClient::new` 的注释，以及 `doc/racebox-cloud-api.md` §1。
const APP_VERSION: &str = "2.3.0";
const USER_AGENT: &str = "RaceBox/2.3.0 (Android 14)";
/// 上传整段会话可能要几十秒（大文件），但也不能无限等。
const UPLOAD_TIMEOUT_SECS: u64 = 300;

/// 登录成功后拿到的东西。
pub struct LoginOk {
    pub auth: String,
    pub user_id: String,
    pub name: String,
    pub email: String,
}

/// 上传成功后的服务端回执（只取界面要显示的那几项）。
#[derive(Debug, Clone, Deserialize)]
pub struct UploadedSession {
    #[serde(default)]
    pub id: String,
    #[serde(default)]
    pub records: u32,
    #[serde(default)]
    pub duration: f64,
}

#[derive(Deserialize)]
struct LoginEnvelope {
    #[serde(default)]
    auth: String,
    #[serde(default)]
    error_code: i32,
    #[serde(default)]
    error_text: String,
    user: Option<LoginUser>,
}

#[derive(Deserialize)]
struct LoginUser {
    #[serde(default)]
    id: String,
    #[serde(default)]
    name: String,
    #[serde(default)]
    email: String,
}

#[derive(Deserialize)]
struct UploadEnvelope {
    session: Option<UploadedSession>,
    #[serde(default)]
    error_code: i32,
    #[serde(default)]
    error_text: String,
}

/// 云接口的错误。
///
/// `retry` 标明「再试一次有没有意义」：Cloudflare 502/520/524、429 限流、
/// 连接超时都是过几秒就好了；401 掉登录、400 参数不对，重试只是白等。
#[derive(Debug, Clone)]
pub struct CloudError {
    pub msg: String,
    pub retry: bool,
}

impl CloudError {
    /// 请求没走完（DNS 失败、连接被拒、超时）：换一次可能就好了。
    fn transient(msg: impl Into<String>) -> Self {
        Self {
            msg: msg.into(),
            retry: true,
        }
    }

    /// 请求本身有问题（没登录、响应缺字段）：重试也是一样的结果。
    fn permanent(msg: impl Into<String>) -> Self {
        Self {
            msg: msg.into(),
            retry: false,
        }
    }

    /// 按 HTTP 状态码判断：408 / 425 / 429 和 5xx 是服务端「现在不行」，
    /// 其余的 4xx 是我们请求本身有问题。
    fn from_status(status: u16, msg: impl Into<String>) -> Self {
        Self {
            msg: msg.into(),
            retry: matches!(status, 408 | 425 | 429) || status >= 500,
        }
    }
}

impl std::fmt::Display for CloudError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.msg)
    }
}

impl std::error::Error for CloudError {}

pub struct CloudClient {
    http: reqwest::Client,
    auth: Option<String>,
}

#[derive(Deserialize)]
struct CloudSessionList {
    #[serde(default)]
    error_code: i32,
    sessions: Vec<CloudSessionIndex>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CloudSessionIndex {
    pub device_serial_number: Option<u64>,
    pub index_in_the_day: Option<u8>,
    #[serde(rename = "dateTimeStartedUTC", alias = "dateTimeStartedUtc")]
    pub started: Option<serde_json::Value>,
    #[serde(rename = "timeZoneOffset", alias = "timezoneOffset")]
    pub tz_offset_minutes: Option<i16>,
}

impl CloudSessionIndex {
    pub fn started_at(&self) -> Option<i64> {
        let value = self.started.as_ref()?;
        if let Some(timestamp) = value.as_i64() {
            // 数值时间戳可用秒或毫秒表示，统一到毫秒排序。
            return Some(if timestamp.unsigned_abs() < 100_000_000_000 {
                timestamp * 1000
            } else {
                timestamp
            });
        }
        let text = value.as_str()?;
        if let Ok(date) = chrono::DateTime::parse_from_rfc3339(text) {
            return Some(date.timestamp_millis());
        }
        chrono::NaiveDateTime::parse_from_str(text, "%Y-%m-%dT%H:%M:%S%.f")
            .ok()
            .map(|date| date.and_utc().timestamp_millis())
    }
}

#[test]
fn cloud_session_index_decodes_utc_timestamp_and_sequence() {
    let item: CloudSessionIndex = serde_json::from_value(serde_json::json!({
        "deviceSerialNumber": 123,
        "indexInTheDay": 19,
        "dateTimeStartedUTC": "2026-11-23T00:05:00Z"
    })).unwrap();
    assert_eq!(item.device_serial_number, Some(123));
    assert_eq!(item.index_in_the_day, Some(19));
    assert_eq!(item.started_at(), Some(1_795_392_300_000));
}

impl CloudClient {
    pub fn new(auth: Option<String>) -> Self {
        // 官方 App（反编译 `BaseApiClient.PrepareRequestMessage`）对 racebox.pro 的
        // **每一个**请求都会带 `X-RaceBox-App: <App 版本>`，UA 也是 App 那套。
        // 实测：用这套头上传的会话，云端会补齐天气等元数据；之前只带 Cookie 上传的
        // 会话 meta 里 weather 一直是 null，网页版详情页也就没有可画的数据。
        // 服务端为什么按客户端标识区分处理没有公开文档，这里只做「和官方一致」。
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            "X-RaceBox-App",
            reqwest::header::HeaderValue::from_static(APP_VERSION),
        );
        // 云接口不需要长连接复用，超时给宽一点：上传一段几百 KB 的会话
        // 在国内网络上可能要好几十秒。
        let http = reqwest::Client::builder()
            .user_agent(USER_AGENT)
            .default_headers(headers)
            .timeout(std::time::Duration::from_secs(UPLOAD_TIMEOUT_SECS))
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());
        Self { http, auth }
    }

    fn cookie_header(&self) -> Option<String> {
        self.auth.as_ref().map(|a| format!("{COOKIE_NAME}={a}"))
    }

    /// 登录。成功后把 auth 记在自己身上，后续请求自动带 Cookie。
    pub async fn login(&mut self, email: &str, password: &str) -> Result<LoginOk, String> {
        // clientId 是设备唯一标识，服务端用它区分「同一账号的哪台设备」。
        // 我们不是真的设备，但接口要求必填，给个随机 GUID 即可。
        let body = serde_json::json!({
            "email": email.trim().to_lowercase(),
            "password": password,
            "clientId": uuid::Uuid::new_v4().to_string(),
            // 和 `tools/rbcloud.py`（已验证能拿到完整云端元数据）保持一致。
            // 用 Windows/desktop + 0.1.x 版本号时，服务端只做最小解析：
            // syncmeta 有 runsCount，但 weather 为 null，网页详情页一片空。
            "platform": "Android",
            "version": APP_VERSION,
            "devManufacturer": "samsung",
            "devModel": "SM-G991B",
            "osVersion": "Android 14",
        });
        let resp = self
            .http
            .post(format!("{BASE}login"))
            .json(&body)
            .send()
            .await
            .map_err(|e| format!("网络错误: {e}"))?;

        // auth 既可能出现在响应体里，也可能只在 Set-Cookie 里，两处都收。
        let from_cookie = extract_auth_cookie(resp.headers());
        let text = resp
            .text()
            .await
            .map_err(|e| format!("读取响应失败: {e}"))?;
        let env: LoginEnvelope =
            serde_json::from_str(&text).map_err(|e| format!("响应不是合法 JSON: {e}"))?;
        if env.error_code != 0 {
            return Err(if env.error_text.is_empty() {
                "登录失败（账号或密码不对）".to_string()
            } else {
                env.error_text
            });
        }
        let auth = if env.auth.is_empty() {
            from_cookie.ok_or_else(|| "响应里没有 auth".to_string())?
        } else {
            env.auth
        };
        self.auth = Some(auth.clone());
        let u = env.user.unwrap_or(LoginUser {
            id: String::new(),
            name: String::new(),
            email: email.to_string(),
        });
        Ok(LoginOk {
            auth,
            user_id: u.id,
            name: u.name,
            email: u.email,
        })
    }

    /// 上传一个已编码好的会话文件。
    ///
    /// `if_unmodified_since` 用文件的修改时间（RFC1123），服务端拿它做增量判断；
    /// 不传也能成功，但保持和 App 一致更稳妥。
    /// 空变更列表只读取云端元数据，不修改手机上传的会话。
    pub async fn session_indices(&self) -> Result<Vec<CloudSessionIndex>, CloudError> {
        let cookie = self.cookie_header().ok_or_else(|| CloudError::permanent("尚未登录"))?;
        let response = self.http.post(format!("{BASE}session/syncmeta"))
            .header("Cookie", cookie)
            .json(&serde_json::json!({"lastSynced": 0, "sessions": []}))
            .send().await.map_err(|e| CloudError::transient(format!("读取云会话列表失败: {e}")))?;
        let status = response.status().as_u16();
        if !(200..300).contains(&status) {
            return Err(CloudError::from_status(status, format!("读取云会话列表失败 (HTTP {status})")));
        }
        let response: CloudSessionList = response.json().await
            .map_err(|e| CloudError::transient(format!("无法解析云会话列表: {e}")))?;
        if response.error_code != 0 {
            return Err(CloudError::permanent("云端拒绝读取会话列表"));
        }
        Ok(response.sessions)
    }

    pub async fn upload_session(
        &self,
        bytes: Vec<u8>,
        if_unmodified_since: &str,
    ) -> Result<UploadedSession, CloudError> {
        let cookie = self
            .cookie_header()
            .ok_or_else(|| CloudError::permanent("尚未登录"))?;
        let resp = self
            .http
            .post(format!("{BASE}session"))
            .header("Cookie", cookie)
            .header("Content-Type", "application/octet-stream")
            .header("If-Unmodified-Since", if_unmodified_since)
            // reqwest 接管这块 Vec，不再从切片复制整段会话；大轨迹上传时
            // 峰值内存能少一份文件大小。
            .body(bytes)
            .send()
            .await
            .map_err(|e| CloudError::transient(format!("网络错误: {e}")))?;

        let status = resp.status().as_u16();
        let text = resp
            .text()
            .await
            .map_err(|e| CloudError::transient(format!("读取响应失败: {e}")))?;
        let env: UploadEnvelope = serde_json::from_str(&text).map_err(|_| {
            CloudError::from_status(
                status,
                format!("HTTP {status}，响应: {}", truncate(&text, 200)),
            )
        })?;
        if env.error_code != 0 {
            return Err(CloudError::from_status(
                status,
                if env.error_text.is_empty() {
                    format!("上传被拒绝（HTTP {status}）")
                } else {
                    env.error_text
                },
            ));
        }
        env.session
            .ok_or_else(|| CloudError::permanent("响应里没有 session"))
    }
}

/// 从 `Set-Cookie` 里取出 auth 值。
fn extract_auth_cookie(headers: &reqwest::header::HeaderMap) -> Option<String> {
    for v in headers.get_all(reqwest::header::SET_COOKIE).iter() {
        let Ok(s) = v.to_str() else { continue };
        let prefix = format!("{COOKIE_NAME}=");
        if let Some(rest) = s.strip_prefix(&prefix) {
            let value = rest.split(';').next().unwrap_or("").trim();
            if !value.is_empty() {
                return Some(value.to_string());
            }
        }
    }
    None
}

fn truncate(s: &str, n: usize) -> String {
    if s.len() <= n {
        return s.to_string();
    }
    let mut end = n;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}…", &s[..end])
}

/// 把文件修改时间格式化成 HTTP 的 RFC1123（`Sat, 31 Oct 2026 06:20:00 GMT`）。
pub fn rfc1123(t: std::time::SystemTime) -> String {
    let dt: chrono::DateTime<chrono::Utc> = t.into();
    dt.format("%a, %d %b %Y %H:%M:%S GMT").to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rfc1123_shape() {
        let t = std::time::UNIX_EPOCH + std::time::Duration::from_secs(1_793_398_597);
        let s = rfc1123(t);
        // 只校验形状：星期、日期、月份、时间、GMT
        assert!(s.ends_with("GMT"), "{s}");
        assert_eq!(s.len(), 29, "{s}");
    }

    #[test]
    fn truncate_keeps_utf8_boundary() {
        let s = "中文中文中文";
        let t = truncate(s, 7);
        assert!(t.ends_with('…'));
    }
}
