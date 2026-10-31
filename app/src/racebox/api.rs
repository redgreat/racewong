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

pub struct CloudClient {
    http: reqwest::Client,
    auth: Option<String>,
}

impl CloudClient {
    pub fn new(auth: Option<String>) -> Self {
        // 云接口不需要长连接复用，超时给宽一点：上传一段几百 KB 的会话
        // 在国内网络上可能要好几十秒。
        let http = reqwest::Client::builder()
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
            "platform": "Windows",
            "version": env!("CARGO_PKG_VERSION"),
            "devManufacturer": "racewong",
            "devModel": "desktop",
            "osVersion": std::env::consts::OS,
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
        let text = resp.text().await.map_err(|e| format!("读取响应失败: {e}"))?;
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
    pub async fn upload_session(
        &self,
        bytes: &[u8],
        if_unmodified_since: &str,
    ) -> Result<UploadedSession, String> {
        let cookie = self.cookie_header().ok_or("尚未登录")?;
        let resp = self
            .http
            .post(format!("{BASE}session"))
            .header("Cookie", cookie)
            .header("Content-Type", "application/octet-stream")
            .header("If-Unmodified-Since", if_unmodified_since)
            .body(bytes.to_vec())
            .send()
            .await
            .map_err(|e| format!("网络错误: {e}"))?;

        let status = resp.status();
        let text = resp.text().await.map_err(|e| format!("读取响应失败: {e}"))?;
        let env: UploadEnvelope = serde_json::from_str(&text)
            .map_err(|_| format!("HTTP {status}，响应: {}", truncate(&text, 200)))?;
        if env.error_code != 0 {
            return Err(if env.error_text.is_empty() {
                format!("上传被拒绝（HTTP {status}）")
            } else {
                env.error_text
            });
        }
        env.session.ok_or_else(|| "响应里没有 session".to_string())
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
