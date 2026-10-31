//! RaceBox 云端账号的本地存储。
//!
//! 为什么不塞进 `config.json`：`config.json` 是给用户看的、可能被拷来拷去的
//! 配置，而这里存着明文密码和 auth token。单独一个文件，权限也收紧，
//! 出问题时删掉它等于退出登录。

use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Clone, Default, Serialize, Deserialize)]
pub struct CloudAccount {
    #[serde(default)]
    pub email: String,
    /// 明文密码。只为了「重启后自动续期 auth」，auth 过期就得重新登录。
    #[serde(default)]
    pub password: String,
    /// 登录拿到的 token，有效期 1 天。
    #[serde(default)]
    pub auth: String,
    #[serde(default)]
    pub user_id: String,
    #[serde(default)]
    pub name: String,
}

impl CloudAccount {
    pub fn logged_in(&self) -> bool {
        !self.auth.is_empty()
    }

    /// 界面上显示的名字：优先昵称，其次邮箱前缀。
    pub fn display_name(&self) -> String {
        if !self.name.is_empty() {
            return self.name.clone();
        }
        self.email
            .split('@')
            .next()
            .unwrap_or("")
            .to_string()
    }
}

pub fn path() -> PathBuf {
    crate::config::base_dir().join("cloud.json")
}

pub fn load() -> CloudAccount {
    let p = path();
    match std::fs::read_to_string(&p) {
        Ok(text) => serde_json::from_str(&text).unwrap_or_default(),
        Err(_) => CloudAccount::default(),
    }
}

pub fn save(account: &CloudAccount) -> std::io::Result<()> {
    let p = path();
    let text = serde_json::to_string_pretty(account).unwrap_or_else(|_| "{}".to_string());
    std::fs::write(&p, text)?;
    // 里面是明文密码，Windows 上靠用户目录的默认 ACL，
    // Unix 上顺手收紧到 0600（失败也不影响功能）。
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(&p, std::fs::Permissions::from_mode(0o600));
    }
    Ok(())
}

pub fn clear() -> std::io::Result<()> {
    let p = path();
    if p.exists() {
        std::fs::remove_file(p)?;
    }
    Ok(())
}
