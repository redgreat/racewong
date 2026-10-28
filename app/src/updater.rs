use futures_util::StreamExt;
use semver::Version;
use serde::Deserialize;
use std::path::PathBuf;

const LATEST_RELEASE: &str = "https://api.github.com/repos/redgreat/racewong/releases/latest";

#[derive(Debug, Clone)]
pub struct UpdateInfo {
    pub version: Version,
    pub asset_name: String,
    pub download_url: String,
}

#[derive(Debug, Deserialize)]
struct Release {
    tag_name: String,
    assets: Vec<Asset>,
}

#[derive(Debug, Deserialize)]
struct Asset {
    name: String,
    browser_download_url: String,
}

fn wanted_asset(name: &str) -> bool {
    let lower = name.to_ascii_lowercase();
    #[cfg(target_os = "windows")]
    return lower.ends_with("-windows-x86_64.msi");
    #[cfg(target_os = "macos")]
    return lower.ends_with("-macos-aarch64.dmg");
    #[cfg(target_os = "linux")]
    return lower.ends_with("-linux-x86_64.deb");
    #[allow(unreachable_code)]
    false
}

pub async fn check() -> Result<Option<UpdateInfo>, String> {
    let client = reqwest::Client::builder()
        .user_agent(concat!("racewong-app/", env!("CARGO_PKG_VERSION")))
        .build()
        .map_err(|e| e.to_string())?;
    let release: Release = client
        .get(LATEST_RELEASE)
        .send()
        .await
        .map_err(|e| format!("GitHub 请求失败: {e}"))?
        .error_for_status()
        .map_err(|e| format!("GitHub 返回错误: {e}"))?
        .json()
        .await
        .map_err(|e| format!("Release 数据无效: {e}"))?;
    let latest = Version::parse(release.tag_name.trim_start_matches('v'))
        .map_err(|e| format!("Release 版本号无效: {e}"))?;
    let current =
        Version::parse(env!("CARGO_PKG_VERSION")).map_err(|e| format!("当前版本号无效: {e}"))?;
    if latest <= current {
        return Ok(None);
    }
    let asset = release
        .assets
        .into_iter()
        .find(|a| wanted_asset(&a.name))
        .ok_or_else(|| format!("v{latest} 没有适用于当前系统的安装包"))?;
    Ok(Some(UpdateInfo {
        version: latest,
        asset_name: asset.name,
        download_url: asset.browser_download_url,
    }))
}

pub async fn download(info: &UpdateInfo) -> Result<PathBuf, String> {
    let dir = std::env::temp_dir().join("racewong-update");
    tokio::fs::create_dir_all(&dir)
        .await
        .map_err(|e| e.to_string())?;
    let target = dir.join(&info.asset_name);
    let response = reqwest::Client::builder()
        .user_agent(concat!("racewong-app/", env!("CARGO_PKG_VERSION")))
        .build()
        .map_err(|e| e.to_string())?
        .get(&info.download_url)
        .send()
        .await
        .map_err(|e| format!("下载安装包失败: {e}"))?
        .error_for_status()
        .map_err(|e| format!("下载安装包失败: {e}"))?;
    let mut file = tokio::fs::File::create(&target)
        .await
        .map_err(|e| e.to_string())?;
    let mut stream = response.bytes_stream();
    use tokio::io::AsyncWriteExt;
    while let Some(chunk) = stream.next().await {
        file.write_all(&chunk.map_err(|e| e.to_string())?)
            .await
            .map_err(|e| e.to_string())?;
    }
    file.flush().await.map_err(|e| e.to_string())?;
    Ok(target)
}

pub fn launch_installer(path: &std::path::Path) -> Result<(), String> {
    #[cfg(target_os = "windows")]
    let mut command = {
        let mut c = std::process::Command::new("msiexec.exe");
        c.args(["/i", &path.to_string_lossy(), "/passive", "/norestart"]);
        c
    };
    #[cfg(target_os = "macos")]
    let mut command = {
        let mut c = std::process::Command::new("open");
        c.arg(path);
        c
    };
    #[cfg(target_os = "linux")]
    let mut command = {
        let mut c = std::process::Command::new("xdg-open");
        c.arg(path);
        c
    };
    command
        .spawn()
        .map(|_| ())
        .map_err(|e| format!("无法启动安装器: {e}"))
}

#[cfg(test)]
mod tests {
    #[test]
    fn platform_asset_filter_is_strict() {
        #[cfg(target_os = "windows")]
        assert!(super::wanted_asset("racewong-1.2.3-windows-x86_64.msi"));
        #[cfg(target_os = "macos")]
        assert!(super::wanted_asset("racewong-1.2.3-macos-aarch64.dmg"));
        assert!(!super::wanted_asset("racewong-1.2.3-windows-x86_64.zip"));
    }
}
