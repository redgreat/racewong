use futures_util::StreamExt;
use semver::Version;
use serde::Deserialize;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

const LATEST_RELEASE: &str = "https://api.github.com/repos/redgreat/racewong/releases/latest";

/// GitHub 未认证 API 每小时限额很紧，撞上 403 后每次启动都重试只会一直撞，
/// 结果还会在「关于」窗口留一条报错。自动检查失败后先歇半小时；
/// 用户手动点「检查并更新」不受这个冷却限制。
const CHECK_COOLDOWN_SECS: u64 = 30 * 60;

#[derive(Debug, Clone)]
pub struct UpdateInfo {
    pub version: Version,
    pub asset_name: String,
    pub download_url: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum UpdateErrorKind {
    /// 网不通、DNS、超时、GitHub 限流 / 服务异常、返回数据没法解析。
    /// 用户什么都没做错，界面不该为它弹东西。
    Unavailable,
    /// 找到了新版本但没有本机安装包，或者下载 / 安装没成：
    /// 需要用户换个方式处理，给一句干净的中文短句。
    Actionable,
}

/// 更新失败的两层信息。
///
/// 以前 `check()` / `download()` 直接把 `reqwest` 的错误文本塞进 `String`
/// 返回，界面就原样显示「GitHub 返回错误: HTTP status client error
/// (403 rate limit exceeded) for url (https://api.github.com/…)」——
/// 带 URL、带英文状态码，用户既看不懂也无从下手。
///
/// 现在原始错误只走 `detail()` 进日志，界面最多拿到 `user_message()`
/// 里的短句；其中 `Unavailable` 连短句都不显示。
#[derive(Debug, Clone)]
pub struct UpdateError {
    kind: UpdateErrorKind,
    message: String,
    detail: String,
}

impl UpdateError {
    fn unavailable(detail: impl Into<String>) -> Self {
        Self {
            kind: UpdateErrorKind::Unavailable,
            message: crate::t!(
                "暂时无法检查更新，请稍后重试",
                "Can't check for updates right now; please try again later"
            )
            .to_string(),
            detail: detail.into(),
        }
    }

    fn download_failed(detail: impl Into<String>) -> Self {
        Self {
            kind: UpdateErrorKind::Actionable,
            message: crate::t!(
                "更新下载失败，请稍后重试或手动下载安装",
                "Update download failed; retry or install manually"
            )
            .to_string(),
            detail: detail.into(),
        }
    }

    fn no_asset(version: &Version) -> Self {
        Self {
            kind: UpdateErrorKind::Actionable,
            message: crate::tr!(
                "v{} 暂无适用于当前系统的安装包",
                "No installer for this platform in v{}",
                version
            ),
            detail: format!("v{version} 没有适用于当前系统的安装包"),
        }
    }

    /// `launch_installer()` 的底层错误（路径、IO、平台）交给这里包一层。
    pub fn install_failed(detail: impl Into<String>) -> Self {
        Self {
            kind: UpdateErrorKind::Actionable,
            message: crate::t!(
                "更新安装失败，请稍后重试或手动下载安装",
                "Update install failed; retry or install manually"
            )
            .to_string(),
            detail: detail.into(),
        }
    }

    /// 环境性失败：界面一律不弹，只在日志里留一句。
    pub fn is_transient(&self) -> bool {
        self.kind == UpdateErrorKind::Unavailable
    }

    /// 给用户看的短句：不含 URL、HTTP 状态码，也不含第三方库的英文报错。
    pub fn user_message(&self) -> &str {
        &self.message
    }

    /// 排障用的原始信息，只写日志。
    pub fn detail(&self) -> &str {
        &self.detail
    }
}

impl std::fmt::Display for UpdateError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
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

/// 上一次「已经尝试装过哪个版本」的记号文件。
///
/// 静默安装失败时升级脚本会把旧版本重新拉起来，而旧版本一启动又会自动更新，
/// 于是 `下载 → 退出 → 装失败 → 重启 → 再下载` 转圈。装之前记一笔，
/// 下次自动检查看到「这一版已经试过了、现在还是旧版」就直接当没有更新。
fn attempted_marker() -> PathBuf {
    std::env::temp_dir()
        .join("racewong-update")
        .join("attempted.txt")
}

fn already_attempted(version: &Version) -> bool {
    std::fs::read_to_string(attempted_marker())
        .map(|s| s.trim() == version.to_string())
        .unwrap_or(false)
}

pub fn mark_attempted(version: &Version) {
    let path = attempted_marker();
    if let Some(dir) = path.parent() {
        let _ = std::fs::create_dir_all(dir);
    }
    let _ = std::fs::write(path, version.to_string());
}

/// 上次「自动检查连不上 GitHub」的时间戳。
fn check_failure_marker() -> PathBuf {
    std::env::temp_dir()
        .join("racewong-update")
        .join("check-failed.txt")
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn mark_check_failed() {
    let path = check_failure_marker();
    if let Some(dir) = path.parent() {
        let _ = std::fs::create_dir_all(dir);
    }
    let _ = std::fs::write(path, now_secs().to_string());
}

fn clear_check_failure() {
    let _ = std::fs::remove_file(check_failure_marker());
}

/// 抽成纯函数好测：时钟回拨（`last > now`）时 `saturating_sub` 得 0，
/// 相当于「刚失败过」，跳过这一次自动检查，下次启动再试。
fn within_cooldown(now: u64, last: u64) -> bool {
    now.saturating_sub(last) < CHECK_COOLDOWN_SECS
}

fn check_failed_recently(now: u64) -> bool {
    std::fs::read_to_string(check_failure_marker())
        .ok()
        .and_then(|s| s.trim().parse::<u64>().ok())
        .is_some_and(|last| within_cooldown(now, last))
}

/// 查最新 Release。
///
/// `skip_failed` 是自动检查用的：上一次已经尝试装过同一版、装完回来还是旧版，
/// 说明那一次没装成，别再自动重试；刚失败过的自动检查也会在冷却期内跳过
/// （手动点「检查并更新」不受这两个限制）。
pub async fn check(skip_failed: bool) -> Result<Option<UpdateInfo>, UpdateError> {
    if skip_failed && check_failed_recently(now_secs()) {
        log::info!("上次自动检查更新失败，冷却期内跳过");
        return Ok(None);
    }
    match check_once(skip_failed).await {
        Ok(info) => {
            clear_check_failure();
            Ok(info)
        }
        Err(e) => {
            // 只有环境性失败才记冷却；「没有本机安装包」是确定结论，
            // 记了反而会让之后真出了安装包也迟迟发现不了。
            if e.is_transient() {
                mark_check_failed();
            }
            Err(e)
        }
    }
}

async fn check_once(skip_failed: bool) -> Result<Option<UpdateInfo>, UpdateError> {
    let client = reqwest::Client::builder()
        .user_agent(concat!("racewong-app/", env!("CARGO_PKG_VERSION")))
        // 连不上时别让请求挂在那儿：内网 / 被墙的机器上 TCP 可能两分钟才超时，
        // 期间「检查更新」一直转圈，还会把用户手动点的那次挡住
        .timeout(std::time::Duration::from_secs(15))
        .build()
        .map_err(|e| UpdateError::unavailable(format!("创建 HTTP 客户端失败: {e}")))?;
    let response = client
        .get(LATEST_RELEASE)
        .send()
        .await
        .map_err(|e| UpdateError::unavailable(format!("请求 GitHub 失败: {e}")))?;
    // 不用 `error_for_status()`：它生成的文本会把状态码和整个 URL 一起带上，
    // 那正是以前糊在「关于」窗口上的东西。这里只分类，原文留给日志。
    let status = response.status();
    if !status.is_success() {
        let reason = match status.as_u16() {
            403 | 429 => "GitHub 访问限流",
            404 => "Release 不存在",
            500..=599 => "GitHub 服务暂时不可用",
            _ => "GitHub 返回异常",
        };
        return Err(UpdateError::unavailable(format!(
            "{reason}: HTTP {status} ({LATEST_RELEASE})"
        )));
    }
    let release: Release = response
        .json()
        .await
        .map_err(|e| UpdateError::unavailable(format!("Release 数据解析失败: {e}")))?;
    let latest = Version::parse(release.tag_name.trim_start_matches('v'))
        .map_err(|e| UpdateError::unavailable(format!("Release 版本号无效: {e}")))?;
    let current = Version::parse(env!("CARGO_PKG_VERSION"))
        .map_err(|e| UpdateError::unavailable(format!("当前版本号无效: {e}")))?;
    if latest <= current {
        return Ok(None);
    }
    if skip_failed && already_attempted(&latest) {
        log::info!("v{latest} 上次已经尝试安装过但没成功，本次自动更新跳过");
        return Ok(None);
    }
    let asset = release
        .assets
        .into_iter()
        .find(|a| wanted_asset(&a.name))
        .ok_or_else(|| UpdateError::no_asset(&latest))?;
    Ok(Some(UpdateInfo {
        version: latest,
        asset_name: asset.name,
        download_url: asset.browser_download_url,
    }))
}

pub async fn download(info: &UpdateInfo) -> Result<PathBuf, UpdateError> {
    let dir = std::env::temp_dir().join("racewong-update");
    tokio::fs::create_dir_all(&dir)
        .await
        .map_err(|e| UpdateError::download_failed(format!("创建下载目录失败: {e}")))?;
    let target = dir.join(&info.asset_name);
    let response = reqwest::Client::builder()
        .user_agent(concat!("racewong-app/", env!("CARGO_PKG_VERSION")))
        // 只限连接阶段：安装包几个 MB，整体超时卡太紧会在慢网上下不完
        .connect_timeout(std::time::Duration::from_secs(15))
        .build()
        .map_err(|e| UpdateError::download_failed(format!("创建 HTTP 客户端失败: {e}")))?
        .get(&info.download_url)
        .send()
        .await
        .map_err(|e| UpdateError::download_failed(format!("下载请求失败: {e}")))?
        .error_for_status()
        .map_err(|e| UpdateError::download_failed(format!("下载返回异常: {e}")))?;
    let mut file = tokio::fs::File::create(&target)
        .await
        .map_err(|e| UpdateError::download_failed(format!("创建安装包文件失败: {e}")))?;
    let mut stream = response.bytes_stream();
    use tokio::io::AsyncWriteExt;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|e| UpdateError::download_failed(format!("下载中断: {e}")))?;
        file.write_all(&chunk)
            .await
            .map_err(|e| UpdateError::download_failed(format!("写入安装包失败: {e}")))?;
    }
    file.flush()
        .await
        .map_err(|e| UpdateError::download_failed(format!("写入安装包失败: {e}")))?;
    Ok(target)
}

/// 启动安装程序。
///
/// Windows 走**完全静默**升级（`/qn`，不弹任何向导）：
///
/// * 安装位置用当前 exe 所在目录传给 MSI（`INSTALLFOLDER=`），这样升级一定
///   落在上次的目录里，不会因为静默安装没人能改路径而被搬回默认位置；
/// * 不能直接 spawn msiexec —— 它要覆盖的就是正在运行的 exe。先写一个短命
///   `cmd` 脚本，等 2 秒（够本进程退出、文件锁释放）再装，装完把程序重新拉起来；
///   这个 `cmd` 用 `CREATE_NO_WINDOW` 起，不会在屏幕上闪一个命令行窗口；
/// * 留一份安装日志在 `%TEMP%\racewong-update\install.log`，静默安装失败时能查。
pub fn launch_installer(path: &std::path::Path) -> Result<(), String> {
    #[cfg(target_os = "windows")]
    {
        let dir = std::env::temp_dir().join("racewong-update");
        std::fs::create_dir_all(&dir).map_err(|e| e.to_string())?;

        let exe = std::env::current_exe().map_err(|e| e.to_string())?;
        let install_dir = exe.parent().map(|p| p.to_path_buf()).unwrap_or_default();
        let script = dir.join("apply-update.cmd");
        let log = dir.join("install.log");

        // ping 当 sleep 用：`timeout` 在无控制台的进程里会直接报错
        let body = format!(
            "@echo off\r\n\
             ping -n 3 127.0.0.1 >nul\r\n\
             msiexec.exe /i \"{msi}\" /qn /norestart INSTALLFOLDER=\"{dir}\" /l*v \"{log}\"\r\n\
             if exist \"{exe}\" start \"\" \"{exe}\"\r\n\
             del \"%~f0\"\r\n",
            msi = path.display(),
            dir = install_dir.display(),
            log = log.display(),
            exe = exe.display(),
        );
        std::fs::write(&script, body).map_err(|e| format!("写入升级脚本失败: {e}"))?;
        // 本程序是 GUI 子系统，spawn cmd.exe 时系统会给它新建一个控制台，
        // 屏幕上就会闪一个黑框——「静默更新」看着一点也不静默。
        // CREATE_NO_WINDOW 让子进程用隐藏的控制台跑，从头到尾看不见窗口。
        use std::os::windows::process::CommandExt;
        const CREATE_NO_WINDOW: u32 = 0x0800_0000;
        let mut cmd = std::process::Command::new("cmd.exe");
        cmd.arg("/c")
            .arg(&script)
            .creation_flags(CREATE_NO_WINDOW);
        return cmd
            .spawn()
            .map(|_| ())
            .map_err(|e| format!("无法启动安装器: {e}"));
    }

    #[cfg(target_os = "macos")]
    return std::process::Command::new("open")
        .arg(path)
        .spawn()
        .map(|_| ())
        .map_err(|e| format!("无法启动安装器: {e}"));

    #[cfg(target_os = "linux")]
    return std::process::Command::new("xdg-open")
        .arg(path)
        .spawn()
        .map(|_| ())
        .map_err(|e| format!("无法启动安装器: {e}"));

    #[allow(unreachable_code)]
    Err("当前平台不支持自动更新".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn platform_asset_filter_is_strict() {
        #[cfg(target_os = "windows")]
        assert!(super::wanted_asset("racewong-1.2.3-windows-x86_64.msi"));
        #[cfg(target_os = "macos")]
        assert!(super::wanted_asset("racewong-1.2.3-macos-aarch64.dmg"));
        assert!(!super::wanted_asset("racewong-1.2.3-windows-x86_64.zip"));
    }

    /// 用户截图里那条报错不能再用任何形式出现在界面上。
    #[test]
    fn http_errors_never_leak_to_the_user() {
        let raw = "HTTP status client error (403 rate limit exceeded) for url \
                   (https://api.github.com/repos/redgreat/racewong/releases/latest)";
        let e = UpdateError::unavailable(raw.to_string());
        assert!(e.is_transient(), "限流属于环境性失败");
        assert_eq!(e.detail(), raw, "原始错误要完整留在日志里供排障");
        let msg = e.user_message();
        assert!(!msg.contains("403"));
        assert!(!msg.contains("http"));
        assert!(!msg.contains("rate limit"));
        assert!(msg.len() < 60, "界面提示应是短句，不是整段报错: {msg}");
    }

    #[test]
    fn actionable_errors_keep_paths_out_of_the_banner() {
        let e = UpdateError::download_failed(
            r"写入安装包失败: C:\Users\someone\AppData\Local\Temp\x.msi 拒绝访问".to_string(),
        );
        assert!(!e.is_transient());
        assert!(!e.user_message().contains("C:\\"));
        assert!(e.detail().contains("拒绝访问"));

        let v = Version::parse("9.8.7").expect("版本号");
        let no_asset = UpdateError::no_asset(&v);
        assert!(!no_asset.is_transient());
        assert!(no_asset.user_message().contains("9.8.7"));
        assert!(!UpdateError::install_failed("msiexec 失败").is_transient());
    }

    #[test]
    fn auto_check_cooldown_boundaries() {
        assert!(within_cooldown(1000, 1000));
        assert!(within_cooldown(1000 + CHECK_COOLDOWN_SECS - 1, 1000));
        assert!(!within_cooldown(1000 + CHECK_COOLDOWN_SECS, 1000));
        // 时钟回拨 / 标记文件里是未来时间：先歇一次，别狂发请求
        assert!(within_cooldown(1000, 5000));
    }
}
