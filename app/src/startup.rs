//! 开机启动注册。
//!
//! Windows 写当前用户的 `Run` 注册表项，macOS 写用户级 LaunchAgent；两者都
//! 不需要管理员权限。启动命令固定带 `--background`，由 `main.rs` 决定首屏隐藏。

const BACKGROUND_ARG: &str = "--background";

pub fn requested_by_args() -> bool {
    std::env::args().any(|arg| arg == BACKGROUND_ARG)
}

pub fn delay_from_args() -> Option<u32> {
    std::env::args().find_map(|arg| {
        arg.strip_prefix("--startup-delay=")
            .and_then(|v| v.parse::<u32>().ok())
            .map(|v| v.min(120))
    })
}

pub fn supported() -> bool {
    cfg!(any(target_os = "windows", target_os = "macos"))
}

#[cfg(target_os = "windows")]
pub fn set_enabled(enabled: bool, delay_secs: u32) -> Result<(), String> {
    const KEY: &str = r"HKCU\Software\Microsoft\Windows\CurrentVersion\Run";
    const VALUE: &str = "RaceBoxSync";

    let mut command = std::process::Command::new("reg.exe");
    if enabled {
        let exe = std::env::current_exe().map_err(|e| format!("无法取得程序路径: {e}"))?;
        // Run 项是一整条命令行；路径必须加引号，安装目录包含空格。
        let data = format!(
            "\"{}\" {BACKGROUND_ARG} --startup-delay={}",
            exe.display(),
            delay_secs.min(120)
        );
        command.args(["ADD", KEY, "/v", VALUE, "/t", "REG_SZ", "/d", &data, "/f"]);
    } else {
        command.args(["DELETE", KEY, "/v", VALUE, "/f"]);
    }
    let output = command
        .creation_flags_no_window()
        .output()
        .map_err(|e| format!("无法修改开机启动项: {e}"))?;
    // 删除一个本来就不存在的值也视为成功，保证“关闭”幂等。
    if output.status.success() || (!enabled && output.status.code() == Some(1)) {
        Ok(())
    } else {
        Err(format!("修改开机启动项失败（状态 {}）", output.status))
    }
}

#[cfg(target_os = "windows")]
trait CommandNoWindow {
    fn creation_flags_no_window(&mut self) -> &mut Self;
}

#[cfg(target_os = "windows")]
impl CommandNoWindow for std::process::Command {
    fn creation_flags_no_window(&mut self) -> &mut Self {
        use std::os::windows::process::CommandExt;
        self.creation_flags(0x0800_0000)
    }
}

#[cfg(target_os = "macos")]
pub fn set_enabled(enabled: bool, delay_secs: u32) -> Result<(), String> {
    let home = directories::BaseDirs::new()
        .ok_or_else(|| "无法取得用户目录".to_string())?
        .home_dir()
        .to_path_buf();
    let dir = home.join("Library/LaunchAgents");
    let path = dir.join("cn.wongcw.racewong.plist");
    if !enabled {
        return match std::fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(format!("删除开机启动项失败: {e}")),
        };
    }

    let exe = std::env::current_exe().map_err(|e| format!("无法取得程序路径: {e}"))?;
    std::fs::create_dir_all(&dir).map_err(|e| format!("创建 LaunchAgents 目录失败: {e}"))?;
    let exe = xml_escape(&exe.to_string_lossy());
    let delay_arg = format!("--startup-delay={}", delay_secs.min(120));
    let plist = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" \
         \"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">\n\
         <plist version=\"1.0\"><dict>\n\
         <key>Label</key><string>cn.wongcw.racewong</string>\n\
         <key>ProgramArguments</key><array><string>{exe}</string><string>{BACKGROUND_ARG}</string><string>{delay_arg}</string></array>\n\
         <key>RunAtLoad</key><true/>\n\
         <key>ProcessType</key><string>Interactive</string>\n\
         </dict></plist>\n"
    );
    std::fs::write(path, plist).map_err(|e| format!("写入开机启动项失败: {e}"))
}

#[cfg(target_os = "macos")]
fn xml_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

#[cfg(not(any(target_os = "windows", target_os = "macos")))]
pub fn set_enabled(enabled: bool, _delay_secs: u32) -> Result<(), String> {
    if enabled {
        Err("当前平台暂不支持开机后台启动".to_string())
    } else {
        Ok(())
    }
}
