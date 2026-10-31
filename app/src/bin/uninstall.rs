//! RaceBox同步 的卸载程序，装到安装目录里，名字叫 `uninstall.exe`。
//!
//! 双击之后依次做这些事：
//!   1. 问一句要不要卸载；
//!   2. 问一句要不要保留个人配置（`%APPDATA%\RaceBox Sync`）；
//!   3. 结束正在运行的 `racewong-app.exe`——托盘图标随之消失；
//!   4. 调 `msiexec /x` 走正规卸载：装的文件、开始菜单、桌面快捷方式、
//!      App Paths 注册表全由 MSI 自己收拾；
//!   5. 按第 2 步的选择删或留个人配置，再扫一遍残留（空的安装目录、
//!      `HKCU\Software\racewong` 这层空壳键）。
//!
//! 三个绕不开的细节：
//!
//! * **自己删不掉自己**。MSI 卸载时会删安装目录里的 `uninstall.exe`，而正在运行的
//!   exe 是锁住的，删不掉就会退化成"重启后删除"甚至弹"文件正在使用"。所以先把自己
//!   复制到 `%TEMP%` 再启动，原进程立刻退出——安装目录里那份就变成普通文件了。
//! * **ProductCode 每版都不一样**。`racewong.wxs` 里写的是 `Id="*"`，让 MSI 每次
//!   生成新 GUID（MajorUpgrade 要靠它区分新旧版本），卸载程序没法把 GUID 写死，
//!   只能读安装时 MSI 写进注册表的 `ProductCode` 值。
//! * **不能闪黑框**。调 `reg` / `taskkill` / `msiexec` 一律带 `CREATE_NO_WINDOW`，
//!   否则一个 GUI 程序会连带弹出一串控制台窗口。

#![cfg_attr(windows, windows_subsystem = "windows")]

const APP_DIR_NAME: &str = "RaceBox Sync";
const APP_EXE: &str = "racewong-app.exe";
const UNINSTALL_EXE: &str = "uninstall.exe";
const REG_APP_KEY: &str = r"HKCU\Software\racewong\RaceBoxSync";
const REG_VENDOR_KEY: &str = r"HKCU\Software\racewong";
const TITLE: &str = "RaceBox同步 卸载程序";
/// 标记"我是被复制到临时目录的那一份"，防止无限自我复制。
const RELOCATED_ENV: &str = "RACEWONG_UNINSTALL_RELOCATED";

fn main() {
    #[cfg(windows)]
    windows_main();

    #[cfg(not(windows))]
    eprintln!("RaceBox同步 的卸载程序只在 Windows 上提供；其他平台直接删掉程序文件即可。");
}

#[cfg(windows)]
fn windows_main() {
    if std::env::var_os(RELOCATED_ENV).is_none() {
        match relocate() {
            // 已经拉起临时目录里那份，本进程可以退出了
            Ok(()) => return,
            Err(e) => {
                error_dialog(&format!("无法启动卸载程序：\n{e}"));
                std::process::exit(1);
            }
        }
    }
    if let Err(e) = run() {
        error_dialog(&e);
        std::process::exit(1);
    }
}

// ---------------------------------------------------------------- 主体流程

#[cfg(windows)]
fn run() -> Result<(), String> {
    // 第 0 个参数是安装目录（由 `relocate` 传进来），卸载完用来兜底清残留
    let install_dir = std::env::args_os().nth(1).map(std::path::PathBuf::from);

    if !ask_ok_cancel(
        "确定要卸载 RaceBox同步 吗？",
        "卸载后需要重新安装才能使用。\n\n点「确定」继续，点「取消」退出。",
    ) {
        return Ok(());
    }

    // 「是」保留、「否」删除。默认理解为保留更安全：删错了配置找不回来，
    // 留错了用户下次还能自己删。
    let keep_data = ask_yes_no(
        "是否保留个人配置？",
        &format!(
            "配置包括服务器 / MQTT 设置、语言选择、RaceBox 云账号登录信息，\
             都存放在：\n{}\n\n\
             点「是」保留——以后重新安装不用重新填。\n\
             点「否」一并删除，卸载后不留任何痕迹。",
            data_dir().display()
        ),
    );

    kill_running_app();

    match product_code() {
        Some(code) => {
            let rc = run_msi_uninstall(&code)?;
            if rc == 1602 {
                // 用户在 MSI 的进度条上点了取消，这时候别再往下删东西
                info_dialog("卸载已取消，没有做任何改动。");
                return Ok(());
            }
        }
        None => {
            // 找不到安装记录（绿色版、或者注册表被清理过）：只能自己删文件
            warn_dialog(
                "没有找到安装记录，将直接从安装目录删除程序文件。\n\n\
                 如果「设置 → 应用」里还留着 RaceBox同步，请在那里再卸载一次。",
            );
        }
    }

    // MSI 正常卸载后安装目录应该已经空了，这里兜底再清一次
    tidy_install_dir(install_dir.as_deref());
    cleanup_registry();

    let data = data_dir();
    let data_gone = if keep_data {
        false
    } else {
        std::fs::remove_dir_all(&data).is_ok() || !data.exists()
    };

    let message = if keep_data {
        format!(
            "卸载完成。\n\n\
             安装目录、开始菜单 / 桌面快捷方式、托盘图标都已清理。\n\n\
             个人配置保留在：\n{}\n\n\
             以后重新安装会继续沿用这份配置。",
            data.display()
        )
    } else if data_gone {
        format!(
            "卸载完成。\n\n\
             安装目录、开始菜单 / 桌面快捷方式、托盘图标都已清理，\n\
             个人配置也已删除：\n{}",
            data.display()
        )
    } else {
        format!(
            "卸载完成。\n\n\
             安装目录、快捷方式和托盘图标都已清理，\n\
             但个人配置没能删掉（可能有文件正被占用），可以手动删除：\n{}",
            data.display()
        )
    };
    info_dialog(&message);
    schedule_self_delete();
    Ok(())
}

// ---------------------------------------------------------------- 自我搬迁

/// 把自身复制到 `%TEMP%` 并启动，然后让调用方退出。
///
/// 复制而不是直接运行，是因为 MSI 卸载要删安装目录里的 `uninstall.exe`，
/// 而正在运行的可执行文件在 Windows 上是删不掉的。
#[cfg(windows)]
fn relocate() -> Result<(), String> {
    let self_path = std::env::current_exe().map_err(|e| format!("取自身路径失败：{e}"))?;
    let install_dir = self_path.parent().map(|p| p.to_path_buf());
    let temp_exe = std::env::temp_dir().join(UNINSTALL_EXE);

    std::fs::copy(&self_path, &temp_exe).map_err(|e| format!("复制到临时目录失败：{e}"))?;

    let mut child = std::process::Command::new(&temp_exe);
    child.env(RELOCATED_ENV, "1");
    if let Some(dir) = install_dir {
        child.arg(dir);
    }
    child
        .spawn()
        .map_err(|e| format!("启动临时卸载程序失败：{e}"))?;
    Ok(())
}

/// 收尾时把自己从 `%TEMP%` 删掉。
///
/// 正在运行的 exe 删不掉，只能交给一个延迟 2 秒的后台 cmd 来做。
/// 失败也无所谓：文件在 `%TEMP%` 里，下次卸载会覆盖同名文件，系统也会定期清理。
#[cfg(windows)]
fn schedule_self_delete() {
    let Ok(self_path) = std::env::current_exe() else {
        return;
    };
    // 注意 `>` 和 `&` 要作为独立参数传：Rust 只在含空格时才加引号，
    // 这两个符号原样落到 cmd 的命令行里才会被当成重定向和分隔符。
    let _ = hidden("cmd")
        .args(["/c", "ping", "-n", "3", "127.0.0.1", ">", "nul", "&", "del", "/f", "/q"])
        .arg(self_path)
        .spawn();
}

// ---------------------------------------------------------------- 系统调用

#[cfg(windows)]
fn hidden(program: &str) -> std::process::Command {
    use std::os::windows::process::CommandExt;
    /// 不创建控制台窗口
    const CREATE_NO_WINDOW: u32 = 0x0800_0000;
    let mut cmd = std::process::Command::new(program);
    cmd.creation_flags(CREATE_NO_WINDOW);
    cmd
}

/// 结束正在运行的主程序。返回码不用管：没在跑时 taskkill 也是非零退出。
#[cfg(windows)]
fn kill_running_app() {
    let _ = hidden("taskkill").args(["/F", "/IM", APP_EXE]).output();
    // 给它一点时间真正退出，否则紧接着的卸载还会撞上被占用的文件
    std::thread::sleep(std::time::Duration::from_millis(400));
}

/// 从注册表读回安装时写下的 ProductCode。
#[cfg(windows)]
fn product_code() -> Option<String> {
    let out = hidden("reg")
        .args(["query", REG_APP_KEY, "/v", "ProductCode"])
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    let text = String::from_utf8_lossy(&out.stdout);
    for line in text.lines() {
        // 形如：    ProductCode    REG_SZ    {xxxxxxxx-....}
        let Some(idx) = line.find("REG_SZ") else {
            continue;
        };
        let value = line[idx + "REG_SZ".len()..].trim();
        if value.starts_with('{') {
            return Some(value.to_string());
        }
    }
    None
}

/// 走 MSI 卸载，返回 msiexec 的退出码。
///
/// `/qb` 是"基础界面"：一个进度条加一个完成提示，比 `/qn` 全静默友好，
/// 又不像完整向导那样要用户一路点下去。
#[cfg(windows)]
fn run_msi_uninstall(code: &str) -> Result<i32, String> {
    let status = hidden("msiexec")
        .args(["/x", code, "/qb"])
        .status()
        .map_err(|e| format!("调用 Windows Installer 失败：{e}"))?;
    Ok(status.code().unwrap_or(-1))
}

/// 清掉 MSI 留下的空壳注册表键。
///
/// MSI 只删它自己写的那些值（`installed` / `InstallDir` / `ProductCode` …），
/// `Software\racewong` 这层由安装程序隐式创建的父键会留下来。
#[cfg(windows)]
fn cleanup_registry() {
    let _ = hidden("reg").args(["delete", REG_VENDOR_KEY, "/f"]).output();
}

/// 兜底清理安装目录。
///
/// 只删我们自己的两个 exe，再尝试删空目录——万一用户往安装目录里放过别的东西，
/// 不能连他的文件一起干掉。
#[cfg(windows)]
fn tidy_install_dir(dir: Option<&std::path::Path>) {
    let Some(dir) = dir else { return };
    for name in [APP_EXE, UNINSTALL_EXE] {
        let _ = std::fs::remove_file(dir.join(name));
    }
    let _ = std::fs::remove_dir(dir);
}

// ---------------------------------------------------------------- 路径与弹窗

/// 个人配置目录，必须和 `src/config.rs` 里的 `base_dir()` 算出来的一致。
#[cfg(windows)]
fn data_dir() -> std::path::PathBuf {
    if let Some(base) = directories::BaseDirs::new() {
        return base.config_dir().join(APP_DIR_NAME);
    }
    std::env::temp_dir().join(APP_DIR_NAME)
}

#[cfg(windows)]
fn ask_ok_cancel(title: &str, text: &str) -> bool {
    use rfd::{MessageButtons, MessageDialog, MessageDialogResult, MessageLevel};
    matches!(
        MessageDialog::new()
            .set_level(MessageLevel::Warning)
            .set_title(TITLE)
            .set_description(format!("{title}\n\n{text}"))
            .set_buttons(MessageButtons::OkCancel)
            .show(),
        MessageDialogResult::Ok
    )
}

#[cfg(windows)]
fn ask_yes_no(title: &str, text: &str) -> bool {
    use rfd::{MessageButtons, MessageDialog, MessageDialogResult, MessageLevel};
    matches!(
        MessageDialog::new()
            .set_level(MessageLevel::Warning)
            .set_title(TITLE)
            .set_description(format!("{title}\n\n{text}"))
            .set_buttons(MessageButtons::YesNo)
            .show(),
        MessageDialogResult::Yes
    )
}

#[cfg(windows)]
fn info_dialog(text: &str) {
    use rfd::{MessageButtons, MessageDialog, MessageLevel};
    let _ = MessageDialog::new()
        .set_level(MessageLevel::Info)
        .set_title(TITLE)
        .set_description(text)
        .set_buttons(MessageButtons::Ok)
        .show();
}

#[cfg(windows)]
fn warn_dialog(text: &str) {
    use rfd::{MessageButtons, MessageDialog, MessageLevel};
    let _ = MessageDialog::new()
        .set_level(MessageLevel::Warning)
        .set_title(TITLE)
        .set_description(text)
        .set_buttons(MessageButtons::Ok)
        .show();
}

#[cfg(windows)]
fn error_dialog(text: &str) {
    use rfd::{MessageButtons, MessageDialog, MessageLevel};
    let _ = MessageDialog::new()
        .set_level(MessageLevel::Error)
        .set_title(TITLE)
        .set_description(text)
        .set_buttons(MessageButtons::Ok)
        .show();
}
