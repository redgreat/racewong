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
/// 注册表路径，相对 HKEY_CURRENT_USER。安装时 MSI 把 ProductCode 写在这里。
const REG_APP_KEY: &str = r"Software\racewong\RaceBoxSync";
const REG_VENDOR_KEY: &str = r"Software\racewong";
const TITLE: &str = "RaceBox同步 卸载程序";
/// 标记"我是被复制到临时目录的那一份"，防止无限自我复制。
const RELOCATED_ENV: &str = "RACEWONG_UNINSTALL_RELOCATED";
/// 安装目录由第一份进程通过环境变量传下来（命令行参数留给用户开关）。
const INSTALL_DIR_ENV: &str = "RACEWONG_UNINSTALL_DIR";

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
                msgbox::error(&format!("无法启动卸载程序：\n{e}"));
                std::process::exit(1);
            }
        }
    }
    if let Err(e) = run() {
        msgbox::error(&e);
        std::process::exit(1);
    }
}

// ---------------------------------------------------------------- 主体流程

#[cfg(windows)]
fn run() -> Result<(), String> {
    // 先把自己的工作目录挪走。
    // 双击 exe 时 Windows 会把当前目录设成 exe 所在目录，也就是安装目录；
    // 而"正在被某个进程当作当前目录"的文件夹是删不掉的（ERROR_ACCESS_DENIED），
    // 结果就是卸载完了安装目录还留一个空壳。MSI 的 RemoveFolders 也一样会失败。
    let _ = std::env::set_current_dir(std::env::temp_dir());

    // 命令行开关（给脚本/自动化用，普通用户双击就走图形界面）：
    //   /s            静默，不弹任何窗口；默认保留个人配置
    //   /s /purge     静默，并把个人配置一起删掉
    let flags: Vec<String> = std::env::args()
        .skip(1)
        .map(|a| a.to_ascii_lowercase())
        .collect();
    let silent = flags.iter().any(|f| f == "/s" || f == "-s" || f == "--silent");
    let purge = flags.iter().any(|f| f == "/purge" || f == "--purge");
    // 安装目录由 relocate 通过环境变量带过来（卸载完用来兜底清残留）
    let install_dir = std::env::var_os(INSTALL_DIR_ENV).map(std::path::PathBuf::from);

    if !silent
        && !msgbox::confirm(
            "确定要卸载 RaceBox同步 吗？\n\n卸载后需要重新安装才能使用。\n\n点「确定」继续，点「取消」退出。",
        )
    {
        return Ok(());
    }

    // 「是」保留、「否」删除。默认理解为保留更安全：删错了配置找不回来，
    // 留错了用户下次还能自己删。
    let keep_data = if silent {
        !purge
    } else {
        msgbox::yes_no(&format!(
            "是否保留个人配置？\n\n\
             配置包括服务器 / MQTT 设置、语言选择、RaceBox 云账号登录信息，\
             都存放在：\n{}\n\n\
             点「是」保留——以后重新安装不用重新填。\n\
             点「否」一并删除，卸载后不留任何痕迹。",
            data_dir().display()
        ))
    };

    kill_running_app();

    match product_code() {
        Some(code) => {
            let rc = run_msi_uninstall(&code, silent)?;
            if rc == 1602 {
                // 用户在 MSI 的进度条上点了取消，这时候别再往下删东西
                if !silent {
                    msgbox::info("卸载已取消，没有做任何改动。");
                }
                return Ok(());
            }
        }
        None => {
            // 找不到安装记录（绿色版、或者注册表被清理过）：只能自己删文件
            if !silent {
                msgbox::warn(
                    "没有找到安装记录，将直接从安装目录删除程序文件。\n\n\
                     如果「设置 → 应用」里还留着 RaceBox同步，请在那里再卸载一次。",
                );
            }
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

    if !silent {
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
        msgbox::info(&message);
    }
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
        child.env(INSTALL_DIR_ENV, dir);
    }
    // 用户传的开关（/s 之类）原样转给临时目录那一份
    for arg in std::env::args_os().skip(1) {
        child.arg(arg);
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
    winreg::read_string(REG_APP_KEY, "ProductCode").filter(|v| v.starts_with('{'))
}

/// 走 MSI 卸载，返回 msiexec 的退出码。
///
/// `/qb` 是"基础界面"：一个进度条加一个完成提示，比 `/qn` 全静默友好，
/// 又不像完整向导那样要用户一路点下去。静默模式下换成 `/qn`。
#[cfg(windows)]
fn run_msi_uninstall(code: &str, silent: bool) -> Result<i32, String> {
    let status = hidden("msiexec")
        .args(["/x", code, if silent { "/qn" } else { "/qb" }])
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
    winreg::delete_tree(REG_VENDOR_KEY);
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

// ---------------------------------------------------------------- 弹窗
//
// 直接用 Win32 的 MessageBoxW，不走 rfd。
// 一是卸载程序是个独立小 exe，为了两个提示框把整条 GUI 依赖链拖进来不划算；
// 二是 rfd 的 MessageDialog 在 Windows 上走 comctl32 v6 的 TaskDialog，没有
// manifest 的 exe 上会直接失败——实测弹不出来，流程静默退出，等于卸载点了没反应。
// MessageBoxW 没有这些前提，双击就弹。
#[cfg(windows)]
mod msgbox {
    use std::ffi::c_void;

    #[link(name = "user32")]
    extern "system" {
        fn MessageBoxW(hwnd: *mut c_void, text: *const u16, caption: *const u16, u_type: u32) -> i32;
    }

    const MB_OK: u32 = 0x0000_0000;
    const MB_OKCANCEL: u32 = 0x0000_0001;
    const MB_YESNO: u32 = 0x0000_0004;
    const MB_ICONERROR: u32 = 0x0000_0010;
    const MB_ICONWARNING: u32 = 0x0000_0030;
    const MB_ICONINFORMATION: u32 = 0x0000_0040;
    const MB_SETFOREGROUND: u32 = 0x0001_0000;
    const MB_TOPMOST: u32 = 0x0004_0000;
    const IDOK: i32 = 1;
    const IDYES: i32 = 6;

    fn wide(s: &str) -> Vec<u16> {
        s.encode_utf16().chain(std::iter::once(0)).collect()
    }

    fn show(text: &str, icon: u32, buttons: u32) -> i32 {
        let body = wide(text);
        let title = wide(super::TITLE);
        // 置顶 + 抢焦点：卸载程序经常是被资源管理器/快捷方式拉起来的，
        // 不加这两个标志，提示框可能开在主窗口后面，用户以为没反应。
        unsafe {
            MessageBoxW(
                std::ptr::null_mut(),
                body.as_ptr(),
                title.as_ptr(),
                buttons | icon | MB_SETFOREGROUND | MB_TOPMOST,
            )
        }
    }

    /// 确定 / 取消，点「确定」返回 true
    pub fn confirm(text: &str) -> bool {
        show(text, MB_ICONWARNING, MB_OKCANCEL) == IDOK
    }

    /// 是 / 否，点「是」返回 true
    pub fn yes_no(text: &str) -> bool {
        show(text, MB_ICONWARNING, MB_YESNO) == IDYES
    }

    pub fn info(text: &str) {
        show(text, MB_ICONINFORMATION, MB_OK);
    }

    pub fn warn(text: &str) {
        show(text, MB_ICONWARNING, MB_OK);
    }

    pub fn error(text: &str) {
        show(text, MB_ICONERROR, MB_OK);
    }
}

// ---------------------------------------------------------------- 注册表
//
// 直接调 advapi32，不 spawn `reg.exe`：少一个进程、不闪黑框，也不会被
// 某些安全策略（企业白名单/沙箱）拦下来——真被拦了卸载就会退化成
// "找不到安装记录"。
#[cfg(windows)]
mod winreg {
    use std::ffi::c_void;

    #[link(name = "advapi32")]
    extern "system" {
        fn RegOpenKeyExW(
            hkey: *mut c_void,
            sub_key: *const u16,
            options: u32,
            sam: u32,
            result: *mut *mut c_void,
        ) -> i32;
        fn RegQueryValueExW(
            hkey: *mut c_void,
            value_name: *const u16,
            reserved: *mut u32,
            ty: *mut u32,
            data: *mut u8,
            data_len: *mut u32,
        ) -> i32;
        fn RegCloseKey(hkey: *mut c_void) -> i32;
        fn RegDeleteTreeW(hkey: *mut c_void, sub_key: *const u16) -> i32;
    }

    /// HKEY_CURRENT_USER。预定义句柄是**符号扩展**过的常量，64 位上必须是
    /// 0xFFFF_FFFF_8000_0001；写成 0x8000_0001 会被当成非法句柄。
    const HKEY_CURRENT_USER: *mut c_void = 0xFFFF_FFFF_8000_0001u64 as usize as *mut c_void;
    const KEY_READ: u32 = 0x0002_0019;
    const KEY_WOW64_64KEY: u32 = 0x0000_0100;
    const KEY_WOW64_32KEY: u32 = 0x0000_0200;
    const REG_SZ: u32 = 1;

    fn wide(s: &str) -> Vec<u16> {
        s.encode_utf16().chain(std::iter::once(0)).collect()
    }

    fn open(sub_key: &str, sam: u32) -> Option<*mut c_void> {
        let sub_key = wide(sub_key);
        let mut handle: *mut c_void = std::ptr::null_mut();
        let rc = unsafe { RegOpenKeyExW(HKEY_CURRENT_USER, sub_key.as_ptr(), 0, sam, &mut handle) };
        (rc == 0).then_some(handle)
    }

    /// 读一个 REG_SZ 值。
    ///
    /// 两个视图都试一遍：MSI 装的是 x64 包，正常写在 64 位视图里，
    /// 但万一哪天换成 32 位安装器，值会落到 Wow6432Node 下面。
    pub fn read_string(sub_key: &str, name: &str) -> Option<String> {
        for view in [KEY_WOW64_64KEY, KEY_WOW64_32KEY] {
            let Some(handle) = open(sub_key, KEY_READ | view) else {
                continue;
            };
            let value = query_string(handle, name);
            unsafe { RegCloseKey(handle) };
            if value.is_some() {
                return value;
            }
        }
        None
    }

    fn query_string(hkey: *mut c_void, name: &str) -> Option<String> {
        let name = wide(name);
        let mut ty: u32 = 0;
        let mut len: u32 = 0;
        let rc = unsafe {
            RegQueryValueExW(
                hkey,
                name.as_ptr(),
                std::ptr::null_mut(),
                &mut ty,
                std::ptr::null_mut(),
                &mut len,
            )
        };
        if rc != 0 || ty != REG_SZ || len == 0 {
            return None;
        }
        let mut buf = vec![0u8; len as usize + 2];
        let mut read = len;
        let rc = unsafe {
            RegQueryValueExW(
                hkey,
                name.as_ptr(),
                std::ptr::null_mut(),
                &mut ty,
                buf.as_mut_ptr(),
                &mut read,
            )
        };
        if rc != 0 {
            return None;
        }
        let units: Vec<u16> = buf[..read as usize]
            .chunks_exact(2)
            .map(|c| u16::from_le_bytes([c[0], c[1]]))
            .collect();
        Some(
            String::from_utf16_lossy(&units)
                .trim_end_matches('\0')
                .to_string(),
        )
    }

    /// 连子键一起删掉。键不存在时返回 false，调用方不用管。
    pub fn delete_tree(sub_key: &str) -> bool {
        let sub_key = wide(sub_key);
        unsafe { RegDeleteTreeW(HKEY_CURRENT_USER, sub_key.as_ptr()) == 0 }
    }
}
