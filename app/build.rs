use std::path::{Path, PathBuf};

// 下面两个函数只在 Unix 主机（WSL/Linux/macOS 交叉编译 Windows 目标）用到，
// Windows 主机上没有 std::os::unix，必须整段条件编译掉，否则 build script 编译不过。
#[cfg(unix)]
fn llvm_bin_dir() -> Option<PathBuf> {
    for dir in [
        "/usr/lib/llvm-18/bin",
        "/usr/lib/llvm-17/bin",
        "/usr/lib/llvm-16/bin",
        "/usr/lib/llvm-15/bin",
    ] {
        if Path::new(&format!("{dir}/llvm-rc")).exists() {
            return Some(PathBuf::from(dir));
        }
    }
    None
}

/// 在没有 MSVC 的机器上（WSL 交叉编译）为 winres 准备一个名为 rc.exe 的入口。
#[cfg(unix)]
fn prepare_rc_shim(out_dir: &Path) -> Option<PathBuf> {
    let llvm = llvm_bin_dir()?;
    let shim_dir = out_dir.join("rcshim");
    std::fs::create_dir_all(&shim_dir).ok()?;
    let rc_exe = shim_dir.join("rc.exe");
    if rc_exe.exists() {
        let _ = std::fs::remove_file(&rc_exe);
    }
    match std::os::unix::fs::symlink(llvm.join("llvm-rc"), &rc_exe) {
        Ok(()) => Some(shim_dir),
        Err(e) => {
            println!("cargo:warning=创建 rc.exe 软链失败: {e}");
            None
        }
    }
}

/// Windows 主机上不需要 llvm-rc 软链（直接用 MSVC 的 rc.exe）。
#[cfg(not(unix))]
fn prepare_rc_shim(_out_dir: &Path) -> Option<PathBuf> {
    None
}

fn main() {
    println!("cargo:rerun-if-changed=assets/icon.ico");
    println!("cargo:rerun-if-changed=build.rs");
    if std::env::var_os("CARGO_CFG_WINDOWS").is_none() {
        return;
    }
    if !Path::new("assets/icon.ico").exists() {
        println!("cargo:warning=assets/icon.ico 不存在，跳过 exe 图标嵌入");
        return;
    }

    let out_dir = PathBuf::from(std::env::var("OUT_DIR").unwrap_or_else(|_| ".".into()));
    // winres 写进 .rc 的是我们给出的路径，构建脚本的工作目录不一定是包根目录，
    // 所以这里传绝对路径，否则 llvm-rc/rc.exe 找不到 ico。
    let icon_abs = std::fs::canonicalize("assets/icon.ico")
        .unwrap_or_else(|_| PathBuf::from("assets/icon.ico"));

    let cross = !cfg!(windows);
    let mut res = winres::WindowsResource::new();
    res.set_icon(icon_abs.to_str().unwrap_or("assets/icon.ico"));
    if cross {
        // llvm-rc 无法按 65001 代码页解析非 ASCII 字符，交叉编译时先用英文版本信息。
        res.set("FileDescription", "RaceBox sync desktop app");
        res.set("ProductName", "RaceBox Sync");
        res.set("LegalCopyright", "racewong");
    } else {
        res.set("FileDescription", "RaceBox 蓝牙同步桌面应用");
        res.set("ProductName", "RaceBox 同步");
        res.set("LegalCopyright", "racewong");
    }

    let result = if cfg!(windows) {
        res.compile()
    } else if let Some(shim) = prepare_rc_shim(&out_dir) {
        res.set_toolkit_path(shim.to_str().unwrap_or("."));
        res.compile()
    } else {
        Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "llvm-rc 不可用",
        ))
    };

    match result {
        Ok(()) => {
            if !cfg!(windows) {
                println!("cargo:warning=exe 图标已通过 llvm-rc 嵌入");
            }
        }
        Err(e) => println!("cargo:warning=未能嵌入 exe 图标: {e}"),
    }
}
