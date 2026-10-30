//! 单实例守卫。
//!
//! # 为什么需要
//!
//! 托盘常驻模式下，点右上角 × 只是把窗口藏起来，进程还在跑。这时候用户再去
//! 开始菜单点一次快捷方式（或者任务栏还挂着图标），就会起**第二个进程**：
//!
//! * 两个进程各注册一个托盘图标 —— 通知区域里出现两个一模一样的「RaceBox同步」；
//! * 旧进程的窗口是隐藏的，它的图标点了也没反应（那正是“只能重启电脑”的由来）；
//! * 两个进程还会同时抢同一个蓝牙设备、同时往数据库写同一批轨迹。
//!
//! 所以启动时先抢一个命名互斥体：抢不到说明已经有一个实例在跑，就把那个实例
//! 的窗口直接拉到前台，然后本次进程立刻退出——用户的意图本来就是“打开 RaceBox”，
//! 不需要弹什么“程序已在运行”的提示。
//!
//! 非 Windows 平台目前不做限制（macOS 的菜单栏图标场景还没遇到这个问题）。

/// 尝试成为唯一实例。
///
/// 返回 `true` 表示可以继续启动；返回 `false` 表示已有实例在跑，
/// 并且已经尽力把它的窗口唤到前台，调用方应直接退出。
#[cfg(target_os = "windows")]
pub fn claim() -> bool {
    imp::claim()
}

#[cfg(not(target_os = "windows"))]
pub fn claim() -> bool {
    true
}

#[cfg(target_os = "windows")]
mod imp {
    use std::sync::atomic::{AtomicIsize, Ordering};

    /// 主窗口标题（`eframe::run_native` 的第一个参数），找旧窗口靠它。
    const WINDOW_TITLE: &str = "RaceBox";
    /// 每个用户会话一个，装的是 perUser 版本，没必要用 Global\。
    const MUTEX_NAME: &str = "Local\\racewong-RaceBoxSync-single-instance";

    const ERROR_ALREADY_EXISTS: u32 = 183;
    const SW_SHOW: i32 = 5;
    const SW_RESTORE: i32 = 9;

    /// 旧实例可能刚起来还没建窗口，重试 2 秒足够覆盖“连点两下图标”。
    const FIND_RETRY: usize = 20;
    const FIND_RETRY_MS: u64 = 100;

    /// 互斥体句柄必须活到进程结束：句柄一关，名字就释放了。
    /// 这里存成静态量，进程退出时由系统回收。
    static MUTEX: AtomicIsize = AtomicIsize::new(0);

    #[link(name = "kernel32")]
    extern "system" {
        fn CreateMutexW(attrs: *const u8, initial_owner: i32, name: *const u16) -> isize;
        fn GetLastError() -> u32;
        fn CloseHandle(handle: isize) -> i32;
    }

    #[link(name = "user32")]
    extern "system" {
        fn FindWindowW(class: *const u16, name: *const u16) -> isize;
        fn IsWindow(hwnd: isize) -> i32;
        fn IsIconic(hwnd: isize) -> i32;
        fn ShowWindow(hwnd: isize, cmd: i32) -> i32;
        fn SetForegroundWindow(hwnd: isize) -> i32;
    }

    fn wide(s: &str) -> Vec<u16> {
        s.encode_utf16().chain(std::iter::once(0)).collect()
    }

    pub fn claim() -> bool {
        let name = wide(MUTEX_NAME);
        let handle = unsafe { CreateMutexW(std::ptr::null(), 0, name.as_ptr()) };
        if handle == 0 {
            // 连互斥体都建不出来（极少见）：宁可放行，也不要让用户打不开程序
            return true;
        }
        if unsafe { GetLastError() } != ERROR_ALREADY_EXISTS {
            MUTEX.store(handle, Ordering::SeqCst);
            return true;
        }

        // 已有实例在跑：自己这份句柄没用了
        unsafe {
            CloseHandle(handle);
        }
        if wake_existing_window() {
            return false;
        }
        // 抢不到互斥体却找不到旧窗口（旧进程可能正在退出、窗口已经销毁）：
        // 这时候退出的话用户点了快捷方式什么都没有，反而更像 bug。放行。
        true
    }

    /// 把旧实例的窗口显示出来并切到前台；找不到窗口返回 `false`。
    ///
    /// 旧窗口可能是「最小化」或「被 Visible(false) 藏起来」两种状态，都要处理：
    /// 对隐藏窗口直接 `SW_SHOW` 就行，最小化的还要先 `SW_RESTORE`。
    fn wake_existing_window() -> bool {
        let title = wide(WINDOW_TITLE);
        for attempt in 0..FIND_RETRY {
            let hwnd = unsafe { FindWindowW(std::ptr::null(), title.as_ptr()) };
            if hwnd != 0 && unsafe { IsWindow(hwnd) } != 0 {
                unsafe {
                    if IsIconic(hwnd) != 0 {
                        ShowWindow(hwnd, SW_RESTORE);
                    }
                    ShowWindow(hwnd, SW_SHOW);
                    SetForegroundWindow(hwnd);
                }
                return true;
            }
            if attempt + 1 < FIND_RETRY {
                std::thread::sleep(std::time::Duration::from_millis(FIND_RETRY_MS));
            }
        }
        false
    }
}
