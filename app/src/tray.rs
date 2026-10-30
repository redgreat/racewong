//! 系统托盘图标（Windows 通知区域 / macOS 菜单栏）。
//!
//! 只在 Windows / macOS 上真正实现：Linux 的托盘要依赖 GTK / AppIndicator 系统库，
//! 交叉编译 Windows 时用不上，索性不引入——非这两个平台直接当作“不支持”，
//! 关闭行为会退回“直接退出”。
//!
//! 注意：托盘图标必须在**主线程**、且事件循环已经跑起来之后创建。
//! eframe 的 `App::new` 正好满足（它在 winit 的事件循环里被调用）。
//!
//! # 为什么菜单点击不排队等 UI 处理
//!
//! 窗口被 `ViewportCommand::Visible(false)` 藏进托盘后，egui 那一侧不再产生
//! 绘制，`ViewportCommand::Visible(true)` / `Close` 这类命令经常落不了地——
//! 表现就是右键菜单里「显示主窗口」「退出」点了没反应，托盘图标赖在通知区域
//! 里，只能去任务管理器结束进程。
//!
//! 所以 Windows 上菜单点击**直接在回调里操作窗口**（`ShowWindow` /
//! `PostMessage(WM_CLOSE)`），不依赖 egui 是否还在出帧；队列只用来让界面
//! 同步内部状态。

use std::sync::atomic::{AtomicBool, Ordering};

/// 用户在托盘菜单里点了什么。
#[derive(Clone, Copy, Debug)]
pub enum TrayCommand {
    /// 显示主窗口。
    Show,
    /// 退出应用。
    Quit,
}

/// 托盘里点过「退出」。
///
/// 用进程级原子量而不是只靠队列：关闭请求会立刻被系统送进来，
/// `update()` 里必须能同步认出“这次关闭是用户主动退出”，否则会被
/// 「关闭到托盘」的逻辑再拦一次，窗口永远关不掉。
static QUIT_REQUESTED: AtomicBool = AtomicBool::new(false);

pub fn quit_requested() -> bool {
    QUIT_REQUESTED.load(Ordering::SeqCst)
}

#[cfg(target_os = "windows")]
mod win32 {
    #[link(name = "user32")]
    extern "system" {
        pub fn IsWindow(hwnd: isize) -> i32;
        pub fn IsIconic(hwnd: isize) -> i32;
        pub fn ShowWindow(hwnd: isize, cmd: i32) -> i32;
        pub fn SetForegroundWindow(hwnd: isize) -> i32;
        pub fn PostMessageW(hwnd: isize, msg: u32, wparam: usize, lparam: isize) -> i32;
    }

    pub const SW_SHOW: i32 = 5;
    pub const SW_RESTORE: i32 = 9;
    pub const WM_CLOSE: u32 = 0x0010;
}

#[cfg(any(target_os = "windows", target_os = "macos"))]
mod platform {
    use super::TrayCommand;
    use crate::{t, tr};
    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex};
    use tray_icon::menu::{Menu, MenuItem};
    use tray_icon::{TrayIcon, TrayIconBuilder};
    #[cfg(target_os = "windows")]
    use super::QUIT_REQUESTED;
    #[cfg(target_os = "windows")]
    use std::sync::atomic::Ordering;

    pub struct Tray {
        // 必须一直持有：句柄一 drop，托盘图标就消失了
        _icon: TrayIcon,
        queue: Arc<Mutex<VecDeque<TrayCommand>>>,
    }

    impl Tray {
        /// 创建托盘图标 + 右键菜单（显示主窗口 / 退出）。
        ///
        /// `hwnd` 是主窗口的原生句柄（非 Windows 传 0）：菜单点击时直接用它
        /// 把窗口拉回来 / 关掉，绕开“藏起来的窗口收不到 egui 命令”这个坑。
        pub fn new(ctx: &eframe::egui::Context, hwnd: isize) -> Result<Self, String> {
            let icon = build_icon()?;

            let menu = Menu::new();
            let show = MenuItem::new(t!("显示主窗口", "Show window"), true, None);
            let quit = MenuItem::new(t!("退出", "Quit"), true, None);
            menu.append(&show).map_err(|e| tr!("托盘菜单创建失败: {e}", "failed to create tray menu: {e}"))?;
            menu.append(&quit).map_err(|e| tr!("托盘菜单创建失败: {e}", "failed to create tray menu: {e}"))?;

            let show_id = show.id().clone();
            let quit_id = quit.id().clone();

            let queue: Arc<Mutex<VecDeque<TrayCommand>>> = Arc::new(Mutex::new(VecDeque::new()));

            // 右键菜单：回调在系统菜单线程里被调用，这里直接动手 + 入队
            let menu_queue = queue.clone();
            let menu_ctx = ctx.clone();
            tray_icon::menu::MenuEvent::set_event_handler(Some(
                move |event: tray_icon::menu::MenuEvent| {
                    let cmd = if event.id == show_id {
                        TrayCommand::Show
                    } else if event.id == quit_id {
                        TrayCommand::Quit
                    } else {
                        return;
                    };
                    dispatch(cmd, hwnd);
                    if let Ok(mut q) = menu_queue.lock() {
                        q.push_back(cmd);
                    }
                    menu_ctx.request_repaint();
                },
            ));

            // 左键单击图标 = 打开主窗口（Windows 上左键不弹菜单）。
            // 多一条不经过菜单的路径：菜单事件万一没送到，用户也还能把窗口捞回来。
            #[cfg(target_os = "windows")]
            {
                let click_queue = queue.clone();
                let click_ctx = ctx.clone();
                tray_icon::TrayIconEvent::set_event_handler(Some(
                    move |event: tray_icon::TrayIconEvent| {
                        if let tray_icon::TrayIconEvent::Click {
                            button: tray_icon::MouseButton::Left,
                            button_state: tray_icon::MouseButtonState::Up,
                            ..
                        } = event
                        {
                            dispatch(TrayCommand::Show, hwnd);
                            if let Ok(mut q) = click_queue.lock() {
                                q.push_back(TrayCommand::Show);
                            }
                            click_ctx.request_repaint();
                        }
                    },
                ));
            }

            let icon = TrayIconBuilder::new()
                .with_menu(Box::new(menu))
                // 通知区域显示的就是这句 tooltip。中文界面按用户要求写成不带空格的
                // “RaceBox同步”，其它语言保持品牌英文名（品牌名不翻译）。
                .with_tooltip(t!("RaceBox同步", "RaceBox Sync"))
                .with_icon(icon)
                .build()
                .map_err(|e| tr!("托盘图标创建失败: {e}", "failed to create tray icon: {e}"))?;

            Ok(Self {
                _icon: icon,
                queue,
            })
        }

        /// 取出待处理的菜单命令（非阻塞，先进先出）。
        pub fn poll(&self) -> Option<TrayCommand> {
            let mut q = self.queue.lock().ok()?;
            q.pop_front()
        }
    }

    /// 立刻执行托盘命令。
    ///
    /// Windows 上直接调 Win32；macOS 上留空（那边菜单事件能正常唤醒 eframe，
    /// 队列里的命令由 `update()` 消费）。
    #[cfg(target_os = "windows")]
    fn dispatch(cmd: TrayCommand, hwnd: isize) {
        if hwnd == 0 {
            return;
        }
        unsafe {
            if super::win32::IsWindow(hwnd) == 0 {
                return;
            }
            match cmd {
                TrayCommand::Show => {
                    if super::win32::IsIconic(hwnd) != 0 {
                        super::win32::ShowWindow(hwnd, super::win32::SW_RESTORE);
                    }
                    super::win32::ShowWindow(hwnd, super::win32::SW_SHOW);
                    super::win32::SetForegroundWindow(hwnd);
                }
                TrayCommand::Quit => {
                    // 先立旗：eframe 收到 WM_CLOSE 会立刻跑一次 update，
                    // 那次必须认出“是用户要退出”，否则会被当成“收进托盘”再藏起来。
                    QUIT_REQUESTED.store(true, Ordering::SeqCst);
                    super::win32::PostMessageW(hwnd, super::win32::WM_CLOSE, 0, 0);
                }
            }
        }
    }

    #[cfg(not(target_os = "windows"))]
    fn dispatch(_cmd: TrayCommand, _hwnd: isize) {}

    /// 用窗口图标缩一份托盘图标：托盘只有 16~32px，直接塞 256px 会糊。
    fn build_icon() -> Result<tray_icon::Icon, String> {
        let bytes = include_bytes!("../assets/icon256.png");
        let img = eframe::icon_data::from_png_bytes(bytes)
            .map_err(|e| tr!("托盘图标解码失败: {e}", "failed to decode tray icon: {e}"))?;
        let (rgba, w, h) = resize(&img.rgba, img.width, img.height, 32);
        tray_icon::Icon::from_rgba(rgba, w, h).map_err(|e| tr!("托盘图标无效: {e}", "invalid tray icon: {e}"))
    }

    /// 面积平均缩小（按 alpha 预乘再还原，避免透明边缘发黑）。
    fn resize(rgba: &[u8], w: u32, h: u32, target: u32) -> (Vec<u8>, u32, u32) {
        if w <= target && h <= target {
            return (rgba.to_vec(), w, h);
        }
        let tw = target.min(w);
        let th = target.min(h);
        let mut out = vec![0u8; (tw * th * 4) as usize];
        for ty in 0..th {
            let y0 = ty * h / th;
            let y1 = (((ty + 1) * h / th).max(y0 + 1)).min(h);
            for tx in 0..tw {
                let x0 = tx * w / tw;
                let x1 = (((tx + 1) * w / tw).max(x0 + 1)).min(w);
                let (mut r, mut g, mut b, mut a, mut n) = (0u64, 0u64, 0u64, 0u64, 0u64);
                for y in y0..y1 {
                    for x in x0..x1 {
                        let i = ((y * w + x) * 4) as usize;
                        let alpha = rgba[i + 3] as u64;
                        r += rgba[i] as u64 * alpha;
                        g += rgba[i + 1] as u64 * alpha;
                        b += rgba[i + 2] as u64 * alpha;
                        a += alpha;
                        n += 1;
                    }
                }
                let o = ((ty * tw + tx) * 4) as usize;
                if a > 0 {
                    out[o] = (r / a) as u8;
                    out[o + 1] = (g / a) as u8;
                    out[o + 2] = (b / a) as u8;
                }
                if n > 0 {
                    out[o + 3] = (a / n) as u8;
                }
            }
        }
        (out, tw, th)
    }
}

#[cfg(not(any(target_os = "windows", target_os = "macos")))]
mod platform {
    use super::TrayCommand;
    use crate::t;

    /// 其它平台：没有托盘，创建直接失败，调用方会退回“关闭即退出”。
    pub struct Tray;

    impl Tray {
        pub fn new(_ctx: &eframe::egui::Context, _hwnd: isize) -> Result<Self, String> {
            Err(t!(
                "当前平台不支持系统托盘",
                "this platform does not support a system tray"
            )
            .to_string())
        }

        pub fn poll(&self) -> Option<TrayCommand> {
            None
        }
    }
}

pub use platform::Tray;
