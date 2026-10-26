//! 系统托盘图标（Windows 通知区域 / macOS 菜单栏）。
//!
//! 只在 Windows / macOS 上真正实现：Linux 的托盘要依赖 GTK / AppIndicator 系统库，
//! 交叉编译 Windows 时用不上，索性不引入——非这两个平台直接当作“不支持”，
//! 关闭行为会退回“直接退出”。
//!
//! 注意：托盘图标必须在**主线程**、且事件循环已经跑起来之后创建。
//! eframe 的 `App::new` 正好满足（它在 winit 的事件循环里被调用）。

/// 用户在托盘菜单里点了什么。
#[derive(Clone, Copy, Debug)]
pub enum TrayCommand {
    /// 显示主窗口。
    Show,
    /// 退出应用。
    Quit,
}

#[cfg(any(target_os = "windows", target_os = "macos"))]
mod platform {
    use super::TrayCommand;
    use crate::{t, tr};
    use std::sync::{Arc, Mutex};

    pub struct Tray {
        // 必须一直持有：句柄一 drop，托盘图标就消失了
        _icon: tray_icon::TrayIcon,
        queue: Arc<Mutex<Vec<TrayCommand>>>,
    }

    impl Tray {
        /// 创建托盘图标 + 右键菜单（显示主窗口 / 退出）。
        /// `ctx` 用来在菜单被点击时立刻唤醒界面，不用靠轮询。
        pub fn new(ctx: &eframe::egui::Context) -> Result<Self, String> {
            let icon = build_icon()?;

            let menu = tray_icon::menu::Menu::new();
            let show = tray_icon::menu::MenuItem::new(t!("显示主窗口", "Show window"), true, None);
            let quit = tray_icon::menu::MenuItem::new(t!("退出", "Quit"), true, None);
            menu.append(&show).map_err(|e| tr!("托盘菜单创建失败: {e}", "failed to create tray menu: {e}"))?;
            menu.append(&quit).map_err(|e| tr!("托盘菜单创建失败: {e}", "failed to create tray menu: {e}"))?;

            let show_id = show.id().clone();
            let quit_id = quit.id().clone();

            let queue: Arc<Mutex<Vec<TrayCommand>>> = Arc::new(Mutex::new(Vec::new()));
            let handler_queue = queue.clone();
            let handler_ctx = ctx.clone();
            // 回调在系统菜单线程里被调用，这里只入队 + 请求重绘，真正的处理放在 update()
            tray_icon::menu::MenuEvent::set_event_handler(Some(
                move |event: tray_icon::menu::MenuEvent| {
                    if let Ok(mut q) = handler_queue.lock() {
                        if event.id == show_id {
                            q.push(TrayCommand::Show);
                        } else if event.id == quit_id {
                            q.push(TrayCommand::Quit);
                        }
                    }
                    handler_ctx.request_repaint();
                },
            ));

            let icon = tray_icon::TrayIconBuilder::new()
                .with_menu(Box::new(menu))
                .with_tooltip(t!("RaceBox 同步", "RaceBox Sync"))
                .with_icon(icon)
                .build()
                .map_err(|e| tr!("托盘图标创建失败: {e}", "failed to create tray icon: {e}"))?;

            Ok(Self {
                _icon: icon,
                queue,
            })
        }

        /// 取出待处理的菜单命令（非阻塞）。
        pub fn poll(&self) -> Option<TrayCommand> {
            let mut q = self.queue.lock().ok()?;
            q.pop()
        }
    }

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
        pub fn new(_ctx: &eframe::egui::Context) -> Result<Self, String> {
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
