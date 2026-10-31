//! 「RaceBox 云」相关组件：账号登录、同步时自动上传、本地文件导入。
//!
//! 这些控件拆给两个页面用：账号与上传配置在设置页的「RaceBox 云」卡，
//! 本地文件导入在同步主页的「本地文件」卡。
//!
//! 和设置页不同，这里的改动**立即生效**（开关、序列号直接写回 config.json
//! 的 cloud 一节）：没有「保存 / 放弃」的心智负担，也不存在改了一半退出的问题。
//!
//! 后台任务统一用 `std::sync::mpsc` 回传结果，和 `updater` 的做法一致——
//! 界面每帧 `poll()` 一次，不阻塞渲染。

use crate::config::{self, Config};
use crate::racebox::store::{self, CloudAccount};
use crate::racebox::CloudClient;
use crate::style::{self, Palette};
use crate::worker::LogLevel;
use crate::{t, tr};
use eframe::egui;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::runtime::Runtime;
// Button::ui() 是 Widget trait 提供的方法，不显式引入会报「找不到方法 ui」。
use eframe::egui::Widget;

/// 后台任务回传给界面的消息。
enum CloudMsg {
    LoggedIn(Box<CloudAccount>),
    LoginFailed(String),
}

/// 目录里扫出来的一个可导入文件。
struct ImportFile {
    path: PathBuf,
    name: String,
    size: u64,
    modified: String,
}

pub struct CloudState {
    email: String,
    password: String,
    account: CloudAccount,
    busy: bool,
    /// 顶部一次性提示（登录结果、上传结果等）。
    message: Option<(String, LogLevel)>,
    /// 目录有没有扫过（每帧都扫一遍磁盘太浪费）。
    scanned: bool,
    /// 设备序列号输入框的原文。单独缓存是为了让「半个数字」也能留在框里——
    /// 每帧拿 cfg 的值重建输入框会把正在输入的内容吞掉。
    serial_buf: String,
    /// 时区偏移输入框的原文，同上。
    tz_buf: String,
    /// 弹窗由 ui.rs 统一渲染：这里立标志，App 下一帧把它打开。
    login_requested: bool,
    dir: String,
    /// 路径改过但还没重新扫描（等回车 / 失焦再扫，避免每敲一个键都扫一遍目录）。
    dir_changed: bool,
    files: Vec<ImportFile>,
    selected: Vec<bool>,
    /// 主窗口的原生句柄：给系统文件对话框当属主用（非 Windows 传 0，不生效）。
    parent: isize,
    tx: std::sync::mpsc::Sender<CloudMsg>,
    rx: std::sync::mpsc::Receiver<CloudMsg>,
}

impl CloudState {
    pub fn new(cfg: &Config, parent: isize) -> Self {
        let account = store::load();
        let (tx, rx) = std::sync::mpsc::channel();
        let dir = if cfg.cloud.import_dir.is_empty() {
            default_import_dir()
        } else {
            cfg.cloud.import_dir.clone()
        };
        let mut me = Self {
            email: account.email.clone(),
            password: account.password.clone(),
            account,
            login_requested: false,
            busy: false,
            message: None,
            scanned: false,
            serial_buf: serial_to_buf(cfg.cloud.device_serial),
            tz_buf: cfg.cloud.tz_offset_min.to_string(),
            dir,
            dir_changed: false,
            files: Vec::new(),
            selected: Vec::new(),
            parent,
            tx,
            rx,
        };
        // 上次记住的目录直接全选：用户上次勾的就是这些，打开就能传
        me.rescan(true);
        me
    }

    /// 重新扫目录。
    ///
    /// `select_all = false` 时按文件名保留原来的勾选（手动改过路径后重扫，
    /// 不该把用户刚挑的挑没了）；`true` 时全选（刚换目录，列表里全是新的，
    /// 默认就该全传）。
    fn rescan(&mut self, select_all: bool) {
        self.scanned = true;
        let keep: Vec<String> = if select_all {
            Vec::new()
        } else {
            self.files
                .iter()
                .zip(self.selected.iter())
                .filter(|(_, sel)| **sel)
                .map(|(f, _)| f.name.clone())
                .collect()
        };
        self.files = scan_dir(Path::new(&self.dir));
        self.selected = self
            .files
            .iter()
            .map(|f| select_all || keep.contains(&f.name))
            .collect();
    }

    /// 弹出系统原生的「选文件夹」对话框，选完把整个文件夹里的轨迹都扫进来。
    ///
    /// 这是本地上传的**唯一入口**：用户的文件通常是一整批（相机/手表导出的一个目录），
    /// 让人一个个勾文件既慢又容易漏。选完目录直接全选，点大圆就能传。
    ///
    /// `pick_folder()` 返回 `None` 有两种可能：用户点了取消，或者对话框根本没弹出来
    /// （COM 没初始化、没有桌面会话、被安全软件拦了）。界面层分不出来，
    /// 所以两种情况都往日志里写一句——线上反馈「点了没反应」时能查到是哪一种。
    pub fn pick_dir(&mut self, cfg: &mut Config) {
        let mut dlg = with_parent(
            rfd::FileDialog::new().set_title(t!("选择轨迹所在文件夹", "Pick the track folder")),
            self.parent,
        );
        if Path::new(&self.dir).is_dir() {
            dlg = dlg.set_directory(&self.dir);
        }
        match dlg.pick_folder() {
            Some(dir) => {
                log::info!("文件夹选择框返回 {}", dir.display());
                self.dir = dir.display().to_string();
                // 换了目录，上一批文件的勾选没有意义了：整个目录重新扫、全部勾上
                self.load_dir_all(cfg);
            }
            None => log::info!("文件夹选择框被取消或无法显示"),
        }
    }

    /// 重新扫当前目录并**全选**，然后记住这个目录。
    ///
    /// 用在「刚选完文件夹」和「第一次打开这张卡」两种场景：这时列表里出现的
    /// 就是用户要传的东西，再让他点一次「全选」是多余的。
    fn load_dir_all(&mut self, cfg: &mut Config) {
        self.rescan(true);
        self.save_dir(cfg);
    }

    /// 记住当前目录：重扫 + 写回 cloud 配置（只写 cloud 段）。
    ///
    /// 手输路径换到**另一个**目录时整批全选：新目录里的文件都是用户没见过的，
    /// 留个「已选 0」还得再点一次「全选」才传得动，纯属白费一步。
    /// 同一个目录里只是敲错又改回来（或点了「扫描」）则保留原勾选。
    fn remember_dir(&mut self, cfg: &mut Config) {
        let switched = self.dir != cfg.cloud.import_dir;
        self.rescan(switched);
        self.save_dir(cfg);
    }

    /// 把 `self.dir` 写回配置。存不下要让用户看见——静默失败会让人以为目录
    /// 已经记住了，下次打开又没了。
    fn save_dir(&mut self, cfg: &mut Config) {
        cfg.cloud.import_dir = self.dir.clone();
        if let Err(e) = config::save_cloud(&cfg.cloud) {
            self.message = Some((
                tr!("配置保存失败：{}", "could not save config: {}", e),
                LogLevel::Error,
            ));
        }
        self.dir_changed = false;
    }

    /// 每帧收一次后台消息（ui.rs 的主循环里统一调用，与当前在哪个页面无关）。
    pub fn poll(&mut self) {
        while let Ok(msg) = self.rx.try_recv() {
            match msg {
                CloudMsg::LoggedIn(acc) => {
                    self.busy = false;
                    self.message = Some((
                        tr!(
                            "已登录：{}",
                            "signed in as {}",
                            if acc.name.is_empty() {
                                acc.email.clone()
                            } else {
                                acc.name.clone()
                            }
                        ),
                        LogLevel::Success,
                    ));
                    self.email = acc.email.clone();
                    self.account = *acc;
                }
                CloudMsg::LoginFailed(e) => {
                    self.busy = false;
                    self.message = Some((e, LogLevel::Error));
                }
            }
        }
    }

    fn start_login(&mut self, rt: &Arc<Runtime>, email: String, password: String) {
        if self.busy {
            return;
        }
        if email.trim().is_empty() || password.is_empty() {
            self.message = Some((
                t!("请先填写邮箱和密码", "enter e-mail and password first").to_string(),
                LogLevel::Warn,
            ));
            return;
        }
        self.busy = true;
        self.message = None;
        let tx = self.tx.clone();
        let email = email.trim().to_string();
        rt.spawn(async move {
            let mut client = CloudClient::new(None);
            match client.login(&email, &password).await {
                Ok(ok) => {
                    let acc = CloudAccount {
                        email: if ok.email.is_empty() {
                            email
                        } else {
                            ok.email
                        },
                        password,
                        auth: ok.auth,
                        user_id: ok.user_id,
                        name: ok.name,
                    };
                    if let Err(e) = store::save(&acc) {
                        let _ = tx.send(CloudMsg::LoginFailed(format!("保存凭据失败: {e}")));
                        return;
                    }
                    let _ = tx.send(CloudMsg::LoggedIn(Box::new(acc)));
                }
                Err(e) => {
                    let _ = tx.send(CloudMsg::LoginFailed(e));
                }
            }
        });
    }

    pub fn account_card(&mut self, ui: &mut egui::Ui, p: &Palette, rt: &Arc<Runtime>) {
        self.show_message(ui, p);
        ui.horizontal(|ui| {
            ui.vertical(|ui| {
                if self.account.logged_in() {
                    style::card_title(ui, p, &self.account.display_name());
                    style::hint(ui, p, &self.account.email);
                } else {
                    style::card_title(ui, p, t!("未登录", "Signed out"));
                    style::hint(
                        ui,
                        p,
                        t!(
                            "登录后可把会话传到官网，同步时的「RaceBox 云」一路要用到它",
                            "sign in to also send sessions to racebox.pro during sync"
                        ),
                    );
                }
            });
            ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                let label = if self.busy {
                    t!("登录中…", "Signing in…")
                } else if self.account.logged_in() {
                    t!("退出登录", "Sign out")
                } else {
                    t!("登录", "Sign in")
                };
                if ui
                    .add_enabled(!self.busy, style::secondary_button(p, label))
                    .clicked()
                {
                    if self.account.logged_in() {
                        self.sign_out();
                    } else {
                        self.login_requested = true;
                    }
                }
            });
        });
        // 弹窗由 ui.rs 统一渲染：App 那边按这个标志打开
        let _ = rt;
    }

    /// 退出登录：把本机保存的凭据删掉。
    pub fn sign_out(&mut self) {
        let _ = store::clear();
        self.account = CloudAccount::default();
        self.email.clear();
        self.password.clear();
        self.message = Some((t!("已退出登录", "signed out").to_string(), LogLevel::Info));
    }

    /// 设置页里的「上传配置」行（开关 + 序列号 + 时区）。返回是否改动过配置。
    ///
    /// 这一段原来在云页面，跟着账号一起搬进设置页：它们是同一件事的两半
    /// （要不要传 / 传的时候挂在哪台设备名下），分开放在两个页面很难找。
    pub fn upload_rows(&mut self, ui: &mut egui::Ui, p: &Palette, cfg: &mut Config) -> bool {
        let mut dirty = false;
        if style::toggle_row(
            ui,
            p,
            t!(
                "同步时把每段会话同时传到 RaceBox 云",
                "Also send each session to RaceBox Cloud while syncing"
            ),
            &mut cfg.cloud.enabled,
        )
        .clicked()
        {
            dirty = true;
        }
        ui.add_space(10.0);

        egui::Grid::new("cloud_settings")
            .num_columns(2)
            .min_col_width(76.0)
            .spacing([10.0, 8.0])
            .show(ui, |ui| {
                ui.colored_label(p.muted, t!("设备序列号", "Device serial"));
                let r = ui.add(
                    egui::TextEdit::singleline(&mut self.serial_buf)
                        .hint_text(t!("留空表示不指定", "leave empty for none"))
                        .desired_width(field_width(ui, 140.0, 240.0)),
                );
                if r.changed() {
                    // 正在敲的那一版就算不合法也留在框里，别每帧拿配置重建把输入吞掉；
                    // 认得出来（纯数字）才写回配置。
                    if let Some(v) = parse_u64(&self.serial_buf) {
                        cfg.cloud.device_serial = v;
                        dirty = true;
                    }
                }
                if r.lost_focus() {
                    // 失焦时把框里的东西按配置值复位：乱敲的内容自动消失。
                    self.serial_buf = serial_to_buf(cfg.cloud.device_serial);
                }
                ui.end_row();

                ui.colored_label(p.muted, t!("时区偏移（分钟）", "UTC offset (minutes)"));
                let r = ui.add(
                    egui::TextEdit::singleline(&mut self.tz_buf)
                        .hint_text("480")
                        .desired_width(field_width(ui, 140.0, 240.0)),
                );
                if r.changed() {
                    if let Some(v) = parse_tz(&self.tz_buf) {
                        cfg.cloud.tz_offset_min = v;
                        dirty = true;
                    }
                }
                if r.lost_focus() {
                    self.tz_buf = cfg.cloud.tz_offset_min.to_string();
                }
                ui.end_row();
            });
        dirty
    }

    /// 把提示写回 cloud 配置（只合并 cloud 这一段）。
    pub fn save_cloud(&mut self, cfg: &Config) {
        if let Err(e) = config::save_cloud(&cfg.cloud) {
            self.message = Some((
                tr!("配置保存失败：{}", "could not save config: {}", e),
                LogLevel::Error,
            ));
        }
    }

    /// 有没有待打开的登录弹窗（App 每帧看一次，打开后自动清掉）。
    pub fn take_login_request(&mut self) -> bool {
        std::mem::take(&mut self.login_requested)
    }

    /// 用弹窗里填的凭据登录。
    pub fn login(&mut self, rt: &Arc<Runtime>, email: String, password: String) {
        self.start_login(rt, email, password);
    }

    /// 有没有排队等着显示的提示。
    pub fn take_message(&mut self) -> Option<(String, LogLevel)> {
        self.message.take()
    }

    /// 把排队的一次性提示画出来（登录结果、导入结果等）。
    pub fn show_message(&mut self, ui: &mut egui::Ui, p: &Palette) {
        let Some((text, level)) = self.take_message() else {
            return;
        };
        let color = match level {
            LogLevel::Error => p.error,
            LogLevel::Warn => p.warn,
            LogLevel::Success => p.success,
            LogLevel::Info => p.info,
        };
        style::status_banner(ui, p, color, &text);
        ui.add_space(8.0);
    }

    /// 同步页下方的「本地文件」卡：路径 + 扫描 + 勾选列表。
    ///
    /// 只在主页点了「选择本地文件」之后才画（见 `ui.rs`）。卡右上角是
    /// 「取消本地上传」，点它把主页切回设备同步，这张卡就收起来。
    /// 返回 `true` 表示用户点了取消。
    ///
    /// 上传本身**不在这张卡里**：整页只有一个主操作（上面那个大圆），
    /// 主页按当前模式决定是读设备还是读这些文件——所以这张卡只负责
    /// 「这次要传什么」，不再自己起一个上传任务。
    pub fn import_card(&mut self, ui: &mut egui::Ui, p: &Palette, cfg: &mut Config) -> bool {
        self.accept_dropped(ui);
        // 目录只在第一次扫一遍（`CloudState::new` 里已经扫过并全选了）：
        // 每帧都扫一遍磁盘太浪费
        if !self.scanned {
            self.rescan(true);
        }
        let picked = self.selected.iter().filter(|s| **s).count();
        let mut cancel = false;

        style::card(p).show(ui, |ui| {
            style::fill_card_width(ui);
            // 标题行：左边卡名，右边「取消本地上传」+ 说明图标
            ui.horizontal(|ui| {
                style::card_title(ui, p, t!("本地文件", "Local files"));
                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    if style::small_button(p, t!("取消本地上传", "Cancel"))
                        .ui(ui)
                        .on_hover_text(t!(
                            "收起这张卡，主页回到设备同步",
                            "collapse this card and go back to the device sync"
                        ))
                        .clicked()
                    {
                        cancel = true;
                    }
                    ui.add_space(10.0);
                    style::help_icon(
                        ui,
                        p,
                        "import_files",
                        t!(
                            "选一个文件夹，自动扫出里面所有 .gpx / .fit 并全部勾选。上传去处和「设备同步」完全一致。",
                            "Pick a folder: every .gpx / .fit inside is listed and checked. The destinations are exactly the same as a device sync."
                        ),
                    );
                });
            });
            ui.add_space(10.0);

            // 路径 + 选择文件夹 / 扫描，从左到右依次排。
            // 「扫描」放最后（用户标注的要求），但不用 right_to_left 把它顶到最右——
            // 那样它和前面的按钮之间会空出一大截，看着像少画了什么。
            ui.horizontal(|ui| {
                // 地址栏太长会把后面的按钮挤出卡片，给一个上限（用户标注：不要太长）
                let path_w = (ui.available_width() - 230.0).clamp(140.0, 340.0);
                let r = ui.add(
                    egui::TextEdit::singleline(&mut self.dir)
                        .hint_text(t!("文件夹路径", "folder path"))
                        .desired_width(path_w),
                );
                let enter = ui.input(|i| i.key_pressed(egui::Key::Enter));
                if (r.lost_focus() || (r.has_focus() && enter)) && self.dir_changed {
                    self.remember_dir(cfg);
                }
                if r.changed() {
                    self.dir_changed = true;
                }
                ui.add_space(10.0);
                // 主入口：选一个文件夹，整个目录里的 .gpx / .fit 一起导进来。
                // 不提供「选单个文件」——用户的轨迹都是一批一批的，一个个挑太慢。
                if style::secondary_button(p, t!("选择文件夹…", "Choose folder…"))
                    .ui(ui)
                    .on_hover_text(t!(
                        "选一个文件夹，自动扫出里面所有 .gpx / .fit 并全部勾选",
                        "pick a folder; every .gpx / .fit inside is listed and checked"
                    ))
                    .clicked()
                {
                    self.pick_dir(cfg);
                }
                ui.add_space(6.0);
                if style::small_button(p, t!("扫描", "Scan"))
                    .ui(ui)
                    .on_hover_text(t!(
                        "重新读一遍这个文件夹里的 .gpx / .fit",
                        "re-read the .gpx / .fit files in this folder"
                    ))
                    .clicked()
                {
                    self.remember_dir(cfg);
                }
            });
            ui.add_space(10.0);

            if self.files.is_empty() {
                ui.vertical_centered(|ui| {
                    ui.add_space(14.0);
                    ui.colored_label(
                        p.muted,
                        t!(
                            "这个文件夹里没有 .gpx / .fit 文件",
                            "no .gpx / .fit file in this folder"
                        ),
                    );
                    ui.add_space(4.0);
                    ui.colored_label(
                        p.muted,
                        t!(
                            "点上面的「选择文件夹…」换一个，或把文件直接拖进窗口",
                            "press Choose folder… above to pick another one, or drop files onto the window"
                        ),
                    );
                    ui.add_space(14.0);
                });
                return;
            }

            // 一行「全选 / 全不选」+ 已选统计
            let all_selected = self.selected.iter().all(|s| *s);
            ui.horizontal(|ui| {
                ui.colored_label(
                    p.muted,
                    tr!(
                        "共 {} 个 · 已选 {}",
                        "{} files · {} selected",
                        self.files.len(),
                        picked
                    ),
                );
                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    if style::small_button(
                        p,
                        if all_selected {
                            t!("全不选", "None")
                        } else {
                            t!("全选", "All")
                        },
                    )
                    .ui(ui)
                    .clicked()
                    {
                        for s in self.selected.iter_mut() {
                            *s = !all_selected;
                        }
                    }
                });
            });
            ui.add_space(6.0);
            self.file_rows(ui, p);
        });
        cancel
    }

    /// 当前勾选的文件（交给上传任务）。
    pub fn selected_paths(&self) -> Vec<PathBuf> {
        self.files
            .iter()
            .zip(self.selected.iter())
            .filter(|(_, sel)| **sel)
            .map(|(f, _)| f.path.clone())
            .collect()
    }

    /// 文件列表：整行可点（点一下切换勾选），右侧是大小和修改时间。
    fn file_rows(&mut self, ui: &mut egui::Ui, p: &Palette) {
        let row_h = 30.0;
        let rows = self.files.len();
        // 一张卡里放得下就全展开，最多留出十行的高度，剩下的滚动
        let height = (rows as f32 * row_h).clamp(60.0, 300.0);
        egui::ScrollArea::vertical()
            .id_salt("import_files")
            .max_height(height)
            .auto_shrink([false, false])
            .show(ui, |ui| {
                let width = ui.available_width();
                for i in 0..rows {
                    let (name, meta) = {
                        let f = &self.files[i];
                        (
                            f.name.clone(),
                            format!("{}  ·  {}", human_size(f.size), f.modified),
                        )
                    };
                    let (rect, resp) =
                        ui.allocate_at_least(egui::vec2(width, row_h), egui::Sense::click());
                    if resp.hovered() {
                        ui.painter()
                            .rect_filled(rect, egui::CornerRadius::same(6), p.hover);
                    }
                    if resp.clicked() {
                        self.selected[i] = !self.selected[i];
                    }
                    let sel = self.selected[i];
                    check_box(ui, p, egui::pos2(rect.min.x + 12.0, rect.center().y), sel);

                    let color = if sel { p.text } else { p.muted };
                    let font = egui::FontId::proportional(13.0);
                    // 右边要留地方给大小和时间，放不下就从中间砍，扩展名留着
                    let meta_w = ui
                        .fonts(|f| f.layout_no_wrap(meta.clone(), egui::FontId::proportional(11.5), p.muted))
                        .size()
                        .x;
                    let max_name = (rect.width() - meta_w - 70.0).max(60.0);
                    let name = elide_middle(ui, &name, &font, color, max_name);
                    let galley = ui.fonts(|f| f.layout_no_wrap(name, font, color));
                    ui.painter().galley(
                        egui::pos2(rect.min.x + 32.0, rect.center().y - galley.size().y / 2.0),
                        galley,
                        color,
                    );
                    let meta_galley = ui.fonts(|f| {
                        f.layout_no_wrap(meta, egui::FontId::proportional(11.5), p.muted)
                    });
                    ui.painter().galley(
                        egui::pos2(
                            rect.max.x - 10.0 - meta_galley.size().x,
                            rect.center().y - meta_galley.size().y / 2.0,
                        ),
                        meta_galley,
                        p.muted,
                    );
                    // 行分隔线：最后一行不画，免得和卡片边框叠一起
                    if i + 1 < rows {
                        ui.painter().line_segment(
                            [
                                egui::pos2(rect.min.x + 4.0, rect.max.y),
                                egui::pos2(rect.max.x - 4.0, rect.max.y),
                            ],
                            egui::Stroke::new(1.0_f32, crate::style::tint(p.card_stroke, 130)),
                        );
                    }
                }
            });
    }

    /// 拖放：把文件直接加到列表里（不改变当前目录）。
    pub fn accept_dropped(&mut self, ui: &mut egui::Ui) {
        let dropped: Vec<PathBuf> = ui.ctx().input(|i| {
            i.raw
                .dropped_files
                .iter()
                .filter_map(|f| f.path.clone())
                .collect()
        });
        for path in dropped {
            if !importable(&path) {
                continue;
            }
            if self.files.iter().any(|f| f.path == path) {
                continue;
            }
            self.files.push(file_entry(&path));
            self.selected.push(true);
        }
    }
}

/// 给系统对话框指定属主窗口。
///
/// 不设属主时，Windows 的资源管理器对话框会飘到所有窗口最上面、而且主窗口还能
/// 被拖动/点击，看着像程序卡住了。设了属主就变成正经的模态子窗口。
///
/// 非 Windows 平台没有「属主」这个概念（macOS 的 sheet 由 AppKit 自己处理），
/// 传 0 直接原样返回。
fn with_parent(dlg: rfd::FileDialog, parent: isize) -> rfd::FileDialog {
    #[cfg(target_os = "windows")]
    {
        use raw_window_handle::{
            DisplayHandle, HandleError, HasDisplayHandle, HasWindowHandle, RawDisplayHandle,
            RawWindowHandle, Win32WindowHandle, WindowHandle, WindowsDisplayHandle,
        };
        use std::num::NonZeroIsize;

        /// 只包住一个 HWND 的壳：`set_parent` 要的是实现了两个 handle trait 的类型，
        /// 而这里手上只有一个从 winit 那边存下来的整数句柄。
        struct Parent(WindowHandle<'static>, DisplayHandle<'static>);

        impl HasWindowHandle for Parent {
            fn window_handle(&self) -> Result<WindowHandle<'_>, HandleError> {
                Ok(self.0)
            }
        }
        impl HasDisplayHandle for Parent {
            fn display_handle(&self) -> Result<DisplayHandle<'_>, HandleError> {
                Ok(self.1)
            }
        }

        if let Some(hwnd) = NonZeroIsize::new(parent) {
            // SAFETY: hwnd 来自主窗口，只要主窗口还在（对话框是从主窗口的 update 里
            // 弹出来的，一定还在），它就一直有效。
            let win = unsafe {
                WindowHandle::borrow_raw(RawWindowHandle::Win32(Win32WindowHandle::new(hwnd)))
            };
            let disp = unsafe {
                DisplayHandle::borrow_raw(RawDisplayHandle::Windows(WindowsDisplayHandle::new()))
            };
            return dlg.set_parent(&Parent(win, disp));
        }
    }
    let _ = parent;
    dlg
}

/// 输入框宽度：跟着可用宽度走。
///
/// 固定宽度在窄窗口会把这行挤到卡片外面（外层滚动区还给不出横向滚动条），
/// 在宽窗口又只占一小条。给一个上下限，中间随窗口变化。
fn field_width(ui: &egui::Ui, min: f32, max: f32) -> f32 {
    (ui.available_width() - 16.0).clamp(min, max)
}

/// 序列号输入框的初值：0 就是“不指定”，显示成空串而不是 "0"。
fn serial_to_buf(v: u64) -> String {
    if v == 0 {
        String::new()
    } else {
        v.to_string()
    }
}

/// 序列号：允许空串（=0）和纯数字，其它一律不认。
fn parse_u64(s: &str) -> Option<u64> {
    let t = s.trim();
    if t.is_empty() {
        return Some(0);
    }
    t.parse::<u64>().ok()
}

/// 时区偏移：分钟，允许 ±14 小时，超范围不认。
fn parse_tz(s: &str) -> Option<i16> {
    let v = s.trim().parse::<i16>().ok()?;
    if (-840..=840).contains(&v) {
        Some(v)
    } else {
        None
    }
}

/// 上传单个文件。解析是同步阻塞的，放到 `spawn_blocking` 里，别堵住 tokio 线程。
fn scan_dir(dir: &Path) -> Vec<ImportFile> {
    let mut out = Vec::new();
    let Ok(rd) = std::fs::read_dir(dir) else {
        return out;
    };
    for e in rd.flatten() {
        let path = e.path();
        if !path.is_file() {
            continue;
        }
        let ext = path
            .extension()
            .and_then(|s| s.to_str())
            .unwrap_or("")
            .to_ascii_lowercase();
        if ext != "gpx" && ext != "fit" {
            continue;
        }
        out.push(file_entry(&path));
    }
    out.sort_by(|a, b| b.modified.cmp(&a.modified));
    out
}

/// 把一个路径变成列表项（名字 / 大小 / 修改时间）。
///
/// 扫描目录、拖放、文件选择框三处都要这一段：大小和时间读不出来（文件刚好被删、
/// 权限不够）不该让整个列表失败，缺的字段留空就是了。
fn file_entry(path: &Path) -> ImportFile {
    let meta = std::fs::metadata(path).ok();
    ImportFile {
        name: path
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or("?")
            .to_string(),
        size: meta.as_ref().map(|m| m.len()).unwrap_or(0),
        modified: meta
            .and_then(|m| m.modified().ok())
            .map(fmt_time)
            .unwrap_or_default(),
        path: path.to_path_buf(),
    }
}

/// 扩展名是不是能导入的（`.gpx` / `.fit`，大小写不敏感）。
fn importable(path: &Path) -> bool {
    matches!(
        path.extension()
            .and_then(|e| e.to_str())
            .unwrap_or("")
            .to_ascii_lowercase()
            .as_str(),
        "gpx" | "fit"
    )
}

fn fmt_time(t: std::time::SystemTime) -> String {
    let dt: chrono::DateTime<chrono::Local> = t.into();
    dt.format("%Y-%m-%d %H:%M").to_string()
}

fn human_size(bytes: u64) -> String {
    const KB: f64 = 1024.0;
    let b = bytes as f64;
    if b < KB {
        format!("{bytes} B")
    } else if b < KB * KB {
        format!("{:.0} KB", b / KB)
    } else {
        format!("{:.1} MB", b / KB / KB)
    }
}

/// 默认导入目录：第一次打开还没记住过目录时的起点，直接用「文档」。
fn default_import_dir() -> String {
    directories::UserDirs::new()
        .and_then(|d| d.document_dir().map(|p| p.display().to_string()))
        .unwrap_or_default()
}

/// 复选方框：选中是品牌红底 + 白色对勾，未选中只有描边。
///
/// 不用 ui.checkbox：那一套是「方框 + 右侧标签」的排布，列表行要自己排版，
/// 而且整行可点比只能点小方框好按。
fn check_box(ui: &mut egui::Ui, p: &Palette, center: egui::Pos2, on: bool) {
    let r = egui::Rect::from_center_size(center, egui::vec2(16.0, 16.0));
    if on {
        ui.painter()
            .rect_filled(r, egui::CornerRadius::same(4), p.accent);
    } else {
        ui.painter().rect_stroke(
            r,
            egui::CornerRadius::same(4),
            egui::Stroke::new(1.0_f32, p.card_stroke),
            egui::epaint::StrokeKind::Inside,
        );
    }
    if on {
        // 对勾用两条线段画（字体里的 ✓ 缺字时会变成空心方框）
        let s = egui::Stroke::new(1.8_f32, p.accent_text);
        let a = egui::pos2(r.center().x - 4.0, r.center().y);
        let b = egui::pos2(r.center().x - 1.0, r.center().y + 3.0);
        let c = egui::pos2(r.center().x + 4.5, r.center().y - 3.5);
        ui.painter().line_segment([a, b], s);
        ui.painter().line_segment([b, c], s);
    }
}

/// 文件名放不下时从中间砍：保留开头的主体和末尾的扩展名。
///
/// 从尾巴一路砍会把 `.gpx` 砍掉——列表里一眼看不出这是什么文件了。
fn elide_middle(
    ui: &egui::Ui,
    name: &str,
    font: &egui::FontId,
    color: egui::Color32,
    max_w: f32,
) -> String {
    let width = |s: &str| ui.fonts(|f| f.layout_no_wrap(s.to_owned(), font.clone(), color)).size().x;
    if width(name) <= max_w {
        return name.to_string();
    }
    let (stem, ext) = match name.rsplit_once('.') {
        Some((s, e)) if s.len() > 4 => (s.to_string(), format!(".{e}")),
        _ => (name.to_string(), String::new()),
    };
    let dots_w = width("…");
    let budget = (max_w - width(&ext) - dots_w).max(10.0);
    let mut s = stem;
    while !s.is_empty() && width(&s) > budget {
        s.pop();
    }
    format!("{s}…{ext}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_gpx_and_fit_are_importable() {
        assert!(importable(Path::new("a.gpx")));
        assert!(importable(Path::new("a.FIT")));
        assert!(importable(Path::new("a.Gpx")));
        // 目录里常见的伴生文件不该混进列表
        assert!(!importable(Path::new("a.gpx.bak")));
        assert!(!importable(Path::new("a.txt")));
        assert!(!importable(Path::new("a")));
    }

    #[test]
    fn file_entry_survives_missing_file() {
        // 文件刚好被删 / 没权限：列表项还是要建出来，只是大小和时间是空的，
        // 否则拖进来的文件会静默消失
        let e = file_entry(Path::new("C:/definitely/not/here/xyz.gpx"));
        assert_eq!(e.name, "xyz.gpx");
        assert_eq!(e.size, 0);
        assert!(e.modified.is_empty());
    }

    #[test]
    fn file_entry_reads_real_file() {
        let dir = std::env::temp_dir();
        let p = dir.join("racewong_cloud_ui_test.gpx");
        std::fs::write(&p, b"<gpx/>").unwrap();
        let e = file_entry(&p);
        assert_eq!(e.name, "racewong_cloud_ui_test.gpx");
        assert_eq!(e.size, 6);
        assert!(!e.modified.is_empty());
        let _ = std::fs::remove_file(&p);
    }

    #[test]
    fn scan_dir_skips_non_tracks() {
        let dir = std::env::temp_dir().join("racewong_scan_test");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("a.gpx"), b"x").unwrap();
        std::fs::write(dir.join("b.fit"), b"x").unwrap();
        std::fs::write(dir.join("c.txt"), b"x").unwrap();
        let found: Vec<String> = scan_dir(&dir).into_iter().map(|f| f.name).collect();
        assert_eq!(found.len(), 2, "只该收 .gpx / .fit: {found:?}");
        assert!(found.contains(&"a.gpx".to_string()));
        assert!(found.contains(&"b.fit".to_string()));
        let _ = std::fs::remove_dir_all(&dir);
    }
}
