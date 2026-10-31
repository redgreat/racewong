//! 全局主题（暗色 / 亮色）与统一视觉样式。
//!
//! 所有颜色、圆角、间距都集中在这里，界面代码只通过 [`Palette`] 取色，
//! 保证两个主题下观感一致。
//!
//! 配色跟 RaceBox 官网（racebox.pro）对齐：主色是品牌红 `#EA2127`，
//! 中性色用官网的 `#2C2C2C` / `#666` / `#808186` / `#F1F1F3` / `#E4E4EC`。
//! 只有 `info` 留了一支低饱和蓝——它同时是超链接色，纯红黑灰里没有能表示
//! “可点、可看”的语义色，硬塞红色会和主按钮抢注意力。

use eframe::egui::{self, Color32, CornerRadius, Margin, Stroke};

use crate::config::Theme;
use crate::t;
use crate::worker::LogLevel;

/// 一套完整配色。
#[derive(Clone, Copy, PartialEq, Debug)]
pub struct Palette {
    pub theme: Theme,
    /// 应用背景（中央面板）。
    pub bg: Color32,
    /// 顶栏背景。
    pub header: Color32,
    /// 卡片 / 按钮背景。
    pub card: Color32,
    /// 卡片描边。
    pub card_stroke: Color32,
    /// 输入框背景。
    pub edit_bg: Color32,
    /// 进度环的底槽。
    ///
    /// 不能用 `edit_bg`：浅色下 `#F7F7F9` 压在 `#F1F1F3` 上、深色下 `#202022`
    /// 压在 `#1A1A1C` 上，几乎看不出还有一条轨道，进度弧就成了孤零零一段。
    pub track: Color32,
    /// 悬停浅底。
    pub hover: Color32,
    /// 主文字。
    pub text: Color32,
    /// 次要文字。
    pub muted: Color32,
    /// 强调色（主按钮、选中态）。
    pub accent: Color32,
    /// 强调色上的文字。
    pub accent_text: Color32,
    pub success: Color32,
    pub warn: Color32,
    pub error: Color32,
    /// 危险操作按钮底色（取消、清除内存等）。
    ///
    /// 主色本身就是红色，这里再用红就和「开始同步」分不出来了，所以取消态用
    /// 官网那种深炭色：状态一换，颜色明显变了，不用读文字也知道在跑。
    pub danger: Color32,
    pub info: Color32,
}

pub fn palette(theme: Theme) -> Palette {
    match theme {
        Theme::Dark => Palette {
            theme,
            bg: Color32::from_rgb(0x1a, 0x1a, 0x1c),
            header: Color32::from_rgb(0x23, 0x23, 0x25),
            card: Color32::from_rgb(0x2c, 0x2c, 0x2e),
            card_stroke: Color32::from_rgb(0x3b, 0x3b, 0x3f),
            edit_bg: Color32::from_rgb(0x20, 0x20, 0x22),
            track: Color32::from_rgb(0x3b, 0x3b, 0x3f),
            hover: Color32::from_rgb(0x38, 0x38, 0x3c),
            text: Color32::from_rgb(0xec, 0xec, 0xee),
            muted: Color32::from_rgb(0x9b, 0x9b, 0xa1),
            accent: Color32::from_rgb(0xea, 0x21, 0x27),
            accent_text: Color32::WHITE,
            success: Color32::from_rgb(0x3f, 0xb9, 0x50),
            warn: Color32::from_rgb(0xd2, 0x99, 0x22),
            error: Color32::from_rgb(0xf8, 0x51, 0x49),
            danger: Color32::from_rgb(0x4c, 0x4c, 0x50),
            info: Color32::from_rgb(0x8f, 0xb0, 0xd4),
        },
        Theme::Light => Palette {
            theme,
            bg: Color32::from_rgb(0xf1, 0xf1, 0xf3),
            header: Color32::WHITE,
            card: Color32::WHITE,
            card_stroke: Color32::from_rgb(0xe4, 0xe4, 0xec),
            edit_bg: Color32::from_rgb(0xf7, 0xf7, 0xf9),
            track: Color32::from_rgb(0xe4, 0xe4, 0xec),
            hover: Color32::from_rgb(0xef, 0xef, 0xf3),
            text: Color32::from_rgb(0x2c, 0x2c, 0x2c),
            muted: Color32::from_rgb(0x6e, 0x6f, 0x75),
            accent: Color32::from_rgb(0xea, 0x21, 0x27),
            accent_text: Color32::WHITE,
            success: Color32::from_rgb(0x17, 0x80, 0x3d),
            warn: Color32::from_rgb(0xb4, 0x53, 0x09),
            error: Color32::from_rgb(0xc8, 0x1e, 0x24),
            danger: Color32::from_rgb(0x2c, 0x2c, 0x2c),
            info: Color32::from_rgb(0x4f, 0x6b, 0x8a),
        },
    }
}

impl Palette {
    pub fn is_dark(self) -> bool {
        matches!(self.theme, Theme::Dark)
    }

    /// 日志级别对应的颜色，Info 用默认文字色。
    pub fn level_color(self, level: LogLevel) -> Option<Color32> {
        match level {
            LogLevel::Info => None,
            LogLevel::Success => Some(self.success),
            LogLevel::Warn => Some(self.warn),
            LogLevel::Error => Some(self.error),
        }
    }

    /// 日志级别标签。
    pub fn level_tag(level: LogLevel) -> &'static str {
        match level {
            LogLevel::Info => "INFO",
            LogLevel::Success => "OK",
            LogLevel::Warn => "WARN",
            LogLevel::Error => "ERROR",
        }
    }
}

fn widget_visuals(
    bg: Color32,
    weak_bg: Color32,
    stroke: Stroke,
    text: Color32,
    corner_radius: CornerRadius,
) -> egui::style::WidgetVisuals {
    egui::style::WidgetVisuals {
        bg_fill: bg,
        weak_bg_fill: weak_bg,
        bg_stroke: stroke,
        corner_radius,
        fg_stroke: Stroke::new(1.0_f32, text),
        expansion: 0.0,
    }
}

/// 把配色和统一的圆角 / 间距写进 egui 全局样式。
pub fn apply(ctx: &egui::Context, theme: Theme) {
    let p = palette(theme);
    let mut style: egui::Style = (*ctx.style()).clone();
    let mut visuals = if p.is_dark() {
        egui::Visuals::dark()
    } else {
        egui::Visuals::light()
    };

    visuals.panel_fill = p.bg;
    visuals.window_fill = p.card;
    visuals.extreme_bg_color = p.edit_bg;
    visuals.faint_bg_color = p.hover;
    visuals.window_corner_radius = CornerRadius::same(10);
    visuals.menu_corner_radius = CornerRadius::same(8);

    let rounding = CornerRadius::same(6);
    let stroke = Stroke::new(1.0_f32, p.card_stroke);

    visuals.widgets.noninteractive =
        widget_visuals(p.card, p.card, stroke, p.muted, rounding);
    visuals.widgets.inactive = widget_visuals(p.card, p.card, stroke, p.text, rounding);
    visuals.widgets.hovered =
        widget_visuals(p.hover, p.hover, Stroke::new(1.0_f32, p.accent), p.text, rounding);
    visuals.widgets.active =
        widget_visuals(p.accent, p.accent, Stroke::NONE, p.accent_text, rounding);

    visuals.selection.bg_fill = p.accent;
    visuals.selection.stroke = Stroke::new(1.0_f32, p.accent_text);
    visuals.hyperlink_color = p.info;

    style.visuals = visuals;
    style.spacing.item_spacing = egui::vec2(8.0, 8.0);
    style.spacing.button_padding = egui::vec2(12.0, 5.0);
    style.spacing.menu_margin = Margin::same(6);
    style.spacing.indent = 18.0;

    ctx.set_style(style);
}

/// 把卡片撑满当前可用宽度。
///
/// 不能写成 `ui.set_width(ui.available_width())`：`set_width` 走的是「下一个控件
/// 占多大地方」那套逻辑，会把 `item_spacing`（默认 8px）一起算进 `min_rect`，
/// 卡片因此比可用宽度宽 8px；更麻烦的是它还会顺手把外层 ui 的可用宽度顶宽，
/// 一页下来越靠后的卡片越往右溢出——设置页从第 5 张卡开始右边就贴到窗口边上了。
/// `expand_to_include_rect` 只改 `min_rect`，不加间距也不动 `max_rect`，没有这个副作用。
pub fn fill_card_width(ui: &mut egui::Ui) {
    let width = ui.available_width();
    if width <= 0.0 {
        return;
    }
    let min = ui.min_rect().min;
    ui.expand_to_include_rect(egui::Rect::from_min_size(
        min,
        egui::vec2(width, 0.0),
    ));
}

/// 浮动窗口（弹窗、关于）的统一外框：底色、描边、大圆角、阴影。
///
/// 默认 `Frame::window` 的圆角只有 2~6px，看着像系统对话框；这里统一放大到 14px，
/// 并和内边距一起收在一处，省得每个窗口各写一遍。
pub fn window(p: &Palette) -> egui::Frame {
    egui::Frame::window(&egui::Style::default())
        .fill(p.card)
        .stroke(Stroke::new(1.0_f32, p.card_stroke))
        .corner_radius(CornerRadius::same(14))
        .inner_margin(Margin::same(16))
}

/// 模态遮罩：盖满整个窗口，把背景压暗并吃掉落在背景上的点击和滚轮。
///
/// egui 没有内置「模态窗口」，不加这层的话弹窗开着时照样能点后面的返回、
/// 设置、输入框——看起来像弹窗，实际还在操作背后的页面。
/// 画在 `Order::Middle`，且要在弹窗之前创建：同层按创建顺序叠放，
/// 这样它在面板（Background）之上、弹窗之下。
pub fn modal_shield(ctx: &egui::Context, p: &Palette) {
    let screen = ctx.screen_rect();
    egui::Area::new(egui::Id::new("modal_shield"))
        .order(egui::Order::Middle)
        .interactable(true)
        .movable(false)
        .anchor(egui::Align2::LEFT_TOP, [0.0, 0.0])
        .show(ctx, |ui| {
            let (rect, _) = ui.allocate_exact_size(screen.size(), egui::Sense::click_and_drag());
            // 深色底下压得更狠一点才压得住，浅色稍微透一点不然太脏
            let alpha = if p.is_dark() { 150 } else { 90 };
            ui.painter()
                .rect_filled(rect, 0.0, Color32::from_rgba_unmultiplied(0, 0, 0, alpha));
        });
}

/// 卡片容器：统一底色、描边、圆角与内边距。
pub fn card(p: &Palette) -> egui::Frame {
    egui::Frame::group(&egui::Style::default())
        .fill(p.card)
        .stroke(Stroke::new(1.0_f32, p.card_stroke))
        .corner_radius(CornerRadius::same(10))
        .inner_margin(Margin::symmetric(14, 12))
}

/// 主按钮（强调色填充）。
pub fn primary_button(p: &Palette, label: &str) -> egui::Button<'static> {
    egui::Button::new(
        egui::RichText::new(label.to_owned())
            .color(p.accent_text)
            .strong(),
    )
    .fill(p.accent)
    .corner_radius(CornerRadius::same(6))
    .min_size(egui::vec2(96.0, 32.0))
}

/// 次按钮（卡片底色 + 描边，悬停有反馈）。
pub fn secondary_button(p: &Palette, label: &str) -> egui::Button<'static> {
    egui::Button::new(egui::RichText::new(label.to_owned()).color(p.text))
        .corner_radius(CornerRadius::same(6))
        .min_size(egui::vec2(96.0, 32.0))
}

/// 顶栏标签页按钮：选中为强调色胶囊，未选中透明、悬停有浅底。
/// 分段按钮：选中为强调色胶囊，未选中透明、悬停有浅底；可指定尺寸（状态栏等小空间用）。
pub fn tab_button_sized(
    ui: &mut egui::Ui,
    p: &Palette,
    selected: bool,
    label: &str,
    size: egui::Vec2,
) -> egui::Response {
    let (rect, resp) = ui.allocate_at_least(size, egui::Sense::click());
    let bg = if selected {
        p.accent
    } else if resp.hovered() {
        p.hover
    } else {
        Color32::TRANSPARENT
    };
    ui.painter()
        .rect_filled(rect, CornerRadius::same(6), bg);
    let color = if selected { p.accent_text } else { p.muted };
    let text = egui::WidgetText::from(
        egui::RichText::new(label.to_owned())
            .color(color)
            .strong(),
    );
    let galley = text.into_galley(ui, None, f32::INFINITY, egui::FontSelection::Default);
    let pos = egui::Align2::CENTER_CENTER
        .align_size_within_rect(galley.size(), rect)
        .min;
    ui.painter().galley(pos, galley, color);
    resp
}

/// 描边按钮：透明底 + 细边框，悬停有浅底；`enabled=false` 时置灰不可点。
pub fn outline_button(
    ui: &mut egui::Ui,
    p: &Palette,
    label: &str,
    text_color: Color32,
    min_size: egui::Vec2,
    enabled: bool,
) -> egui::Response {
    let (text_color, border) = if enabled {
        (text_color, p.card_stroke)
    } else {
        (p.muted, p.card_stroke)
    };
    let font = egui::FontId::proportional(14.0);
    let galley = ui.fonts(|f| f.layout_no_wrap(label.to_owned(), font.clone(), text_color));
    let size = egui::vec2(
        min_size.x.max(galley.size().x + 36.0),
        min_size.y,
    );
    let sense = if enabled {
        egui::Sense::click()
    } else {
        egui::Sense::hover()
    };
    let (rect, resp) = ui.allocate_at_least(size, sense);
    let bg = if resp.hovered() && enabled {
        p.hover
    } else {
        Color32::TRANSPARENT
    };
    ui.painter().rect_filled(rect, CornerRadius::same(6), bg);
    ui.painter().rect_stroke(
        rect,
        CornerRadius::same(6),
        Stroke::new(1.0_f32, border),
        egui::epaint::StrokeKind::Inside,
    );
    let pos = rect.center() - galley.size() / 2.0;
    ui.painter().galley(pos, galley, text_color);
    resp
}

/// 描边按钮，但多一个「已选中」状态：选中时是强调色实心胶囊。
///
/// 用来做“切换开关”类的按钮（比如主页上的「选择本地文件」）：点一下打开、
/// 再点一下关掉，光靠描边看不出来现在是开还是关，所以选中态必须有实色。
pub fn toggle_outline_button(
    ui: &mut egui::Ui,
    p: &Palette,
    label: &str,
    selected: bool,
    min_size: egui::Vec2,
    enabled: bool,
) -> egui::Response {
    let font = egui::FontId::proportional(14.0);
    let text_color = if selected {
        p.accent_text
    } else if enabled {
        p.text
    } else {
        p.muted
    };
    let galley = ui.fonts(|f| f.layout_no_wrap(label.to_owned(), font, text_color));
    let size = egui::vec2(
        min_size.x.max(galley.size().x + 36.0),
        min_size.y,
    );
    let sense = if enabled {
        egui::Sense::click()
    } else {
        egui::Sense::hover()
    };
    let (rect, resp) = ui.allocate_at_least(size, sense);
    let bg = if selected {
        p.accent
    } else if resp.hovered() && enabled {
        p.hover
    } else {
        Color32::TRANSPARENT
    };
    let border = if selected { p.accent } else { p.card_stroke };
    ui.painter().rect_filled(rect, CornerRadius::same(6), bg);
    ui.painter().rect_stroke(
        rect,
        CornerRadius::same(6),
        Stroke::new(1.0_f32, border),
        egui::epaint::StrokeKind::Inside,
    );
    let pos = rect.center() - galley.size() / 2.0;
    ui.painter().galley(pos, galley, text_color);
    resp
}

/// 弹窗底部的一对按钮：两个等宽、整体居中（不再从左边排起）。
///
/// 返回 `(确定/登录, 取消)` 两个布尔，方便调用方自己决定关窗之后要做什么。
pub fn dialog_buttons(
    ui: &mut egui::Ui,
    p: &Palette,
    ok: &str,
    cancel: &str,
    ok_enabled: bool,
) -> (bool, bool) {
    let mut ok_clicked = false;
    let mut cancel_clicked = false;
    ui.vertical_centered(|ui| {
        ui.horizontal(|ui| {
            ui.spacing_mut().item_spacing.x = 12.0;
            let w = ((ui.available_width() - 12.0) / 2.0).max(96.0);
            if ui
                .add_enabled(
                    ok_enabled,
                    primary_button(p, ok).min_size(egui::vec2(w, 32.0)),
                )
                .clicked()
            {
                ok_clicked = true;
            }
            if ui
                .add(secondary_button(p, cancel).min_size(egui::vec2(w, 32.0)))
                .clicked()
            {
                cancel_clicked = true;
            }
        });
    });
    (ok_clicked, cancel_clicked)
}

/// 弹窗里的一行字段：小号标签在上、输入框撑满宽度。
pub fn dialog_field(
    ui: &mut egui::Ui,
    p: &Palette,
    label: &str,
    value: &mut String,
    password: bool,
    revealed: bool,
) {
    ui.colored_label(p.muted, label.to_owned());
    ui.add_space(4.0);
    let mut edit = egui::TextEdit::singleline(value).desired_width(f32::INFINITY);
    if password && !revealed {
        edit = edit.password(true);
    }
    ui.add(edit);
    ui.add_space(8.0);
}

/// 小号圆角按钮（日志抽屉的收起/清空）。
pub fn small_button(p: &Palette, label: &str) -> egui::Button<'static> {
    egui::Button::new(egui::RichText::new(label.to_owned()).color(p.muted))
        .corner_radius(CornerRadius::same(6))
        .min_size(egui::vec2(56.0, 24.0))
}

/// 卡片标题：小号加粗文字。
pub fn card_title(ui: &mut egui::Ui, p: &Palette, title: &str) {
    ui.label(
        egui::RichText::new(title.to_owned())
            .strong()
            .size(15.0)
            .color(p.text),
    );
}

/// 次要说明文字。
pub fn hint(ui: &mut egui::Ui, p: &Palette, text: &str) {
    ui.label(egui::RichText::new(text.to_owned()).color(p.muted));
}

/// 卡片标题 + 右上角说明图标。
///
/// 说明文字以前是直接铺在卡片里的（见 [`hint`]），一屏要滚很久才看完内容区域；
/// 现在统一收进这个按钮：点一下才出，同一张卡的说明可以有好几条而不会互相挤。
pub fn card_title_with_help(
    ui: &mut egui::Ui,
    p: &Palette,
    title: &str,
    id_source: impl std::hash::Hash,
    help: &str,
) {
    ui.horizontal(|ui| {
        card_title(ui, p, title);
        ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
            help_icon(ui, p, id_source, help);
        });
    });
}

/// 说明图标：一个小小的「?」，点一下才把说明弹出来（点击别处收起）。
///
/// 不用 [`egui::Response::on_hover_text`]：悬停提示在正要伸手点输入框时容易自己冒出来，
/// 挡住下面的控件；点开再看的节奏更可控，同一行放好几个也不会互相干扰。
pub fn help_icon(
    ui: &mut egui::Ui,
    p: &Palette,
    id_source: impl std::hash::Hash,
    text: &str,
) -> egui::Response {
    let popup_id = ui.make_persistent_id(&id_source);
    let (rect, resp) = ui.allocate_at_least(egui::vec2(16.0, 16.0), egui::Sense::click());
    let active = resp.hovered() || ui.memory(|m| m.is_popup_open(popup_id));
    let color = if active { p.accent } else { p.muted };
    let c = rect.center();
    ui.painter()
        .circle_stroke(c, 7.0, Stroke::new(1.0_f32, color));
    let galley = ui.fonts(|f| {
        f.layout_no_wrap("?".to_owned(), egui::FontId::proportional(10.5), color)
    });
    ui.painter().galley(
        egui::pos2(c.x - galley.size().x / 2.0, c.y - galley.size().y / 2.0),
        galley,
        color,
    );

    let popup_id = ui.make_persistent_id(&id_source);
    if resp.clicked() {
        ui.memory_mut(|m| m.toggle_popup(popup_id));
    }
    egui::popup::popup_below_widget(
        ui,
        popup_id,
        &resp,
        egui::popup::PopupCloseBehavior::CloseOnClickOutside,
        |ui| {
            ui.set_max_width(300.0);
            ui.style_mut().wrap_mode = Some(egui::TextWrapMode::Wrap);
            ui.add_space(2.0);
            ui.colored_label(p.text, text.to_owned());
            ui.add_space(2.0);
        },
    );
    resp
}

/// 选项卡片：标题 + 一行说明，选中时强调色描边 + 左侧竖条。
///
/// 取代原来的圆点单选（[`radio_button`]）：两个圆点加一行字的排法在深色卡片里
/// 既不显眼也不好读，换成整块可点的卡片，选项的含义（副标题）能一起说出来。
pub fn choice_card(
    ui: &mut egui::Ui,
    p: &Palette,
    selected: bool,
    title: &str,
    subtitle: &str,
    size: egui::Vec2,
) -> egui::Response {
    let (rect, resp) = ui.allocate_at_least(size, egui::Sense::click());
    let radius = CornerRadius::same(9);
    let fill = if selected {
        tint(p.accent, if p.is_dark() { 30 } else { 20 })
    } else if resp.hovered() {
        p.hover
    } else {
        p.card
    };
    ui.painter().rect_filled(rect, radius, fill);
    ui.painter().rect_stroke(
        rect,
        radius,
        Stroke::new(
            if selected { 1.6_f32 } else { 1.0_f32 },
            if selected { p.accent } else { p.card_stroke },
        ),
        egui::epaint::StrokeKind::Inside,
    );
    // 选中标记：左侧一条强调色竖条，比圆点更容易一眼扫到
    if selected {
        ui.painter().rect_filled(
            egui::Rect::from_min_size(
                egui::pos2(rect.min.x + 8.0, rect.center().y - 9.0),
                egui::vec2(3.0, 18.0),
            ),
            CornerRadius::same(2),
            p.accent,
        );
    }
    let left = rect.min.x + 22.0;
    let max_text = (rect.width() - 34.0).max(30.0);
    let title_galley = ui.fonts(|f| {
        f.layout_no_wrap(
            title.to_owned(),
            egui::FontId::proportional(13.5),
            if selected { p.text } else { p.text },
        )
    });
    let sub_job = egui::text::LayoutJob::simple(
        subtitle.to_owned(),
        egui::FontId::proportional(11.5),
        p.muted,
        max_text,
    );
    let sub_galley = ui.fonts(|f| f.layout_job(sub_job));
    let top = rect.center().y - (title_galley.size().y + 3.0 + sub_galley.size().y) / 2.0;
    ui.painter()
        .galley(egui::pos2(left, top), title_galley.clone(), p.text);
    ui.painter().galley(
        egui::pos2(left, top + title_galley.size().y + 3.0),
        sub_galley,
        p.muted,
    );
    resp.on_hover_cursor(egui::CursorIcon::PointingHand)
}

/// 给颜色加透明度，用来做浅色底/描边。
pub fn tint(color: Color32, alpha: u8) -> Color32 {
    Color32::from_rgba_unmultiplied(color.r(), color.g(), color.b(), alpha)
}

/// 状态小图标：对勾 / 叉，直接用线段画。
///
/// 不用 "✓" "✗" 这类字符：默认字体里缺字时会被渲染成空心方框（看起来像个空复选框）。
pub fn status_icon(ui: &mut egui::Ui, color: Color32, ok: bool) {
    let (rect, _) = ui.allocate_at_least(egui::vec2(14.0, 14.0), egui::Sense::hover());
    let c = rect.center();
    let stroke = Stroke::new(1.8_f32, color);
    if ok {
        ui.painter().line_segment(
            [
                egui::pos2(c.x - 4.2, c.y + 0.4),
                egui::pos2(c.x - 1.4, c.y + 3.4),
            ],
            stroke,
        );
        ui.painter().line_segment(
            [
                egui::pos2(c.x - 1.4, c.y + 3.4),
                egui::pos2(c.x + 4.4, c.y - 3.4),
            ],
            stroke,
        );
    } else {
        ui.painter().line_segment(
            [
                egui::pos2(c.x - 3.6, c.y - 3.6),
                egui::pos2(c.x + 3.6, c.y + 3.6),
            ],
            stroke,
        );
        ui.painter().line_segment(
            [
                egui::pos2(c.x + 3.6, c.y - 3.6),
                egui::pos2(c.x - 3.6, c.y + 3.6),
            ],
            stroke,
        );
    }
}

/// 状态横幅：同色系浅底 + 描边 + 彩色加粗文字，用于测试结果等提示。
pub fn status_banner(ui: &mut egui::Ui, p: &Palette, color: Color32, text: &str) {
    let alpha = if p.is_dark() { 40 } else { 24 };
    egui::Frame::NONE
        .fill(tint(color, alpha))
        .stroke(Stroke::new(1.0_f32, tint(color, 100)))
        .corner_radius(CornerRadius::same(6))
        .inner_margin(Margin::symmetric(10, 8))
        .show(ui, |ui| {
            ui.colored_label(
                color,
                egui::RichText::new(text.to_owned()).strong(),
            );
        });
}

/// 日志级别徽标：小圆角浅底块 + 彩色文字。
pub fn level_badge(ui: &mut egui::Ui, p: &Palette, tag: &str, color: Color32) {
    let galley = ui.fonts(|f| {
        f.layout_no_wrap(
            tag.to_owned(),
            egui::FontId::proportional(10.5),
            color,
        )
    });
    let size = galley.size() + egui::vec2(10.0, 4.0);
    let (rect, _) = ui.allocate_at_least(size, egui::Sense::hover());
    let alpha = if p.is_dark() { 45 } else { 28 };
    ui.painter()
        .rect_filled(rect, CornerRadius::same(4), tint(color, alpha));
    let pos = rect.center() - galley.size() / 2.0;
    ui.painter().galley(pos, galley, color);
}

/// 齿轮图标按钮（设置入口，无文字）。
pub fn gear_button(ui: &mut egui::Ui, p: &Palette) -> egui::Response {
    let (rect, resp) = ui.allocate_at_least(egui::vec2(32.0, 32.0), egui::Sense::click());
    if resp.hovered() {
        ui.painter()
            .rect_filled(rect, CornerRadius::same(6), p.hover);
    }
    let c = rect.center();
    let color = if resp.hovered() { p.text } else { p.muted };
    // 8 个齿 + 外圈 + 中心轴
    for i in 0..8 {
        let a = i as f32 * std::f32::consts::PI / 4.0;
        let tooth = egui::pos2(c.x + a.cos() * 7.4, c.y + a.sin() * 7.4);
        ui.painter().circle_filled(tooth, 1.9, color);
    }
    ui.painter()
        .circle_stroke(c, 6.2, Stroke::new(2.2_f32, color));
    ui.painter().circle_filled(c, 2.0, color);
    resp
}

/// 云图标按钮（RaceBox 云页面入口，无文字）。
/// 信息图标按钮（关于入口，无文字）。
pub fn info_button(ui: &mut egui::Ui, p: &Palette) -> egui::Response {
    let (rect, resp) = ui.allocate_at_least(egui::vec2(32.0, 32.0), egui::Sense::click());
    if resp.hovered() {
        ui.painter()
            .rect_filled(rect, CornerRadius::same(6), p.hover);
    }
    let c = rect.center();
    let color = if resp.hovered() { p.text } else { p.muted };
    ui.painter()
        .circle_stroke(c, 9.0, Stroke::new(1.6_f32, color));
    // 上面的点
    ui.painter()
        .circle_filled(egui::pos2(c.x, c.y - 4.6), 1.3, color);
    // 下面的竖线
    ui.painter().line_segment(
        [egui::pos2(c.x, c.y - 1.6), egui::pos2(c.x, c.y + 4.6)],
        Stroke::new(1.8_f32, color),
    );
    resp
}

/// 返回按钮：左箭头 + “返回”文字（设置页左上角，替代右上角关闭）。
pub fn back_button(ui: &mut egui::Ui, p: &Palette) -> egui::Response {
    let font = egui::FontId::proportional(13.5);
    let galley = ui.fonts(|f| f.layout_no_wrap(t!("返回", "Back").to_owned(), font.clone(), p.muted));
    let (rect, resp) = ui.allocate_at_least(
        egui::vec2(galley.size().x + 44.0, 30.0),
        egui::Sense::click(),
    );
    if resp.hovered() {
        ui.painter()
            .rect_filled(rect, CornerRadius::same(6), p.hover);
    }
    let color = if resp.hovered() { p.text } else { p.muted };
    // 画左箭头
    let tip = egui::pos2(rect.min.x + 15.0, rect.center().y);
    ui.painter().line_segment(
        [
            egui::pos2(tip.x + 5.0, tip.y - 5.0),
            egui::pos2(tip.x, tip.y),
        ],
        Stroke::new(1.8_f32, color),
    );
    ui.painter().line_segment(
        [
            egui::pos2(tip.x, tip.y),
            egui::pos2(tip.x + 5.0, tip.y + 5.0),
        ],
        Stroke::new(1.8_f32, color),
    );
    let galley = ui.fonts(|f| f.layout_no_wrap(t!("返回", "Back").to_owned(), font, color));
    ui.painter().galley(
        egui::pos2(rect.min.x + 26.0, rect.center().y - galley.size().y / 2.0),
        galley,
        color,
    );
    resp
}

/// 主题预览卡：缩略窗口示意图 + 名称，选中带强调色描边。
pub fn theme_card(ui: &mut egui::Ui, p: &Palette, selected: bool, label: &str, dark: bool) -> egui::Response {
    let size = egui::vec2(132.0, 92.0);
    let (rect, resp) = ui.allocate_at_least(size, egui::Sense::click());
    let preview = egui::Rect::from_min_size(rect.min, egui::vec2(132.0, 68.0));
    let (bg, header, line, accent) = if dark {
        (
            Color32::from_rgb(0x1a, 0x1a, 0x1c),
            Color32::from_rgb(0x23, 0x23, 0x25),
            Color32::from_rgb(0x45, 0x45, 0x4a),
            p.accent,
        )
    } else {
        (
            Color32::from_rgb(0xf1, 0xf1, 0xf3),
            Color32::WHITE,
            Color32::from_rgb(0xe4, 0xe4, 0xec),
            p.accent,
        )
    };
    ui.painter()
        .rect_filled(preview, CornerRadius::same(8), bg);
    // 顶栏
    ui.painter().rect_filled(
        egui::Rect::from_min_size(preview.min, egui::vec2(132.0, 14.0)),
        CornerRadius { nw: 8, ne: 8, sw: 0, se: 0 },
        header,
    );
    // 内容块
    ui.painter().rect_filled(
        egui::Rect::from_min_size(
            egui::pos2(preview.min.x + 10.0, preview.min.y + 24.0),
            egui::vec2(48.0, 30.0),
        ),
        CornerRadius::same(4),
        accent,
    );
    for i in 0..2 {
        ui.painter().rect_filled(
            egui::Rect::from_min_size(
                egui::pos2(preview.min.x + 66.0, preview.min.y + 26.0 + i as f32 * 14.0),
                egui::vec2(56.0, 8.0),
            ),
            CornerRadius::same(4),
            line,
        );
    }
    // 选中描边
    if selected {
        ui.painter().rect_stroke(
            preview,
            CornerRadius::same(8),
            Stroke::new(2.0_f32, p.accent),
            egui::epaint::StrokeKind::Inside,
        );
    }
    // 名称
    let name_color = if selected { p.accent } else { p.muted };
    let galley = ui.fonts(|f| {
        f.layout_no_wrap(
            label.to_owned(),
            egui::FontId::proportional(13.0),
            name_color,
        )
    });
    ui.painter().galley(
        egui::pos2(
            rect.center().x - galley.size().x / 2.0,
            preview.max.y + 8.0,
        ),
        galley,
        name_color,
    );
    resp
}

/// 圆形主按钮外圈的进度环形态。
#[derive(Clone, Copy)]
pub enum RingMode {
    /// 不画任何环（未开始同步 / 已结束）。
    Hidden,
    /// 双环：内圈下载进度、外圈上传进度。
    Dual {
        download: Option<f32>,
        upload: Option<f32>,
    },
    /// 单环（只画外圈）：清理内存这类只有一个百分比的阶段。
    Single {
        progress: Option<f32>,
        color: Color32,
    },
}

/// 圆形主操作按钮：默认内圈是下载进度、外圈是上传进度，圈内是开始/取消。
/// 只有一条进度时（清理内存等）画在最贴近大圆的那一圈，不会出现“中间空一层”的观感。
/// `enabled = false` 时不可点击（清理设备内存阶段不允许中断）。`size` 是整体直径。
pub fn round_action_button(
    ui: &mut egui::Ui,
    p: &Palette,
    label: &str,
    running: bool,
    enabled: bool,
    ring: RingMode,
    size: f32,
) -> egui::Response {
    let sense = if enabled {
        egui::Sense::click()
    } else {
        egui::Sense::hover()
    };
    let (rect, resp) = ui.allocate_at_least(egui::vec2(size, size), sense);
    let c = rect.center();

    // 几何：所有环都从大圆边缘往外一层层排，环和圆之间只留一条窄缝。
    // 以前环是按整个方框的半径算的（0.455 / 0.375），单环时那道弧离圆有十几像素，
    // 看起来就像中间空了一层“占位圈”。现在单环直接贴在大圆上，双环再往外紧挨一圈。
    let ring_w = (size * 0.028).clamp(3.0, 5.0); // 环宽
    let gap = ring_w * 1.1; // 圆边到第一层环的缝
    let r_circle = size * 0.5 - ring_w * 2.0 - gap * 2.0 - 1.5; // 大圆半径
    let r_ring1 = r_circle + gap + ring_w * 0.5; // 内环：下载进度 / 单环进度
    let r_ring2 = r_ring1 + ring_w + gap * 0.85; // 外环：上传进度

    // 圆按钮本体：可取消时是危险色，清理中（不可中断）用信息色，空闲时是强调色
    let bg = if !enabled {
        p.info
    } else if running {
        p.danger
    } else {
        p.accent
    };

    // 画一段进度弧（从顶部开始，顺时针）
    let arc = |radius: f32, frac: f32, color: Color32| {
        let frac = frac.clamp(0.0, 1.0);
        if frac <= 0.0 {
            return;
        }
        // 段数跟着弧长走：短弧也够平滑，长弧不会白画一堆点
        let segments = ((frac * 96.0).ceil() as usize).max(3);
        let start = -std::f32::consts::FRAC_PI_2;
        for i in 0..segments {
            // t 直接是“圆周比例”，不用再乘一次 frac（乘两次会让弧长变成 frac²，
            // 50% 只画出 1/4 圈）
            let t0 = frac * i as f32 / segments as f32;
            let t1 = frac * (i + 1) as f32 / segments as f32;
            let a0 = start + t0 * std::f32::consts::TAU;
            let a1 = start + t1 * std::f32::consts::TAU;
            let p0 = egui::pos2(c.x + a0.cos() * radius, c.y + a0.sin() * radius);
            let p1 = egui::pos2(c.x + a1.cos() * radius, c.y + a1.sin() * radius);
            ui.painter().line_segment([p0, p1], Stroke::new(ring_w, color));
        }
    };

    // 没有百分比时画一段旋转弧，表示“正在干活”
    let spinner = |radius: f32, color: Color32| {
        let t = ui.ctx().input(|i| i.time) as f32;
        let segments = 32;
        let start = t * 3.2;
        let sweep = 1.5;
        for i in 0..segments {
            let a0 = start + (i as f32 / segments as f32) * sweep;
            let a1 = start + ((i + 1) as f32 / segments as f32) * sweep;
            let p0 = egui::pos2(c.x + a0.cos() * radius, c.y + a0.sin() * radius);
            let p1 = egui::pos2(c.x + a1.cos() * radius, c.y + a1.sin() * radius);
            ui.painter()
                .line_segment([p0, p1], Stroke::new(ring_w, color));
        }
    };

    match ring {
        RingMode::Hidden => {}
        RingMode::Dual { download, upload } => {
            // 外圈：上传进度
            ui.painter()
                .circle_stroke(c, r_ring2, Stroke::new(ring_w, p.track));
            if let Some(f) = upload {
                arc(r_ring2, f, p.accent);
            }
            // 内圈：下载进度（未知时转圈）
            ui.painter()
                .circle_stroke(c, r_ring1, Stroke::new(ring_w, p.track));
            match download {
                Some(f) => arc(r_ring1, f, p.success),
                None if running => spinner(r_ring1, p.info),
                None => {}
            }
        }
        RingMode::Single { progress, color } => {
            // 单环同样贴在大圆上，并补一条浅色轨道：
            // 只有一段弧孤零零飘着会很奇怪，有轨道才像“进度条绕了一圈”
            ui.painter()
                .circle_stroke(c, r_ring1, Stroke::new(ring_w, p.track));
            match progress {
                Some(f) => arc(r_ring1, f, color),
                None if running => spinner(r_ring1, color),
                None => {}
            }
        }
    }

    // 悬停光晕：贴在大圆边上一圈的淡色，不再占用环的位置
    if enabled && resp.hovered() {
        ui.painter().circle_filled(c, r_circle + 2.4, tint(bg, 110));
    }
    ui.painter().circle_filled(c, r_circle, bg);

    // 圈内文字：先在圆内可用宽度上按词换行，还是塞不下就逐档缩小字号。
    //
    // 只按直径定字号是不够的：中文「开始同步」4 个字，德语
    // 「Synchronisierung starten」是它的四倍宽，固定字号必然横着捅出圆外。
    //
    // 宽度按「内接矩形」算：一行文字的四个角到圆心的距离都不能超过半径，
    // 所以可用宽度 ≈ 2·sqrt(r² - (0.62·字号)²)。取 1.95 倍半径是个偏保守的
    // 近似值，既能放下「上传 1 个文件」，又不会让文字顶到圆边上。
    let max_w = (r_circle * 1.95).max(44.0);
    let max_h = (r_circle * 1.9).max(44.0);
    let make_job = |font_size: f32| {
        let mut job = egui::text::LayoutJob::simple(
            label.to_owned(),
            egui::FontId::proportional(font_size),
            p.accent_text,
            max_w,
        );
        // 只在词边界换行：德语那种超长单词被拦腰截断比缩小字号更难看
        job.wrap.break_anywhere = false;
        job.wrap.max_rows = 2;
        job
    };
    let mut font_size = (size * 0.115).max(12.0);
    let mut galley = ui.fonts(|f| f.layout_job(make_job(font_size)));
    // 先尽量压成**一行**：多行时 galley 的宽高是「最宽那一行」和「总行高」，
    // 光看这两个数会误判成放得下，于是永远停在两行（「上传 1 个 / 文件」就是这么来的）。
    while font_size > 9.0 && galley.rows.len() > 1 {
        font_size -= 0.5;
        galley = ui.fonts(|f| f.layout_job(make_job(font_size)));
    }
    // 压到一行之后（或本来就一行）再检查有没有超出圆内接矩形
    while font_size > 9.0 && (galley.size().x > max_w + 0.5 || galley.size().y > max_h) {
        font_size -= 0.5;
        galley = ui.fonts(|f| f.layout_job(make_job(font_size)));
    }
    let pos = c - galley.size() / 2.0;
    ui.painter().galley(pos, galley, p.accent_text);
    resp
}

/// 小圆角徽标（序号等）。
pub fn mini_badge(ui: &mut egui::Ui, p: &Palette, text: &str) {
    let galley = ui.fonts(|f| {
        f.layout_no_wrap(
            text.to_owned(),
            egui::FontId::proportional(11.0),
            p.muted,
        )
    });
    let size = galley.size() + egui::vec2(10.0, 4.0);
    let (rect, _) = ui.allocate_at_least(size, egui::Sense::hover());
    ui.painter()
        .rect_filled(rect, CornerRadius::same(4), p.edit_bg);
    let pos = rect.center() - galley.size() / 2.0;
    ui.painter().galley(pos, galley, p.muted);
}

/// iOS 风格开关：圆角轨道 + 圆形滑块，开为强调色。
pub fn toggle_switch(ui: &mut egui::Ui, p: &Palette, on: &mut bool) -> egui::Response {
    let (rect, resp) = ui.allocate_at_least(egui::vec2(46.0, 24.0), egui::Sense::click());
    if resp.clicked() {
        *on = !*on;
    }
    let bg = if *on { p.accent } else { p.edit_bg };
    ui.painter()
        .rect_filled(rect, CornerRadius::same(12), bg);
    if !*on {
        ui.painter().rect_stroke(
            rect,
            CornerRadius::same(12),
            Stroke::new(1.0_f32, p.card_stroke),
            egui::epaint::StrokeKind::Inside,
        );
    }
    let knob_d = 18.0_f32;
    let knob_x = if *on {
        rect.max.x - knob_d - 3.0
    } else {
        rect.min.x + 3.0
    };
    let knob = egui::Rect::from_min_size(
        egui::pos2(knob_x, rect.center().y - knob_d / 2.0),
        egui::vec2(knob_d, knob_d),
    );
    ui.painter()
        .rect_filled(knob, CornerRadius::same(9), if *on { p.accent_text } else { p.muted });
    resp
}

/// 带标签的开关行：标签在左，开关靠右。
pub fn toggle_row(ui: &mut egui::Ui, p: &Palette, label: &str, on: &mut bool) -> egui::Response {
    let mut resp = None;
    ui.horizontal(|ui| {
        ui.label(egui::RichText::new(label.to_owned()).color(p.text));
        ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
            resp = Some(toggle_switch(ui, p, on));
        });
    });
    resp.unwrap()
}
