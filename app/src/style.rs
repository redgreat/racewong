//! 全局主题（暗色 / 亮色）与统一视觉样式。
//!
//! 所有颜色、圆角、间距都集中在这里，界面代码只通过 [`Palette`] 取色，
//! 保证两个主题下观感一致。

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
    pub danger: Color32,
    pub info: Color32,
}

pub fn palette(theme: Theme) -> Palette {
    match theme {
        Theme::Dark => Palette {
            theme,
            bg: Color32::from_rgb(0x14, 0x16, 0x1a),
            header: Color32::from_rgb(0x1b, 0x1e, 0x24),
            card: Color32::from_rgb(0x20, 0x24, 0x2b),
            card_stroke: Color32::from_rgb(0x2e, 0x34, 0x3d),
            edit_bg: Color32::from_rgb(0x16, 0x19, 0x1f),
            hover: Color32::from_rgb(0x2a, 0x30, 0x39),
            text: Color32::from_rgb(0xe8, 0xea, 0xed),
            muted: Color32::from_rgb(0x99, 0xa2, 0xad),
            accent: Color32::from_rgb(0x4c, 0x8d, 0xff),
            accent_text: Color32::WHITE,
            success: Color32::from_rgb(0x3f, 0xb9, 0x50),
            warn: Color32::from_rgb(0xd2, 0x99, 0x22),
            error: Color32::from_rgb(0xf8, 0x51, 0x49),
            danger: Color32::from_rgb(0xc4, 0x3c, 0x33),
            info: Color32::from_rgb(0x6c, 0xb6, 0xff),
        },
        Theme::Light => Palette {
            theme,
            bg: Color32::from_rgb(0xf2, 0xf4, 0xf7),
            header: Color32::WHITE,
            card: Color32::WHITE,
            card_stroke: Color32::from_rgb(0xd8, 0xdd, 0xe4),
            edit_bg: Color32::from_rgb(0xf6, 0xf8, 0xfa),
            hover: Color32::from_rgb(0xe8, 0xee, 0xf7),
            text: Color32::from_rgb(0x1f, 0x23, 0x28),
            muted: Color32::from_rgb(0x5c, 0x66, 0x70),
            accent: Color32::from_rgb(0x25, 0x63, 0xeb),
            accent_text: Color32::WHITE,
            success: Color32::from_rgb(0x1a, 0x7f, 0x37),
            warn: Color32::from_rgb(0x9a, 0x67, 0x00),
            error: Color32::from_rgb(0xcf, 0x22, 0x2e),
            danger: Color32::from_rgb(0xcf, 0x22, 0x2e),
            info: Color32::from_rgb(0x09, 0x63, 0xd8),
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

/// 给颜色加透明度，用来做浅色底/描边。
pub fn tint(color: Color32, alpha: u8) -> Color32 {
    Color32::from_rgba_unmultiplied(color.r(), color.g(), color.b(), alpha)
}

/// 单选框：左侧圆点 + 文字，整行可点。
///
/// 同一组里永远只有一个被选中——分段按钮那种“看起来像按钮”的样式容易被误读成
/// 多选/多开，单选语义用单选控件表达最直观。
pub fn radio_button(ui: &mut egui::Ui, p: &Palette, selected: bool, label: &str) -> egui::Response {
    let text_color = if selected { p.text } else { p.muted };
    let galley = ui.fonts(|f| {
        f.layout_no_wrap(
            label.to_owned(),
            egui::FontId::proportional(13.5),
            text_color,
        )
    });
    let size = egui::vec2(galley.size().x + 34.0, 28.0);
    let (rect, resp) = ui.allocate_at_least(size, egui::Sense::click());
    if resp.hovered() {
        ui.painter()
            .rect_filled(rect, CornerRadius::same(6), p.hover);
    }
    let c = egui::pos2(rect.min.x + 13.0, rect.center().y);
    let ring = if selected { p.accent } else { p.muted };
    ui.painter()
        .circle_stroke(c, 6.0, Stroke::new(1.6_f32, ring));
    if selected {
        ui.painter().circle_filled(c, 3.2, p.accent);
    }
    let pos = egui::pos2(c.x + 13.0, rect.center().y - galley.size().y / 2.0);
    ui.painter().galley(pos, galley, text_color);
    resp
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
            Color32::from_rgb(0x14, 0x16, 0x1a),
            Color32::from_rgb(0x1b, 0x1e, 0x24),
            Color32::from_rgb(0x3a, 0x40, 0x4a),
            p.accent,
        )
    } else {
        (
            Color32::from_rgb(0xf2, 0xf4, 0xf7),
            Color32::WHITE,
            Color32::from_rgb(0xd8, 0xdd, 0xe4),
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
                .circle_stroke(c, r_ring2, Stroke::new(ring_w, p.edit_bg));
            if let Some(f) = upload {
                arc(r_ring2, f, p.accent);
            }
            // 内圈：下载进度（未知时转圈）
            ui.painter()
                .circle_stroke(c, r_ring1, Stroke::new(ring_w, p.edit_bg));
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
                .circle_stroke(c, r_ring1, Stroke::new(ring_w, p.edit_bg));
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
    let max_w = (r_circle * 1.62).max(40.0);
    let max_h = (r_circle * 1.5).max(40.0);
    let make_job = |font_size: f32| {
        let mut job = egui::text::LayoutJob::simple(
            label.to_owned(),
            egui::FontId::proportional(font_size),
            p.accent_text,
            max_w,
        );
        // 只在词边界换行：德语那种超长单词被拦腰截断比缩小字号更难看
        job.wrap.break_anywhere = false;
        job.wrap.max_rows = 3;
        job
    };
    let mut font_size = (size * 0.115).max(12.0);
    let mut galley = ui.fonts(|f| f.layout_job(make_job(font_size)));
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
