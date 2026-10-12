//! 全局主题（暗色 / 亮色）与统一视觉样式。
//!
//! 所有颜色、圆角、间距都集中在这里，界面代码只通过 [`Palette`] 取色，
//! 保证两个主题下观感一致。

use eframe::egui::{self, Color32, CornerRadius, Margin, Stroke};

use crate::config::Theme;
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
    .min_size(egui::vec2(92.0, 30.0))
}

/// 次按钮（卡片底色 + 描边，悬停有反馈）。
pub fn secondary_button(p: &Palette, label: &str) -> egui::Button<'static> {
    egui::Button::new(egui::RichText::new(label.to_owned()).color(p.text))
        .corner_radius(CornerRadius::same(6))
        .min_size(egui::vec2(92.0, 30.0))
}

/// 顶栏标签页按钮：选中为强调色胶囊，未选中透明、悬停有浅底。
pub fn tab_button(
    ui: &mut egui::Ui,
    p: &Palette,
    selected: bool,
    label: &str,
) -> egui::Response {
    let (rect, resp) = ui.allocate_at_least(egui::vec2(64.0, 28.0), egui::Sense::click());
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

/// 小号圆角按钮（主题切换等）。
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
