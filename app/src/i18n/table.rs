//! 中文 / 英文之外的 8 种语言文案表。
//!
//! 一行一条文案：`row(中文, English, 日本語, 한국어, Português, Español, Русский, Français, Deutsch, Italiano)`。
//! 参数是**按位置**传的，顺序写死在 [`Row::other`] 的注释里，加语言时只改
//! [`super::Lang::other_index`] 和这里的 `row()` 签名，调用点一行都不用动。
//!
//! 查找 key 用 `(中文, English)` 这一对，而不是只用中文：`主题` 在 MQTT 配置里
//! 是 Topic、在外观里是 Theme，只用中文当 key 会串。

use super::Lang;
use std::collections::HashMap;
use std::sync::OnceLock;

use super::table_msg::ROWS as MSG_ROWS;
use super::table_ui::ROWS as UI_ROWS;

pub struct Row {
    pub zh: &'static str,
    pub en: &'static str,
    /// 顺序：日 韩 葡 西 俄 法 德 意（与 [`Lang::other_index`] 一一对应）
    pub other: [&'static str; 8],
}

pub const fn row(
    zh: &'static str,
    en: &'static str,
    ja: &'static str,
    ko: &'static str,
    pt: &'static str,
    es: &'static str,
    ru: &'static str,
    fr: &'static str,
    de: &'static str,
    it: &'static str,
) -> Row {
    Row {
        zh,
        en,
        other: [ja, ko, pt, es, ru, fr, de, it],
    }
}

fn index() -> &'static HashMap<(&'static str, &'static str), &'static Row> {
    static INDEX: OnceLock<HashMap<(&'static str, &'static str), &'static Row>> = OnceLock::new();
    INDEX.get_or_init(|| {
        let mut map = HashMap::with_capacity(UI_ROWS.len() + MSG_ROWS.len());
        for r in UI_ROWS.iter().chain(MSG_ROWS.iter()) {
            map.insert((r.zh, r.en), r);
        }
        map
    })
}

/// 查一条译文；`lang` 是中文 / 英文或表里没这条时返回 `None`。
pub fn translate(lang: Lang, zh: &str, en: &str) -> Option<&'static str> {
    let idx = lang.other_index()?;
    let row = index().get(&(zh, en))?;
    let text = row.other[idx];
    if text.is_empty() {
        None
    } else {
        Some(text)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_are_unique() {
        let mut seen = std::collections::HashSet::new();
        for r in UI_ROWS.iter().chain(MSG_ROWS.iter()) {
            assert!(seen.insert((r.zh, r.en)), "文案重复: {:?} / {:?}", r.zh, r.en);
        }
    }

    #[test]
    fn every_row_is_fully_translated() {
        for r in UI_ROWS.iter().chain(MSG_ROWS.iter()) {
            for (i, text) in r.other.iter().enumerate() {
                assert!(!text.is_empty(), "{:?} 的第 {i} 种语言还没翻译", r.zh);
            }
        }
    }

    #[test]
    fn placeholder_count_matches() {
        // 译文里的 {} 个数必须和中文一致，否则运行时会回退英文（不崩，但白翻了）
        for r in MSG_ROWS.iter() {
            let want = r.zh.matches("{}").count();
            for (i, text) in r.other.iter().enumerate() {
                assert_eq!(
                    text.matches("{}").count(),
                    want,
                    "{:?} 的第 {i} 种语言占位符个数不对",
                    r.zh
                );
            }
        }
    }
}
