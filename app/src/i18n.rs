//! 界面文案的多语言支持。
//!
//! 设计取舍：不引入第三方 i18n crate，也不用 YAML/JSON 语言文件。
//!
//! * 中文 / English 两版**就地**写在调用点，用 [`t!`] / [`tr!`] 取用：
//!
//!   ```ignore
//!   ui.colored_label(p.muted, t!("主机", "Host"));          // 静态文案
//!   let s = tr!("已上传 {} 条", "{} records uploaded", n);  // 带占位符
//!   ```
//!
//!   好处是改文案时中英就在眼前，不会“改了中文忘了英文”。
//!
//! * 其余语言（日 / 韩 / 葡 / 西 / 俄 / 法 / 德 / 意）放在 [`table`] 里，
//!   以 `(中文, English)` 这一对字符串作为 key —— 单用中文当 key 不够，
//!   `主题` 在 MQTT 里是 Topic、在外观里是 Theme，会撞车。
//!
//! 查表 miss 时**回退到英文**（不是中文）：非中文用户读英文比读中文有用。
//!
//! 当前语言放在一个全局原子量里：`worker` / `ble` / `sink` 这些模块在后台
//! 线程里也要产出提示文字，逐个把语言参数传下去成本太高。

use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU8, Ordering};

mod table;
mod table_msg;
mod table_ui;

/// 界面语言。默认中文（`#[serde(default)]` 也用它）。
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default, Serialize, Deserialize)]
pub enum Lang {
    #[default]
    #[serde(rename = "zh")]
    Zh,
    #[serde(rename = "en")]
    En,
    #[serde(rename = "ja")]
    Ja,
    #[serde(rename = "ko")]
    Ko,
    #[serde(rename = "pt")]
    Pt,
    #[serde(rename = "es")]
    Es,
    #[serde(rename = "ru")]
    Ru,
    #[serde(rename = "fr")]
    Fr,
    #[serde(rename = "de")]
    De,
    #[serde(rename = "it")]
    It,
}

impl Lang {
    /// 下拉列表里的顺序：中文、英文，然后按地区排。
    pub const ALL: [Lang; 10] = [
        Lang::Zh,
        Lang::En,
        Lang::Ja,
        Lang::Ko,
        Lang::Pt,
        Lang::Es,
        Lang::Ru,
        Lang::Fr,
        Lang::De,
        Lang::It,
    ];

    /// 语言名一律用母语写：设置页里不该出现用户看不懂的选项。
    pub fn native(self) -> &'static str {
        match self {
            Lang::Zh => "中文",
            Lang::En => "English",
            Lang::Ja => "日本語",
            Lang::Ko => "한국어",
            Lang::Pt => "Português",
            Lang::Es => "Español",
            Lang::Ru => "Русский",
            Lang::Fr => "Français",
            Lang::De => "Deutsch",
            Lang::It => "Italiano",
        }
    }

    const fn code(self) -> u8 {
        match self {
            Lang::Zh => 0,
            Lang::En => 1,
            Lang::Ja => 2,
            Lang::Ko => 3,
            Lang::Pt => 4,
            Lang::Es => 5,
            Lang::Ru => 6,
            Lang::Fr => 7,
            Lang::De => 8,
            Lang::It => 9,
        }
    }

    fn from_code(c: u8) -> Lang {
        match c {
            1 => Lang::En,
            2 => Lang::Ja,
            3 => Lang::Ko,
            4 => Lang::Pt,
            5 => Lang::Es,
            6 => Lang::Ru,
            7 => Lang::Fr,
            8 => Lang::De,
            9 => Lang::It,
            _ => Lang::Zh,
        }
    }

    /// 在 [`table::Row::other`] 里的下标；中文和英文不查表，返回 `None`。
    const fn other_index(self) -> Option<usize> {
        match self {
            Lang::Zh | Lang::En => None,
            Lang::Ja => Some(0),
            Lang::Ko => Some(1),
            Lang::Pt => Some(2),
            Lang::Es => Some(3),
            Lang::Ru => Some(4),
            Lang::Fr => Some(5),
            Lang::De => Some(6),
            Lang::It => Some(7),
        }
    }
}

static CURRENT: AtomicU8 = AtomicU8::new(0);

/// 设置当前语言（进程内全局生效，调用后下一帧界面就是新语言）。
pub fn set(lang: Lang) {
    CURRENT.store(lang.code(), Ordering::Relaxed);
}

/// 当前语言。
pub fn current() -> Lang {
    Lang::from_code(CURRENT.load(Ordering::Relaxed))
}

/// 当前语言是不是中文（中文是默认分支，单独问一句省掉一次 match）。
pub fn is_zh() -> bool {
    current() == Lang::Zh
}

/// 当前语言是不是英文。
pub fn is_en() -> bool {
    current() == Lang::En
}

/// 取静态文案：`t!("中文", "English")`。
///
/// 中文 / 英文直接用调用点的字面量；其余语言查表，查不到回退英文。
pub fn text(zh: &'static str, en: &'static str) -> &'static str {
    match current() {
        Lang::Zh => zh,
        Lang::En => en,
        other => table::translate(other, zh, en).unwrap_or(en),
    }
}

/// 非中英语言下渲染带占位符的文案。
///
/// `args` 是调用点显式传进来的位置参数，`en_fallback` 是调用点用 `format!`
/// 算好的英文结果。只有满足这两个条件才用表里的译文：
///
/// * 表里有这一条；
/// * 译文里 `{}` 的个数和 `args.len()` 一致。
///
/// 第二个条件是安全阀：`tr!("…: {e}", "…: {e}")` 这种用**命名捕获**的调用点
/// 不会显式传参（`args` 为空），一旦表里给它配了带占位符的译文，渲染出来就是
/// 缺值的半截句子，所以宁可回退英文。
pub fn render_other(
    zh: &'static str,
    en: &'static str,
    args: &[&dyn std::fmt::Display],
    en_fallback: &str,
) -> String {
    match table::translate(current(), zh, en) {
        Some(tpl) if tpl.matches("{}").count() == args.len() => render(tpl, args),
        _ => en_fallback.to_string(),
    }
}

/// 把模板里的 `{}` 按顺序替换成参数。
///
/// 只认裸 `{}`（不带名字、不带格式说明），因为运行时拿不到命名参数。
fn render(tpl: &str, args: &[&dyn std::fmt::Display]) -> String {
    let mut out = String::with_capacity(tpl.len() + 16);
    let mut rest = tpl;
    let mut i = 0;
    while let Some(pos) = rest.find("{}") {
        out.push_str(&rest[..pos]);
        if let Some(arg) = args.get(i) {
            out.push_str(&arg.to_string());
        }
        i += 1;
        rest = &rest[pos + 2..];
    }
    out.push_str(rest);
    out
}

/// 取静态文案（简写）。
///
/// 两个文案参数用 `tt` 而不是 `literal` 匹配：`tt` 会把字符串字面量**原样**
/// 交给下游，`literal` 片段会被包一层隐式分隔符，个别情况下 `format!` 会报
/// “format argument must be a string literal”。
#[macro_export]
macro_rules! t {
    ($zh:tt, $en:tt $(,)?) => {
        $crate::i18n::text($zh, $en)
    };
}

/// 取带占位符的文案：`tr!("已上传 {} 条", "{} records uploaded", n)`。
///
/// 中英两版仍然是就地展开成两个 `format!`（编译期字面量，命名捕获和格式说明
/// 都能用）；只有第三种语言才走查表 + 运行时替换，替换不了就用英文结果兜底。
#[macro_export]
macro_rules! tr {
    ($zh:tt, $en:tt $(, $arg:expr)* $(,)?) => {{
        if $crate::i18n::is_zh() {
            format!($zh $(, $arg)*)
        } else if $crate::i18n::is_en() {
            format!($en $(, $arg)*)
        } else {
            $crate::i18n::render_other(
                $zh,
                $en,
                &[$(&$arg as &dyn ::std::fmt::Display),*],
                &format!($en $(, $arg)*),
            )
        }
    }};
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Mutex, MutexGuard};

    /// 语言是进程级全局量，测试默认是并行跑的，改来改去会互相干扰。
    static TEST_LOCK: Mutex<()> = Mutex::new(());

    fn guard() -> MutexGuard<'static, ()> {
        // 前面某个测试 panic 把锁毒化了也继续跑，这里只是互斥，没有不变量要保护
        TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner())
    }

    #[test]
    fn default_and_switch() {
        let _g = guard();
        // 测试之间会互相影响，前后都改回默认值
        set(Lang::Zh);
        assert!(is_zh());
        set(Lang::Ja);
        assert!(!is_zh() && !is_en());
        assert_eq!(current(), Lang::Ja);
        set(Lang::Zh);
    }

    #[test]
    fn every_lang_roundtrips_through_code() {
        let _g = guard();
        for lang in Lang::ALL {
            set(lang);
            assert_eq!(current(), lang, "{lang:?} 的 code() / from_code() 不对称");
        }
        set(Lang::Zh);
    }

    #[test]
    fn serde_roundtrip() {
        for lang in Lang::ALL {
            let text = serde_json::to_string(&lang).unwrap();
            assert_eq!(serde_json::from_str::<Lang>(&text).unwrap(), lang);
        }
    }

    #[test]
    fn render_substitutes_in_order() {
        let a = 7;
        let b = "x";
        let args: [&dyn std::fmt::Display; 2] = [&a, &b];
        assert_eq!(render("{} / {}", &args), "7 / x");
        assert_eq!(render("no placeholder", &args), "no placeholder");
    }

    #[test]
    fn missing_translation_falls_back_to_english() {
        let _g = guard();
        set(Lang::Ja);
        // 表里肯定没有这句
        assert_eq!(text("不存在的文案", "not in the table"), "not in the table");
        set(Lang::Zh);
    }
}
