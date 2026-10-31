//! 本地轨迹文件导入：GPX 与 Garmin FIT。
//!
//! 为什么不引第三方解析库：GPX 只是「一堆带属性的 trkpt」，FIT 是自描述的
//! 二进制格式，两者手写都比引依赖更好控制——尤其是 FIT，各库的 API 版本差异大，
//! 而我们只需要 record 消息里的几个字段。
//!
//! 解析结果统一成 [`TrackPoint`]，之后交给 [`crate::racebox::session`] 编码。

use crate::racebox::session::{itow_from_unix, TrackPoint};
use std::path::Path;

/// 导入结果。
pub struct ImportedTrack {
    pub points: Vec<TrackPoint>,
    /// 首个采样点的 UTC 时间 (年, 月, 日, 时, 分, 秒)。
    pub start_utc: (i32, u32, u32, u32, u32, u32),
    /// 时长，秒（按首尾时间戳算）。
    pub duration_secs: f64,
}

/// 解析过程中的中间形态：只带物理量，iTOW 与 G 力在收尾时统一算。
struct RawPoint {
    unix_ms: i64,
    lat: f64,
    lng: f64,
    alt: f64,
    /// 文件里自带的速度（m/s）；没有就是 None，后面用位移推算。
    speed_mps: Option<f64>,
}

/// 按扩展名分派。返回 Err 时 message 直接给界面显示。
pub fn load(path: &Path) -> Result<ImportedTrack, String> {
    let ext = path
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("")
        .to_ascii_lowercase();
    let raw = match ext.as_str() {
        "gpx" => {
            let text = std::fs::read_to_string(path).map_err(|e| format!("读取失败: {e}"))?;
            parse_gpx(&text)?
        }
        "fit" => {
            let bytes = std::fs::read(path).map_err(|e| format!("读取失败: {e}"))?;
            parse_fit(&bytes)?
        }
        other => return Err(format!("不支持的格式 .{other}（只认 .gpx / .fit）")),
    };
    finalize(raw)
}

/// 把原始点补全成 [`TrackPoint`]：算 iTOW、速度、航向、纵向 G。
fn finalize(raw: Vec<RawPoint>) -> Result<ImportedTrack, String> {
    // 时间戳非递增、坐标为空（0,0）的点会让时间轴和服务端的统计错乱，先剔掉。
    let mut clean: Vec<RawPoint> = Vec::with_capacity(raw.len());
    for p in raw {
        if !p.lat.is_finite() || !p.lng.is_finite() {
            continue;
        }
        if p.lat == 0.0 && p.lng == 0.0 {
            continue;
        }
        if let Some(last) = clean.last() {
            // 同一毫秒的重复点（多设备合并、轨迹抽稀失败）直接丢，否则速度为 0
            if p.unix_ms <= last.unix_ms {
                continue;
            }
        }
        clean.push(p);
    }
    if clean.len() < 2 {
        return Err("文件里有效轨迹点不足 2 个".to_string());
    }

    let first_ms = clean[0].unix_ms;
    let mut points = Vec::with_capacity(clean.len());
    // (上一个点, 它的速度) —— 速度和 G 力都是差分算出来的，得带着走
    let mut prev: Option<(&RawPoint, f64)> = None;
    for p in &clean {
        let secs = match prev {
            Some((q, _)) => (p.unix_ms - q.unix_ms) as f64 / 1000.0,
            None => 0.0,
        };
        let speed_mps = match prev {
            None => p.speed_mps.unwrap_or(0.0),
            Some((q, _)) => {
                let d = haversine(q.lat, q.lng, p.lat, p.lng);
                let derived = if secs > 0.0 { d / secs } else { 0.0 };
                // 文件自带速度优先：有些设备会做平滑，比逐点差分稳
                p.speed_mps.unwrap_or(derived)
            }
        };
        let heading = match prev {
            None => 0.0,
            Some((q, _)) => bearing(q.lat, q.lng, p.lat, p.lng),
        };
        // 纵向加速度 = 速度变化 / 时间。没有加速度计，这是能给出的最诚实的 G 值。
        let gx = match prev {
            None => 0.0,
            Some((_, pspeed)) => {
                if secs > 0.0 {
                    ((speed_mps - pspeed) / secs / 9.806_65).clamp(-2.0, 2.0)
                } else {
                    0.0
                }
            }
        };
        points.push(TrackPoint {
            itow: itow_from_unix(p.unix_ms),
            lat: p.lat,
            lng: p.lng,
            alt: p.alt,
            speed_mmps: (speed_mps.max(0.0) * 1000.0) as u32,
            heading,
            gx,
            gy: 0.0,
            gz: 1.0,
            rx: 0.0,
            ry: 0.0,
            rz: 0.0,
        });
        prev = Some((p, speed_mps));
    }

    let start = chrono::DateTime::from_timestamp_millis(first_ms)
        .ok_or_else(|| "起始时间超出可表示范围".to_string())?;
    use chrono::{Datelike, Timelike};
    let start_utc = (
        start.year(),
        start.month(),
        start.day(),
        start.hour(),
        start.minute(),
        start.second(),
    );
    let duration_secs = (clean[clean.len() - 1].unix_ms - first_ms) as f64 / 1000.0;
    Ok(ImportedTrack {
        points,
        start_utc,
        duration_secs,
    })
}

fn haversine(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    const R: f64 = 6_371_000.0;
    let p1 = lat1.to_radians();
    let p2 = lat2.to_radians();
    let dp = p2 - p1;
    let dl = (lon2 - lon1).to_radians();
    let a = (dp / 2.0).sin().powi(2) + p1.cos() * p2.cos() * (dl / 2.0).sin().powi(2);
    2.0 * R * a.sqrt().asin()
}

fn bearing(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    let p1 = lat1.to_radians();
    let p2 = lat2.to_radians();
    let dl = (lon2 - lon1).to_radians();
    let y = dl.sin() * p2.cos();
    let x = p1.cos() * p2.sin() - p1.sin() * p2.cos() * dl.cos();
    let deg = y.atan2(x).to_degrees();
    (deg + 360.0) % 360.0
}

// ---------------------------------------------------------------- GPX

/// 取某个标签里第一个子元素的文本，例如 `<time>...</time>`。
fn inner_text<'a>(body: &'a str, tag: &str) -> Option<&'a str> {
    let open = format!("<{tag}");
    let start = body.find(&open)?;
    let after = &body[start..];
    let gt = after.find('>')?;
    let rest = &after[gt + 1..];
    let close = format!("</{tag}>");
    let end = rest.find(&close)?;
    Some(rest[..end].trim())
}

/// 取属性值：`lat="36.4"` → `36.4`。
fn attr<'a>(tag_text: &'a str, name: &str) -> Option<&'a str> {
    let pat = format!("{name}=\"");
    let start = tag_text.find(&pat)? + pat.len();
    let rest = &tag_text[start..];
    let end = rest.find('"')?;
    Some(&rest[..end])
}

/// 扫描所有指定标签的轨迹点。
fn scan_points(text: &str, tag: &str) -> Vec<RawPoint> {
    let mut out = Vec::new();
    let open = format!("<{tag}");
    let close = format!("</{tag}>");
    let mut pos = 0usize;
    while let Some(i) = text[pos..].find(&open) {
        let abs = pos + i;
        let after = &text[abs..];
        let Some(gt) = after.find('>') else { break };
        let head = &after[..gt];
        // 自闭合的 <trkpt ... /> 没有子元素，海拔和时间都拿不到，跳过。
        let self_closing = head.trim_end().ends_with('/');
        let body_start = abs + gt + 1;
        let (body, next) = if self_closing {
            ("", body_start)
        } else {
            match text[body_start..].find(&close) {
                Some(rel) => (
                    &text[body_start..body_start + rel],
                    body_start + rel + close.len(),
                ),
                None => ("", body_start),
            }
        };
        pos = next;

        let (Some(lat), Some(lon)) = (attr(head, "lat"), attr(head, "lon")) else {
            continue;
        };
        let (Ok(lat), Ok(lon)) = (lat.trim().parse::<f64>(), lon.trim().parse::<f64>()) else {
            continue;
        };
        let Some(time_text) = inner_text(body, "time") else {
            continue;
        };
        let Some(unix_ms) = parse_rfc3339_ms(time_text) else {
            continue;
        };
        let alt = inner_text(body, "ele")
            .and_then(|v| v.parse::<f64>().ok())
            .unwrap_or(0.0);
        out.push(RawPoint {
            unix_ms,
            lat,
            lng: lon,
            alt,
            speed_mps: None,
        });
    }
    out
}

/// 解析 ISO8601 / RFC3339 时间到 Unix 毫秒。
fn parse_rfc3339_ms(s: &str) -> Option<i64> {
    chrono::DateTime::parse_from_rfc3339(s)
        .ok()
        .map(|dt| dt.timestamp_millis())
}

fn parse_gpx(text: &str) -> Result<Vec<RawPoint>, String> {
    // 轨迹点优先；没有 trk 再退到路线点、航点（有些设备只写 wpt）。
    for tag in ["trkpt", "rtept", "wpt"] {
        let pts = scan_points(text, tag);
        if !pts.is_empty() {
            return Ok(pts);
        }
    }
    Err("GPX 里没有找到带时间的轨迹点".to_string())
}

// ---------------------------------------------------------------- FIT

const FIT_EPOCH_UNIX: i64 = 631_065_600; // 1989-12-31T00:00:00Z
const SEMICIRCLE: f64 = 180.0 / 2_147_483_648.0;

/// 一条 definition 消息描述的数据布局。
struct LocalDef {
    global_num: u16,
    big_endian: bool,
    /// (字段号, 字节数)
    fields: Vec<(u8, u8)>,
}

fn read_u16(b: &[u8], at: usize, be: bool) -> u16 {
    let s = [b[at], b[at + 1]];
    if be {
        u16::from_be_bytes(s)
    } else {
        u16::from_le_bytes(s)
    }
}

fn read_u32(b: &[u8], at: usize, be: bool) -> u32 {
    let s = [b[at], b[at + 1], b[at + 2], b[at + 3]];
    if be {
        u32::from_be_bytes(s)
    } else {
        u32::from_le_bytes(s)
    }
}

fn parse_fit(bytes: &[u8]) -> Result<Vec<RawPoint>, String> {
    if bytes.len() < 12 {
        return Err("FIT 文件太短".to_string());
    }
    if &bytes[8..12] != b".FIT" {
        return Err("不是 FIT 文件（缺少 .FIT 签名）".to_string());
    }
    let header_size = bytes[0] as usize;
    if !(12..=14).contains(&header_size) || bytes.len() < header_size {
        return Err("FIT 头部长度异常".to_string());
    }
    let data_size = read_u32(bytes, 4, false) as usize;
    let end = (header_size + data_size).min(bytes.len());
    let data = &bytes[header_size..end];

    let mut defs: std::collections::HashMap<u8, LocalDef> = std::collections::HashMap::new();
    let mut out = Vec::new();
    let mut p = 0usize;

    while p < data.len() {
        let header = data[p];
        p += 1;

        // 压缩时间戳头（bit7 置位）只省掉时间字段，record 里本来就有完整时间，
        // 所以按普通数据消息处理即可。
        let local_type = if header & 0x80 != 0 {
            (header >> 5) & 0x03
        } else {
            header & 0x0F
        };
        let is_def = header & 0x80 == 0 && header & 0x40 != 0;
        let has_dev = header & 0x80 == 0 && header & 0x20 != 0;

        if is_def {
            if p + 5 > data.len() {
                break;
            }
            let be = data[p + 1] == 1;
            let global_num = read_u16(data, p + 2, be);
            let n = data[p + 4] as usize;
            p += 5;
            let mut fields = Vec::with_capacity(n);
            for _ in 0..n {
                if p + 3 > data.len() {
                    return Err("FIT 定义消息被截断".to_string());
                }
                fields.push((data[p], data[p + 1]));
                p += 3;
            }
            if has_dev {
                if p >= data.len() {
                    break;
                }
                let dn = data[p] as usize;
                p += 1 + dn * 3;
            }
            defs.insert(
                local_type,
                LocalDef {
                    global_num,
                    big_endian: be,
                    fields,
                },
            );
            continue;
        }

        let Some(def) = defs.get(&local_type) else {
            return Err("FIT 数据消息出现在定义之前".to_string());
        };

        let be = def.big_endian;
        let mut cursor = p;
        let mut ts: Option<u32> = None;
        let mut lat: Option<i32> = None;
        let mut lon: Option<i32> = None;
        let mut alt: Option<f64> = None;
        let mut speed: Option<f64> = None;

        for &(num, size) in &def.fields {
            let size = size as usize;
            if cursor + size > data.len() {
                return Err("FIT 数据消息被截断".to_string());
            }
            if def.global_num == 20 {
                match num {
                    253 if size >= 4 => ts = Some(read_u32(data, cursor, be)),
                    0 if size >= 4 => lat = Some(read_u32(data, cursor, be) as i32),
                    1 if size >= 4 => lon = Some(read_u32(data, cursor, be) as i32),
                    // 原始海拔：uint16，单位 5 米，偏移 -500
                    2 if size >= 2 => alt = Some(read_u16(data, cursor, be) as f64 / 5.0 - 500.0),
                    // 增强海拔：uint32，同样的换算
                    73 if size >= 4 => alt = Some(read_u32(data, cursor, be) as f64 / 5.0 - 500.0),
                    // 速度：uint16，单位 0.001 m/s
                    6 if size >= 2 => speed = Some(read_u16(data, cursor, be) as f64 / 1000.0),
                    78 if size >= 4 => speed = Some(read_u32(data, cursor, be) as f64 / 1000.0),
                    _ => {}
                }
            }
            cursor += size;
        }
        p = cursor;

        if def.global_num != 20 {
            continue;
        }
        let (Some(ts), Some(lat), Some(lon)) = (ts, lat, lon) else {
            continue;
        };
        out.push(RawPoint {
            unix_ms: (FIT_EPOCH_UNIX + ts as i64) * 1000,
            lat: lat as f64 * SEMICIRCLE,
            lng: lon as f64 * SEMICIRCLE,
            alt: alt.unwrap_or(0.0),
            speed_mps: speed,
        });
    }

    if out.is_empty() {
        return Err("FIT 里没有找到轨迹记录".to_string());
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gpx_reads_points() {
        let text = r#"<?xml version="1.0"?>
<gpx version="1.1"><trk><trkseg>
<trkpt lat="36.4112858" lon="120.5058180"><ele>70.3</ele><time>2026-08-18T00:12:50.000Z</time></trkpt>
<trkpt lat="36.4113000" lon="120.5058300"><ele>70.5</ele><time>2026-08-18T00:12:51.000Z</time></trkpt>
</trkseg></trk></gpx>"#;
        let pts = parse_gpx(text).expect("应能解析");
        assert_eq!(pts.len(), 2);
        assert!((pts[0].lat - 36.4112858).abs() < 1e-9);
        assert!((pts[0].alt - 70.3).abs() < 1e-6);
        let track = finalize(pts).expect("应能收尾");
        assert_eq!(track.points.len(), 2);
        // 1 秒走了约 1.7 米
        assert!(track.points[1].speed_mmps > 1000);
        assert!(track.points[1].speed_mmps < 3000);
        assert!((track.duration_secs - 1.0).abs() < 1e-6);
    }

    #[test]
    fn gpx_without_time_is_rejected() {
        let text = r#"<gpx><trk><trkseg>
<trkpt lat="1.0" lon="2.0"><ele>1</ele></trkpt></trkseg></trk></gpx>"#;
        assert!(parse_gpx(text).is_err());
    }

    #[test]
    fn rfc3339_parses() {
        assert_eq!(
            parse_rfc3339_ms("2026-08-18T00:12:50.000Z"),
            Some(1_787_011_970_000)
        );
    }

    /// 手工拼一个最小 FIT：header + definition(record) + 两条 record。
    #[test]
    fn fit_minimal_roundtrip() {
        let mut f: Vec<u8> = Vec::new();
        let mut body: Vec<u8> = Vec::new();
        // definition: local 0, 小端, global 20, 3 个字段
        body.extend_from_slice(&[0x40, 0x00, 0x00, 0x14, 0x00, 0x03]);
        body.extend_from_slice(&[253, 4, 0x86]); // timestamp uint32
        body.extend_from_slice(&[0, 4, 0x85]); // position_lat sint32
        body.extend_from_slice(&[1, 4, 0x85]); // position_long sint32
        // 两条 record
        for (ts, lat, lon) in [(1_000_000u32, 400_000_000i32, 1_300_000_000i32),
                               (1_000_001u32, 400_000_100i32, 1_300_000_100i32)] {
            body.push(0x00);
            body.extend_from_slice(&ts.to_le_bytes());
            body.extend_from_slice(&(lat as u32).to_le_bytes());
            body.extend_from_slice(&(lon as u32).to_le_bytes());
        }
        // header: 12 字节，data_size 在 4..8，签名在 8..12
        f.push(12);
        f.push(0x10);
        f.extend_from_slice(&0x100u16.to_le_bytes());
        f.extend_from_slice(&(body.len() as u32).to_le_bytes());
        f.extend_from_slice(b".FIT");
        f.extend_from_slice(&body);

        let pts = parse_fit(&f).expect("应能解析");
        assert_eq!(pts.len(), 2);
        assert!((pts[0].lat - 400_000_000.0 * SEMICIRCLE).abs() < 1e-9);
        let track = finalize(pts).expect("应能收尾");
        assert_eq!(track.points.len(), 2);
    }
}
