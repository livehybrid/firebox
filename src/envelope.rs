//! The generated event and its wire encodings.
//!
//! The Stoker socket protocol is one NDJSON envelope per event:
//! `{"time","host","source","sourcetype","index","event"}` with `null` for
//! unset metadata. The HEC body is the same object with null keys omitted.

use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum TimeVal {
    /// `int(time.mktime(...))`: the default and perdayvolume generators.
    Int(i64),
    /// Float epoch: the replay generator.
    Float(f64),
    None,
}

#[derive(Debug, Clone, Default)]
pub struct Meta {
    pub index: Option<Arc<str>>,
    pub host: Option<Arc<str>>,
    pub source: Option<Arc<str>>,
    pub sourcetype: Option<Arc<str>>,
}

#[derive(Debug, Clone)]
pub struct EventOut {
    /// The event text without its trailing newline.
    pub raw: String,
    pub time: TimeVal,
    pub meta: Arc<Meta>,
}

/// Write `s` as a JSON string literal (with quotes). Mirrors
/// `json.dumps(ensure_ascii=False)`: only `"`, `\` and control characters are
/// escaped; everything else is copied as UTF-8.
pub fn write_json_str(out: &mut Vec<u8>, s: &str) {
    out.push(b'"');
    let bytes = s.as_bytes();
    let mut start = 0;
    for (i, &b) in bytes.iter().enumerate() {
        let esc: Option<&[u8]> = match b {
            b'"' => Some(b"\\\""),
            b'\\' => Some(b"\\\\"),
            b'\n' => Some(b"\\n"),
            b'\r' => Some(b"\\r"),
            b'\t' => Some(b"\\t"),
            0x08 => Some(b"\\b"),
            0x0c => Some(b"\\f"),
            0x00..=0x1f => None,
            _ => continue,
        };
        out.extend_from_slice(&bytes[start..i]);
        match esc {
            Some(e) => out.extend_from_slice(e),
            None => {
                const HEX: &[u8; 16] = b"0123456789abcdef";
                out.extend_from_slice(b"\\u00");
                out.push(HEX[(b >> 4) as usize]);
                out.push(HEX[(b & 0xf) as usize]);
            }
        }
        start = i + 1;
    }
    out.extend_from_slice(&bytes[start..]);
    out.push(b'"');
}

fn write_opt(out: &mut Vec<u8>, v: &Option<Arc<str>>) {
    match v {
        Some(s) => write_json_str(out, s),
        None => out.extend_from_slice(b"null"),
    }
}

fn write_time(out: &mut Vec<u8>, t: TimeVal) {
    match t {
        TimeVal::Int(i) => {
            let mut buf = itoa_buf(i);
            out.append(&mut buf);
        }
        TimeVal::Float(f) => {
            if f.is_finite() {
                // shortest round-trip repr, like Python's json.dumps
                let s = format!("{}", f);
                out.extend_from_slice(s.as_bytes());
                if !s.contains('.') && !s.contains('e') {
                    out.extend_from_slice(b".0");
                }
            } else {
                out.extend_from_slice(b"null");
            }
        }
        TimeVal::None => out.extend_from_slice(b"null"),
    }
}

fn itoa_buf(v: i64) -> Vec<u8> {
    v.to_string().into_bytes()
}

/// One Stoker envelope line (with trailing newline).
pub fn write_stoker_line(out: &mut Vec<u8>, ev: &EventOut) {
    out.extend_from_slice(b"{\"time\":");
    write_time(out, ev.time);
    out.extend_from_slice(b",\"host\":");
    write_opt(out, &ev.meta.host);
    out.extend_from_slice(b",\"source\":");
    write_opt(out, &ev.meta.source);
    out.extend_from_slice(b",\"sourcetype\":");
    write_opt(out, &ev.meta.sourcetype);
    out.extend_from_slice(b",\"index\":");
    write_opt(out, &ev.meta.index);
    out.extend_from_slice(b",\"event\":");
    write_json_str(out, &ev.raw);
    out.extend_from_slice(b"}\n");
}

/// One HEC event object (no trailing newline): null metadata omitted so Splunk
/// applies the token's defaults.
pub fn write_hec_object(out: &mut Vec<u8>, ev: &EventOut) {
    out.push(b'{');
    let mut first = true;
    let mut sep = |out: &mut Vec<u8>| {
        if !first {
            out.push(b',');
        }
        first = false;
    };
    if !matches!(ev.time, TimeVal::None) {
        sep(out);
        out.extend_from_slice(b"\"time\":");
        write_time(out, ev.time);
    }
    for (key, val) in [
        ("host", &ev.meta.host),
        ("source", &ev.meta.source),
        ("sourcetype", &ev.meta.sourcetype),
        ("index", &ev.meta.index),
    ] {
        if let Some(v) = val {
            sep(out);
            out.push(b'"');
            out.extend_from_slice(key.as_bytes());
            out.extend_from_slice(b"\":");
            write_json_str(out, v);
        }
    }
    sep(out);
    out.extend_from_slice(b"\"event\":");
    write_json_str(out, &ev.raw);
    out.push(b'}');
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ev(raw: &str) -> EventOut {
        EventOut {
            raw: raw.to_string(),
            time: TimeVal::Int(1700000000),
            meta: Arc::new(Meta { index: Some("main".into()), host: None, source: Some("s".into()), sourcetype: None }),
        }
    }

    #[test]
    fn stoker_line_shape() {
        let mut out = Vec::new();
        write_stoker_line(&mut out, &ev("a \"q\" \\ \n\tz"));
        let s = String::from_utf8(out).unwrap();
        assert_eq!(
            s,
            "{\"time\":1700000000,\"host\":null,\"source\":\"s\",\"sourcetype\":null,\"index\":\"main\",\"event\":\"a \\\"q\\\" \\\\ \\n\\tz\"}\n"
        );
        let v: serde_json::Value = serde_json::from_str(s.trim()).unwrap();
        assert_eq!(v["event"], "a \"q\" \\ \n\tz");
    }

    #[test]
    fn hec_object_omits_nulls_and_keeps_unicode() {
        let mut out = Vec::new();
        write_hec_object(&mut out, &ev("héllo \u{1}"));
        let s = String::from_utf8(out).unwrap();
        assert_eq!(s, "{\"time\":1700000000,\"source\":\"s\",\"index\":\"main\",\"event\":\"héllo \\u0001\"}");
    }

    #[test]
    fn float_time_repr() {
        let mut e = ev("x");
        e.time = TimeVal::Float(1700000000.5);
        let mut out = Vec::new();
        write_stoker_line(&mut out, &e);
        assert!(String::from_utf8(out).unwrap().starts_with("{\"time\":1700000000.5,"));
        e.time = TimeVal::Float(1700000000.0);
        let mut out = Vec::new();
        write_stoker_line(&mut out, &e);
        assert!(String::from_utf8(out).unwrap().starts_with("{\"time\":1700000000.0,"));
    }
}
