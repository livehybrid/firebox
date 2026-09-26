//! Splunk-style relative time modifiers (`-60s`, `now`, `-1d@d+3h`) and a
//! liberal absolute-time parser, mirroring eventgen's `lib/timeparser.py`.
//!
//! Differences from upstream are deliberate fixes: `@q` snaps to the real
//! quarter start (upstream's `floor(month/3.3+1)*3` is wrong for every month),
//! and relative expressions are evaluated against the *current* `now` on every
//! call instead of being frozen as an offset at first use.

use chrono::{Datelike, Duration, Months, NaiveDate, NaiveDateTime, NaiveTime, Timelike};

/// Parse `spec` relative to `now` (a naive local datetime).
pub fn parse_time(spec: &str, now: NaiveDateTime) -> Result<NaiveDateTime, String> {
    let spec = spec.trim();
    if spec.is_empty() {
        return Err("empty time specifier".into());
    }
    if spec == "now" {
        return Ok(now);
    }
    if spec.starts_with('+') || spec.starts_with('-') {
        return parse_relative(spec, now);
    }
    parse_absolute(spec, now)
}

fn parse_relative(spec: &str, now: NaiveDateTime) -> Result<NaiveDateTime, String> {
    let bytes = spec.as_bytes();
    let mut i = 0;
    // upstream allows a run of sign characters; the first decides
    let negative = bytes[0] == b'-';
    while i < bytes.len() && (bytes[i] == b'+' || bytes[i] == b'-') {
        i += 1;
    }
    let (num, unit, rest) =
        take_num_unit(&spec[i..]).ok_or_else(|| format!("cannot parse relative time string for {}", spec))?;
    let mut ret = time_math(negative, num, &unit, now)?;
    let mut rest = rest;
    if let Some(after_at) = rest.strip_prefix('@') {
        let (snap_unit, more) = take_unit(after_at).ok_or_else(|| format!("cannot parse snap unit in {}", spec))?;
        ret = snap(&snap_unit, ret)?;
        rest = more;
        if let Some(first) = rest.chars().next() {
            if first == '+' || first == '-' {
                let neg2 = first == '-';
                let (n2, u2, more2) =
                    take_num_unit(&rest[1..]).ok_or_else(|| format!("cannot parse snap offset in {}", spec))?;
                ret = time_math(neg2, n2, &u2, ret)?;
                rest = more2;
            }
        }
    }
    if !rest.trim().is_empty() {
        // upstream's regex silently ignores trailing garbage; be lenient too
        log::debug!("ignoring trailing text {:?} in time specifier {:?}", rest, spec);
    }
    Ok(ret)
}

fn take_num_unit(s: &str) -> Option<(i64, String, &str)> {
    let digits_end = s.bytes().take_while(|b| b.is_ascii_digit()).count();
    if digits_end == 0 {
        return None;
    }
    let num: i64 = s[..digits_end].parse().ok()?;
    let (unit, rest) = take_unit(&s[digits_end..])?;
    Some((num, unit, rest))
}

/// Longest-first unit match, the same alternation upstream uses.
fn take_unit(s: &str) -> Option<(String, &str)> {
    const UNITS: &[&str] = &[
        "seconds", "second", "secs", "sec", "minutes", "minute", "min", "hours", "hour", "hrs", "hr", "days", "day",
        "weeks", "week", "months", "month", "mon", "quarters", "quarter", "qtrs", "qtr", "years", "year", "yrs", "yr",
    ];
    for u in UNITS {
        if let Some(rest) = s.strip_prefix(u) {
            return Some((u.to_string(), rest));
        }
    }
    // w0..w6 before the bare single-letter units
    let b = s.as_bytes();
    if b.len() >= 2 && b[0] == b'w' && (b'0'..=b'6').contains(&b[1]) {
        return Some((s[..2].to_string(), &s[2..]));
    }
    if let Some(first) = s.chars().next() {
        if "shmdwyq".contains(first) {
            return Some((first.to_string(), &s[1..]));
        }
    }
    None
}

fn unit_class(unit: &str) -> &'static str {
    match unit {
        "s" | "sec" | "secs" | "second" | "seconds" => "s",
        "m" | "min" | "minute" | "minutes" => "m",
        "h" | "hr" | "hrs" | "hour" | "hours" => "h",
        "d" | "day" | "days" => "d",
        "w" | "week" | "weeks" => "w",
        "mon" | "month" | "months" => "mon",
        "q" | "qtr" | "qtrs" | "quarter" | "quarters" => "q",
        "y" | "yr" | "yrs" | "year" | "years" => "y",
        u if u.len() == 2 && u.starts_with('w') => "wN",
        _ => "?",
    }
}

fn time_math(negative: bool, num: i64, unit: &str, base: NaiveDateTime) -> Result<NaiveDateTime, String> {
    let signed = if negative { -num } else { num };
    let ret = match unit_class(unit) {
        "s" => base + Duration::seconds(signed),
        "m" => base + Duration::minutes(signed),
        "h" => base + Duration::hours(signed),
        "d" => base + Duration::days(signed),
        "w" => base + Duration::days(signed * 7),
        "mon" | "q" | "y" => {
            let months = match unit_class(unit) {
                "q" => signed * 3,
                "y" => signed * 12,
                _ => signed,
            };
            add_months(base, months)?
        }
        "wN" => return Err("day of week is only available in snap-to".into()),
        _ => return Err(format!("unknown time unit {}", unit)),
    };
    // upstream always chops microseconds after time math
    Ok(ret.with_nanosecond(0).unwrap_or(ret))
}

fn add_months(base: NaiveDateTime, months: i64) -> Result<NaiveDateTime, String> {
    let m = months.unsigned_abs() as u32;
    let r = if months >= 0 { base.checked_add_months(Months::new(m)) } else { base.checked_sub_months(Months::new(m)) };
    r.ok_or_else(|| "month arithmetic out of range".to_string())
}

fn snap(unit: &str, t: NaiveDateTime) -> Result<NaiveDateTime, String> {
    let midnight = |d: NaiveDate| d.and_time(NaiveTime::from_hms_opt(0, 0, 0).unwrap());
    Ok(match unit_class(unit) {
        "s" => t.with_nanosecond(0).unwrap(),
        "m" => t.with_second(0).unwrap().with_nanosecond(0).unwrap(),
        "h" => t.with_minute(0).unwrap().with_second(0).unwrap().with_nanosecond(0).unwrap(),
        "d" => midnight(t.date()),
        "w" | "wN" => {
            // Splunk weekdays: Sunday = 0 .. Saturday = 6; `@w` means `@w0`.
            let want: i64 = if unit_class(unit) == "w" { 0 } else { unit[1..].parse().unwrap_or(0) };
            let cur = t.weekday().num_days_from_sunday() as i64;
            let back = if want <= cur { cur - want } else { 7 - (want - cur) };
            midnight(t.date() - Duration::days(back))
        }
        "mon" => midnight(NaiveDate::from_ymd_opt(t.year(), t.month(), 1).unwrap()),
        "q" => {
            let qm = ((t.month() - 1) / 3) * 3 + 1;
            midnight(NaiveDate::from_ymd_opt(t.year(), qm, 1).unwrap())
        }
        "y" => midnight(NaiveDate::from_ymd_opt(t.year(), 1, 1).unwrap()),
        _ => return Err(format!("unknown snap unit {}", unit)),
    })
}

/// A liberal subset of `dateutil.parser.parse`: ISO 8601 date/time (with an
/// optional fractional second and a `Z`/offset that is dropped, since eventgen
/// works in naive local time), `MM/DD/YYYY [HH:MM[:SS]]`, and 10- or 13-digit
/// epochs.
fn parse_absolute(spec: &str, now: NaiveDateTime) -> Result<NaiveDateTime, String> {
    let s = spec.trim();
    if s.bytes().all(|b| b.is_ascii_digit()) && (s.len() == 10 || s.len() == 13) {
        let n: i64 = s.parse().map_err(|_| "bad epoch")?;
        let secs = if s.len() == 13 { n / 1000 } else { n };
        return crate::clock::local_from_epoch(secs).ok_or_else(|| format!("epoch {} out of range", s));
    }
    let mut body = s.to_string();
    // drop a trailing Z / +HH:MM / -HHMM offset on ISO forms
    if body.len() > 10 && (body.contains('T') || body.contains(' ')) {
        if body.ends_with('Z') || body.ends_with('z') {
            body.pop();
        } else if let Some(pos) = body.rfind(['+', '-']) {
            if pos > 10 && body[pos + 1..].bytes().all(|b| b.is_ascii_digit() || b == b':') {
                body.truncate(pos);
            }
        }
    }
    let body = body.trim().replace('T', " ");
    const FORMATS: &[&str] = &[
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%m/%d/%Y %H:%M:%S",
        "%m/%d/%Y %H:%M",
        "%Y/%m/%d %H:%M:%S",
        "%d/%b/%Y:%H:%M:%S",
        "%b %d %Y %H:%M:%S",
    ];
    for f in FORMATS {
        if let Ok(dt) = NaiveDateTime::parse_from_str(&body, f) {
            return Ok(dt);
        }
    }
    const DATE_FORMATS: &[&str] = &["%Y-%m-%d", "%m/%d/%Y", "%Y/%m/%d"];
    for f in DATE_FORMATS {
        if let Ok(d) = NaiveDate::parse_from_str(&body, f) {
            return Ok(d.and_time(NaiveTime::from_hms_opt(0, 0, 0).unwrap()));
        }
    }
    // time only: dateutil fills the date from today
    if let Ok(t) = NaiveTime::parse_from_str(&body, "%H:%M:%S") {
        return Ok(now.date().and_time(t));
    }
    Err(format!("cannot parse date/time for {}", spec))
}

/// `now`, or `[+-]N(ms|s|m|h|d)` as signed seconds (eventgen's
/// `_convert_time_difference_to_seconds`).
pub fn offset_seconds(spec: &str) -> f64 {
    let s = spec.trim();
    if s == "now" || s.len() < 2 {
        return 0.0;
    }
    let negative = s.starts_with('-');
    let body = &s[1..];
    let (num, mult) = if let Some(n) = body.strip_suffix("ms") {
        (n, 0.001)
    } else if let Some(n) = body.strip_suffix('s') {
        (n, 1.0)
    } else if let Some(n) = body.strip_suffix('m') {
        (n, 60.0)
    } else if let Some(n) = body.strip_suffix('h') {
        (n, 3600.0)
    } else if let Some(n) = body.strip_suffix('d') {
        (n, 86400.0)
    } else {
        return 0.0;
    };
    let v: f64 = num.parse().unwrap_or(0.0) * mult;
    if negative {
        -v
    } else {
        v
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(s: &str) -> NaiveDateTime {
        NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S").unwrap()
    }

    #[test]
    fn relative_basic() {
        let now = at("2026-09-25 14:30:15");
        assert_eq!(parse_time("now", now).unwrap(), now);
        assert_eq!(parse_time("-60s", now).unwrap(), at("2026-09-25 14:29:15"));
        assert_eq!(parse_time("+2h", now).unwrap(), at("2026-09-25 16:30:15"));
        assert_eq!(parse_time("-1d", now).unwrap(), at("2026-09-24 14:30:15"));
        assert_eq!(parse_time("-2w", now).unwrap(), at("2026-09-11 14:30:15"));
        assert_eq!(parse_time("-1mon", now).unwrap(), at("2026-08-25 14:30:15"));
        assert_eq!(parse_time("-1y", now).unwrap(), at("2025-09-25 14:30:15"));
        assert_eq!(parse_time("-15minutes", now).unwrap(), at("2026-09-25 14:15:15"));
    }

    #[test]
    fn snaps() {
        let now = at("2026-09-25 14:30:15"); // a Friday
        assert_eq!(parse_time("-1d@d", now).unwrap(), at("2026-09-24 00:00:00"));
        assert_eq!(parse_time("-0h@h", now).unwrap(), at("2026-09-25 14:00:00"));
        assert_eq!(parse_time("-0m@m+30s", now).unwrap(), at("2026-09-25 14:30:30"));
        assert_eq!(parse_time("-0d@w0", now).unwrap(), at("2026-09-20 00:00:00")); // Sunday
        assert_eq!(parse_time("-0d@w1", now).unwrap(), at("2026-09-21 00:00:00")); // Monday
        assert_eq!(parse_time("-0d@w6", now).unwrap(), at("2026-09-19 00:00:00")); // Saturday
        assert_eq!(parse_time("-0d@mon", now).unwrap(), at("2026-09-01 00:00:00"));
        assert_eq!(parse_time("-0d@q", now).unwrap(), at("2026-07-01 00:00:00"));
        assert_eq!(parse_time("-0d@y", now).unwrap(), at("2026-01-01 00:00:00"));
    }

    #[test]
    fn absolute_forms() {
        let now = at("2026-09-25 14:30:15");
        assert_eq!(parse_time("2026-01-02 03:04:05", now).unwrap(), at("2026-01-02 03:04:05"));
        assert_eq!(parse_time("2026-01-02T03:04:05Z", now).unwrap(), at("2026-01-02 03:04:05"));
        assert_eq!(parse_time("2026-01-02T03:04:05+01:00", now).unwrap(), at("2026-01-02 03:04:05"));
        assert_eq!(parse_time("2026-01-02", now).unwrap(), at("2026-01-02 00:00:00"));
        assert_eq!(parse_time("01/02/2026 03:04:05", now).unwrap(), at("2026-01-02 03:04:05"));
        assert!(parse_time("yesterday-ish", now).is_err());
    }

    #[test]
    fn offsets() {
        assert_eq!(offset_seconds("now"), 0.0);
        assert_eq!(offset_seconds("-60s"), -60.0);
        assert_eq!(offset_seconds("+2m"), 120.0);
        assert_eq!(offset_seconds("-500ms"), -0.5);
        assert_eq!(offset_seconds("-1h"), -3600.0);
        assert_eq!(offset_seconds("-1d"), -86400.0);
    }
}
