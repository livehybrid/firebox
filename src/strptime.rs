//! A `datetime.strptime` subset for replay mode's timestamp extraction.
//!
//! Python's strptime is lenient about digit widths (`%d` accepts `5` or `05`),
//! treats whitespace in the format as "one or more whitespace" and fills a
//! missing year with 1900 (eventgen then substitutes the current year).

use chrono::{NaiveDate, NaiveDateTime, NaiveTime};

const MONTHS: [&str; 12] = ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"];
const MONTHS_FULL: [&str; 12] = [
    "january",
    "february",
    "march",
    "april",
    "may",
    "june",
    "july",
    "august",
    "september",
    "october",
    "november",
    "december",
];
const DAYS: [&str; 7] = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"];
const DAYS_FULL: [&str; 7] = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"];

#[derive(Default)]
struct Parts {
    year: Option<i32>,
    month: Option<u32>,
    day: Option<u32>,
    hour: Option<u32>,
    minute: Option<u32>,
    second: Option<u32>,
    micro: Option<u32>,
    pm: Option<bool>,
    yday: Option<u32>,
    epoch: Option<f64>,
}

/// Parse `text` with a strftime-style `fmt`. Returns a naive datetime
/// (year 1900 when the format has no year, like Python).
pub fn strptime(text: &str, fmt: &str) -> Option<NaiveDateTime> {
    let t = text.as_bytes();
    let f = fmt.as_bytes();
    let mut ti = 0usize;
    let mut fi = 0usize;
    let mut p = Parts::default();
    while fi < f.len() {
        let fc = f[fi];
        if fc == b'%' && fi + 1 < f.len() {
            let mut conv = f[fi + 1];
            fi += 2;
            // skip glibc flags/width: %-d %_d %3f
            while matches!(conv, b'-' | b'_' | b'0' | b'^' | b'#') || conv.is_ascii_digit() {
                if fi >= f.len() {
                    return None;
                }
                conv = f[fi];
                fi += 1;
            }
            match conv {
                b'Y' => p.year = Some(take_digits(t, &mut ti, 4, 4)? as i32),
                b'y' => {
                    let y = take_digits(t, &mut ti, 2, 2)? as i32;
                    p.year = Some(if y < 69 { 2000 + y } else { 1900 + y });
                }
                b'm' => p.month = Some(take_digits(t, &mut ti, 1, 2)? as u32),
                b'd' | b'e' => {
                    skip_spaces(t, &mut ti);
                    p.day = Some(take_digits(t, &mut ti, 1, 2)? as u32)
                }
                b'H' | b'k' => {
                    skip_spaces(t, &mut ti);
                    p.hour = Some(take_digits(t, &mut ti, 1, 2)? as u32)
                }
                b'I' | b'l' => {
                    skip_spaces(t, &mut ti);
                    p.hour = Some(take_digits(t, &mut ti, 1, 2)? as u32)
                }
                b'M' => p.minute = Some(take_digits(t, &mut ti, 1, 2)? as u32),
                b'S' => p.second = Some(take_digits(t, &mut ti, 1, 2)? as u32),
                b'f' => {
                    let start = ti;
                    let v = take_digits(t, &mut ti, 1, 6)?;
                    let width = ti - start;
                    p.micro = Some((v * 10u64.pow(6 - width as u32)) as u32);
                }
                b'j' => p.yday = Some(take_digits(t, &mut ti, 1, 3)? as u32),
                b'b' | b'h' => p.month = Some(take_name(t, &mut ti, &MONTHS)? as u32 + 1),
                b'B' => p.month = Some(take_name(t, &mut ti, &MONTHS_FULL)? as u32 + 1),
                b'a' => {
                    take_name(t, &mut ti, &DAYS)?;
                }
                b'A' => {
                    take_name(t, &mut ti, &DAYS_FULL)?;
                }
                b'p' => {
                    let rest = &t[ti..];
                    if rest.len() >= 2 {
                        let two = std::str::from_utf8(&rest[..2]).ok()?.to_ascii_lowercase();
                        p.pm = Some(match two.as_str() {
                            "am" => false,
                            "pm" => true,
                            _ => return None,
                        });
                        ti += 2;
                    } else {
                        return None;
                    }
                }
                b's' => {
                    let start = ti;
                    while ti < t.len() && (t[ti].is_ascii_digit() || t[ti] == b'.') {
                        ti += 1;
                    }
                    p.epoch = Some(std::str::from_utf8(&t[start..ti]).ok()?.parse().ok()?);
                }
                b'z' => {
                    // +HHMM, +HH:MM or Z; the offset is parsed and dropped (naive result)
                    if ti < t.len() && (t[ti] == b'Z' || t[ti] == b'z') {
                        ti += 1;
                    } else {
                        if ti >= t.len() || !(t[ti] == b'+' || t[ti] == b'-') {
                            return None;
                        }
                        ti += 1;
                        take_digits(t, &mut ti, 2, 2)?;
                        if ti < t.len() && t[ti] == b':' {
                            ti += 1;
                        }
                        take_digits(t, &mut ti, 2, 2)?;
                    }
                }
                b'Z' => {
                    while ti < t.len() && t[ti].is_ascii_alphabetic() {
                        ti += 1;
                    }
                }
                b'%' => {
                    if ti < t.len() && t[ti] == b'%' {
                        ti += 1;
                    } else {
                        return None;
                    }
                }
                b'T' => {
                    let sub = strptime_at(t, &mut ti, "%H:%M:%S")?;
                    p.hour = sub.hour;
                    p.minute = sub.minute;
                    p.second = sub.second;
                }
                b'F' => {
                    let sub = strptime_at(t, &mut ti, "%Y-%m-%d")?;
                    p.year = sub.year;
                    p.month = sub.month;
                    p.day = sub.day;
                }
                _ => return None,
            }
            continue;
        }
        if fc.is_ascii_whitespace() {
            // whitespace in the format matches one or more whitespace chars
            if ti >= t.len() || !t[ti].is_ascii_whitespace() {
                return None;
            }
            while ti < t.len() && t[ti].is_ascii_whitespace() {
                ti += 1;
            }
            fi += 1;
            continue;
        }
        if ti >= t.len() || t[ti] != fc {
            return None;
        }
        ti += 1;
        fi += 1;
    }
    if ti != t.len() {
        return None; // unconverted data remains
    }
    build(p)
}

fn strptime_at(t: &[u8], ti: &mut usize, fmt: &str) -> Option<Parts> {
    // parse a sub-format greedily from the current position
    let rest = std::str::from_utf8(&t[*ti..]).ok()?;
    // find the longest prefix that parses
    for end in (0..=rest.len()).rev() {
        if !rest.is_char_boundary(end) {
            continue;
        }
        if let Some(dt) = strptime(&rest[..end], fmt) {
            *ti += end;
            return Some(Parts {
                year: Some(dt.date().year()),
                month: Some(dt.date().month()),
                day: Some(dt.date().day()),
                hour: Some(dt.time().hour()),
                minute: Some(dt.time().minute()),
                second: Some(dt.time().second()),
                micro: Some(dt.time().nanosecond() / 1000),
                ..Default::default()
            });
        }
    }
    None
}

use chrono::{Datelike, Timelike};

fn skip_spaces(t: &[u8], ti: &mut usize) {
    while *ti < t.len() && t[*ti] == b' ' {
        *ti += 1;
    }
}

fn take_digits(t: &[u8], ti: &mut usize, min: usize, max: usize) -> Option<u64> {
    let start = *ti;
    while *ti < t.len() && *ti - start < max && t[*ti].is_ascii_digit() {
        *ti += 1;
    }
    let n = *ti - start;
    if n < min {
        return None;
    }
    std::str::from_utf8(&t[start..*ti]).ok()?.parse().ok()
}

fn take_name(t: &[u8], ti: &mut usize, names: &[&str]) -> Option<usize> {
    let rest = std::str::from_utf8(&t[*ti..]).ok()?;
    let lower = rest.to_ascii_lowercase();
    // longest names first so "june" beats "jun"
    let mut best: Option<(usize, usize)> = None;
    for (i, n) in names.iter().enumerate() {
        if lower.starts_with(n) && best.map(|(_, l)| n.len() > l).unwrap_or(true) {
            best = Some((i, n.len()));
        }
    }
    let (i, len) = best?;
    *ti += len;
    Some(i)
}

fn build(p: Parts) -> Option<NaiveDateTime> {
    if let Some(e) = p.epoch {
        let secs = e.floor() as i64;
        let nanos = ((e - e.floor()) * 1e9) as u32;
        return crate::clock::local_from_epoch_nanos(secs, nanos);
    }
    let year = p.year.unwrap_or(1900);
    let date = if let Some(yd) = p.yday {
        NaiveDate::from_yo_opt(year, yd)?
    } else {
        NaiveDate::from_ymd_opt(year, p.month.unwrap_or(1), p.day.unwrap_or(1))?
    };
    let mut hour = p.hour.unwrap_or(0);
    if let Some(pm) = p.pm {
        hour = match (pm, hour) {
            (false, 12) => 0,
            (true, h) if h < 12 => h + 12,
            (_, h) => h,
        };
    }
    let time = NaiveTime::from_hms_micro_opt(hour, p.minute.unwrap_or(0), p.second.unwrap_or(0), p.micro.unwrap_or(0))?;
    Some(date.and_time(time))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dt(s: &str) -> NaiveDateTime {
        NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f")
            .or_else(|_| NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S"))
            .unwrap()
    }

    #[test]
    fn common_formats() {
        assert_eq!(strptime("2026-09-25T14:03:09", "%Y-%m-%dT%H:%M:%S"), Some(dt("2026-09-25 14:03:09")));
        assert_eq!(strptime("25/Sep/2026:14:03:09", "%d/%b/%Y:%H:%M:%S"), Some(dt("2026-09-25 14:03:09")));
        assert_eq!(strptime("Sep  5 14:03:09", "%b %e %H:%M:%S"), Some(dt("1900-09-05 14:03:09")));
        assert_eq!(strptime("Sep 5 14:03:09", "%b %d %H:%M:%S"), Some(dt("1900-09-05 14:03:09")));
        assert_eq!(strptime("2026-09-25T14:03:09.123Z", "%Y-%m-%dT%H:%M:%S.%f%z"), Some(dt("2026-09-25 14:03:09.123")));
        assert_eq!(
            strptime("2026-09-25T14:03:09.123456000Z", "%Y-%m-%dT%H:%M:%S.%f000Z"),
            Some(dt("2026-09-25 14:03:09.123456"))
        );
        assert_eq!(strptime("01/02/2026 03:04:05 PM", "%m/%d/%Y %I:%M:%S %p"), Some(dt("2026-01-02 15:04:05")));
        assert_eq!(strptime("Fri Sep 25 2026 14:03:09", "%a %b %d %Y %H:%M:%S"), Some(dt("2026-09-25 14:03:09")));
        assert!(strptime("2026-09-25", "%Y-%m-%d %H").is_none());
        assert!(strptime("2026-09-25 extra", "%Y-%m-%d").is_none());
        assert!(strptime("not a date", "%Y-%m-%d").is_none());
    }
}
