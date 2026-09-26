//! A precompiled `strftime` for the C locale.
//!
//! eventgen formats every timestamp token with Python's `strftime` (glibc
//! underneath). The format string is fixed per token, so we parse it once into
//! a layout and write straight into a byte buffer per event. Supported: the
//! glibc set plus Python's `%f`; `%s` writes the full epoch (upstream mangles
//! it); `%z`/`%Z` write nothing, exactly what a naive Python datetime gives.

use chrono::{Datelike, NaiveDateTime, Timelike};

const MONTH_ABBR: [&str; 12] = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
const MONTH_FULL: [&str; 12] = [
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December",
];
const WDAY_ABBR: [&str; 7] = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"];
const WDAY_FULL: [&str; 7] = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"];

/// Broken-down local time plus its epoch, the only thing a layout needs.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Ts {
    pub year: i32,
    pub month: u32,
    pub day: u32,
    pub hour: u32,
    pub minute: u32,
    pub second: u32,
    pub micro: u32,
    /// 0 = Monday .. 6 = Sunday (Python `weekday()`)
    pub weekday: u32,
    /// 1-based day of year
    pub yday: u32,
    pub epoch: i64,
}

impl Ts {
    pub fn from_naive(dt: &NaiveDateTime, epoch: i64) -> Ts {
        Ts {
            year: dt.year(),
            month: dt.month(),
            day: dt.day(),
            hour: dt.hour(),
            minute: dt.minute(),
            second: dt.second(),
            micro: dt.nanosecond() / 1000,
            weekday: dt.weekday().num_days_from_monday(),
            yday: dt.ordinal(),
            epoch,
        }
    }

    /// Local broken-down time for an epoch (naive local, like
    /// `datetime.fromtimestamp`).
    pub fn from_epoch(epoch: i64, micro: u32) -> Option<Ts> {
        let dt = crate::clock::local_from_epoch_nanos(epoch, micro * 1000)?;
        Some(Ts::from_naive(&dt, epoch))
    }

    /// Splunk-style weekday: Sunday = 0 .. Saturday = 6.
    pub fn splunk_weekday(&self) -> u32 {
        (self.weekday + 1) % 7
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Pad {
    Zero,
    Space,
    None,
}

#[derive(Debug, Clone, PartialEq)]
enum Item {
    Lit(Vec<u8>),
    Year(Pad),
    Year2(Pad),
    Century(Pad),
    Month(Pad),
    Day(Pad),
    DaySpace,
    Hour(Pad),
    Hour12(Pad),
    HourSpace,
    Hour12Space,
    Minute(Pad),
    Second(Pad),
    Micro,
    MonthAbbr,
    MonthFull,
    WdayAbbr,
    WdayFull,
    AmPm,
    Yday(Pad),
    Epoch,
    WeekdayMon1,
    WeekdaySun0,
    WeekSun(Pad),
    WeekMon(Pad),
    IsoWeek(Pad),
    IsoYear(Pad),
    IsoYear2(Pad),
    Tz,
    Newline,
    Tab,
}

/// A compiled strftime format.
#[derive(Debug, Clone, PartialEq)]
pub struct Layout {
    items: Vec<Item>,
    specifiers: usize,
}

impl Layout {
    pub fn compile(fmt: &str) -> Layout {
        let mut items = Vec::new();
        let mut lit: Vec<u8> = Vec::new();
        let mut specifiers = 0;
        let b = fmt.as_bytes();
        let mut i = 0;
        while i < b.len() {
            if b[i] != b'%' {
                lit.push(b[i]);
                i += 1;
                continue;
            }
            // parse: % [flag] [width] conv
            let start = i;
            i += 1;
            let mut pad: Option<Pad> = None;
            while i < b.len() && matches!(b[i], b'-' | b'_' | b'0' | b'^' | b'#') {
                pad = match b[i] {
                    b'-' => Some(Pad::None),
                    b'_' => Some(Pad::Space),
                    b'0' => Some(Pad::Zero),
                    _ => pad,
                };
                i += 1;
            }
            while i < b.len() && b[i].is_ascii_digit() {
                i += 1; // width is accepted and ignored
            }
            // glibc E and O modifiers
            while i < b.len() && (b[i] == b'E' || b[i] == b'O') {
                i += 1;
            }
            if i >= b.len() {
                // dangling % : Python/glibc emit it literally
                lit.extend_from_slice(&b[start..]);
                break;
            }
            let conv = b[i];
            i += 1;
            let p = |d: Pad| pad.unwrap_or(d);
            let item = match conv {
                b'Y' => Item::Year(p(Pad::Zero)),
                b'y' => Item::Year2(p(Pad::Zero)),
                b'C' => Item::Century(p(Pad::Zero)),
                b'm' => Item::Month(p(Pad::Zero)),
                b'd' => Item::Day(p(Pad::Zero)),
                b'e' => match pad {
                    Some(Pad::Zero) => Item::Day(Pad::Zero),
                    Some(Pad::None) => Item::Day(Pad::None),
                    _ => Item::DaySpace,
                },
                b'H' => Item::Hour(p(Pad::Zero)),
                b'k' => match pad {
                    Some(Pad::Zero) => Item::Hour(Pad::Zero),
                    Some(Pad::None) => Item::Hour(Pad::None),
                    _ => Item::HourSpace,
                },
                b'I' => Item::Hour12(p(Pad::Zero)),
                b'l' => match pad {
                    Some(Pad::Zero) => Item::Hour12(Pad::Zero),
                    Some(Pad::None) => Item::Hour12(Pad::None),
                    _ => Item::Hour12Space,
                },
                b'M' => Item::Minute(p(Pad::Zero)),
                b'S' => Item::Second(p(Pad::Zero)),
                b'f' => Item::Micro,
                b'b' | b'h' => Item::MonthAbbr,
                b'B' => Item::MonthFull,
                b'a' => Item::WdayAbbr,
                b'A' => Item::WdayFull,
                b'p' => Item::AmPm,
                b'j' => Item::Yday(p(Pad::Zero)),
                b's' => Item::Epoch,
                b'u' => Item::WeekdayMon1,
                b'w' => Item::WeekdaySun0,
                b'U' => Item::WeekSun(p(Pad::Zero)),
                b'W' => Item::WeekMon(p(Pad::Zero)),
                b'V' => Item::IsoWeek(p(Pad::Zero)),
                b'G' => Item::IsoYear(p(Pad::Zero)),
                b'g' => Item::IsoYear2(p(Pad::Zero)),
                b'z' | b'Z' => Item::Tz,
                b'n' => Item::Newline,
                b't' => Item::Tab,
                b'%' => {
                    lit.push(b'%');
                    continue;
                }
                // composite formats expand to their C-locale definitions
                b'T' | b'X' => {
                    flush_lit(&mut items, &mut lit);
                    items.extend(Layout::compile("%H:%M:%S").items);
                    specifiers += 3;
                    continue;
                }
                b'R' => {
                    flush_lit(&mut items, &mut lit);
                    items.extend(Layout::compile("%H:%M").items);
                    specifiers += 2;
                    continue;
                }
                b'D' | b'x' => {
                    flush_lit(&mut items, &mut lit);
                    items.extend(Layout::compile("%m/%d/%y").items);
                    specifiers += 3;
                    continue;
                }
                b'F' => {
                    flush_lit(&mut items, &mut lit);
                    items.extend(Layout::compile("%Y-%m-%d").items);
                    specifiers += 3;
                    continue;
                }
                b'c' => {
                    flush_lit(&mut items, &mut lit);
                    items.extend(Layout::compile("%a %b %e %H:%M:%S %Y").items);
                    specifiers += 6;
                    continue;
                }
                b'r' => {
                    flush_lit(&mut items, &mut lit);
                    items.extend(Layout::compile("%I:%M:%S %p").items);
                    specifiers += 4;
                    continue;
                }
                _ => {
                    // unknown conversion: emitted literally (glibc behaviour)
                    lit.extend_from_slice(&b[start..i]);
                    continue;
                }
            };
            flush_lit(&mut items, &mut lit);
            items.push(item);
            specifiers += 1;
        }
        flush_lit(&mut items, &mut lit);
        Layout { items, specifiers }
    }

    /// True when the format carries at least one conversion. eventgen refuses
    /// to replace with a format that has no valid specifier.
    pub fn has_specifiers(&self) -> bool {
        self.specifiers > 0
    }

    pub fn write(&self, out: &mut Vec<u8>, ts: &Ts) {
        for item in &self.items {
            match item {
                Item::Lit(s) => out.extend_from_slice(s),
                Item::Year(p) => push_num(out, ts.year as i64, 4, *p),
                Item::Year2(p) => push_num(out, (ts.year.rem_euclid(100)) as i64, 2, *p),
                Item::Century(p) => push_num(out, (ts.year.div_euclid(100)) as i64, 2, *p),
                Item::Month(p) => push_num(out, ts.month as i64, 2, *p),
                Item::Day(p) => push_num(out, ts.day as i64, 2, *p),
                Item::DaySpace => push_num(out, ts.day as i64, 2, Pad::Space),
                Item::Hour(p) => push_num(out, ts.hour as i64, 2, *p),
                Item::HourSpace => push_num(out, ts.hour as i64, 2, Pad::Space),
                Item::Hour12(p) => push_num(out, hour12(ts.hour) as i64, 2, *p),
                Item::Hour12Space => push_num(out, hour12(ts.hour) as i64, 2, Pad::Space),
                Item::Minute(p) => push_num(out, ts.minute as i64, 2, *p),
                Item::Second(p) => push_num(out, ts.second as i64, 2, *p),
                Item::Micro => push_num(out, ts.micro as i64, 6, Pad::Zero),
                Item::MonthAbbr => out.extend_from_slice(MONTH_ABBR[(ts.month - 1) as usize].as_bytes()),
                Item::MonthFull => out.extend_from_slice(MONTH_FULL[(ts.month - 1) as usize].as_bytes()),
                Item::WdayAbbr => out.extend_from_slice(WDAY_ABBR[ts.weekday as usize].as_bytes()),
                Item::WdayFull => out.extend_from_slice(WDAY_FULL[ts.weekday as usize].as_bytes()),
                Item::AmPm => out.extend_from_slice(if ts.hour < 12 { b"AM" } else { b"PM" }),
                Item::Yday(p) => push_num(out, ts.yday as i64, 3, *p),
                Item::Epoch => push_num(out, ts.epoch, 1, Pad::None),
                Item::WeekdayMon1 => push_num(out, (ts.weekday + 1) as i64, 1, Pad::None),
                Item::WeekdaySun0 => push_num(out, ts.splunk_weekday() as i64, 1, Pad::None),
                Item::WeekSun(p) => {
                    let w = (ts.yday + 6 - ts.splunk_weekday()) / 7;
                    push_num(out, w as i64, 2, *p)
                }
                Item::WeekMon(p) => {
                    let w = (ts.yday + 6 - ts.weekday) / 7;
                    push_num(out, w as i64, 2, *p)
                }
                Item::IsoWeek(p) | Item::IsoYear(p) | Item::IsoYear2(p) => {
                    if let Some(d) = chrono::NaiveDate::from_yo_opt(ts.year, ts.yday) {
                        let iso = d.iso_week();
                        match item {
                            Item::IsoWeek(_) => push_num(out, iso.week() as i64, 2, *p),
                            Item::IsoYear(_) => push_num(out, iso.year() as i64, 4, *p),
                            _ => push_num(out, iso.year().rem_euclid(100) as i64, 2, *p),
                        }
                    }
                }
                Item::Tz => {}
                Item::Newline => out.push(b'\n'),
                Item::Tab => out.push(b'\t'),
            }
        }
    }

    /// Append the formatted time to a String. Every byte a layout emits is
    /// ASCII or a literal copied from the (UTF-8) format split at ASCII `%`,
    /// so the buffer stays valid UTF-8.
    pub fn write_str(&self, out: &mut String, ts: &Ts) {
        // SAFETY: see above; only valid UTF-8 is appended.
        let bytes = unsafe { out.as_mut_vec() };
        self.write(bytes, ts);
    }

    pub fn format(&self, ts: &Ts) -> String {
        let mut out = Vec::with_capacity(32);
        self.write(&mut out, ts);
        // every emitted byte is ASCII or a copied literal from a &str
        String::from_utf8(out).unwrap_or_default()
    }
}

fn flush_lit(items: &mut Vec<Item>, lit: &mut Vec<u8>) {
    if !lit.is_empty() {
        items.push(Item::Lit(std::mem::take(lit)));
    }
}

fn hour12(h: u32) -> u32 {
    match h % 12 {
        0 => 12,
        x => x,
    }
}

#[inline]
fn push_num(out: &mut Vec<u8>, v: i64, width: usize, pad: Pad) {
    let mut buf = [0u8; 20];
    let mut n = v.unsigned_abs();
    let mut i = buf.len();
    if n == 0 {
        i -= 1;
        buf[i] = b'0';
    }
    while n > 0 {
        i -= 1;
        buf[i] = b'0' + (n % 10) as u8;
        n /= 10;
    }
    let digits = buf.len() - i;
    if v < 0 {
        out.push(b'-');
    }
    if digits < width {
        let fill = match pad {
            Pad::Zero => Some(b'0'),
            Pad::Space => Some(b' '),
            Pad::None => None,
        };
        if let Some(c) = fill {
            for _ in digits..width {
                out.push(c);
            }
        }
    }
    out.extend_from_slice(&buf[i..]);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ts() -> Ts {
        Ts {
            year: 2026,
            month: 9,
            day: 5,
            hour: 14,
            minute: 3,
            second: 9,
            micro: 123456,
            weekday: 5, // Saturday
            yday: 248,
            epoch: 1788000000,
        }
    }

    #[test]
    fn common_formats() {
        let t = ts();
        assert_eq!(Layout::compile("%Y-%m-%dT%H:%M:%S").format(&t), "2026-09-05T14:03:09");
        assert_eq!(Layout::compile("%d/%b/%Y:%H:%M:%S").format(&t), "05/Sep/2026:14:03:09");
        assert_eq!(Layout::compile("%a %b %d %Y %H:%M:%S").format(&t), "Sat Sep 05 2026 14:03:09");
        assert_eq!(Layout::compile("%b %e %H:%M:%S").format(&t), "Sep  5 14:03:09");
        assert_eq!(Layout::compile("%Y-%m-%dT%H:%M:%S.%f000Z").format(&t), "2026-09-05T14:03:09.123456000Z");
        assert_eq!(Layout::compile("%s").format(&t), "1788000000");
        assert_eq!(Layout::compile("%I:%M %p").format(&t), "02:03 PM");
        assert_eq!(Layout::compile("%y %j %A %B").format(&t), "26 248 Saturday September");
        assert_eq!(Layout::compile("%z|%Z|%%|%-d|%_d").format(&t), "||%|5| 5");
        assert_eq!(Layout::compile("%T %F").format(&t), "14:03:09 2026-09-05");
        assert_eq!(Layout::compile("%u %w").format(&t), "6 6");
    }

    #[test]
    fn specifier_detection() {
        assert!(!Layout::compile("no specifiers").has_specifiers());
        assert!(Layout::compile("%Y").has_specifiers());
        assert_eq!(Layout::compile("%Q").format(&ts()), "%Q");
        assert_eq!(Layout::compile("trailing %").format(&ts()), "trailing %");
    }
}
