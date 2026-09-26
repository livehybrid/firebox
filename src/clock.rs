//! Local-time helpers. eventgen works in naive local datetimes
//! (`datetime.now()`) and converts to epochs with `time.mktime`; the container
//! is normally UTC, but the semantics are "local", so we keep them.

use chrono::{Local, NaiveDateTime, TimeZone};

/// Epoch seconds for a naive local datetime (`time.mktime(dt.timetuple())`).
/// During a DST fold the earlier instant is used; a gap resolves to the later.
pub fn epoch_from_local(dt: &NaiveDateTime) -> i64 {
    match Local.from_local_datetime(dt) {
        chrono::LocalResult::Single(t) => t.timestamp(),
        chrono::LocalResult::Ambiguous(a, _) => a.timestamp(),
        chrono::LocalResult::None => {
            // inside a DST gap: shift forward an hour the way mktime normalises
            let later = *dt + chrono::Duration::hours(1);
            match Local.from_local_datetime(&later) {
                chrono::LocalResult::Single(t) => t.timestamp(),
                chrono::LocalResult::Ambiguous(a, _) => a.timestamp(),
                chrono::LocalResult::None => dt.and_utc().timestamp(),
            }
        }
    }
}

/// Naive local datetime for an epoch (`datetime.fromtimestamp`).
pub fn local_from_epoch(secs: i64) -> Option<NaiveDateTime> {
    Local.timestamp_opt(secs, 0).single().map(|t| t.naive_local())
}

/// Naive local datetime for an epoch with sub-second nanos.
pub fn local_from_epoch_nanos(secs: i64, nanos: u32) -> Option<NaiveDateTime> {
    Local.timestamp_opt(secs, nanos).single().map(|t| t.naive_local())
}

/// The current naive local time (`datetime.datetime.now()`).
pub fn now_local() -> NaiveDateTime {
    Local::now().naive_local()
}

/// Current epoch seconds as f64.
pub fn now_epoch_f64() -> f64 {
    let d = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default();
    d.as_secs_f64()
}

/// The UTC offset in seconds currently in effect for local time.
pub fn local_offset_seconds() -> i64 {
    Local::now().offset().local_minus_utc() as i64
}
