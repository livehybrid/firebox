//! Raters: how many events (or bytes) an interval produces
//! (`lib/raterplugin.py`, `plugins/rater/config.py`, `plugins/rater/perdayvolume.py`).

use std::collections::HashMap;

use rand::Rng;

use crate::strftime::Ts;

/// The diurnal shaping maps. Keys are parsed from the JSON object's string
/// keys; a value looked up for a missing key leaves the factor unchanged.
#[derive(Debug, Clone, Default)]
pub struct RateMaps {
    pub hour_of_day: Option<HashMap<u32, f64>>,
    pub day_of_week: Option<HashMap<u32, f64>>,
    pub minute_of_hour: Option<HashMap<u32, f64>>,
    pub day_of_month: Option<HashMap<u32, f64>>,
    pub month_of_year: Option<HashMap<u32, f64>>,
}

impl RateMaps {
    pub fn parse(get: impl Fn(&str) -> Option<String>) -> RateMaps {
        RateMaps {
            hour_of_day: get("hourOfDayRate").and_then(|v| parse_map("hourOfDayRate", &v)),
            day_of_week: get("dayOfWeekRate").and_then(|v| parse_map("dayOfWeekRate", &v)),
            minute_of_hour: get("minuteOfHourRate").and_then(|v| parse_map("minuteOfHourRate", &v)),
            day_of_month: get("dayOfMonthRate").and_then(|v| parse_map("dayOfMonthRate", &v)),
            month_of_year: get("monthOfYearRate").and_then(|v| parse_map("monthOfYearRate", &v)),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.hour_of_day.is_none()
            && self.day_of_week.is_none()
            && self.minute_of_hour.is_none()
            && self.day_of_month.is_none()
            && self.month_of_year.is_none()
    }

    /// The combined multiplier at `now`.
    pub fn factor(&self, now: &Ts) -> f64 {
        let mut f = 1.0;
        f *= lookup(&self.hour_of_day, now.hour, "hourOfDayRate");
        f *= lookup(&self.day_of_week, now.splunk_weekday(), "dayOfWeekRate");
        f *= lookup(&self.minute_of_hour, now.minute, "minuteOfHourRate");
        f *= lookup(&self.day_of_month, now.day, "dayOfMonthRate");
        f *= lookup(&self.month_of_year, now.month, "monthOfYearRate");
        f
    }

    /// Only the hour and weekday maps, as `rated` tokens apply them.
    pub fn rated_factor(&self, now: &Ts) -> f64 {
        lookup(&self.hour_of_day, now.hour, "hourOfDayRate")
            * lookup(&self.day_of_week, now.splunk_weekday(), "dayOfWeekRate")
    }
}

fn lookup(map: &Option<HashMap<u32, f64>>, key: u32, name: &str) -> f64 {
    match map {
        Some(m) => match m.get(&key) {
            Some(v) => *v,
            None => {
                log::debug!("{} has no key {}; factor unchanged", name, key);
                1.0
            }
        },
        None => 1.0,
    }
}

fn parse_map(name: &str, text: &str) -> Option<HashMap<u32, f64>> {
    let v: serde_json::Value = match serde_json::from_str(text) {
        Ok(v) => v,
        Err(e) => {
            log::error!("Could not parse json for '{}': {}", name, e);
            return None;
        }
    };
    let obj = v.as_object()?;
    let mut out = HashMap::with_capacity(obj.len());
    for (k, val) in obj {
        let (Ok(key), Some(f)) = (k.trim().parse::<u32>(), val.as_f64()) else {
            log::warn!("{}: ignoring entry {:?}: {}", name, k, val);
            continue;
        };
        out.insert(key, f);
    }
    Some(out)
}

/// Python's `round()` (half to even) to an integer.
#[inline]
pub fn py_round(x: f64) -> i64 {
    x.round_ties_even() as i64
}

/// `randomizeCount`: a uniform jitter of +/- half the value. Upstream draws
/// `randint(0, round(rc * 1000))` and maps it to `1 + (rand - bound/2)/1000`.
pub fn randomize_count_factor<R: Rng>(rc: f64, rng: &mut R) -> f64 {
    let bound = py_round(rc * 1000.0).max(0);
    let r = rng.random_range(0..=bound) as f64;
    1.0 + (r - bound as f64 / 2.0) / 1000.0
}

/// The config rater: `int(round(count * factor))`.
pub fn rated_count(count: i64, factor: f64) -> i64 {
    py_round(count as f64 * factor)
}

/// The perdayvolume rater: bytes per interval with the carry-over rule.
#[derive(Debug, Clone)]
pub struct PerDayVolumeRater {
    pub per_interval_bytes: f64,
    pub raw_event_size: f64,
    carry: f64,
}

impl PerDayVolumeRater {
    pub fn new(per_day_gb: f64, interval: i64, raw_event_size: f64) -> PerDayVolumeRater {
        let per_day_bytes = per_day_gb * 1024.0 * 1024.0 * 1024.0;
        let interval = if interval == 0 { 86400.0 } else { interval as f64 };
        let intervals_per_day = 86400.0 / interval;
        PerDayVolumeRater { per_interval_bytes: per_day_bytes / intervals_per_day, raw_event_size, carry: 0.0 }
    }

    /// Bytes to emit this interval, or `None` when the running total is still
    /// smaller than one event (carried into the next interval).
    pub fn next_size(&mut self, factor: f64) -> Option<f64> {
        let rated = py_round(self.per_interval_bytes * factor) as f64;
        let count = rated + self.carry;
        if count > 0.0 && count < self.raw_event_size {
            log::info!(
                "current interval size is {}, which is smaller than a raw event size {}. Wait for the next turn.",
                count,
                self.raw_event_size
            );
            self.carry = count;
            None
        } else {
            self.carry = 0.0;
            Some(count)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::rngs::SmallRng;
    use rand::SeedableRng;

    fn ts_at(hour: u32, weekday: u32) -> Ts {
        Ts { year: 2026, month: 9, day: 25, hour, minute: 5, second: 0, micro: 0, weekday, yday: 268, epoch: 0 }
    }

    #[test]
    fn maps_and_factors() {
        let maps = RateMaps::parse(|k| match k {
            "hourOfDayRate" => Some(r#"{"0": 0.5, "14": 2.0}"#.into()),
            "dayOfWeekRate" => Some(r#"{"0": 0.1, "5": 3.0}"#.into()),
            _ => None,
        });
        // Friday 14:00 -> python weekday 4 -> splunk 5
        assert_eq!(maps.factor(&ts_at(14, 4)), 6.0);
        assert_eq!(maps.factor(&ts_at(0, 6)), 0.05); // Sunday 00:00
        assert_eq!(maps.factor(&ts_at(3, 1)), 1.0); // missing keys
    }

    #[test]
    fn rounding_and_jitter() {
        assert_eq!(py_round(2.5), 2);
        assert_eq!(py_round(3.5), 4);
        assert_eq!(rated_count(100, 1.005), 100); // 100.49999999999999 in binary, like Python
        assert_eq!(rated_count(100, 1.5), 150);
        assert_eq!(rated_count(3, 0.5), 2); // 1.5 rounds to even
        assert_eq!(rated_count(5, 0.5), 2); // 2.5 rounds to even
        let mut rng = SmallRng::seed_from_u64(1);
        for _ in 0..1000 {
            let f = randomize_count_factor(0.2, &mut rng);
            assert!((0.9..=1.1).contains(&f), "{}", f);
        }
    }

    #[test]
    fn perdayvolume_carry() {
        // 1 GB/day at 60 s intervals = 745654.6 bytes per interval
        let mut r = PerDayVolumeRater::new(1.0, 60, 100.0);
        assert_eq!(r.next_size(1.0), Some(745654.0));
        let mut zero = PerDayVolumeRater::new(0.0000001, 60, 100.0); // 0.07 bytes/interval rounds to 0
        assert_eq!(zero.next_size(1.0), Some(0.0));
        let mut mid = PerDayVolumeRater::new(0.00001, 60, 100.0); // ~7.5 bytes: carried
        assert_eq!(mid.next_size(1.0), None);
        assert_eq!(mid.next_size(1.0), None);
        assert!(mid.carry > 0.0);
        // carry accumulates until it covers one event
        for _ in 0..20 {
            if let Some(v) = mid.next_size(1.0) {
                assert!(v >= 100.0);
                assert_eq!(mid.carry, 0.0);
                return;
            }
        }
        panic!("carry never released");
    }
}
