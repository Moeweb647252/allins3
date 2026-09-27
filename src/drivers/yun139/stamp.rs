//! Timestamp formatting and parsing with no extra dependencies.
//!
//! The 139 request signature is bound to a `YYYY-MM-DD HH:mm:ss` timestamp in
//! the service's local timezone (China Standard Time, UTC+8), and object
//! metadata carries ISO-8601 `updatedAt` values. Both are handled here with
//! plain integer civil-date arithmetic so the driver needs no date crate
//! features beyond what the project already enables.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Seconds of offset from UTC for the drive service's local timezone.
const SHANGHAI_OFFSET_SECS: i64 = 8 * 3600;

/// The current time as `YYYY-MM-DD HH:mm:ss` in the service's timezone.
pub(crate) fn now_shanghai() -> String {
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    format_shanghai(secs)
}

/// Formats a UTC unix timestamp as the service's `YYYY-MM-DD HH:mm:ss`.
pub(crate) fn format_shanghai(unix_secs: i64) -> String {
    let local = unix_secs + SHANGHAI_OFFSET_SECS;
    let days = local.div_euclid(86_400);
    let rem = local.rem_euclid(86_400);
    let (year, month, day) = civil_from_days(days);
    let hour = rem / 3600;
    let minute = (rem % 3600) / 60;
    let second = rem % 60;
    format!("{year:04}-{month:02}-{day:02} {hour:02}:{minute:02}:{second:02}")
}

/// Parses an ISO-8601 timestamp such as `2026-04-06T15:27:23.787+08:00`.
///
/// Only the shape the drive actually emits is accepted: a `T`-separated
/// date/time, optional fractional seconds, and a `Z` or `±HH:MM` offset.
/// Returns unix seconds, or `None` when the value is not in that shape.
pub(crate) fn parse_iso8601(value: &str) -> Option<i64> {
    let value = value.trim();
    let bytes = value.as_bytes();
    if bytes.len() < 19 {
        return None;
    }
    let year: i64 = value.get(0..4)?.parse().ok()?;
    if value.as_bytes().get(4)? != &b'-' {
        return None;
    }
    let month: i64 = value.get(5..7)?.parse().ok()?;
    if value.as_bytes().get(7)? != &b'-' {
        return None;
    }
    let day: i64 = value.get(8..10)?.parse().ok()?;
    let sep = *value.as_bytes().get(10)?;
    if sep != b'T' && sep != b' ' {
        return None;
    }
    let hour: i64 = value.get(11..13)?.parse().ok()?;
    if value.as_bytes().get(13)? != &b':' {
        return None;
    }
    let minute: i64 = value.get(14..16)?.parse().ok()?;
    if value.as_bytes().get(16)? != &b':' {
        return None;
    }
    let second: i64 = value.get(17..19)?.parse().ok()?;

    // Optional fractional seconds, ignored (sub-second precision is not kept).
    let mut rest = &value[19..];
    if let Some(fraction) = rest.strip_prefix('.') {
        let digits = fraction.bytes().take_while(u8::is_ascii_digit).count();
        rest = &fraction[digits..];
    }

    let offset_secs = match rest {
        "" | "Z" | "z" => 0,
        other => {
            let sign_byte = *other.as_bytes().first()?;
            let sign = match sign_byte {
                b'+' => 1,
                b'-' => -1,
                _ => return None,
            };
            let digits: String = other.chars().filter(char::is_ascii_digit).collect();
            if digits.len() != 4 {
                return None;
            }
            let oh: i64 = digits.get(0..2)?.parse().ok()?;
            let om: i64 = digits.get(2..4)?.parse().ok()?;
            sign * (oh * 3600 + om * 60)
        }
    };

    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }
    let days = days_from_civil(year, month, day);
    Some(days * 86_400 + hour * 3600 + minute * 60 + second - offset_secs)
}

/// Converts unix seconds to a [`SystemTime`], saturating pre-epoch values.
pub(crate) fn to_system_time(unix_secs: i64) -> SystemTime {
    if unix_secs <= 0 {
        UNIX_EPOCH
    } else {
        UNIX_EPOCH + Duration::from_secs(unix_secs as u64)
    }
}

/// Howard Hinnant's `civil_from_days`: days since the Unix epoch to `(y, m, d)`.
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let year = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let month = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if month <= 2 { year + 1 } else { year }, month, day)
}

/// Howard Hinnant's `days_from_civil`: `(y, m, d)` to days since the epoch.
fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let yoe = year - era * 400;
    let mp = if month > 2 { month - 3 } else { month + 9 };
    let doy = (153 * mp + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_a_known_instant_in_shanghai() {
        // 2026-09-27T11:40:57Z is 19:40:57 in UTC+8, the timestamp the captured
        // signature vector used.
        assert_eq!(format_shanghai(1_790_509_257), "2026-09-27 19:40:57");
    }

    #[test]
    fn parses_the_drive_updated_at_shape() {
        // 2026-04-06T15:27:23.787+08:00.
        assert_eq!(
            parse_iso8601("2026-04-06T15:27:23.787+08:00"),
            Some(1_775_460_443)
        );
        // The same instant expressed in UTC.
        assert_eq!(parse_iso8601("2026-04-06T07:27:23Z"), Some(1_775_460_443));
    }

    #[test]
    fn rejects_values_outside_the_supported_shape() {
        assert_eq!(parse_iso8601("not a date"), None);
        assert_eq!(parse_iso8601("2026-04-06"), None);
        assert_eq!(parse_iso8601("2026-13-06T00:00:00Z"), None);
    }

    #[test]
    fn civil_round_trips() {
        for days in [-719_468, -1, 0, 1, 20_000, 100_000] {
            let (y, m, d) = civil_from_days(days);
            assert_eq!(days_from_civil(y, m as i64, d as i64), days);
        }
    }
}
