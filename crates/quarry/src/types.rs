//! The type rules of DESIGN.md, "Types": which Arrow types expressions take, and the common type of two.

use arrow::datatypes::DataType;

/// The integer types expressions take. `UInt64` isn't one: no signed type holds all of it.
pub fn is_integer(t: &DataType) -> bool {
    matches!(
        t,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
    )
}

pub fn is_float(t: &DataType) -> bool {
    matches!(t, DataType::Float32 | DataType::Float64)
}

pub fn is_numeric(t: &DataType) -> bool {
    is_integer(t) || is_float(t)
}

pub fn is_string(t: &DataType) -> bool {
    matches!(t, DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View)
}

/// Whether expressions are defined over the type (any type can still be scanned and passed through).
pub fn is_supported(t: &DataType) -> bool {
    is_numeric(t)
        || is_string(t)
        || matches!(t, DataType::Boolean | DataType::Date32 | DataType::Null)
}

/// The common type of two types (rules 1–4 of DESIGN.md; rule 5, a date and a string literal, needs to see the
/// literal and is the binder's). `None` when there is none.
pub fn common_type(a: &DataType, b: &DataType) -> Option<DataType> {
    if a == b {
        return Some(a.clone());
    }
    match (a, b) {
        (DataType::Null, t) | (t, DataType::Null) => Some(t.clone()),
        (x, y) if is_integer(x) && is_integer(y) => Some(DataType::Int64),
        (x, y) if is_numeric(x) && is_numeric(y) => Some(DataType::Float64),
        (x, y) if is_string(x) && is_string(y) => Some(DataType::Utf8),
        _ => None,
    }
}

/// The type arithmetic on numbers produces: integers widen to `Int64`, floats to `Float64`.
pub fn widen_numeric(t: &DataType) -> DataType {
    if is_float(t) {
        DataType::Float64
    } else {
        DataType::Int64
    }
}

/// Parses `YYYY-MM-DD` (exactly: four digits, two, two) into days since 1970-01-01, if it is a real date.
pub fn parse_date32(s: &str) -> Option<i32> {
    let b = s.as_bytes();
    if b.len() != 10 || b[4] != b'-' || b[7] != b'-' {
        return None;
    }
    let digits = |r: std::ops::Range<usize>| -> Option<i64> {
        b[r].iter().try_fold(0i64, |acc, &c| {
            c.is_ascii_digit().then(|| acc * 10 + i64::from(c - b'0'))
        })
    };
    let (year, month, day) = (digits(0..4)?, digits(5..7)?, digits(8..10)?);
    if !(1..=12).contains(&month) || day < 1 || day > days_in_month(year, month) {
        return None;
    }
    i32::try_from(days_from_civil(year, month, day)).ok()
}

/// Formats days since 1970-01-01 as `YYYY-MM-DD` (years outside 0..=9999 print with more or fewer digits).
pub fn format_date32(days: i32) -> String {
    let (year, month, day) = civil_from_days(i64::from(days));
    format!("{year:04}-{month:02}-{day:02}")
}

fn is_leap(year: i64) -> bool {
    (year % 4 == 0 && year % 100 != 0) || year % 400 == 0
}

fn days_in_month(year: i64, month: i64) -> i64 {
    match month {
        2 if is_leap(year) => 29,
        2 => 28,
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    }
}

// Howard Hinnant's days_from_civil and civil_from_days (proleptic Gregorian calendar), from
// http://howardhinnant.github.io/date_algorithms.html.
fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let y = if month <= 2 { year - 1 } else { year };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = (month + 9) % 12;
    let doy = (153 * mp + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

fn civil_from_days(days: i64) -> (i64, i64, i64) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    (year, month, day)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn common_types_follow_the_rules() {
        use DataType::*;
        assert_eq!(common_type(&Int32, &Int32), Some(Int32));
        assert_eq!(common_type(&Null, &Utf8), Some(Utf8));
        assert_eq!(common_type(&Int32, &Null), Some(Int32));
        assert_eq!(common_type(&Int32, &UInt8), Some(Int64));
        assert_eq!(common_type(&Int64, &Float32), Some(Float64));
        assert_eq!(common_type(&Float32, &Float64), Some(Float64));
        assert_eq!(common_type(&Utf8, &LargeUtf8), Some(Utf8));
        assert_eq!(common_type(&Utf8, &Int64), None);
        assert_eq!(common_type(&Date32, &Utf8), None); // rule 5 is the binder's, for literals only
        assert_eq!(common_type(&Boolean, &Int64), None);
        assert_eq!(common_type(&UInt64, &Int64), None);
    }

    #[test]
    fn dates_parse_strictly() {
        assert_eq!(parse_date32("1970-01-01"), Some(0));
        assert_eq!(parse_date32("1969-12-31"), Some(-1));
        assert_eq!(parse_date32("1998-09-02"), Some(10_471));
        assert_eq!(parse_date32("2000-02-29"), Some(11_016));
        assert_eq!(parse_date32("1900-02-29"), None); // not a leap year
        assert_eq!(parse_date32("2024-04-31"), None);
        assert_eq!(parse_date32("2024-13-01"), None);
        assert_eq!(parse_date32("2024-00-10"), None);
        assert_eq!(parse_date32("2024-1-01"), None);
        assert_eq!(parse_date32("2024/01/01"), None);
        assert_eq!(parse_date32("+024-01-01"), None);
        assert_eq!(parse_date32("２０２４-01-01"), None); // non-ASCII digits
        assert_eq!(parse_date32(""), None);
    }

    #[test]
    fn dates_round_trip_over_four_centuries_and_more() {
        // Every day from 1600-01-01 to 2400-12-31: format, parse back, and check the calendar steps by one day.
        let start = parse_date32("1600-01-01").unwrap();
        let end = parse_date32("2400-12-31").unwrap();
        let mut previous = format_date32(start - 1);
        for days in start..=end {
            let text = format_date32(days);
            assert_eq!(parse_date32(&text), Some(days), "{text}");
            assert!(text > previous, "{text} after {previous}");
            previous = text;
        }
        assert_eq!(end - start + 1, 801 * 365 + 195); // 801 years, 195 of them leap years
    }

    #[test]
    fn dates_agree_with_arrow() {
        use arrow::array::{Array, Date32Array, StringArray};
        use arrow::compute::cast;
        let texts = [
            "1970-01-01",
            "1992-01-01",
            "1998-12-01",
            "2000-02-29",
            "1900-03-01",
            "0001-01-01",
            "9999-12-31",
        ];
        let array = StringArray::from(texts.to_vec());
        let dates = cast(&array, &DataType::Date32).unwrap();
        let dates = dates.as_any().downcast_ref::<Date32Array>().unwrap();
        for (i, text) in texts.iter().enumerate() {
            assert_eq!(parse_date32(text), Some(dates.value(i)), "{text}");
            assert_eq!(format_date32(dates.value(i)), *text);
        }
    }
}
