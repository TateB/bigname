use sqlx::types::time::{OffsetDateTime, UtcOffset};

pub(crate) fn format_timestamp(value: OffsetDateTime) -> String {
    let value = value.to_offset(UtcOffset::UTC);
    let seconds = format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}",
        value.year(),
        value.month() as u8,
        value.day(),
        value.hour(),
        value.minute(),
        value.second()
    );
    if value.nanosecond() == 0 {
        return format!("{seconds}Z");
    }

    let fraction = format!("{:09}", value.nanosecond());
    format!("{seconds}.{}Z", fraction.trim_end_matches('0'))
}
