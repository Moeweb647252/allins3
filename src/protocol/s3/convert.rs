//! Conversions between the S3 DTOs and the service's domain types.
//!
//! Every value that crosses the adapter boundary is converted here: timestamps,
//! content properties, user metadata, preconditions, ranges and listing
//! encodings. Keeping them together means the adapter body reads as request
//! handling rather than as header translation, and a rejected form (an
//! unparseable `Expires`, over-limit metadata, a key XML 1.0 cannot represent)
//! is rejected in exactly one place.

use s3s::dto::{Metadata as S3Metadata, Timestamp};

use crate::domain::{ContentProperties, EntityTag, Preconditions, RangeSpec, UserMetadata};
use crate::service::{ReadConditions, ServiceError};

use super::checksum::ChecksumHeaders;
use super::map_error;

pub(super) fn to_timestamp(time: std::time::SystemTime) -> Timestamp {
    Timestamp::from(time)
}

pub(super) fn to_system_time(ts: &Timestamp) -> std::time::SystemTime {
    std::time::SystemTime::from(time::OffsetDateTime::from(ts.clone()))
}

pub(super) fn to_content_properties(
    input: &S3ContentInput,
) -> Result<ContentProperties, ServiceError> {
    // `Expires` is an object property that must survive the round trip (spec
    // §5.4). A value the service cannot store is rejected rather than dropped:
    // silently publishing an object without it would change what the client
    // asked for.
    let expires = match input.expires.as_deref() {
        None => None,
        Some(raw) => Some(parse_http_date(raw).ok_or_else(|| {
            ServiceError::InvalidArgument(
                "`Expires` must be an HTTP date in IMF-fixdate form \
                 (e.g. `Wed, 21 Oct 2015 07:28:00 GMT`)"
                    .into(),
            )
        })?),
    };
    Ok(ContentProperties {
        content_type: input.content_type.clone(),
        content_encoding: input.content_encoding.clone(),
        content_language: input.content_language.clone(),
        content_disposition: input.content_disposition.clone(),
        cache_control: input.cache_control.clone(),
        expires,
    })
}

/// The subset of content headers shared by `PutObject`/`CopyObject`.
pub(super) struct S3ContentInput {
    pub(super) content_type: Option<String>,
    pub(super) content_encoding: Option<String>,
    pub(super) content_language: Option<String>,
    pub(super) content_disposition: Option<String>,
    pub(super) cache_control: Option<String>,
    pub(super) expires: Option<String>,
}

/// Formats a `SystemTime` as an RFC 7231 IMF-fixdate.
///
/// Only the components are used, so this needs no formatting feature: the shape
/// is fixed (`Sun, 06 Nov 1994 08:49:37 GMT`).
pub(super) fn format_http_date(time: std::time::SystemTime) -> String {
    use time::{Month, OffsetDateTime, Weekday};
    let value = OffsetDateTime::from(time);
    let weekday = match value.weekday() {
        Weekday::Monday => "Mon",
        Weekday::Tuesday => "Tue",
        Weekday::Wednesday => "Wed",
        Weekday::Thursday => "Thu",
        Weekday::Friday => "Fri",
        Weekday::Saturday => "Sat",
        Weekday::Sunday => "Sun",
    };
    let month = match value.month() {
        Month::January => "Jan",
        Month::February => "Feb",
        Month::March => "Mar",
        Month::April => "Apr",
        Month::May => "May",
        Month::June => "Jun",
        Month::July => "Jul",
        Month::August => "Aug",
        Month::September => "Sep",
        Month::October => "Oct",
        Month::November => "Nov",
        Month::December => "Dec",
    };
    format!(
        "{}, {:02} {} {:04} {:02}:{:02}:{:02} GMT",
        weekday,
        value.day(),
        month,
        value.year(),
        value.hour(),
        value.minute(),
        value.second(),
    )
}

/// Parses the IMF-fixdate form of an HTTP date, the only form S3 clients send.
pub(super) fn parse_http_date(value: &str) -> Option<std::time::SystemTime> {
    let (weekday, rest) = value.trim().split_once(',')?;
    if !matches!(
        weekday,
        "Mon" | "Tue" | "Wed" | "Thu" | "Fri" | "Sat" | "Sun"
    ) {
        return None;
    }
    let mut fields = rest.trim_start().split(' ');
    let day: u8 = fields.next()?.parse().ok()?;
    let month = match fields.next()? {
        "Jan" => 1,
        "Feb" => 2,
        "Mar" => 3,
        "Apr" => 4,
        "May" => 5,
        "Jun" => 6,
        "Jul" => 7,
        "Aug" => 8,
        "Sep" => 9,
        "Oct" => 10,
        "Nov" => 11,
        "Dec" => 12,
        _ => return None,
    };
    let year: i32 = fields.next()?.parse().ok()?;
    let mut clock = fields.next()?.split(':');
    let hour: u8 = clock.next()?.parse().ok()?;
    let minute: u8 = clock.next()?.parse().ok()?;
    let second: u8 = clock.next()?.parse().ok()?;
    if clock.next().is_some() || fields.next()? != "GMT" || fields.next().is_some() {
        return None;
    }
    use time::{Date, Month, PrimitiveDateTime, Time};
    let date = Date::from_calendar_date(year, Month::try_from(month).ok()?, day).ok()?;
    let time = Time::from_hms(hour, minute, second).ok()?;
    Some(std::time::SystemTime::from(
        PrimitiveDateTime::new(date, time).assume_utc(),
    ))
}

/// Whether `value` can appear as XML 1.0 character data.
///
/// XML 1.0 excludes most C0 control characters. A key containing one cannot be
/// serialised without producing a document XML parsers reject, so such a
/// listing asks the client for `encoding-type=url` instead (spec §4.2).
pub(super) fn is_xml_1_0_text(value: &str) -> bool {
    value.chars().all(|c| {
        matches!(c, '\u{9}' | '\u{A}' | '\u{D}')
            || ('\u{20}'..='\u{D7FF}').contains(&c)
            || ('\u{E000}'..='\u{FFFD}').contains(&c)
            || ('\u{10000}'..='\u{10FFFF}').contains(&c)
    })
}

/// Rejects a listing whose result could not be represented in XML 1.0.
///
/// The alternative — emitting a broken document or rewriting the key — is
/// explicitly refused by spec §4.2.
pub(super) fn require_encodable(
    encoding: Option<&str>,
    values: impl IntoIterator<Item = String>,
    request_id: &str,
) -> s3s::S3Result<()> {
    if encoding.is_some() {
        return Ok(());
    }
    if values.into_iter().any(|value| !is_xml_1_0_text(&value)) {
        return Err(map_error(
            ServiceError::InvalidRequest(
                "the listing contains characters that XML 1.0 cannot represent; \
                 retry with `encoding-type=url`"
                    .into(),
            ),
            request_id,
        ));
    }
    Ok(())
}

/// Converts request metadata, rejecting anything over the metadata limits.
///
/// Silently dropping an over-limit entry would publish an object whose metadata
/// differs from what the client sent.
pub(super) fn to_user_metadata(
    metadata: &Option<S3Metadata>,
    request_id: &str,
) -> Result<UserMetadata, s3s::S3Error> {
    let mut out = UserMetadata::new();
    if let Some(map) = metadata {
        for (k, v) in map {
            out.insert(k.clone(), v.clone()).map_err(|e| {
                map_error(
                    ServiceError::InvalidRequest(format!("invalid user metadata: {e}")),
                    request_id,
                )
            })?;
        }
    }
    Ok(out)
}

pub(super) fn apply_metadata(meta: &crate::domain::ObjectMetadata, out: &mut CommonObjectOutput) {
    out.content_length = Some(meta.size as i64);
    out.e_tag = meta
        .etag
        .as_ref()
        .map(|t| s3s::dto::ETag::Strong(t.as_str().to_string()));
    out.last_modified = Some(to_timestamp(meta.last_modified));
    let mut metadata = std::collections::HashMap::new();
    for (k, v) in meta.user_metadata.iter() {
        metadata.insert(k.to_string(), v.to_string());
    }
    if !metadata.is_empty() {
        out.metadata = Some(metadata);
    }
    out.content_type = meta.content.content_type.clone();
    out.content_encoding = meta.content.content_encoding.clone();
    out.content_language = meta.content.content_language.clone();
    out.content_disposition = meta.content.content_disposition.clone();
    out.cache_control = meta.content.cache_control.clone();
    out.expires = meta.content.expires.map(format_http_date);

    out.checksums = ChecksumHeaders::from_checksums(&meta.checksums);
}

/// The fields shared by the object responses, filled in from object metadata.
pub(super) struct CommonObjectOutput {
    pub(super) content_length: Option<i64>,
    pub(super) e_tag: Option<s3s::dto::ETag>,
    pub(super) last_modified: Option<Timestamp>,
    pub(super) metadata: Option<S3Metadata>,
    pub(super) content_type: Option<String>,
    pub(super) content_encoding: Option<String>,
    pub(super) content_language: Option<String>,
    pub(super) content_disposition: Option<String>,
    pub(super) cache_control: Option<String>,
    pub(super) expires: Option<String>,
    pub(super) checksums: ChecksumHeaders,
}

impl CommonObjectOutput {
    pub(super) fn new() -> Self {
        Self {
            content_length: None,
            e_tag: None,
            last_modified: None,
            metadata: None,
            content_type: None,
            content_encoding: None,
            content_language: None,
            content_disposition: None,
            cache_control: None,
            expires: None,
            checksums: ChecksumHeaders::default(),
        }
    }
}

pub(super) fn read_conditions(
    if_match: &Option<s3s::dto::ETagCondition>,
    if_none_match: &Option<s3s::dto::ETagCondition>,
    if_modified_since: &Option<Timestamp>,
    if_unmodified_since: &Option<Timestamp>,
) -> ReadConditions {
    ReadConditions {
        if_match: single_tag(if_match),
        if_none_match: single_tag(if_none_match),
        if_modified_since: if_modified_since.as_ref().map(to_system_time),
        if_unmodified_since: if_unmodified_since.as_ref().map(to_system_time),
    }
}

pub(super) fn single_tag(condition: &Option<s3s::dto::ETagCondition>) -> Option<Vec<EntityTag>> {
    match condition {
        None | Some(s3s::dto::ETagCondition::Any) => None,
        Some(s3s::dto::ETagCondition::ETag(tag)) => EntityTag::new(tag.value().to_string())
            .ok()
            .map(|t| vec![t]),
    }
}

/// Converts S3 write preconditions, rejecting forms the first phase does not
/// implement atomically rather than silently ignoring them.
pub(super) fn write_preconditions(
    if_match: &Option<s3s::dto::ETagCondition>,
    if_none_match: &Option<s3s::dto::ETagCondition>,
) -> Result<Preconditions, ServiceError> {
    let mut preconditions = Preconditions::none();
    match if_match {
        None => {}
        Some(s3s::dto::ETagCondition::ETag(tag)) => {
            let entity = EntityTag::new(tag.value().to_string())
                .map_err(|e| ServiceError::InvalidArgument(e.to_string()))?;
            preconditions.if_match = Some(entity);
        }
        Some(s3s::dto::ETagCondition::Any) => {
            return Err(ServiceError::NotImplemented(
                "`If-Match: *` is not supported for this operation".into(),
            ));
        }
    }
    match if_none_match {
        None => {}
        Some(s3s::dto::ETagCondition::Any) => preconditions.if_none_match = true,
        Some(s3s::dto::ETagCondition::ETag(_)) => {
            return Err(ServiceError::NotImplemented(
                "`If-None-Match` with a specific ETag is not supported for this operation".into(),
            ));
        }
    }
    Ok(preconditions)
}

pub(super) fn to_range(range: &Option<s3s::dto::Range>) -> Option<RangeSpec> {
    range.map(|r| match r {
        s3s::dto::Range::Int { first, last } => match last {
            Some(last) => RangeSpec::Closed {
                start: first,
                end: last,
            },
            None => RangeSpec::Open { start: first },
        },
        s3s::dto::Range::Suffix { length } => RangeSpec::Suffix { suffix: length },
    })
}

/// Encodes a listing value for the response's requested `encoding-type`.
pub(super) fn encode_value(value: &str, encoding: &Option<String>) -> String {
    if encoding.as_deref() == Some("url") {
        url_encode_key(value)
    } else {
        value.to_string()
    }
}

/// Percent-encodes a key for `encoding-type=url`.
pub(super) fn url_encode_key(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(byte as char)
            }
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}
