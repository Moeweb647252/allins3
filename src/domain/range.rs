//! Byte ranges.
//!
//! Range satisfiability is resolved against an already-checked object length,
//! *after* conditional request checks. Condition failures (`304`/`412`) and
//! range failures (`416`) must never race each other (spec §5.3).

use std::fmt;

use thiserror::Error;

/// A parsed, single byte range as sent by a client.
///
/// S3 does not support multipart/byteranges for a single object `GET`, so a
/// header containing several ranges is rejected at parse time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RangeSpec {
    /// `bytes=start-end`, inclusive at both ends.
    Closed { start: u64, end: u64 },
    /// `bytes=start-`, from `start` to the end.
    Open { start: u64 },
    /// `bytes=-suffix`, the final `suffix` bytes.
    Suffix { suffix: u64 },
}

/// The result of resolving a [`RangeSpec`] against a known object length.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RangeResolution {
    /// The range covers the whole object; return `200` with the full body.
    Full,
    /// A satisfiable partial range; return `206`.
    Partial(ResolvedRange),
    /// No satisfiable range; return `416`.
    Unsatisfiable,
}

/// A validated partial range against a known object length.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResolvedRange {
    start: u64,
    end_inclusive: u64,
    total_len: u64,
}

impl ResolvedRange {
    /// The full-object range `0..len`, for adapters that need a concrete range.
    pub fn full(total_len: u64) -> Self {
        Self {
            start: 0,
            end_inclusive: total_len.saturating_sub(1),
            total_len,
        }
    }

    /// First byte offset (inclusive).
    pub fn start(&self) -> u64 {
        self.start
    }

    /// Last byte offset (inclusive).
    pub fn end_inclusive(&self) -> u64 {
        self.end_inclusive
    }

    /// The object length this range was resolved against.
    pub fn total_len(&self) -> u64 {
        self.total_len
    }

    /// Number of bytes in the range.
    pub fn len(&self) -> u64 {
        if self.total_len == 0 {
            0
        } else {
            self.end_inclusive - self.start + 1
        }
    }

    /// Whether the range is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The `Content-Range` header value for a `206` response.
    pub fn content_range(&self) -> String {
        format!(
            "bytes {}-{}/{}",
            self.start, self.end_inclusive, self.total_len
        )
    }
}

impl fmt::Display for ResolvedRange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.content_range())
    }
}

impl RangeSpec {
    /// Parses a single-range `Range` header value, e.g. `bytes=0-99`.
    ///
    /// Only the `bytes` unit is accepted and multiple ranges are rejected.
    pub fn parse_header(value: &str) -> Result<Self, RangeSpecError> {
        let value = value.trim();
        let rest = value
            .strip_prefix("bytes=")
            .ok_or(RangeSpecError::UnsupportedUnit)?;
        if rest.contains(',') {
            return Err(RangeSpecError::MultipleRangesUnsupported);
        }
        let (first, last) = rest.split_once('-').ok_or(RangeSpecError::Malformed)?;
        let first = first.trim();
        let last = last.trim();

        match (first.is_empty(), last.is_empty()) {
            (true, true) => Err(RangeSpecError::Malformed),
            (true, false) => {
                let suffix = last.parse::<u64>().map_err(|_| RangeSpecError::Malformed)?;
                Ok(Self::Suffix { suffix })
            }
            (false, true) => {
                let start = first
                    .parse::<u64>()
                    .map_err(|_| RangeSpecError::Malformed)?;
                Ok(Self::Open { start })
            }
            (false, false) => {
                let start = first
                    .parse::<u64>()
                    .map_err(|_| RangeSpecError::Malformed)?;
                let end = last.parse::<u64>().map_err(|_| RangeSpecError::Malformed)?;
                if end < start {
                    return Err(RangeSpecError::Inverted);
                }
                Ok(Self::Closed { start, end })
            }
        }
    }

    /// Resolves this spec against a known object length.
    pub fn resolve(self, total_len: u64) -> RangeResolution {
        if total_len == 0 {
            return RangeResolution::Unsatisfiable;
        }
        match self {
            Self::Closed { start, end } => {
                if start >= total_len {
                    return RangeResolution::Unsatisfiable;
                }
                let end_inclusive = end.min(total_len - 1);
                if end_inclusive >= total_len - 1 && start == 0 {
                    RangeResolution::Full
                } else {
                    RangeResolution::Partial(ResolvedRange {
                        start,
                        end_inclusive,
                        total_len,
                    })
                }
            }
            Self::Open { start } => {
                if start >= total_len {
                    return RangeResolution::Unsatisfiable;
                }
                if start == 0 {
                    RangeResolution::Full
                } else {
                    RangeResolution::Partial(ResolvedRange {
                        start,
                        end_inclusive: total_len - 1,
                        total_len,
                    })
                }
            }
            Self::Suffix { suffix } => {
                if suffix == 0 {
                    return RangeResolution::Unsatisfiable;
                }
                if suffix >= total_len {
                    RangeResolution::Full
                } else {
                    RangeResolution::Partial(ResolvedRange {
                        start: total_len - suffix,
                        end_inclusive: total_len - 1,
                        total_len,
                    })
                }
            }
        }
    }
}

/// A `Range` header could not be accepted.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum RangeSpecError {
    /// Only `bytes` ranges are supported.
    #[error("unsupported range unit; only `bytes` is supported")]
    UnsupportedUnit,
    /// Multiple ranges are not supported for a single object `GET`.
    #[error("multiple byte ranges are not supported")]
    MultipleRangesUnsupported,
    /// The range was syntactically malformed.
    #[error("malformed range header")]
    Malformed,
    /// The end offset was before the start offset.
    #[error("range end precedes range start")]
    Inverted,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn closed_range_is_inclusive() {
        let r = RangeSpec::Closed { start: 0, end: 99 }.resolve(1000);
        assert_eq!(
            r,
            RangeResolution::Partial(ResolvedRange {
                start: 0,
                end_inclusive: 99,
                total_len: 1000
            })
        );
        let RangeResolution::Partial(r) = r else {
            unreachable!()
        };
        assert_eq!(r.len(), 100);
        assert_eq!(r.content_range(), "bytes 0-99/1000");
    }

    #[test]
    fn closed_range_clamps_end() {
        let r = RangeSpec::Closed {
            start: 5,
            end: u64::MAX,
        }
        .resolve(10);
        let RangeResolution::Partial(r) = r else {
            panic!("expected partial");
        };
        assert_eq!(r.end_inclusive(), 9);
        assert_eq!(r.len(), 5);
    }

    #[test]
    fn whole_object_range_is_full() {
        assert_eq!(
            RangeSpec::Closed { start: 0, end: 9 }.resolve(10),
            RangeResolution::Full
        );
        assert_eq!(
            RangeSpec::Open { start: 0 }.resolve(10),
            RangeResolution::Full
        );
        assert_eq!(
            RangeSpec::Suffix { suffix: 999 }.resolve(10),
            RangeResolution::Full
        );
    }

    #[test]
    fn open_and_suffix() {
        let RangeResolution::Partial(r) = RangeSpec::Open { start: 4 }.resolve(10) else {
            panic!()
        };
        assert_eq!((r.start(), r.end_inclusive()), (4, 9));

        let RangeResolution::Partial(r) = RangeSpec::Suffix { suffix: 3 }.resolve(10) else {
            panic!()
        };
        assert_eq!((r.start(), r.end_inclusive()), (7, 9));
    }

    #[test]
    fn unsatisfiable_cases() {
        assert_eq!(
            RangeSpec::Closed { start: 10, end: 20 }.resolve(10),
            RangeResolution::Unsatisfiable
        );
        assert_eq!(
            RangeSpec::Open { start: 10 }.resolve(10),
            RangeResolution::Unsatisfiable
        );
        assert_eq!(
            RangeSpec::Suffix { suffix: 0 }.resolve(10),
            RangeResolution::Unsatisfiable
        );
        assert_eq!(
            RangeSpec::Closed { start: 0, end: 0 }.resolve(0),
            RangeResolution::Unsatisfiable
        );
    }

    #[test]
    fn parse_header_forms() {
        assert_eq!(
            RangeSpec::parse_header("bytes=0-99").unwrap(),
            RangeSpec::Closed { start: 0, end: 99 }
        );
        assert_eq!(
            RangeSpec::parse_header("bytes=5-").unwrap(),
            RangeSpec::Open { start: 5 }
        );
        assert_eq!(
            RangeSpec::parse_header("bytes=-5").unwrap(),
            RangeSpec::Suffix { suffix: 5 }
        );
    }

    #[test]
    fn parse_header_rejects_bad_forms() {
        assert_eq!(
            RangeSpec::parse_header("items=0-1"),
            Err(RangeSpecError::UnsupportedUnit)
        );
        assert_eq!(
            RangeSpec::parse_header("bytes=0-1,5-6"),
            Err(RangeSpecError::MultipleRangesUnsupported)
        );
        assert_eq!(
            RangeSpec::parse_header("bytes=9-1"),
            Err(RangeSpecError::Inverted)
        );
        assert_eq!(
            RangeSpec::parse_header("bytes=-"),
            Err(RangeSpecError::Malformed)
        );
    }
}
