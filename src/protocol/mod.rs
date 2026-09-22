//! Protocol adapters.
//!
//! The S3 adapter translates HTTP/S3 into object-service calls. The candidate
//! protocol library is confined to this module (`ADR-0007`): the driver and
//! service layers never see it.

pub mod s3;
