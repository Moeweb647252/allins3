//! Capabilities, limits and capability planning.
//!
//! A driver declares *native* abilities and semantic properties only. The
//! service layer derives `Native / Emulated / Unsupported` from those
//! declarations. A driver never answers this with a single
//! `supports_s3 = true` flag (spec §3.3).

use thiserror::Error;

/// How a namespace supports byte ranges.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RangeSupport {
    /// Ranges are not supported at all.
    Unsupported,
    /// Ranges are served natively with no full-object scan.
    Native,
    /// The service may emulate ranges with an explicitly enabled, scan-bounded
    /// stable stream trim. The bound is the maximum number of bytes the service
    /// is willing to scan.
    EmulatedWithScanLimit {
        /// Maximum bytes the emulation may scan per request.
        max_scan_bytes: u64,
    },
}

/// How a namespace supports atomic conditional writes and deletes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConditionalSupport {
    /// No atomic condition evaluation.
    Unsupported,
    /// Conditions are evaluated atomically with the mutation.
    Native,
}

/// The consistency a namespace promises.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Consistency {
    /// Reads may not observe recent writes.
    Eventual,
    /// Reads observe writes that happened before the read started.
    ReadAfterWrite,
    /// Reads always observe the latest committed state.
    Strong,
}

/// How durable committed data is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Persistence {
    /// Lost on process exit. Never acceptable for production writes.
    Volatile,
    /// Survives a restart of the user process but not necessarily a machine
    /// failure (for example a local file or database).
    ProcessDurable,
    /// Survives a machine/crash failure as declared by the deployment.
    Durable,
}

/// Resource and size limits declared by a driver.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct DriverLimits {
    /// Maximum object size in bytes.
    pub max_object_size: Option<u64>,
    /// Maximum size of a single non-multipart write.
    pub max_single_write_size: Option<u64>,
    /// Maximum number of multipart parts.
    pub max_multipart_parts: Option<u32>,
    /// Minimum size of any non-final multipart part.
    pub min_part_size: Option<u64>,
    /// Maximum number of user metadata entries.
    pub max_metadata_count: Option<u32>,
    /// Maximum bytes of user metadata.
    pub max_metadata_bytes: Option<u64>,
}

/// Native abilities and semantic properties of one namespace.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamespaceCapabilities {
    /// Ordered `stat` + stable `read` are supported.
    pub read: bool,
    /// Ordered listing is supported.
    pub list: bool,
    /// Hidden staging + atomic commit + atomic delete are supported.
    pub write: bool,
    /// Native multipart is supported.
    pub multipart: bool,
    /// Same-domain native copy is supported.
    pub native_copy: bool,
    /// Durable commit-outcome queries are supported.
    pub recovery: bool,
    /// Range support.
    pub range: RangeSupport,
    /// Atomic conditional support.
    pub conditional: ConditionalSupport,
    /// Consistency promise.
    pub consistency: Consistency,
    /// Persistence promise.
    pub persistence: Persistence,
    /// Declared limits.
    pub limits: DriverLimits,
    /// Whether the adapter guarantees it is the only writer of this namespace.
    /// Once external writes are allowed, metadata/revision/list/conditional
    /// semantics must be re-proven.
    pub exclusive_writer: bool,
}

impl NamespaceCapabilities {
    /// A read-only namespace with no optional features.
    pub fn read_only() -> Self {
        Self {
            read: true,
            list: true,
            write: false,
            multipart: false,
            native_copy: false,
            recovery: false,
            range: RangeSupport::Unsupported,
            conditional: ConditionalSupport::Unsupported,
            consistency: Consistency::Strong,
            persistence: Persistence::Volatile,
            limits: DriverLimits::default(),
            exclusive_writer: true,
        }
    }

    /// A fully-featured in-memory namespace used by test drivers and the
    /// interface preview.
    pub fn full_memory() -> Self {
        Self {
            read: true,
            list: true,
            write: true,
            multipart: false,
            native_copy: true,
            recovery: true,
            range: RangeSupport::Native,
            conditional: ConditionalSupport::Native,
            consistency: Consistency::Strong,
            persistence: Persistence::Volatile,
            limits: DriverLimits {
                max_object_size: None,
                max_single_write_size: Some(5 * 1024 * 1024 * 1024),
                max_multipart_parts: Some(10_000),
                min_part_size: Some(5 * 1024 * 1024),
                max_metadata_count: Some(100),
                max_metadata_bytes: Some(8 * 1024),
            },
            exclusive_writer: true,
        }
    }

    /// Whether this namespace may be used for production writes.
    ///
    /// Volatile storage and non-exclusive writers are both disqualifying.
    pub fn is_production_write_capable(&self) -> bool {
        self.write
            && self.persistence != Persistence::Volatile
            && self.consistency != Consistency::Eventual
            && self.exclusive_writer
    }
}

/// A feature the service may need to provide.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum PlannedFeature {
    /// `stat` / `read`.
    Read,
    /// Ordered listing.
    List,
    /// Staged write + atomic commit + delete.
    Write,
    /// Byte ranges.
    Range,
    /// Atomic conditional write/delete.
    Conditional,
    /// Object copy.
    Copy,
    /// Multipart upload.
    Multipart,
}

impl std::fmt::Display for PlannedFeature {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl PlannedFeature {
    /// A stable label.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Read => "read",
            Self::List => "list",
            Self::Write => "write",
            Self::Range => "range",
            Self::Conditional => "conditional",
            Self::Copy => "copy",
            Self::Multipart => "multipart",
        }
    }
}

/// How the service can provide a feature.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FeatureSupport {
    /// Provided directly by the driver.
    Native,
    /// Provided by a service-layer adapter over declared native primitives.
    Emulated,
    /// Not available in any form.
    Unsupported,
}

/// The resolved plan for a set of features.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CapabilityPlan {
    entries: Vec<(PlannedFeature, FeatureSupport)>,
}

impl CapabilityPlan {
    /// The support level for a feature.
    pub fn support(&self, feature: PlannedFeature) -> FeatureSupport {
        self.entries
            .iter()
            .find(|(f, _)| *f == feature)
            .map(|(_, s)| *s)
            .unwrap_or(FeatureSupport::Unsupported)
    }

    /// Whether every planned feature is provided.
    pub fn all_provided(&self) -> bool {
        self.entries
            .iter()
            .all(|(_, s)| *s != FeatureSupport::Unsupported)
    }

    /// Iterates over the plan entries.
    pub fn iter(&self) -> impl Iterator<Item = (PlannedFeature, FeatureSupport)> + '_ {
        self.entries.iter().copied()
    }
}

/// A planned feature cannot be provided.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum CapabilityError {
    /// The feature is unavailable in native and emulated form.
    #[error("feature `{feature}` is not available (native or emulated)")]
    Unsupported {
        /// The unavailable feature.
        feature: PlannedFeature,
    },
}

/// A bucket profile cannot be satisfied by a namespace's capabilities.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("profile `{profile:?}` cannot be satisfied: missing {missing:?}")]
pub struct ProfileError {
    /// The profile being validated.
    pub profile: Profile,
    /// The requirements that were not met.
    pub missing: Vec<Requirement>,
}

/// One unmet profile requirement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Requirement {
    /// Ordered `stat` + stable `read`.
    Read,
    /// Ordered listing.
    List,
    /// Hidden staging + atomic commit + atomic delete.
    AtomicWrite,
    /// Write-after-write strong consistency.
    StrongConsistency,
    /// Multipart (native or via the generic coordinator).
    Multipart,
}

/// The compatibility tier a bucket is configured for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Profile {
    /// Read-only namespace.
    CoreRead,
    /// Writable namespace with atomic publish semantics.
    CoreWrite,
    /// `CoreWrite` plus multipart.
    Multipart,
}

/// Computes the support level for each requested feature.
pub fn plan(
    requested: &[PlannedFeature],
    caps: &NamespaceCapabilities,
) -> Result<CapabilityPlan, CapabilityError> {
    let mut entries = Vec::with_capacity(requested.len());
    for &feature in requested {
        let support = support_for(feature, caps);
        if support == FeatureSupport::Unsupported {
            return Err(CapabilityError::Unsupported { feature });
        }
        entries.push((feature, support));
    }
    Ok(CapabilityPlan { entries })
}

/// The support level for a single feature.
pub fn support_for(feature: PlannedFeature, caps: &NamespaceCapabilities) -> FeatureSupport {
    match feature {
        PlannedFeature::Read => native_if(caps.read),
        PlannedFeature::List => native_if(caps.list),
        PlannedFeature::Write => native_if(caps.write),
        PlannedFeature::Range => match caps.range {
            RangeSupport::Native => FeatureSupport::Native,
            RangeSupport::EmulatedWithScanLimit { .. } => FeatureSupport::Emulated,
            RangeSupport::Unsupported => FeatureSupport::Unsupported,
        },
        PlannedFeature::Conditional => match caps.conditional {
            ConditionalSupport::Native => FeatureSupport::Native,
            ConditionalSupport::Unsupported => FeatureSupport::Unsupported,
        },
        // A stable read stream feeding a target staged write is a valid bounded
        // fallback; without both primitives copy is unavailable.
        PlannedFeature::Copy => {
            if caps.native_copy {
                FeatureSupport::Native
            } else if caps.read && caps.write {
                FeatureSupport::Emulated
            } else {
                FeatureSupport::Unsupported
            }
        }
        // The generic coordinator publishes through a staged write, so a
        // writable namespace is enough to emulate multipart.
        PlannedFeature::Multipart => {
            if caps.multipart {
                FeatureSupport::Native
            } else if caps.write {
                FeatureSupport::Emulated
            } else {
                FeatureSupport::Unsupported
            }
        }
    }
}

fn native_if(ok: bool) -> FeatureSupport {
    if ok {
        FeatureSupport::Native
    } else {
        FeatureSupport::Unsupported
    }
}

/// Validates that a namespace can satisfy a bucket profile.
///
/// Capability loss is rejected at startup rather than silently degrading a
/// profile's required abilities at runtime.
pub fn validate_profile(
    profile: Profile,
    caps: &NamespaceCapabilities,
) -> Result<(), ProfileError> {
    let mut missing = Vec::new();
    if !caps.read {
        missing.push(Requirement::Read);
    }
    if !caps.list {
        missing.push(Requirement::List);
    }
    if !matches!(profile, Profile::CoreRead) {
        if !caps.write {
            missing.push(Requirement::AtomicWrite);
        }
        if caps.consistency == Consistency::Eventual {
            missing.push(Requirement::StrongConsistency);
        }
    }
    if matches!(profile, Profile::Multipart)
        && !matches!(
            support_for(PlannedFeature::Multipart, caps),
            FeatureSupport::Native | FeatureSupport::Emulated
        )
    {
        missing.push(Requirement::Multipart);
    }

    if missing.is_empty() {
        Ok(())
    } else {
        Err(ProfileError { profile, missing })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn core_read_profile() {
        assert!(validate_profile(Profile::CoreRead, &NamespaceCapabilities::read_only()).is_ok());
    }

    #[test]
    fn core_write_requires_atomic_write_and_strong_consistency() {
        let mut caps = NamespaceCapabilities::full_memory();
        caps.consistency = Consistency::Eventual;
        let err = validate_profile(Profile::CoreWrite, &caps).unwrap_err();
        assert!(err.missing.contains(&Requirement::StrongConsistency));
        assert!(!err.missing.contains(&Requirement::AtomicWrite));
    }

    #[test]
    fn read_only_namespace_cannot_be_core_write() {
        let err =
            validate_profile(Profile::CoreWrite, &NamespaceCapabilities::read_only()).unwrap_err();
        assert!(err.missing.contains(&Requirement::AtomicWrite));
    }

    #[test]
    fn copy_emulated_from_read_and_write() {
        let mut caps = NamespaceCapabilities::full_memory();
        caps.native_copy = false;
        assert_eq!(
            support_for(PlannedFeature::Copy, &caps),
            FeatureSupport::Emulated
        );
    }

    #[test]
    fn copy_unsupported_without_write() {
        let caps = NamespaceCapabilities::read_only();
        assert_eq!(
            support_for(PlannedFeature::Copy, &caps),
            FeatureSupport::Unsupported
        );
    }

    #[test]
    fn range_emulation_and_native() {
        let mut caps = NamespaceCapabilities::full_memory();
        assert_eq!(
            support_for(PlannedFeature::Range, &caps),
            FeatureSupport::Native
        );
        caps.range = RangeSupport::EmulatedWithScanLimit {
            max_scan_bytes: 64 * 1024 * 1024,
        };
        assert_eq!(
            support_for(PlannedFeature::Range, &caps),
            FeatureSupport::Emulated
        );
    }

    #[test]
    fn planning_rejects_unsupported_feature() {
        let caps = NamespaceCapabilities::read_only();
        let err = plan(&[PlannedFeature::Read, PlannedFeature::Multipart], &caps).unwrap_err();
        assert_eq!(
            err,
            CapabilityError::Unsupported {
                feature: PlannedFeature::Multipart
            }
        );
    }

    #[test]
    fn multipart_emulated_from_write() {
        let caps = NamespaceCapabilities::full_memory();
        assert_eq!(
            support_for(PlannedFeature::Multipart, &caps),
            FeatureSupport::Emulated
        );
    }

    #[test]
    fn production_write_requires_durable_exclusive_strong() {
        let mut caps = NamespaceCapabilities::full_memory();
        assert!(
            !caps.is_production_write_capable(),
            "volatile memory driver"
        );

        caps.persistence = Persistence::Durable;
        assert!(caps.is_production_write_capable());

        caps.exclusive_writer = false;
        assert!(!caps.is_production_write_capable());
    }
}
