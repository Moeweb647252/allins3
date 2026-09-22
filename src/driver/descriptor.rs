//! Driver identity and health.

use std::fmt;

use crate::domain::DriverInstanceId;

/// The registered name of a driver *kind* (an adapter implementation).
///
/// One kind may be instantiated several times; instances are identified by
/// [`DriverInstanceId`], not by the kind.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct DriverKind(String);

impl DriverKind {
    /// Wraps a kind name.
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Borrows the kind name.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for DriverKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Static description of a driver instance.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DriverDescriptor {
    /// The registered kind.
    pub kind: DriverKind,
    /// The configured instance id.
    pub instance_id: DriverInstanceId,
    /// Human-readable name for logs and the capability report.
    pub display_name: String,
    /// Adapter version, when the adapter exposes one.
    pub version: Option<String>,
}

/// The health of a driver instance.
///
/// A health probe must not modify user objects, and an availability failure is
/// not a capability change: capabilities do not vary with health.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DriverHealth {
    /// Serving normally.
    Healthy,
    /// Serving, but with a recorded degradation.
    Degraded {
        /// Short, safe reason.
        reason: String,
    },
    /// Not serving.
    Unhealthy {
        /// Short, safe reason.
        reason: String,
    },
}

impl DriverHealth {
    /// Whether the instance can currently serve traffic.
    pub fn is_serving(&self) -> bool {
        !matches!(self, Self::Unhealthy { .. })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn health_serving() {
        assert!(DriverHealth::Healthy.is_serving());
        assert!(
            DriverHealth::Degraded {
                reason: "slow".into()
            }
            .is_serving()
        );
        assert!(
            !DriverHealth::Unhealthy {
                reason: "down".into()
            }
            .is_serving()
        );
    }
}
