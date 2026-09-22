//! The read-only bucket catalog.
//!
//! A `BucketCatalog` binds a logical bucket name to a driver instance and a
//! namespace, plus a binding generation (spec §2.1, ADR-0004). It never stores
//! object content or a second copy of object metadata.

use std::collections::HashMap;

use crate::domain::{BucketId, DriverInstanceId, NamespaceId};
use crate::driver::Profile;

/// One immutable bucket binding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BucketBinding {
    /// The logical bucket name.
    pub bucket: BucketId,
    /// The driver instance serving this bucket.
    pub driver: DriverInstanceId,
    /// The isolation space inside that instance.
    pub namespace: NamespaceId,
    /// Stable binding generation; every request and token is pinned to it.
    pub generation: u64,
    /// The compatibility tier this bucket is configured for.
    pub profile: Profile,
    /// Deployment policy: a read-only bucket rejects writes with `AccessDenied`.
    pub readonly: bool,
    /// The region reported for this bucket.
    pub region: String,
}

/// A catalog of configured buckets.
#[derive(Debug, Default, Clone)]
pub struct BucketCatalog {
    buckets: HashMap<String, BucketBinding>,
}

impl BucketCatalog {
    /// Builds a catalog from bindings, rejecting duplicates.
    pub fn from_bindings(bindings: Vec<BucketBinding>) -> Result<Self, CatalogError> {
        let mut buckets = HashMap::with_capacity(bindings.len());
        for binding in bindings {
            let name = binding.bucket.to_string();
            if buckets.insert(name.clone(), binding).is_some() {
                return Err(CatalogError::DuplicateBucket(name));
            }
        }
        Ok(Self { buckets })
    }

    /// Looks up a binding by logical bucket name.
    pub fn get(&self, bucket: &str) -> Option<&BucketBinding> {
        self.buckets.get(bucket)
    }

    /// All bindings in a deterministic order.
    pub fn iter(&self) -> impl Iterator<Item = &BucketBinding> {
        let mut all: Vec<&BucketBinding> = self.buckets.values().collect();
        all.sort_by(|a, b| a.bucket.as_str().cmp(b.bucket.as_str()));
        all.into_iter()
    }

    /// Number of configured buckets.
    pub fn len(&self) -> usize {
        self.buckets.len()
    }

    /// Whether the catalog is empty.
    pub fn is_empty(&self) -> bool {
        self.buckets.is_empty()
    }
}

/// Catalog construction failure.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CatalogError {
    /// Two buckets share a logical name.
    #[error("duplicate bucket name `{0}`")]
    DuplicateBucket(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    fn binding(name: &str, driver: &str, ns: &str) -> BucketBinding {
        BucketBinding {
            bucket: BucketId::parse(name).unwrap(),
            driver: DriverInstanceId::parse(driver).unwrap(),
            namespace: NamespaceId::parse(ns).unwrap(),
            generation: 1,
            profile: Profile::CoreWrite,
            readonly: false,
            region: "us-east-1".into(),
        }
    }

    #[test]
    fn rejects_duplicates() {
        let err =
            BucketCatalog::from_bindings(vec![binding("a", "d1", "n1"), binding("a", "d2", "n2")]);
        assert!(matches!(
            err,
            Err(CatalogError::DuplicateBucket(name)) if name == "a"
        ));
    }

    #[test]
    fn iterates_in_order() {
        let catalog =
            BucketCatalog::from_bindings(vec![binding("b", "d1", "n1"), binding("a", "d1", "n1")])
                .unwrap();
        let names: Vec<&str> = catalog.iter().map(|b| b.bucket.as_str()).collect();
        assert_eq!(names, vec!["a", "b"]);
    }
}
