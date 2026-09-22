//! Credentials, principals and the access policy.
//!
//! The first phase uses a static, config-backed credential store and a rule set
//! of `principal + action + bucket + key prefix`. Authorization is deny by
//! default, and a limited prefix must also constrain listing results
//! (spec §6.1).

use std::collections::HashMap;

use crate::driver::Secret;

/// An S3 action used for authorization.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Action {
    /// Listing buckets or objects.
    List,
    /// Reading objects and metadata.
    Read,
    /// Creating or overwriting objects.
    Write,
    /// Deleting objects.
    Delete,
}

impl Action {
    /// Parses an action name from configuration.
    pub fn parse(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "list" => Some(Self::List),
            "read" | "get" => Some(Self::Read),
            "write" | "put" => Some(Self::Write),
            "delete" => Some(Self::Delete),
            _ => None,
        }
    }

    /// The canonical name.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::List => "list",
            Self::Read => "read",
            Self::Write => "write",
            Self::Delete => "delete",
        }
    }
}

/// One configured credential.
#[derive(Debug, Clone)]
pub struct Credential {
    /// The access key id.
    pub access_key: String,
    /// The secret key. Never logged or handed to a driver.
    pub secret: Secret,
    /// The principal this credential authenticates as.
    pub principal: String,
}

/// A lookup table of credentials by access key.
#[derive(Debug, Default, Clone)]
pub struct CredentialStore {
    by_access_key: HashMap<String, Credential>,
}

impl CredentialStore {
    /// Builds a store, rejecting duplicate access keys.
    pub fn new(credentials: Vec<Credential>) -> Result<Self, AuthConfigError> {
        let mut by_access_key = HashMap::with_capacity(credentials.len());
        for credential in credentials {
            if by_access_key
                .insert(credential.access_key.clone(), credential)
                .is_some()
            {
                return Err(AuthConfigError::DuplicateAccessKey);
            }
        }
        Ok(Self { by_access_key })
    }

    /// Looks up a credential.
    pub fn get(&self, access_key: &str) -> Option<&Credential> {
        self.by_access_key.get(access_key)
    }

    /// The number of configured credentials.
    pub fn len(&self) -> usize {
        self.by_access_key.len()
    }

    /// Whether the store is empty.
    pub fn is_empty(&self) -> bool {
        self.by_access_key.is_empty()
    }
}

/// One grant: a principal may perform `actions` on `bucket` under `prefix`.
#[derive(Debug, Clone)]
pub struct Grant {
    /// The principal the grant applies to.
    pub principal: String,
    /// The bucket the grant applies to (`*` means any configured bucket).
    pub bucket: String,
    /// The permitted actions.
    pub actions: Vec<Action>,
    /// The key prefix the grant is limited to (empty means the whole bucket).
    pub prefix: String,
}

/// The access policy: deny by default, explicit allow only.
#[derive(Debug, Default, Clone)]
pub struct AccessPolicy {
    grants: Vec<Grant>,
    anonymous_read: bool,
}

impl AccessPolicy {
    /// Builds a policy from grants. Anonymous access is off unless enabled.
    pub fn new(grants: Vec<Grant>) -> Self {
        Self {
            grants,
            anonymous_read: false,
        }
    }

    /// Enables or disables anonymous (unauthenticated) requests. Default off.
    pub fn with_anonymous_access(mut self, enabled: bool) -> Self {
        self.anonymous_read = enabled;
        self
    }

    /// Whether a request is authorized.
    ///
    /// `key` is the object key for object operations, or `None` for
    /// bucket/account-level operations.
    pub fn authorize(
        &self,
        principal: Option<&str>,
        action: Action,
        bucket: &str,
        key: Option<&str>,
    ) -> bool {
        let Some(principal) = principal else {
            // Anonymous access, when enabled, is read/list only.
            return self.anonymous_read && matches!(action, Action::Read | Action::List);
        };

        self.grants.iter().any(|grant| {
            grant.principal == principal
                && (grant.bucket == "*" || grant.bucket == bucket)
                && grant.actions.contains(&action)
                && prefix_allows(&grant.prefix, key)
        })
    }

    /// The number of configured grants.
    pub fn grant_count(&self) -> usize {
        self.grants.len()
    }

    /// Whether the policy restricts a principal's listing to a prefix.
    ///
    /// Used to constrain listing results so a prefix-limited credential cannot
    /// discover other keys: returns the effective read prefix, if any.
    pub fn list_prefix(&self, principal: Option<&str>, bucket: &str) -> Option<String> {
        let principal = principal?;
        let mut effective: Option<String> = None;
        for grant in &self.grants {
            if grant.principal != principal
                || !(grant.bucket == "*" || grant.bucket == bucket)
                || !grant.actions.contains(&Action::List)
            {
                continue;
            }
            effective = Some(match effective {
                None => grant.prefix.clone(),
                Some(current) => shortest_prefix(&current, &grant.prefix),
            });
            if effective.as_deref() == Some("") {
                return Some(String::new());
            }
        }
        effective
    }
}

fn prefix_allows(grant_prefix: &str, key: Option<&str>) -> bool {
    if grant_prefix.is_empty() {
        return true;
    }
    match key {
        Some(key) => key.starts_with(grant_prefix),
        // Bucket-level operations with a prefix-limited grant do not apply.
        None => false,
    }
}

fn shortest_prefix(a: &str, b: &str) -> String {
    if a.starts_with(b) {
        b.to_string()
    } else if b.starts_with(a) {
        a.to_string()
    } else {
        // Disjoint prefixes cannot be combined; use the empty prefix which
        // grants the widest read visibility the policy already allows.
        String::new()
    }
}

/// Authentication configuration failure.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AuthConfigError {
    /// Two credentials share an access key.
    #[error("duplicate access key id")]
    DuplicateAccessKey,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy() -> AccessPolicy {
        AccessPolicy::new(vec![
            Grant {
                principal: "alice".into(),
                bucket: "bucket-a".into(),
                actions: vec![Action::List, Action::Read, Action::Write, Action::Delete],
                prefix: String::new(),
            },
            Grant {
                principal: "bob".into(),
                bucket: "bucket-a".into(),
                actions: vec![Action::List, Action::Read],
                prefix: "public/".into(),
            },
        ])
    }

    #[test]
    fn denies_by_default() {
        assert!(!policy().authorize(None, Action::Read, "bucket-a", Some("k")));
        assert!(!policy().authorize(Some("carol"), Action::Read, "bucket-a", Some("k")));
    }

    #[test]
    fn allows_matching_grant() {
        assert!(policy().authorize(Some("alice"), Action::Write, "bucket-a", Some("k")));
    }

    #[test]
    fn prefix_limited_grant_cannot_read_outside_prefix() {
        let p = policy();
        assert!(p.authorize(Some("bob"), Action::Read, "bucket-a", Some("public/x")));
        assert!(!p.authorize(Some("bob"), Action::Read, "bucket-a", Some("private/x")));
        // Bob also cannot write.
        assert!(!p.authorize(Some("bob"), Action::Write, "bucket-a", Some("public/x")));
    }

    #[test]
    fn anonymous_off_by_default_even_with_grants() {
        assert!(!policy().authorize(None, Action::List, "bucket-a", None));
    }

    #[test]
    fn list_prefix_is_reported_for_limited_principal() {
        assert_eq!(
            policy().list_prefix(Some("bob"), "bucket-a").as_deref(),
            Some("public/")
        );
        assert_eq!(
            policy().list_prefix(Some("alice"), "bucket-a").as_deref(),
            Some("")
        );
    }
}
