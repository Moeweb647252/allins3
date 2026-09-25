//! Server-side listing continuation tokens (spec §4.3).
//!
//! A token is an **opaque server-side handle**, not a signed blob: the record
//! lives in this module, so a client can neither forge one nor read a backend
//! cursor out of it. Each record is bound to the bucket, binding generation,
//! prefix, delimiter, encoding and authorization scope it was issued for, and
//! the use-case layer re-authorizes and re-checks that binding on every page.
//!
//! The lifetime belongs here too. Callers hand over *what* the token is scoped
//! to; when it expires is this module's business, so the spec §4.3 expiry is
//! configured once instead of being recomputed (and forgotten) at each issue
//! site. Binding is not a substitute for authorization: a token never carries a
//! permission.

use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use crate::domain::WriteId;
use crate::sync;

/// What a token was issued for: the scope it may be resumed under.
pub(crate) struct TokenRecord {
    pub(crate) bucket: String,
    pub(crate) prefix: Option<String>,
    pub(crate) delimiter: Option<char>,
    pub(crate) encoding: Option<String>,
    pub(crate) cursor: Option<String>,
    pub(crate) principal: Option<String>,
    pub(crate) generation: u64,
}

/// A validated token: the scope the caller must match before resuming.
pub(crate) struct ResolvedToken {
    pub(crate) bucket: String,
    pub(crate) prefix: Option<String>,
    pub(crate) delimiter: Option<char>,
    pub(crate) encoding: Option<String>,
    pub(crate) cursor: Option<String>,
    pub(crate) principal: Option<String>,
    pub(crate) generation: u64,
}

/// One stored token plus the expiry this module decided for it.
struct StoredToken {
    record: TokenRecord,
    expires: Instant,
}

/// A mint for opaque listing handles, with a deployment-configured lifetime.
pub(crate) struct TokenStore {
    records: Mutex<HashMap<String, StoredToken>>,
    ttl: Duration,
}

static COUNTER: AtomicU64 = AtomicU64::new(0);

impl TokenStore {
    /// A store whose tokens live for `ttl`.
    pub(crate) fn new(ttl: Duration) -> Self {
        Self {
            records: Mutex::new(HashMap::new()),
            ttl,
        }
    }

    /// Issues an opaque handle for `record` and returns it.
    ///
    /// Expired handles are dropped on the way in, so the table cannot grow
    /// without bound even if no client ever resumes a token.
    pub(crate) fn issue(&self, record: TokenRecord) -> String {
        let mut counter = COUNTER.fetch_add(1, Ordering::Relaxed);
        let id = format!(
            "tok-{:016x}-{}",
            {
                counter = counter.wrapping_mul(0x9E37_79B9_7F4A_7C15).wrapping_add(1);
                counter
            },
            WriteId::generate()
        );
        let mut guard = sync::lock(&self.records);
        let now = Instant::now();
        guard.retain(|_, token| token.expires > now);
        guard.insert(
            id.clone(),
            StoredToken {
                record,
                expires: now + self.ttl,
            },
        );
        id
    }

    /// Resolves a handle, dropping it if it has expired.
    pub(crate) fn resolve(&self, token: &str) -> Option<ResolvedToken> {
        let mut guard = sync::lock(&self.records);
        let stored = guard.get(token)?;
        if stored.expires <= Instant::now() {
            guard.remove(token);
            return None;
        }
        Some(ResolvedToken {
            bucket: stored.record.bucket.clone(),
            prefix: stored.record.prefix.clone(),
            delimiter: stored.record.delimiter,
            encoding: stored.record.encoding.clone(),
            cursor: stored.record.cursor.clone(),
            principal: stored.record.principal.clone(),
            generation: stored.record.generation,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record() -> TokenRecord {
        TokenRecord {
            bucket: "b".into(),
            prefix: Some("p/".into()),
            delimiter: Some('/'),
            encoding: None,
            cursor: Some("i:5".into()),
            principal: Some("alice".into()),
            generation: 7,
        }
    }

    #[test]
    fn a_token_round_trips_with_its_scope() {
        let store = TokenStore::new(Duration::from_secs(3600));
        let token = store.issue(record());
        let resolved = store.resolve(&token).expect("token resolves");
        assert_eq!(resolved.bucket, "b");
        assert_eq!(resolved.generation, 7);
        assert_eq!(resolved.cursor.as_deref(), Some("i:5"));
        assert_eq!(resolved.prefix.as_deref(), Some("p/"));
        assert_eq!(resolved.delimiter, Some('/'));
        assert_eq!(resolved.principal.as_deref(), Some("alice"));
        assert!(store.resolve("tok-forged").is_none());
    }

    #[test]
    fn an_expired_token_is_rejected() {
        // The lifetime is the store's business: a zero TTL expires everything
        // without the caller touching a clock.
        let store = TokenStore::new(Duration::ZERO);
        let token = store.issue(record());
        assert!(store.resolve(&token).is_none());
    }

    #[test]
    fn issuing_prunes_expired_handles() {
        let store = TokenStore::new(Duration::ZERO);
        for _ in 0..8 {
            let _ = store.issue(record());
        }
        assert_eq!(
            sync::lock(&store.records).len(),
            1,
            "expired handles must not accumulate"
        );
    }
}
