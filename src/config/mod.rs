//! Configuration schema, parsing and validation.
//!
//! Everything the service needs is validated *before* startup: duplicate ids,
//! unknown driver kinds, missing secrets, invalid bucket names and capability
//! mismatches all fail before the listener opens (spec §7.1). Configuration
//! output is always redacted; secrets are never printed.

use std::collections::HashSet;
use std::path::Path;

use serde::Deserialize;

use crate::auth::{AccessPolicy, Action, AuthConfigError, Credential, CredentialStore, Grant};
use crate::catalog::{BucketBinding, BucketCatalog, CatalogError};
use crate::domain::{BucketId, DriverError, DriverInstanceId, NamespaceId};
use crate::driver::{Profile, Secret, SecretResolver};

/// The raw, unvalidated configuration file.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawConfig {
    /// Server settings.
    pub server: RawServer,
    /// Driver instances.
    #[serde(default)]
    pub drivers: Vec<RawDriver>,
    /// Bucket bindings.
    #[serde(default)]
    pub buckets: Vec<RawBucket>,
    /// Credentials.
    #[serde(default)]
    pub credentials: Vec<RawCredential>,
    /// Access grants.
    #[serde(default)]
    pub grants: Vec<RawGrant>,
    /// Resource limits.
    #[serde(default)]
    pub limits: RawLimits,
}

/// `[server]` section.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawServer {
    /// Listen address, `host:port`.
    pub listen: String,
    /// Region name.
    #[serde(default = "default_region")]
    pub region: String,
    /// `path` or `virtual`.
    #[serde(default = "default_addressing")]
    pub addressing: String,
    /// Development mode; required to register test drivers for durable writes.
    #[serde(default)]
    pub development: bool,
    /// Whether to expose unauthenticated liveness/readiness endpoints.
    #[serde(default = "default_true")]
    pub health_endpoints: bool,
    /// Directory holding the multipart coordinator's durable state.
    ///
    /// Required in a production deployment with at least one writable bucket:
    /// without it, a part the service acknowledged could not survive a restart
    /// (`ADR-0014`, spec §8.1). A development deployment may omit it.
    #[serde(default)]
    pub state_dir: Option<String>,
}

/// One `[[drivers]]` entry.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawDriver {
    /// Instance id, unique across the file.
    pub id: String,
    /// Registered driver kind.
    pub kind: String,
    /// Driver-private options (opaque to the core).
    #[serde(default)]
    pub options: toml::Table,
    /// Secret references this instance may resolve, e.g. `env:NAME`.
    ///
    /// The core only proves each reference resolves before startup; which
    /// reference maps to which private option is the driver's decision
    /// (spec §3.2.5). Secret values are never logged or echoed.
    #[serde(default)]
    pub secret_refs: Vec<String>,
}

/// One `[[buckets]]` entry.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawBucket {
    /// Logical bucket name.
    pub name: String,
    /// Driver instance id to bind to.
    pub driver: String,
    /// Namespace inside that instance.
    pub namespace: String,
    /// Compatibility tier: `core-read`, `core-write`, `multipart`.
    #[serde(default = "default_profile")]
    pub profile: String,
    /// Read-only deployment policy.
    #[serde(default)]
    pub readonly: bool,
    /// Explicit binding generation; derived stably if omitted.
    #[serde(default)]
    pub generation: Option<u64>,
}

/// One `[[credentials]]` entry.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawCredential {
    /// Access key id.
    pub access_key_id: String,
    /// Secret reference, e.g. `env:NAME`.
    pub secret_ref: String,
    /// Principal name.
    pub principal: String,
}

/// One `[[grants]]` entry.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawGrant {
    /// Principal name.
    pub principal: String,
    /// Bucket name, or `*`.
    pub bucket: String,
    /// Action names.
    pub actions: Vec<String>,
    /// Key prefix.
    #[serde(default)]
    pub prefix: String,
}

/// `[limits]` section.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawLimits {
    /// Maximum request body the service will buffer for XML/control operations.
    #[serde(default = "default_xml_limit")]
    pub max_xml_bytes: usize,
    /// Maximum object size accepted.
    #[serde(default)]
    pub max_object_size: Option<u64>,
    /// Maximum size of a single non-multipart write.
    #[serde(default = "default_single_write")]
    pub max_single_write_size: u64,
    /// Maximum keys returned in one listing page.
    #[serde(default = "default_page")]
    pub max_list_keys: u32,
    /// Maximum concurrent in-flight requests.
    #[serde(default = "default_concurrency")]
    pub max_concurrent_requests: usize,
    /// Maximum concurrent in-flight requests from one principal. Omit to keep
    /// the default; set to 0 to disable the per-principal limit.
    #[serde(default = "default_per_principal")]
    pub max_concurrent_requests_per_principal: Option<usize>,
    /// Maximum concurrent in-flight operations against any one driver instance,
    /// however many buckets it serves. Omit to keep the default; set to 0 to
    /// disable the per-driver limit.
    #[serde(default = "default_per_driver")]
    pub max_concurrent_requests_per_driver: Option<usize>,
    /// Ceiling on blocking backend operations across all drivers (`0` leaves the
    /// bound to each driver). A synchronous backend runs its work here rather
    /// than on a runtime worker thread (spec §10).
    #[serde(default = "default_blocking_operations")]
    pub max_blocking_operations: usize,
    /// Header timeout in seconds.
    #[serde(default = "default_header_timeout")]
    pub header_timeout_secs: u64,
    /// Body idle timeout in seconds.
    #[serde(default = "default_body_timeout")]
    pub body_idle_timeout_secs: u64,
    /// Total request timeout in seconds (0 disables).
    #[serde(default)]
    pub request_timeout_secs: u64,
    /// Multipart staging quota in bytes.
    #[serde(default = "default_staging_quota")]
    pub staging_quota_bytes: u64,
    /// How long a multipart upload may sit without activity before its staged
    /// parts may be reclaimed. Values below 1 second are raised to 1.
    #[serde(default = "default_multipart_lease")]
    pub multipart_lease_secs: u64,
    /// How often the multipart lease sweeper runs, in seconds. `0` disables it.
    #[serde(default = "default_multipart_sweep_interval")]
    pub multipart_sweep_interval_secs: u64,
    /// Lifetime of a server-side listing continuation token, in seconds.
    /// Values below 1 second are raised to 1.
    #[serde(default = "default_list_token_ttl")]
    pub list_token_ttl_secs: u64,
}

fn default_list_token_ttl() -> u64 {
    3600
}

fn default_staging_quota() -> u64 {
    10 * 1024 * 1024 * 1024
}

fn default_multipart_lease() -> u64 {
    3600
}

fn default_multipart_sweep_interval() -> u64 {
    60
}

fn default_region() -> String {
    "us-east-1".into()
}
fn default_addressing() -> String {
    "path".into()
}
fn default_true() -> bool {
    true
}
fn default_profile() -> String {
    "core-write".into()
}
fn default_xml_limit() -> usize {
    10 * 1024 * 1024
}
fn default_single_write() -> u64 {
    5 * 1024 * 1024 * 1024
}
fn default_page() -> u32 {
    1000
}
fn default_concurrency() -> usize {
    1024
}
fn default_per_principal() -> Option<usize> {
    Some(256)
}
fn default_per_driver() -> Option<usize> {
    Some(256)
}
fn default_blocking_operations() -> usize {
    64
}
fn default_header_timeout() -> u64 {
    30
}
fn default_body_timeout() -> u64 {
    60
}

impl Default for RawLimits {
    fn default() -> Self {
        Self {
            max_xml_bytes: default_xml_limit(),
            max_object_size: None,
            max_single_write_size: default_single_write(),
            max_list_keys: default_page(),
            max_concurrent_requests: default_concurrency(),
            max_concurrent_requests_per_principal: default_per_principal(),
            max_concurrent_requests_per_driver: default_per_driver(),
            max_blocking_operations: default_blocking_operations(),
            header_timeout_secs: default_header_timeout(),
            body_idle_timeout_secs: default_body_timeout(),
            request_timeout_secs: 0,
            staging_quota_bytes: default_staging_quota(),
            multipart_lease_secs: default_multipart_lease(),
            multipart_sweep_interval_secs: default_multipart_sweep_interval(),
            list_token_ttl_secs: default_list_token_ttl(),
        }
    }
}

/// API addressing style.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Addressing {
    /// `http://host/bucket/key`.
    Path,
    /// `http://bucket.host/key`.
    VirtualHosted,
}

/// Resolved resource limits.
#[derive(Debug, Clone)]
pub struct Limits {
    /// Maximum buffered XML/control body.
    pub max_xml_bytes: usize,
    /// Maximum object size.
    pub max_object_size: Option<u64>,
    /// Maximum single non-multipart write.
    pub max_single_write_size: u64,
    /// Maximum keys per listing page.
    pub max_list_keys: u32,
    /// Maximum concurrent requests, server-wide.
    pub max_concurrent_requests: usize,
    /// Maximum concurrent requests per principal; `0` disables the limit.
    pub max_concurrent_requests_per_principal: usize,
    /// Maximum concurrent operations per driver instance; `0` disables the
    /// limit. Bounds what one backend can be asked to do at once, so a slow or
    /// saturated driver cannot consume every connection the server has.
    pub max_concurrent_requests_per_driver: usize,
    /// Ceiling on blocking backend operations; `0` leaves the bound to each
    /// driver. Shared by every driver through one executor owned by the
    /// composition root (spec §10).
    pub max_blocking_operations: usize,
    /// Header timeout.
    pub header_timeout: std::time::Duration,
    /// Body idle timeout.
    pub body_idle_timeout: std::time::Duration,
    /// Total request timeout; `None` disables.
    pub request_timeout: Option<std::time::Duration>,
    /// Multipart staging quota in bytes.
    pub staging_quota_bytes: u64,
    /// Lease after which an inactive multipart upload may be reclaimed.
    pub multipart_lease: std::time::Duration,
    /// How often the multipart sweeper runs; `None` disables it.
    pub multipart_sweep_interval: Option<std::time::Duration>,
    /// Lifetime of a server-side listing continuation token.
    pub list_token_ttl: std::time::Duration,
}

impl Limits {
    /// Converts raw limits.
    pub fn from_raw(raw: &RawLimits) -> Self {
        Self {
            max_xml_bytes: raw.max_xml_bytes,
            max_object_size: raw.max_object_size,
            max_single_write_size: raw.max_single_write_size,
            max_list_keys: raw.max_list_keys.clamp(1, 1000),
            max_concurrent_requests: raw.max_concurrent_requests.max(1),
            max_concurrent_requests_per_principal: raw
                .max_concurrent_requests_per_principal
                .unwrap_or(0),
            max_concurrent_requests_per_driver: raw.max_concurrent_requests_per_driver.unwrap_or(0),
            max_blocking_operations: raw.max_blocking_operations,
            header_timeout: std::time::Duration::from_secs(raw.header_timeout_secs),
            body_idle_timeout: std::time::Duration::from_secs(raw.body_idle_timeout_secs),
            request_timeout: (raw.request_timeout_secs > 0)
                .then(|| std::time::Duration::from_secs(raw.request_timeout_secs)),
            staging_quota_bytes: raw.staging_quota_bytes,
            // A zero lease would reclaim an upload the instant it is created, so
            // the floor is one second.
            multipart_lease: std::time::Duration::from_secs(raw.multipart_lease_secs.max(1)),
            multipart_sweep_interval: (raw.multipart_sweep_interval_secs > 0)
                .then(|| std::time::Duration::from_secs(raw.multipart_sweep_interval_secs)),
            list_token_ttl: std::time::Duration::from_secs(raw.list_token_ttl_secs.max(1)),
        }
    }
}

/// The validated application configuration.
#[derive(Debug, Clone)]
pub struct AppConfig {
    /// Listen address.
    pub listen: String,
    /// Region.
    pub region: String,
    /// Addressing style.
    pub addressing: Addressing,
    /// Development mode.
    pub development: bool,
    /// Whether health endpoints are exposed.
    pub health_endpoints: bool,
    /// Directory holding the multipart coordinator's durable state, absent in a
    /// deployment that did not configure one.
    pub state_dir: Option<std::path::PathBuf>,
    /// Driver instances.
    pub drivers: Vec<RawDriver>,
    /// Bucket catalog.
    pub catalog: BucketCatalog,
    /// Credentials.
    pub credentials: CredentialStore,
    /// Access policy.
    pub access: AccessPolicy,
    /// Limits.
    pub limits: Limits,
}

impl AppConfig {
    /// Loads and validates a configuration file.
    pub fn load(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let path = path.as_ref();
        let text = std::fs::read_to_string(path).map_err(|source| ConfigError::Io {
            path: path.display().to_string(),
            source,
        })?;
        Self::from_toml(&text, &EnvSecretResolver)
    }

    /// Parses and validates TOML text.
    pub fn from_toml(text: &str, secrets: &dyn SecretResolver) -> Result<Self, ConfigError> {
        let raw: RawConfig = toml::from_str(text).map_err(|e| ConfigError::Parse(e.to_string()))?;
        Self::validate(raw, secrets)
    }

    /// Validates a parsed configuration.
    pub fn validate(raw: RawConfig, secrets: &dyn SecretResolver) -> Result<Self, ConfigError> {
        let addressing = match raw.server.addressing.as_str() {
            "path" => Addressing::Path,
            // Virtual-hosted routing is not implemented. Accepting the setting
            // while serving path-style would silently misroute `bucket.host`
            // requests, so it fails closed here and stays "unsupported" in the
            // compatibility matrix.
            "virtual" => return Err(ConfigError::UnsupportedAddressing("virtual-hosted".into())),
            other => return Err(ConfigError::InvalidAddressing(other.to_string())),
        };
        if !valid_listen(&raw.server.listen) {
            return Err(ConfigError::InvalidListen(raw.server.listen.clone()));
        }

        // Drivers: unique ids, non-empty kind.
        let mut driver_ids = HashSet::new();
        for driver in &raw.drivers {
            if driver.kind.trim().is_empty() {
                return Err(ConfigError::EmptyDriverKind(driver.id.clone()));
            }
            if !driver_ids.insert(driver.id.clone()) {
                return Err(ConfigError::DuplicateDriverId(driver.id.clone()));
            }
            // Every reference a driver may resolve is proved resolvable now, so
            // a missing driver secret fails startup rather than the first
            // backend call (spec §7.1). The value is never held here.
            for reference in &driver.secret_refs {
                if !reference.contains(':') {
                    return Err(ConfigError::InvalidDriverSecretReference {
                        driver: driver.id.clone(),
                        reference: reference.clone(),
                    });
                }
                if secrets.resolve(reference).is_err() {
                    return Err(ConfigError::MissingDriverSecret {
                        driver: driver.id.clone(),
                        reference: reference.clone(),
                    });
                }
            }
        }

        // Buckets: valid names, known driver, distinct namespaces per driver.
        let mut bucket_names = HashSet::new();
        let mut namespace_owners: HashSet<(String, String)> = HashSet::new();
        let mut bindings = Vec::with_capacity(raw.buckets.len());
        for bucket in &raw.buckets {
            if !valid_bucket_name(&bucket.name) {
                return Err(ConfigError::InvalidBucketName(bucket.name.clone()));
            }
            if !bucket_names.insert(bucket.name.clone()) {
                return Err(ConfigError::DuplicateBucketName(bucket.name.clone()));
            }
            if !driver_ids.contains(&bucket.driver) {
                return Err(ConfigError::UnknownDriver {
                    bucket: bucket.name.clone(),
                    driver: bucket.driver.clone(),
                });
            }
            if !namespace_owners.insert((bucket.driver.clone(), bucket.namespace.clone())) {
                return Err(ConfigError::SharedNamespace {
                    driver: bucket.driver.clone(),
                    namespace: bucket.namespace.clone(),
                });
            }
            let profile = parse_profile(&bucket.profile)?;
            let generation = bucket.generation.unwrap_or_else(|| {
                derived_generation(&bucket.name, &bucket.driver, &bucket.namespace)
            });
            bindings.push(BucketBinding {
                bucket: BucketId::parse(bucket.name.clone())
                    .map_err(|_| ConfigError::InvalidBucketName(bucket.name.clone()))?,
                driver: DriverInstanceId::parse(bucket.driver.clone()).map_err(|_| {
                    ConfigError::UnknownDriver {
                        bucket: bucket.name.clone(),
                        driver: bucket.driver.clone(),
                    }
                })?,
                namespace: NamespaceId::parse(bucket.namespace.clone())
                    .map_err(|_| ConfigError::InvalidNamespace(bucket.namespace.clone()))?,
                generation,
                profile,
                readonly: bucket.readonly,
                region: raw.server.region.clone(),
            });
        }
        let catalog = BucketCatalog::from_bindings(bindings).map_err(ConfigError::Catalog)?;

        // Credentials: resolve every secret now, so a missing secret fails
        // startup rather than a request.
        let mut credentials = Vec::with_capacity(raw.credentials.len());
        for credential in &raw.credentials {
            let secret =
                resolve_secret(secrets, &credential.secret_ref, &credential.access_key_id)?;
            credentials.push(Credential {
                access_key: credential.access_key_id.clone(),
                secret,
                principal: credential.principal.clone(),
            });
        }
        let store = CredentialStore::new(credentials).map_err(ConfigError::Auth)?;

        // Grants.
        let mut grants = Vec::with_capacity(raw.grants.len());
        for grant in &raw.grants {
            if !bucket_names.contains(&grant.bucket) && grant.bucket != "*" {
                return Err(ConfigError::UnknownBucketInGrant(grant.bucket.clone()));
            }
            let mut actions = Vec::with_capacity(grant.actions.len());
            for action in &grant.actions {
                let parsed = Action::parse(action).ok_or_else(|| ConfigError::UnknownAction {
                    principal: grant.principal.clone(),
                    action: action.clone(),
                })?;
                actions.push(parsed);
            }
            grants.push(Grant {
                principal: grant.principal.clone(),
                bucket: grant.bucket.clone(),
                actions,
                prefix: grant.prefix.clone(),
            });
        }

        // Development guard: durable writes must not be backed by a volatile
        // test driver. Which kinds are test drivers is registry data
        // (`driver::builtins`), not a name convention, so this guard cannot
        // drift from what is registered.
        if !raw.server.development {
            for driver in &raw.drivers {
                if crate::driver::builtins::is_test_kind(&driver.kind) {
                    return Err(ConfigError::TestDriverInProduction(driver.id.clone()));
                }
            }
        }

        // A production deployment that can write objects must be able to
        // persist the multipart coordinator's state: without a `state_dir`, a
        // part it acknowledged could not survive a restart (`ADR-0014`,
        // spec §8.1). A deployment with no writable bucket never holds any, so
        // it needs none.
        let writable = raw.buckets.iter().any(|bucket| !bucket.readonly);
        let state_dir = match raw.server.state_dir.as_deref() {
            Some(dir) if !dir.trim().is_empty() => Some(std::path::PathBuf::from(dir)),
            _ if !raw.server.development && writable => {
                return Err(ConfigError::StateDirRequired);
            }
            _ => None,
        };

        Ok(Self {
            listen: raw.server.listen,
            region: raw.server.region,
            addressing,
            development: raw.server.development,
            health_endpoints: raw.server.health_endpoints,
            state_dir,
            drivers: raw.drivers,
            catalog,
            credentials: store,
            access: AccessPolicy::new(grants),
            limits: Limits::from_raw(&raw.limits),
        })
    }

    /// A redacted summary safe to log.
    pub fn redacted_summary(&self) -> String {
        let buckets: Vec<&str> = self.catalog.iter().map(|b| b.bucket.as_str()).collect();
        format!(
            "listen={} region={} addressing={:?} development={} buckets={:?} credentials={} grants={}",
            self.listen,
            self.region,
            self.addressing,
            self.development,
            buckets,
            self.credentials.len(),
            self.access.grant_count(),
        )
    }
}

fn resolve_secret(
    secrets: &dyn SecretResolver,
    reference: &str,
    access_key: &str,
) -> Result<Secret, ConfigError> {
    if !reference.contains(':') {
        return Err(ConfigError::InvalidSecretReference(reference.to_string()));
    }
    secrets
        .resolve(reference)
        .map_err(|_| ConfigError::MissingSecret {
            access_key: access_key.to_string(),
            reference: reference.to_string(),
        })
}

fn parse_profile(value: &str) -> Result<Profile, ConfigError> {
    match value.trim().to_ascii_lowercase().as_str() {
        "core-read" | "core_read" => Ok(Profile::CoreRead),
        "core-write" | "core_write" => Ok(Profile::CoreWrite),
        "multipart" => Ok(Profile::Multipart),
        other => Err(ConfigError::UnknownProfile(other.to_string())),
    }
}

/// A stable binding generation derived from the binding identity.
fn derived_generation(bucket: &str, driver: &str, namespace: &str) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    bucket.hash(&mut hasher);
    driver.hash(&mut hasher);
    namespace.hash(&mut hasher);
    // Keep it in a readable range and never zero.
    (hasher.finish() % 1_000_000_000) + 1
}

/// Validates an S3 bucket name (3–63 chars, `a-z0-9.-`, starts/ends alnum).
pub fn valid_bucket_name(name: &str) -> bool {
    let bytes = name.as_bytes();
    if bytes.len() < 3 || bytes.len() > 63 {
        return false;
    }
    let alnum = |b: u8| b.is_ascii_lowercase() || b.is_ascii_digit();
    if !alnum(bytes[0]) || !alnum(bytes[bytes.len() - 1]) {
        return false;
    }
    if bytes
        .windows(2)
        .any(|w| (w[0] == b'.' && w[1] == b'.') || (w[0] == b'-' && w[1] == b'-'))
    {
        return false;
    }
    if name.contains(".-") || name.contains("-.") {
        return false;
    }
    name.bytes().all(|b| alnum(b) || b == b'.' || b == b'-')
}

fn valid_listen(value: &str) -> bool {
    let Some((host, port)) = value.rsplit_once(':') else {
        return false;
    };
    !host.is_empty() && port.parse::<u16>().is_ok()
}

/// Resolves `env:NAME` secret references from the process environment.
#[derive(Debug, Default, Clone, Copy)]
pub struct EnvSecretResolver;

impl SecretResolver for EnvSecretResolver {
    fn resolve(&self, reference: &str) -> Result<Secret, DriverError> {
        let Some(name) = reference.strip_prefix("env:") else {
            return Err(DriverError::invalid_argument(format!(
                "unsupported secret reference scheme in `{reference}`"
            )));
        };
        match std::env::var(name) {
            Ok(value) => Ok(Secret::new(value)),
            Err(_) => Err(DriverError::not_found(format!(
                "environment variable `{name}` is not set"
            ))),
        }
    }
}

/// Configuration failure.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    /// Reading the file failed.
    #[error("failed to read configuration `{path}`: {source}")]
    Io {
        /// Path that failed.
        path: String,
        /// Underlying IO error.
        #[source]
        source: std::io::Error,
    },
    /// TOML parsing failed.
    #[error("invalid configuration: {0}")]
    Parse(String),
    /// Two drivers share an id.
    #[error("duplicate driver id `{0}`")]
    DuplicateDriverId(String),
    /// A driver kind was empty.
    #[error("driver `{0}` has an empty kind")]
    EmptyDriverKind(String),
    /// Two buckets share a name.
    #[error("duplicate bucket name `{0}`")]
    DuplicateBucketName(String),
    /// A bucket name is invalid.
    #[error("invalid bucket name `{0}`")]
    InvalidBucketName(String),
    /// A namespace id is invalid.
    #[error("invalid namespace `{0}`")]
    InvalidNamespace(String),
    /// A bucket references an unknown driver.
    #[error("bucket `{bucket}` references unknown driver `{driver}`")]
    UnknownDriver {
        /// Bucket name.
        bucket: String,
        /// Driver id.
        driver: String,
    },
    /// Two writable buckets share a driver namespace.
    #[error("driver `{driver}` namespace `{namespace}` is bound by more than one bucket")]
    SharedNamespace {
        /// Driver id.
        driver: String,
        /// Namespace.
        namespace: String,
    },
    /// The profile name is unknown.
    #[error("unknown profile `{0}`")]
    UnknownProfile(String),
    /// The addressing style is unknown.
    #[error("unknown addressing style `{0}` (expected `path` or `virtual`)")]
    InvalidAddressing(String),
    /// The listen address is malformed.
    #[error("invalid listen address `{0}`")]
    InvalidListen(String),
    /// A secret reference is malformed.
    #[error("invalid secret reference `{0}`")]
    InvalidSecretReference(String),
    /// A secret could not be resolved.
    #[error("missing secret for access key `{access_key}` (reference `{reference}`)")]
    MissingSecret {
        /// Access key id.
        access_key: String,
        /// Secret reference.
        reference: String,
    },
    /// A grant references an unknown bucket.
    #[error("grant references unknown bucket `{0}`")]
    UnknownBucketInGrant(String),
    /// A grant references an unknown action.
    #[error("grant for `{principal}` has unknown action `{action}`")]
    UnknownAction {
        /// Principal.
        principal: String,
        /// Action.
        action: String,
    },
    /// A test driver was configured for production.
    #[error("test driver `{0}` requires `development = true`")]
    TestDriverInProduction(String),
    /// A driver secret reference is malformed.
    #[error("driver `{driver}` has an invalid secret reference `{reference}`")]
    InvalidDriverSecretReference {
        /// Driver instance id.
        driver: String,
        /// The malformed reference.
        reference: String,
    },
    /// A driver secret reference could not be resolved.
    #[error("missing secret for driver `{driver}` (reference `{reference}`)")]
    MissingDriverSecret {
        /// Driver instance id.
        driver: String,
        /// Secret reference.
        reference: String,
    },
    /// A recognised but unimplemented addressing style was configured.
    #[error("addressing style `{0}` is not implemented")]
    UnsupportedAddressing(String),
    /// A production deployment can write objects but has nowhere to persist the
    /// multipart coordinator's state.
    #[error(
        "`server.state_dir` is required: a production deployment with a writable bucket must be able to persist multipart state (`ADR-0014`)"
    )]
    StateDirRequired,
    /// Catalog construction failed.
    #[error(transparent)]
    Catalog(#[from] CatalogError),
    /// Credential store construction failed.
    #[error(transparent)]
    Auth(#[from] AuthConfigError),
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    /// A resolver that only knows `env:ALLINS3_TEST_SECRET`; tests never mutate
    /// the process environment, which is unsynchronised with other test threads.
    struct TestResolver;

    impl SecretResolver for TestResolver {
        fn resolve(&self, reference: &str) -> Result<Secret, DriverError> {
            if reference == "env:ALLINS3_TEST_SECRET" {
                Ok(Secret::new("topsecret".to_string()))
            } else {
                Err(DriverError::not_found("test resolver knows one secret"))
            }
        }
    }

    fn resolver() -> TestResolver {
        TestResolver
    }

    fn base_toml() -> String {
        r#"
[server]
listen = "127.0.0.1:9000"
region = "us-east-1"
addressing = "path"
development = true

[[drivers]]
id = "instance-a"
kind = "test-reference"

[[drivers]]
id = "instance-b"
kind = "test-reference"

[[buckets]]
name = "example-a"
driver = "instance-a"
namespace = "namespace-a"
profile = "core-write"

[[buckets]]
name = "example-b"
driver = "instance-b"
namespace = "namespace-b"
profile = "core-read"
readonly = true

[[credentials]]
access_key_id = "EXAMPLEACCESSKEY"
secret_ref = "env:ALLINS3_TEST_SECRET"
principal = "example-user"

[[grants]]
principal = "example-user"
bucket = "example-a"
actions = ["list", "read", "write", "delete"]
prefix = ""

[[grants]]
principal = "example-user"
bucket = "example-b"
actions = ["list", "read"]
prefix = ""
"#
        .to_string()
    }

    #[test]
    fn parses_valid_config() {
        let cfg = AppConfig::from_toml(&base_toml(), &resolver()).unwrap();
        assert_eq!(cfg.catalog.len(), 2);
        assert_eq!(cfg.credentials.len(), 1);
        assert!(cfg.catalog.get("example-a").unwrap().generation > 0);
        // Stable across parses.
        let again = AppConfig::from_toml(&base_toml(), &resolver()).unwrap();
        assert_eq!(
            cfg.catalog.get("example-a").unwrap().generation,
            again.catalog.get("example-a").unwrap().generation
        );
    }

    #[test]
    fn rejects_virtual_hosted_addressing() {
        let toml = base_toml().replace("addressing = \"path\"", "addressing = \"virtual\"");
        assert!(matches!(
            AppConfig::from_toml(&toml, &resolver()),
            Err(ConfigError::UnsupportedAddressing(_))
        ));
    }

    #[test]
    fn per_principal_limit_defaults_and_can_be_disabled() {
        let default = AppConfig::from_toml(&base_toml(), &resolver()).expect("valid");
        assert!(default.limits.max_concurrent_requests_per_principal > 0);

        let toml = format!(
            "{}\n[limits]\nmax_concurrent_requests_per_principal = 0\n",
            base_toml()
        );
        let disabled = AppConfig::from_toml(&toml, &resolver()).expect("valid");
        assert_eq!(disabled.limits.max_concurrent_requests_per_principal, 0);
    }

    #[test]
    fn per_driver_limit_defaults_and_can_be_disabled() {
        let default = AppConfig::from_toml(&base_toml(), &resolver()).expect("valid");
        assert!(default.limits.max_concurrent_requests_per_driver > 0);

        let toml = format!(
            "{}\n[limits]\nmax_concurrent_requests_per_driver = 0\n",
            base_toml()
        );
        let disabled = AppConfig::from_toml(&toml, &resolver()).expect("valid");
        assert_eq!(disabled.limits.max_concurrent_requests_per_driver, 0);
    }

    #[test]
    fn multipart_sweeper_defaults_on_and_can_be_disabled() {
        let default = AppConfig::from_toml(&base_toml(), &resolver()).expect("valid");
        assert!(default.limits.multipart_lease >= Duration::from_secs(1));
        assert!(default.limits.multipart_sweep_interval.is_some());

        let toml = format!(
            "{}\n[limits]\nmultipart_sweep_interval_secs = 0\n",
            base_toml()
        );
        let disabled = AppConfig::from_toml(&toml, &resolver()).expect("valid");
        assert!(disabled.limits.multipart_sweep_interval.is_none());
    }

    #[test]
    fn the_continuation_token_lifetime_is_configurable() {
        // Spec §4.3 requires the token expiry to be a deployment policy rather
        // than a constant.
        let default = AppConfig::from_toml(&base_toml(), &resolver()).expect("valid");
        assert!(default.limits.list_token_ttl >= Duration::from_secs(1));

        let toml = format!("{}\n[limits]\nlist_token_ttl_secs = 5\n", base_toml());
        let configured = AppConfig::from_toml(&toml, &resolver()).expect("valid");
        assert_eq!(configured.limits.list_token_ttl, Duration::from_secs(5));
    }

    #[test]
    fn rejects_unknown_field() {
        let toml = base_toml().replace(
            "region = \"us-east-1\"",
            "region = \"us-east-1\"\nbogus = 1",
        );
        assert!(matches!(
            AppConfig::from_toml(&toml, &resolver()),
            Err(ConfigError::Parse(_))
        ));
    }

    #[test]
    fn rejects_duplicate_driver() {
        let toml = base_toml().replace(
            "id = \"instance-b\"\nkind = \"test-reference\"",
            "id = \"instance-a\"\nkind = \"test-reference\"",
        );
        assert!(matches!(
            AppConfig::from_toml(&toml, &resolver()),
            Err(ConfigError::DuplicateDriverId(_))
        ));
    }

    #[test]
    fn rejects_unknown_driver_reference() {
        let toml = base_toml().replace("driver = \"instance-b\"", "driver = \"nope\"");
        assert!(matches!(
            AppConfig::from_toml(&toml, &resolver()),
            Err(ConfigError::UnknownDriver { .. })
        ));
    }

    #[test]
    fn rejects_shared_namespace() {
        // Namespace identity is (driver instance, namespace id), so two buckets
        // on the *same* instance may not share one namespace.
        let toml = base_toml().replace(
            "name = \"example-b\"\ndriver = \"instance-b\"\nnamespace = \"namespace-b\"",
            "name = \"example-b\"\ndriver = \"instance-a\"\nnamespace = \"namespace-a\"",
        );
        assert!(matches!(
            AppConfig::from_toml(&toml, &resolver()),
            Err(ConfigError::SharedNamespace { .. })
        ));
    }

    #[test]
    fn allows_same_namespace_name_on_different_drivers() {
        let toml =
            base_toml().replace("namespace = \"namespace-b\"", "namespace = \"namespace-a\"");
        assert!(AppConfig::from_toml(&toml, &resolver()).is_ok());
    }

    #[test]
    fn rejects_invalid_bucket_name() {
        let toml = base_toml().replace("name = \"example-b\"", "name = \"AB\"");
        assert!(matches!(
            AppConfig::from_toml(&toml, &resolver()),
            Err(ConfigError::InvalidBucketName(_))
        ));
    }

    #[test]
    fn driver_secret_refs_are_validated_and_kept() {
        let toml = base_toml().replace(
            "kind = \"test-reference\"",
            "kind = \"test-reference\"\nsecret_refs = [\"env:ALLINS3_TEST_SECRET\"]",
        );
        let cfg = AppConfig::from_toml(&toml, &resolver()).expect("valid");
        assert_eq!(cfg.drivers[0].secret_refs, vec!["env:ALLINS3_TEST_SECRET"]);
    }

    #[test]
    fn rejects_unresolvable_driver_secret() {
        // A reference a driver may resolve must be proved resolvable before
        // startup, not on the first backend call (spec §7.1).
        let toml = base_toml().replace(
            "kind = \"test-reference\"",
            "kind = \"test-reference\"\nsecret_refs = [\"env:ALLINS3_DRIVER_SECRET\"]",
        );
        assert!(matches!(
            AppConfig::from_toml(&toml, &resolver()),
            Err(ConfigError::MissingDriverSecret { .. })
        ));
    }

    #[test]
    fn rejects_malformed_driver_secret_reference() {
        let toml = base_toml().replace(
            "kind = \"test-reference\"",
            "kind = \"test-reference\"\nsecret_refs = [\"just-a-name\"]",
        );
        assert!(matches!(
            AppConfig::from_toml(&toml, &resolver()),
            Err(ConfigError::InvalidDriverSecretReference { .. })
        ));
    }

    #[test]
    fn driver_options_survive_validation_unparsed() {
        // `options` is opaque to the core: it is carried through, not validated.
        let toml = base_toml().replace(
            "kind = \"test-reference\"",
            "kind = \"test-reference\"\noptions = { root = \"/tmp/allins3\", depth = 3 }",
        );
        let cfg = AppConfig::from_toml(&toml, &resolver()).expect("valid");
        assert_eq!(
            cfg.drivers[0].options.get("root").and_then(|v| v.as_str()),
            Some("/tmp/allins3")
        );
    }

    #[test]
    fn rejects_missing_secret() {
        let toml = base_toml().replace("env:ALLINS3_TEST_SECRET", "env:ALLINS3_MISSING_SECRET");
        assert!(matches!(
            AppConfig::from_toml(&toml, &resolver()),
            Err(ConfigError::MissingSecret { .. })
        ));
    }

    // Test kinds are only registered under the `testing` feature, so this guard
    // can only be exercised where those kinds exist.
    #[cfg(feature = "testing")]
    #[test]
    fn rejects_test_driver_in_production() {
        let toml = base_toml().replace("development = true", "development = false");
        assert!(matches!(
            AppConfig::from_toml(&toml, &resolver()),
            Err(ConfigError::TestDriverInProduction(_))
        ));
    }

    /// A production config with a real (non-test) kind, so the request reaches
    /// the `state_dir` rule instead of the test-driver guard.
    fn production_toml() -> String {
        base_toml()
            .replace("development = true", "development = false")
            .replace("kind = \"test-reference\"", "kind = \"fs\"")
    }

    #[test]
    fn a_production_deployment_with_a_writable_bucket_requires_a_state_dir() {
        // Without one, an acknowledged multipart part could not survive a
        // restart, so the deployment is refused before it starts (ADR-0014).
        assert!(matches!(
            AppConfig::from_toml(&production_toml(), &resolver()),
            Err(ConfigError::StateDirRequired)
        ));

        // With one, it is accepted and resolved.
        let toml = production_toml().replace(
            "region = \"us-east-1\"",
            "region = \"us-east-1\"\nstate_dir = \"/var/lib/allins3-state\"",
        );
        let cfg = AppConfig::from_toml(&toml, &resolver()).expect("valid");
        assert_eq!(
            cfg.state_dir.as_deref(),
            Some(std::path::Path::new("/var/lib/allins3-state"))
        );
    }

    #[test]
    fn a_readonly_production_deployment_needs_no_state_dir() {
        // Nothing writable means the coordinator never holds a multipart part,
        // so requiring a directory would be friction with nothing behind it.
        let toml = production_toml().replace(
            "namespace = \"namespace-a\"",
            "namespace = \"namespace-a\"\nreadonly = true",
        );
        let cfg = AppConfig::from_toml(&toml, &resolver()).expect("valid");
        assert!(cfg.state_dir.is_none());
    }

    #[test]
    fn rejects_unknown_action() {
        let toml = base_toml().replace(
            "\"list\", \"read\", \"write\", \"delete\"",
            "\"frobnicate\"",
        );
        assert!(matches!(
            AppConfig::from_toml(&toml, &resolver()),
            Err(ConfigError::UnknownAction { .. })
        ));
    }

    #[test]
    fn bucket_name_rules() {
        assert!(valid_bucket_name("example-a"));
        assert!(valid_bucket_name("a1.b2"));
        assert!(!valid_bucket_name("ab"));
        assert!(!valid_bucket_name("-abc"));
        assert!(!valid_bucket_name("a..b"));
        assert!(!valid_bucket_name("ABC"));
    }
}
