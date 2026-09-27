//! Private configuration and declared capabilities for the 139 driver.
//!
//! The core passes the opaque `options` table through untouched; this module
//! owns its schema and rejects anything it does not understand, so a typo in a
//! deployment file fails `check-config` rather than being silently ignored.
//!
//! The session credentials live here, in the configuration file, by operator
//! choice. They are held as [`Secret`] values whose `Debug` output is redacted,
//! so a driver `Debug` can never leak the token; the configuration file itself
//! must be kept out of version control.

use std::collections::HashMap;

use crate::domain::DriverError;
use crate::driver::{
    ConditionalSupport, Consistency, DriverLimits, NamespaceCapabilities, Persistence,
    RangeSupport, Secret,
};

/// The kind name configuration uses.
pub(crate) const KIND: &str = "yun139";

/// Default API origin for the personal cloud.
const DEFAULT_BASE_URL: &str = "https://personal-kd-njs.yun.139.com";
/// Default page size when enumerating a folder.
const DEFAULT_PAGE_SIZE: u32 = 100;
/// Default per-request timeout.
const DEFAULT_TIMEOUT_SECS: u64 = 30;
/// Default ceiling on entries enumerated for one listing.
const DEFAULT_MAX_OBJECTS: u64 = 20_000;

/// Every key this driver understands.
const KNOWN_KEYS: [&str; 9] = [
    "phone",
    "auth_token",
    "cookie",
    "base_url",
    "page_size",
    "timeout_secs",
    "folder_id",
    "namespaces",
    "max_objects",
];

/// Validated private options.
pub(crate) struct Yun139Options {
    /// The account phone number, used in the `Authorization` header.
    pub(crate) phone: String,
    /// The session token (`auth_token`).
    pub(crate) auth_token: Secret,
    /// The session cookie string, when the operator supplies one.
    pub(crate) cookie: Option<Secret>,
    /// The API origin, without a trailing slash.
    pub(crate) base_url: String,
    /// Page size used when enumerating a folder.
    pub(crate) page_size: u32,
    /// Per-request timeout in seconds.
    pub(crate) timeout_secs: u64,
    /// Default folder backing every namespace without an explicit mapping.
    pub(crate) folder_id: Option<String>,
    /// Explicit namespace-to-folder mapping, when the operator supplies one.
    pub(crate) namespaces: HashMap<String, String>,
    /// Ceiling on entries enumerated for one listing.
    pub(crate) max_objects: u64,
}

impl std::fmt::Debug for Yun139Options {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // `Secret` already redacts; the phone is masked too so a driver `Debug`
        // never echoes a full account identifier.
        f.debug_struct("Yun139Options")
            .field("phone", &mask_phone(&self.phone))
            .field("auth_token", &self.auth_token)
            .field("cookie", &self.cookie)
            .field("base_url", &self.base_url)
            .field("page_size", &self.page_size)
            .field("timeout_secs", &self.timeout_secs)
            .field("folder_id", &self.folder_id)
            .field("namespaces", &self.namespaces.keys().collect::<Vec<_>>())
            .field("max_objects", &self.max_objects)
            .finish()
    }
}

fn mask_phone(phone: &str) -> String {
    let chars: Vec<char> = phone.chars().collect();
    if chars.len() <= 4 {
        return "*".repeat(chars.len());
    }
    let head: String = chars[..3].iter().collect();
    let tail: String = chars[chars.len() - 4..].iter().collect();
    format!("{head}****{tail}")
}

impl Yun139Options {
    /// Parses and validates the private schema.
    ///
    /// Unknown keys are rejected: accepting them would let a deployment believe
    /// it configured something the driver never reads. Parsing performs no I/O.
    pub(crate) fn parse(options: &toml::Table) -> Result<Self, DriverError> {
        for key in options.keys() {
            if !KNOWN_KEYS.contains(&key.as_str()) {
                return Err(DriverError::invalid_argument(format!(
                    "unknown yun139 option `{key}` (known: {})",
                    KNOWN_KEYS.join(", ")
                )));
            }
        }

        let phone = required_string(options, "phone")?;
        let auth_token = Secret::new(required_string(options, "auth_token")?);
        let cookie = optional_string(options, "cookie")?.map(Secret::new);

        let base_url =
            optional_string(options, "base_url")?.unwrap_or_else(|| DEFAULT_BASE_URL.to_string());
        if !base_url.starts_with("http://") && !base_url.starts_with("https://") {
            return Err(DriverError::invalid_argument(
                "yun139 option `base_url` must start with http:// or https://",
            ));
        }
        let base_url = base_url.trim_end_matches('/').to_string();

        let page_size = optional_u64(options, "page_size")?.unwrap_or(DEFAULT_PAGE_SIZE as u64);
        if !(1..=1000).contains(&page_size) {
            return Err(DriverError::invalid_argument(
                "yun139 option `page_size` must be between 1 and 1000",
            ));
        }
        let page_size = page_size as u32;

        let timeout_secs = optional_u64(options, "timeout_secs")?.unwrap_or(DEFAULT_TIMEOUT_SECS);
        if timeout_secs == 0 {
            return Err(DriverError::invalid_argument(
                "yun139 option `timeout_secs` must be positive",
            ));
        }

        let max_objects = optional_u64(options, "max_objects")?.unwrap_or(DEFAULT_MAX_OBJECTS);
        if max_objects == 0 {
            return Err(DriverError::invalid_argument(
                "yun139 option `max_objects` must be positive",
            ));
        }

        let folder_id = optional_string(options, "folder_id")?;
        let namespaces = parse_namespaces(options)?;

        if folder_id.is_none() && namespaces.is_empty() {
            return Err(DriverError::invalid_argument(
                "yun139 requires at least one of `folder_id` or `namespaces` to map a namespace to a drive folder",
            ));
        }

        Ok(Self {
            phone,
            auth_token,
            cookie,
            base_url,
            page_size,
            timeout_secs,
            folder_id,
            namespaces,
            max_objects,
        })
    }

    /// The drive folder that backs `namespace`.
    ///
    /// An explicit `namespaces` entry wins; otherwise `folder_id` is the
    /// fallback for any namespace.
    pub(crate) fn folder_for(&self, namespace: &str) -> Result<&str, DriverError> {
        if let Some(folder) = self.namespaces.get(namespace) {
            return Ok(folder.as_str());
        }
        self.folder_id.as_deref().ok_or_else(|| {
            DriverError::not_found(format!(
                "no yun139 folder is configured for namespace `{namespace}`"
            ))
        })
    }

    /// A folder used only to probe health, without touching user objects.
    pub(crate) fn probe_folder(&self) -> Option<&str> {
        if let Some(folder) = &self.folder_id {
            return Some(folder.as_str());
        }
        self.namespaces.values().next().map(String::as_str)
    }

    /// The declared capabilities of a namespace on this backend.
    ///
    /// The drive exposes ordered reads (with byte ranges through presigned
    /// URLs) but no hidden staging, no atomic publish, no atomic conditional
    /// write and no commit-outcome record; the same folder is also written by
    /// other clients. So this is a read-only namespace, and `exclusive_writer`
    /// is honestly `false`.
    pub(crate) fn capabilities(&self) -> NamespaceCapabilities {
        NamespaceCapabilities {
            read: true,
            list: true,
            write: false,
            multipart: false,
            native_copy: false,
            recovery: false,
            // Presigned object URLs honour `Range`.
            range: RangeSupport::Native,
            conditional: ConditionalSupport::Unsupported,
            // Reads follow the service's own writes only loosely: the drive is a
            // global, multi-writer, eventually-settled store. Declared as the
            // weakest tier rather than claiming a guarantee that is not proven.
            consistency: Consistency::Eventual,
            // Objects live in China Mobile's object storage, not this process.
            persistence: Persistence::Durable,
            limits: DriverLimits::default(),
            exclusive_writer: false,
        }
    }
}

fn required_string(options: &toml::Table, key: &str) -> Result<String, DriverError> {
    optional_string(options, key)?.ok_or_else(|| {
        DriverError::invalid_argument(format!(
            "yun139 option `{key}` is required and must be a non-empty string"
        ))
    })
}

fn optional_string(options: &toml::Table, key: &str) -> Result<Option<String>, DriverError> {
    match options.get(key) {
        None | Some(toml::Value::Boolean(false)) => Ok(None),
        Some(value) => {
            let text = value.as_str().ok_or_else(|| {
                DriverError::invalid_argument(format!("yun139 option `{key}` must be a string"))
            })?;
            if text.trim().is_empty() {
                return Ok(None);
            }
            Ok(Some(text.to_string()))
        }
    }
}

fn optional_u64(options: &toml::Table, key: &str) -> Result<Option<u64>, DriverError> {
    match options.get(key) {
        None => Ok(None),
        Some(value) => {
            let number = value.as_integer().ok_or_else(|| {
                DriverError::invalid_argument(format!("yun139 option `{key}` must be an integer"))
            })?;
            if number <= 0 {
                return Err(DriverError::invalid_argument(format!(
                    "yun139 option `{key}` must be positive"
                )));
            }
            Ok(Some(number as u64))
        }
    }
}

fn parse_namespaces(options: &toml::Table) -> Result<HashMap<String, String>, DriverError> {
    let Some(value) = options.get("namespaces") else {
        return Ok(HashMap::new());
    };
    let table = value.as_table().ok_or_else(|| {
        DriverError::invalid_argument("yun139 option `namespaces` must be a table of string values")
    })?;
    let mut out = HashMap::with_capacity(table.len());
    for (namespace, folder) in table {
        let folder = folder.as_str().ok_or_else(|| {
            DriverError::invalid_argument(format!(
                "yun139 `namespaces.{namespace}` must be a string folder id"
            ))
        })?;
        if namespace.trim().is_empty() || folder.trim().is_empty() {
            return Err(DriverError::invalid_argument(
                "yun139 `namespaces` entries must not be empty",
            ));
        }
        out.insert(namespace.clone(), folder.to_string());
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(text: &str) -> Result<Yun139Options, DriverError> {
        let table: toml::Table = toml::from_str(text).expect("valid toml");
        Yun139Options::parse(&table)
    }

    #[test]
    fn requires_credentials_and_a_folder() {
        assert!(parse("phone = \"19800000000\"").is_err());
        assert!(parse("phone = \"19800000000\"\nauth_token = \"t\"").is_err());
        assert!(parse("phone = \"19800000000\"\nauth_token = \"t\"\nfolder_id = \"F1\"").is_ok());
    }

    #[test]
    fn rejects_unknown_private_options() {
        assert!(parse("phone = \"1\"\nauth_token = \"t\"\nfolder_id = \"F\"\ntypo = 1").is_err());
    }

    #[test]
    fn namespaces_map_overrides_and_falls_back() {
        let options = parse(
            "phone = \"1\"\nauth_token = \"t\"\nfolder_id = \"fallback\"\nnamespaces = { photos = \"F-photos\" }",
        )
        .expect("valid");
        assert_eq!(options.folder_for("photos").unwrap(), "F-photos");
        assert_eq!(options.folder_for("anything").unwrap(), "fallback");
    }

    #[test]
    fn a_namespace_without_a_folder_is_not_found() {
        let options =
            parse("phone = \"1\"\nauth_token = \"t\"\nnamespaces = { a = \"F\" }").expect("valid");
        assert!(options.folder_for("b").is_err());
    }

    #[test]
    fn base_url_trims_a_trailing_slash_and_must_be_http() {
        assert_eq!(
            parse("phone = \"1\"\nauth_token = \"t\"\nfolder_id = \"F\"\nbase_url = \"https://example.test/\"")
                .expect("valid")
                .base_url,
            "https://example.test"
        );
        assert!(
            parse("phone = \"1\"\nauth_token = \"t\"\nfolder_id = \"F\"\nbase_url = \"ftp://x\"")
                .is_err()
        );
    }

    #[test]
    fn rejects_out_of_range_page_sizes() {
        assert!(
            parse("phone = \"1\"\nauth_token = \"t\"\nfolder_id = \"F\"\npage_size = 0").is_err()
        );
        assert!(
            parse("phone = \"1\"\nauth_token = \"t\"\nfolder_id = \"F\"\npage_size = 5000")
                .is_err()
        );
    }

    #[test]
    fn debug_never_echoes_credentials() {
        let options = parse(
            "phone = \"19800000000\"\nauth_token = \"super-secret-token\"\ncookie = \"a=1\"\nfolder_id = \"F\"",
        )
        .expect("valid");
        let rendered = format!("{options:?}");
        assert!(!rendered.contains("super-secret-token"));
        assert!(!rendered.contains("19800000000"));
        assert!(rendered.contains("<redacted>"));
    }

    #[test]
    fn declares_a_read_only_namespace() {
        let options = parse("phone = \"1\"\nauth_token = \"t\"\nfolder_id = \"F\"").expect("valid");
        let caps = options.capabilities();
        assert!(caps.read && caps.list);
        assert!(!caps.write && !caps.multipart && !caps.native_copy && !caps.recovery);
        assert_eq!(caps.range, RangeSupport::Native);
        assert_eq!(caps.persistence, Persistence::Durable);
        assert!(!caps.is_production_write_capable());
    }
}
