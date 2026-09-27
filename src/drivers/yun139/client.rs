//! The outbound client for the 139 drive API.
//!
//! Every call carries the static headers the web client sends plus a fresh
//! `mcloud-sign`. Authentication is the `Authorization: Basic pc:<phone>:<token>`
//! header, optionally accompanied by the session cookie.
//!
//! Downloads do not go through this API: `file/getDownloadUrl` returns a
//! short-lived presigned object URL on the backing S3-compatible store, which
//! is fetched directly and honours `Range`.

use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures_core::Stream;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::sync::mpsc;

use crate::domain::{DriverError, DriverErrorKind, ResolvedRange};

use super::options::Yun139Options;
use super::sign;

/// The static headers the web client sends. Values are the ones the live
/// client used for a desktop Chrome session; none of them is a credential.
const STATIC_HEADERS: [(&str, &str); 20] = [
    ("Accept", "application/json, text/plain, */*"),
    ("Content-Type", "application/json;charset=UTF-8"),
    ("caller", "web"),
    ("mcloud-route", "001"),
    ("mcloud-version", "7.18.0"),
    ("mcloud-channel", "1000101"),
    ("mcloud-client", "10701"),
    ("x-yun-api-version", "v1"),
    ("x-SvcType", "1"),
    ("x-yun-svc-type", "1"),
    ("x-yun-module-type", "100"),
    ("x-yun-app-channel", "10000034"),
    ("x-m4c-caller", "PC"),
    ("x-m4c-src", "10002"),
    ("x-inner-ntwk", "2"),
    ("x-yun-channel-source", "10000034"),
    ("x-huawei-channelSrc", "10000034"),
    ("CMS-DEVICE", "default"),
    ("INNER-HCY-ROUTER-HTTPS", "1"),
    (
        "x-yun-client-info",
        "||9|7.18.0|chrome|152.0.0.0|35ef4609f86b844962e26e528e36c94a||macos 10.15.7||zh-CN|||dW5kZWZpbmVk||",
    ),
];

/// One entry returned by `file/list` (and `file/get`).
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct FileItem {
    /// Stable drive file id.
    pub(crate) file_id: String,
    /// Name within the parent folder.
    pub(crate) name: String,
    /// `folder` or `file`.
    #[serde(rename = "type", default)]
    pub(crate) kind: String,
    /// Size in bytes; absent for folders.
    #[serde(default)]
    pub(crate) size: Option<u64>,
    /// Content digest, when the service exposes one.
    #[serde(default)]
    pub(crate) content_hash: Option<String>,
    /// Opaque revision token.
    #[serde(default)]
    pub(crate) revision_id: Option<String>,
    /// Last modification time, ISO-8601.
    #[serde(default)]
    pub(crate) updated_at: Option<String>,
}

impl FileItem {
    /// Whether this entry is a folder.
    pub(crate) fn is_folder(&self) -> bool {
        self.kind == "folder"
    }
}

/// One page of a folder listing.
pub(crate) struct ListChildren {
    /// Entries in the folder.
    pub(crate) items: Vec<FileItem>,
    /// Cursor for the next page, when more entries follow.
    pub(crate) next_page_cursor: Option<String>,
}

/// A presigned download descriptor.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DownloadInfo {
    /// The presigned object URL.
    pub(crate) url: String,
    /// The revision the URL was minted for.
    #[serde(default)]
    pub(crate) revision_id: Option<String>,
}

/// The JSON envelope every endpoint wraps its payload in.
#[derive(Debug, Deserialize)]
struct Envelope {
    #[serde(default)]
    success: bool,
    #[serde(default)]
    code: String,
    #[serde(default)]
    message: String,
    #[serde(default)]
    data: Option<Value>,
}

/// An authenticated client for one drive account.
pub(crate) struct Yun139Client {
    http: reqwest::Client,
    base_url: String,
    authorization: String,
    cookie: Option<String>,
    page_size: u32,
}

impl std::fmt::Debug for Yun139Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Yun139Client")
            .field("base_url", &self.base_url)
            .field("page_size", &self.page_size)
            .finish()
    }
}

impl Yun139Client {
    /// Builds a client from validated options.
    pub(crate) fn new(options: &Yun139Options) -> Result<Arc<Self>, DriverError> {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(options.timeout_secs))
            .user_agent(concat!("allins3/", env!("CARGO_PKG_VERSION")))
            .build()
            .map_err(|e| {
                DriverError::internal("failed to build the yun139 HTTP client").with_cause(e)
            })?;

        let basic = crate::integrity::base64(
            format!("pc:{}:{}", options.phone, options.auth_token.expose()).as_bytes(),
        );

        Ok(Arc::new(Self {
            http,
            base_url: options.base_url.clone(),
            authorization: format!("Basic {basic}"),
            cookie: options.cookie.as_ref().map(|c| c.expose().to_string()),
            page_size: options.page_size,
        }))
    }

    /// Signs and sends one JSON request, returning the payload.
    pub(crate) async fn post(&self, path: &str, body: &Value) -> Result<Value, DriverError> {
        let headers = self.signed_headers(body)?;
        let url = format!("{}{}", self.base_url, path);
        let response = self
            .http
            .post(&url)
            .headers(headers)
            .json(body)
            .send()
            .await
            .map_err(map_transport_error)?;

        let status = response.status();
        let text = response.text().await.map_err(map_transport_error)?;

        let envelope: Envelope =
            serde_json::from_str(&text).map_err(|e| map_status_or_parse(status, e))?;

        if envelope.success {
            return Ok(envelope.data.unwrap_or(Value::Null));
        }
        Err(api_error(status, &envelope.code, &envelope.message))
    }

    /// Lists one page of a folder.
    pub(crate) async fn list_children(
        &self,
        folder_id: &str,
        page_size: u32,
        cursor: Option<&str>,
    ) -> Result<ListChildren, DriverError> {
        let body = json!({
            "pageInfo": { "pageSize": page_size, "pageCursor": cursor },
            "orderBy": "updated_at",
            "orderDirection": "DESC",
            "parentFileId": folder_id,
            "imageThumbnailStyleList": ["Small", "Large"],
        });
        let data = self.post("/hcy/file/list", &body).await?;
        let Some(object) = data.as_object() else {
            return Ok(ListChildren {
                items: Vec::new(),
                next_page_cursor: None,
            });
        };
        let items = match object.get("items") {
            Some(value) => serde_json::from_value::<Vec<FileItem>>(value.clone())
                .map_err(|e| internal("file/list returned an unexpected item shape", e))?,
            None => Vec::new(),
        };
        let next_page_cursor = object
            .get("nextPageCursor")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .map(str::to_string);
        Ok(ListChildren {
            items,
            next_page_cursor,
        })
    }

    /// Mints a presigned download descriptor for a file.
    pub(crate) async fn download_info(&self, file_id: &str) -> Result<DownloadInfo, DriverError> {
        let body = json!({ "fileId": file_id });
        let data = self.post("/hcy/file/getDownloadUrl", &body).await?;
        if data.is_null() {
            return Err(DriverError::not_found("the drive returned no download URL"));
        }
        serde_json::from_value(data)
            .map_err(|e| internal("file/getDownloadUrl returned an unexpected shape", e))
    }

    /// Probes the API with a single cheap, read-only call.
    pub(crate) async fn probe(&self, folder_id: &str) -> Result<(), DriverError> {
        self.list_children(folder_id, 1, None).await.map(|_| ())
    }

    /// Streams a presigned object URL, optionally over a byte range.
    ///
    /// The response body is bridged through a bounded channel so the returned
    /// stream is `Send + Sync` (a raw reqwest body stream is not) and so a
    /// consumer that stops reading applies back-pressure to the transfer.
    pub(crate) async fn download_stream(
        &self,
        url: &str,
        range: Option<ResolvedRange>,
    ) -> Result<crate::driver::ObjectByteStream, DriverError> {
        let mut request = self.http.get(url);
        if let Some(range) = range {
            request = request.header(
                "Range",
                format!("bytes={}-{}", range.start(), range.end_inclusive()),
            );
        }
        let response = request.send().await.map_err(map_transport_error)?;

        let status = response.status();
        if !status.is_success() {
            return Err(http_status_error(status));
        }

        let (sender, receiver) = mpsc::channel::<Result<Bytes, DriverError>>(4);
        tokio::spawn(async move {
            let stream = response.bytes_stream();
            let mut stream = std::pin::pin!(stream);
            loop {
                let next = std::future::poll_fn(|cx| stream.as_mut().poll_next(cx)).await;
                match next {
                    Some(Ok(chunk)) => {
                        if sender.send(Ok(chunk)).await.is_err() {
                            break;
                        }
                    }
                    Some(Err(e)) => {
                        let _ = sender
                            .send(Err(DriverError::new(
                                DriverErrorKind::TemporarilyUnavailable,
                                "the presigned download failed mid-stream",
                            )
                            .with_cause(e)))
                            .await;
                        break;
                    }
                    None => break,
                }
            }
        });

        Ok(Box::pin(ChannelStream { receiver }))
    }

    /// The configured default page size.
    pub(crate) fn page_size(&self) -> u32 {
        self.page_size
    }

    /// Builds the full header set for one signed request.
    fn signed_headers(&self, body: &Value) -> Result<HeaderMap, DriverError> {
        let timestamp = super::stamp::now_shanghai();
        let random = sign::random_token();
        let signature = sign::signature(Some(body), &timestamp, &random);

        let mut headers = HeaderMap::with_capacity(STATIC_HEADERS.len() + 4);
        for (name, value) in STATIC_HEADERS {
            headers.insert(header_name(name), header_value(name, value)?);
        }
        headers.insert(
            header_name("Authorization"),
            header_value("Authorization", &self.authorization)?,
        );
        headers.insert(
            header_name("mcloud-sign"),
            header_value("mcloud-sign", &format!("{timestamp},{random},{signature}"))?,
        );
        if let Some(cookie) = &self.cookie {
            headers.insert(
                HeaderName::from_static("cookie"),
                header_value("Cookie", cookie)?,
            );
        }
        Ok(headers)
    }
}

fn header_name(name: &str) -> HeaderName {
    HeaderName::from_bytes(name.as_bytes()).expect("static header name")
}

fn header_value(name: &str, value: &str) -> Result<HeaderValue, DriverError> {
    HeaderValue::from_str(value).map_err(|e| {
        DriverError::internal(format!("invalid header value for `{name}`")).with_cause(e)
    })
}

/// A stream over a channel of object chunks.
struct ChannelStream {
    receiver: mpsc::Receiver<Result<Bytes, DriverError>>,
}

impl Stream for ChannelStream {
    type Item = Result<Bytes, DriverError>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.receiver.poll_recv(cx)
    }
}

fn internal(message: &str, cause: impl std::error::Error + Send + Sync + 'static) -> DriverError {
    DriverError::internal(message).with_cause(cause)
}

/// Maps a reqwest transport failure onto a stable driver error kind.
fn map_transport_error(error: reqwest::Error) -> DriverError {
    let kind = if error.is_timeout() || error.is_connect() {
        DriverErrorKind::TemporarilyUnavailable
    } else {
        DriverErrorKind::Internal
    };
    DriverError::new(kind, "the 139 API request failed").with_cause(error)
}

fn map_status_or_parse(status: reqwest::StatusCode, error: serde_json::Error) -> DriverError {
    if !status.is_success() {
        return http_status_error(status);
    }
    DriverError::internal("the 139 API returned a non-JSON response").with_cause(error)
}

fn http_status_error(status: reqwest::StatusCode) -> DriverError {
    match status.as_u16() {
        401 | 403 => DriverError::access_denied("the drive rejected the request credentials"),
        404 => DriverError::not_found("the drive resource does not exist"),
        429 => DriverError::throttled("the drive is rate limiting the request"),
        code if (500..600).contains(&code) => {
            DriverError::temporarily_unavailable("the drive reported a server error")
        }
        _ => DriverError::internal(format!("the drive returned HTTP {status}")),
    }
}

/// Maps a 139 envelope failure onto a stable driver error kind.
fn api_error(status: reqwest::StatusCode, code: &str, message: &str) -> DriverError {
    if !status.is_success() {
        return http_status_error(status);
    }
    let unauthorized = code.contains("401")
        || message.contains("鉴权")
        || message.contains("登录")
        || message.contains("token");
    if unauthorized {
        return DriverError::access_denied("the drive session is not authorized");
    }
    if code.contains("404") || message.contains("不存在") {
        return DriverError::not_found("the drive resource does not exist");
    }
    if code.starts_with("0400") {
        // Request-shape rejections (for example `04000003 请求头不合法`) mean
        // this driver built a request the service refused; that is our bug.
        return DriverError::internal(format!("the drive rejected the request ({code})"));
    }
    DriverError::internal(format!("the drive returned an error ({code})"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn file_items_parse_the_camel_case_shape() {
        let item: FileItem = serde_json::from_str(
            r#"{"fileId":"F1","name":"a.bin","type":"file","size":12,
                "contentHash":"abc","contentHashAlgorithm":"sha256",
                "revisionId":"r1","updatedAt":"2026-04-06T15:27:23.787+08:00"}"#,
        )
        .expect("parses");
        assert_eq!(item.file_id, "F1");
        assert_eq!(item.size, Some(12));
        assert!(!item.is_folder());
        assert_eq!(item.revision_id.as_deref(), Some("r1"));
    }

    #[test]
    fn folders_have_no_size_and_are_recognised() {
        let item: FileItem = serde_json::from_str(
            r#"{"fileId":"/synbox","name":"同步盘","type":"folder","size":null}"#,
        )
        .expect("parses");
        assert!(item.is_folder());
        assert_eq!(item.size, None);
    }

    #[test]
    fn unauthorized_codes_map_to_access_denied() {
        assert_eq!(
            api_error(reqwest::StatusCode::OK, "200000401", "").kind(),
            DriverErrorKind::AccessDenied
        );
    }

    #[test]
    fn a_rejected_request_shape_is_internal() {
        assert_eq!(
            api_error(reqwest::StatusCode::OK, "04000003", "请求头不合法").kind(),
            DriverErrorKind::Internal
        );
    }

    #[test]
    fn http_statuses_map_to_stable_kinds() {
        assert_eq!(
            http_status_error(reqwest::StatusCode::TOO_MANY_REQUESTS).kind(),
            DriverErrorKind::Throttled
        );
        assert_eq!(
            http_status_error(reqwest::StatusCode::UNAUTHORIZED).kind(),
            DriverErrorKind::AccessDenied
        );
    }
}
