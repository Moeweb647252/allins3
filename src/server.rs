//! HTTP server and process lifecycle.
//!
//! Startup order: parse/validate config → build driver instances → open and
//! capability-check namespaces → readiness → accept traffic. Shutdown stops
//! accepting, waits for in-flight requests up to a deadline, then exits
//! (spec §7.2).

use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use bytes::Bytes;
use hyper::body::Incoming;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder as ConnBuilder;
use s3s::HttpResponse;
use tokio::net::TcpListener;
use tokio::sync::Semaphore;

use crate::config::{Addressing, AppConfig};
use crate::domain::{DriverContext, DriverInstanceId, RequestId};
use crate::driver::{DriverFactory, DriverKind, DriverRegistry, StorageDriver};
use crate::protocol::s3::{PrincipalLimiter, S3Handler, build_service};
use crate::service::ObjectService;

/// Builds the driver registry with the kinds this build supports.
pub fn build_registry(config: &AppConfig) -> Result<DriverRegistry, String> {
    let mut registry = DriverRegistry::new();
    let kinds: Vec<String> = {
        let mut kinds: Vec<String> = config.drivers.iter().map(|d| d.kind.clone()).collect();
        kinds.sort();
        kinds.dedup();
        kinds
    };

    for kind in kinds {
        let factory: Arc<dyn DriverFactory> = match kind.as_str() {
            #[cfg(feature = "testing")]
            "test-reference" => Arc::new(crate::testing::reference::ReferenceDriverFactory::new(
                kind.clone(),
            )),
            #[cfg(feature = "testing")]
            "test-vec" => Arc::new(crate::testing::second::VecDriverFactory::new(kind.clone())),
            other => {
                return Err(format!(
                    "driver kind `{other}` is not registered in this build"
                ));
            }
        };
        registry
            .register(factory)
            .map_err(|e| format!("failed to register driver kind `{kind}`: {e}"))?;
    }
    Ok(registry)
}

/// Starts the service and serves until a shutdown signal.
pub async fn run(config: AppConfig) -> Result<(), String> {
    log(&format!("config: {}", config.redacted_summary()));

    let ctx = DriverContext::new(RequestId::new("startup"));
    let mut registry = build_registry(&config)?;

    // Create every driver instance referenced by a bucket.
    let mut instances: HashMap<String, Arc<dyn StorageDriver>> = HashMap::new();
    for driver in &config.drivers {
        let kind = DriverKind::new(driver.kind.clone());
        let instance_id = DriverInstanceId::parse(driver.id.clone())
            .map_err(|e| format!("invalid driver id `{}`: {e}", driver.id))?;
        let instance = registry
            .create_instance(&ctx, &kind, &instance_id, &crate::config::EnvSecretResolver)
            .await
            .map_err(|e| format!("failed to create driver `{}`: {e}", driver.id))?;
        instances.insert(driver.id.clone(), instance);
    }

    // Config validation already produced a duplicate-free catalog.
    let catalog = config.catalog.clone();

    let credentials = Arc::new(config.credentials.clone());
    let policy = Arc::new(config.access.clone());
    let service = ObjectService::new(
        catalog,
        instances,
        config.limits.clone(),
        config.access.clone(),
    );

    // Open and capability-check all namespaces before readiness.
    service
        .warmup(&ctx)
        .await
        .map_err(|e| format!("startup capability check failed: {e}"))?;

    let ready = Arc::new(AtomicBool::new(true));
    let s3_config = s3s_config(&config);
    let limiter = PrincipalLimiter::new(config.limits.max_concurrent_requests_per_principal);
    let handler = S3Handler::new(service.clone(), credentials.clone(), limiter);
    let s3_service = build_service(handler, credentials.clone(), policy, s3_config);

    let http = HttpService {
        s3: s3_service,
        ready: ready.clone(),
        health_endpoints: config.health_endpoints,
        metrics: Arc::new(Metrics::new()),
    };

    let listener = TcpListener::bind(&config.listen)
        .await
        .map_err(|e| format!("failed to bind {}: {e}", config.listen))?;
    log(&format!("listening on http://{}", config.listen));

    let inflight = Arc::new(Semaphore::new(config.limits.max_concurrent_requests));
    let mut shutdown = Box::pin(shutdown_signal());

    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let (stream, peer) = match accepted {
                    Ok(value) => value,
                    Err(e) => {
                        log(&format!("accept failed: {e}"));
                        continue;
                    }
                };
                let http = http.clone();
                let inflight = inflight.clone();
                let header_timeout = config.limits.header_timeout;
                tokio::spawn(async move {
                    let Ok(permit) = inflight.acquire_owned().await else {
                        return;
                    };
                    let io = TokioIo::new(stream);
                    let builder = ConnBuilder::new(TokioExecutor::new());
                    let conn = builder.serve_connection_with_upgrades(io, http);
                    match tokio::time::timeout(header_timeout, conn).await {
                        Ok(Ok(())) => {}
                        Ok(Err(e)) => log(&format!("connection error from {peer}: {e}")),
                        Err(_) => log(&format!("connection from {peer} timed out")),
                    }
                    drop(permit);
                });
            }
            _ = &mut shutdown => {
                log("shutdown signal received; draining");
                break;
            }
        }
    }

    ready.store(false, Ordering::SeqCst);
    let drain = async {
        let _ = inflight
            .acquire_many(config.limits.max_concurrent_requests as u32)
            .await;
    };
    if tokio::time::timeout(Duration::from_secs(10), drain)
        .await
        .is_err()
    {
        log("drain deadline exceeded; exiting with in-flight requests");
    }
    log("shutdown complete");
    Ok(())
}

fn s3s_config(config: &AppConfig) -> s3s::config::S3Config {
    let mut s3_config = s3s::config::S3Config::default();
    // SigV4 only (spec §6.1); the candidate defaults to off, assert it anyway.
    s3_config.enable_sig_v2 = false;
    // Preserve significant key bytes (`a//b` != `a/b`).
    s3_config.normalize_forward_slash_path = false;
    // Only accept the `s3` service scope.
    s3_config.sig_v4_allowed_services = vec!["s3".to_string()];
    s3_config.put_object_max_size = Some(config.limits.max_single_write_size);
    s3_config.xml_max_body_size = config.limits.max_xml_bytes;
    s3_config
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
}

fn log(message: &str) {
    eprintln!("allins3 level=info msg=\"{message}\"");
}

/// Process counters exposed at `GET /metrics`.
///
/// Deliberately small and dependency-free: counts that answer "is it serving,
/// failing, or saturated" without a metrics backend.
#[derive(Debug)]
struct Metrics {
    started: std::time::Instant,
    requests: AtomicU64,
    client_errors: AtomicU64,
    server_errors: AtomicU64,
    inflight: AtomicU64,
}

impl Metrics {
    fn new() -> Self {
        Self {
            started: std::time::Instant::now(),
            requests: AtomicU64::new(0),
            client_errors: AtomicU64::new(0),
            server_errors: AtomicU64::new(0),
            inflight: AtomicU64::new(0),
        }
    }

    fn render(&self) -> String {
        let order = Ordering::Relaxed;
        format!(
            "# TYPE allins3_requests_total counter\n\
             allins3_requests_total {}\n\
             # TYPE allins3_client_errors_total counter\n\
             allins3_client_errors_total {}\n\
             # TYPE allins3_server_errors_total counter\n\
             allins3_server_errors_total {}\n\
             # TYPE allins3_inflight_requests gauge\n\
             allins3_inflight_requests {}\n\
             # TYPE allins3_uptime_seconds gauge\n\
             allins3_uptime_seconds {}\n",
            self.requests.load(order),
            self.client_errors.load(order),
            self.server_errors.load(order),
            self.inflight.load(order),
            self.started.elapsed().as_secs(),
        )
    }
}

/// Decrements the in-flight gauge however the request ends.
struct InflightGuard(Arc<Metrics>);

impl Drop for InflightGuard {
    fn drop(&mut self) {
        self.0.inflight.fetch_sub(1, Ordering::Relaxed);
    }
}

/// The top-level HTTP service: health endpoints plus the S3 service.
#[derive(Clone)]
struct HttpService {
    s3: s3s::service::S3Service,
    ready: Arc<AtomicBool>,
    health_endpoints: bool,
    metrics: Arc<Metrics>,
}

impl hyper::service::Service<Request<Incoming>> for HttpService {
    type Response = HttpResponse;
    type Error = Infallible;
    type Future = std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<HttpResponse, Infallible>> + Send>,
    >;

    fn call(&self, req: Request<Incoming>) -> Self::Future {
        if self.health_endpoints {
            match (req.method().as_str(), req.uri().path()) {
                ("GET", "/healthz") => {
                    return Box::pin(async { Ok(text_response(StatusCode::OK, "ok")) });
                }
                ("GET", "/readyz") => {
                    let ready = self.ready.load(Ordering::SeqCst);
                    return Box::pin(async move {
                        if ready {
                            Ok(text_response(StatusCode::OK, "ready"))
                        } else {
                            Ok(text_response(StatusCode::SERVICE_UNAVAILABLE, "draining"))
                        }
                    });
                }
                ("GET", "/metrics") => {
                    let body = self.metrics.render();
                    return Box::pin(async move { Ok(owned_text_response(StatusCode::OK, body)) });
                }
                _ => {}
            }
        }
        let s3 = self.s3.clone();
        let metrics = self.metrics.clone();
        Box::pin(async move {
            metrics.requests.fetch_add(1, Ordering::Relaxed);
            metrics.inflight.fetch_add(1, Ordering::Relaxed);
            let guard = InflightGuard(metrics.clone());
            let result =
                <s3s::service::S3Service as hyper::service::Service<Request<Incoming>>>::call(
                    &s3, req,
                )
                .await;
            let response = result.unwrap_or_else(internal_error);
            match response.status().as_u16() {
                status if status >= 500 => {
                    metrics.server_errors.fetch_add(1, Ordering::Relaxed);
                }
                status if status >= 400 => {
                    metrics.client_errors.fetch_add(1, Ordering::Relaxed);
                }
                _ => {}
            }
            drop(guard);
            Ok(response)
        })
    }
}

fn text_response(status: StatusCode, body: &'static str) -> HttpResponse {
    let mut response = Response::new(s3s::Body::from(Bytes::from_static(body.as_bytes())));
    *response.status_mut() = status;
    response
}

fn owned_text_response(status: StatusCode, body: String) -> HttpResponse {
    let mut response = Response::new(s3s::Body::from(Bytes::from(body)));
    *response.status_mut() = status;
    response
}

fn internal_error(_e: s3s::HttpError) -> HttpResponse {
    let mut response = Response::new(s3s::Body::from(Bytes::from_static(b"internal error")));
    *response.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
    response
}

/// The addressing style, exposed for tests.
pub fn addressing(config: &AppConfig) -> Addressing {
    config.addressing
}
