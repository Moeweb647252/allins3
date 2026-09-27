//! HTTP server and process lifecycle.
//!
//! Startup order: parse/validate config → build driver instances → open and
//! capability-check namespaces → recover state and clean up → readiness →
//! accept traffic. Shutdown stops
//! accepting, waits for in-flight requests up to a deadline, closes every
//! driver instance, then exits (spec §7.2).
//!
//! `/healthz` is liveness and `/readyz` is readiness: the latter reports one
//! process draining flag *and* the health of every driver instance, so a
//! replica whose backend is unreachable leaves the load balancer.

use std::convert::Infallible;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use bytes::Bytes;
use hyper::body::Incoming;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::{TokioExecutor, TokioIo, TokioTimer};
use hyper_util::server::conn::auto::Builder as ConnBuilder;
use tokio::net::TcpListener;

use crate::compose;
use crate::config::{AppConfig, EnvSecretResolver};
use crate::domain::{DriverContext, RequestId};
use crate::driver::{DriverHealth, DriverRegistry};
use crate::protocol::s3::{HttpError, HttpResponse, S3Body, S3Error, S3ErrorCode, S3Service};
use crate::timeouts::IdleTimeoutBody;

/// How long the process waits for in-flight operations to drain, and then for
/// driver instances to close, before exiting anyway (spec §7.2).
const SHUTDOWN_DEADLINE: Duration = Duration::from_secs(10);

/// How long a readiness probe waits for the driver instances to answer.
///
/// `/readyz` is polled by a load balancer and the probe has no request deadline
/// of its own, so a driver that never answers would hold the probe (and the
/// poller) open. The health call is bounded here, the way shutdown is bounded
/// by [`SHUTDOWN_DEADLINE`].
const HEALTH_PROBE_TIMEOUT: Duration = Duration::from_secs(5);

/// Proves a configuration can actually start in this build, without serving.
///
/// Validation rejects a structurally invalid file, but whether a driver `kind`
/// exists is a property of the build, not the file: a kind that is not
/// registered here can never be constructed. Beyond registration, this runs
/// each factory's `validate`, so a private option a driver rejects is caught
/// here rather than at `run`. `check-config` calls this so an operator finds
/// that out from the preflight rather than from the listener failing to bind,
/// which is what spec §7.1 means by failing before startup.
pub async fn preflight(config: &AppConfig) -> Result<(), String> {
    compose::preflight(config, &EnvSecretResolver).await
}

/// Starts the service and serves until a shutdown signal.
pub async fn run(config: AppConfig) -> Result<(), String> {
    log(&format!("config: {}", config.redacted_summary()));

    // Wiring lives in one place (`compose`); this function owns only the
    // process concerns: the listener, the sweeper's lifecycle, and shutdown.
    let mut registry = compose::build_registry(&config)?;
    let instances =
        compose::instances_from_config(&mut registry, &config, &EnvSecretResolver).await?;
    // The instance table outlives composition: readiness probes every instance
    // through it, and shutdown closes them through the same table, so both see
    // exactly the instances that were created.
    let registry = Arc::new(registry);

    // Make the deployment's identity visible: a driver reports its kind and
    // version, and this is the one place an operator sees them without reading
    // the config back. Nothing here is secret.
    for descriptor in registry.descriptors() {
        match &descriptor.version {
            Some(version) => log(&format!(
                "driver id={} kind={} name=\"{}\" version={version}",
                descriptor.instance_id.as_str(),
                descriptor.kind,
                descriptor.display_name,
            )),
            None => log(&format!(
                "driver id={} kind={} name=\"{}\"",
                descriptor.instance_id.as_str(),
                descriptor.kind,
                descriptor.display_name,
            )),
        }
    }

    // `object_service_with_state` ends with recovery, so this returning is what
    // "recovered" means: the readiness flag below is only ever set after it, so
    // a replica never serves traffic before it has reconciled what its
    // predecessor left behind (`ADR-0013`).
    let service = compose::object_service_with_state(
        config.catalog.clone(),
        instances,
        config.limits.clone(),
        config.access.clone(),
        !config.development,
        config.state_dir.clone(),
    )
    .await
    .map_err(|e| format!("startup capability check failed: {e}"))?;

    // Report what recovery restored and reconciled. A non-zero `pending` is the
    // one an operator must act on: an upload whose completion outcome could not
    // be proven is retained, never replayed.
    if let Some(report) = service.recovery_report() {
        log(&format!(
            "multipart recovery: sessions={} parts={} committed={} retryable={} pending={} reclaimed={}",
            report.sessions_restored,
            report.parts_restored,
            report.committed,
            report.retryable,
            report.pending,
            report.reclaimed_uploads,
        ));
    }

    let ready = Arc::new(AtomicBool::new(true));
    let credentials = Arc::new(config.credentials.clone());
    let policy = Arc::new(config.access.clone());
    let s3_service = compose::s3_service(service.clone(), credentials, policy, &config.limits);

    // Each bound is resolved into `None` when disabled, so the enforcement
    // points can treat "no bound" as a plain absence rather than a sentinel.
    let http = HttpService {
        s3: s3_service,
        ready: ready.clone(),
        registry: registry.clone(),
        health_endpoints: config.health_endpoints,
        metrics: Arc::new(Metrics::new()),
        body_idle_timeout: (config.limits.body_idle_timeout > Duration::ZERO)
            .then_some(config.limits.body_idle_timeout),
        request_timeout: config.limits.request_timeout,
    };

    let listener = TcpListener::bind(&config.listen)
        .await
        .map_err(|e| format!("failed to bind {}: {e}", config.listen))?;
    log(&format!("listening on http://{}", config.listen));

    // The multipart staging sweeper: staged parts of an abandoned upload hold
    // the staging quota forever unless something reclaims them, so the server
    // owns the task, gives it a cancel path and joins it before exit (spec §10).
    // Its lease is measured from the upload's last activity, so an upload that
    // is still receiving parts is never reclaimed under a client.
    let (sweep_stop, sweep_stopped) = tokio::sync::watch::channel(false);
    let mut sweeper = config.limits.multipart_sweep_interval.map(|interval| {
        let service = service.clone();
        let lease = config.limits.multipart_lease;
        tokio::spawn(async move {
            service
                .run_multipart_sweeper(
                    lease,
                    interval,
                    async move {
                        let mut stop = sweep_stopped;
                        let _ = stop.changed().await;
                    },
                    |outcome| {
                        if outcome.reclaimed_uploads > 0 {
                            log(&format!(
                                "multipart sweep reclaimed {} upload(s) and {} byte(s)",
                                outcome.reclaimed_uploads, outcome.reclaimed_bytes
                            ));
                        }
                    },
                )
                .await;
        })
    });

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
                // The header timeout belongs to the header read, not to the
                // whole connection: wrapping the connection future cut every
                // transfer that outlived it (spec §9.3 forbids one short total
                // timeout blocking all large objects).
                let header_timeout = (config.limits.header_timeout.as_secs() > 0)
                    .then_some(config.limits.header_timeout);
                tokio::spawn(async move {
                    let io = TokioIo::new(stream);
                    let mut builder = ConnBuilder::new(TokioExecutor::new());
                    builder
                        .http1()
                        .timer(TokioTimer::new())
                        .header_read_timeout(header_timeout);
                    let conn = builder.serve_connection_with_upgrades(io, http);
                    if let Err(e) = conn.await {
                        log(&format!("connection error from {peer}: {e}"));
                    }
                });
            }
            _ = &mut shutdown => {
                log("shutdown signal received; draining");
                break;
            }
        }
    }

    ready.store(false, Ordering::SeqCst);
    // Stop the sweeper before draining, so it cannot hold the multipart lock
    // while in-flight requests finish.
    let _ = sweep_stop.send(true);
    if let Some(sweeper) = sweeper.take() {
        let _ = sweeper.await;
    }
    // Server-wide admission lives in the service (spec §9.3), so waiting for
    // every one of its slots is the same "no operation is still running"
    // signal the listener semaphore used to provide.
    if tokio::time::timeout(SHUTDOWN_DEADLINE, service.drain())
        .await
        .is_err()
    {
        log("drain deadline exceeded; exiting with in-flight requests");
    }
    // Close every driver instance once no operation can reach it: an instance
    // may hold a connection, a background queue or unflushed state, and the
    // process is about to exit, so this is the last point at which it can be
    // released. A failure is logged, never fatal — exiting with one driver
    // still open beats hanging the process (spec §7.2).
    let shutdown_ctx = DriverContext::new(RequestId::new("shutdown"));
    match tokio::time::timeout(SHUTDOWN_DEADLINE, registry.shutdown_all(&shutdown_ctx)).await {
        Ok(failures) => {
            for (id, error) in failures {
                log(&format!("driver `{id}` shutdown failed: {error}"));
            }
        }
        Err(_) => log("driver shutdown deadline exceeded; exiting"),
    }
    log("shutdown complete");
    Ok(())
}

/// Resolves when the process is asked to stop: `SIGINT` (Ctrl-C) or `SIGTERM`.
///
/// `SIGTERM` is the signal a process manager actually sends (systemd, a
/// container runtime, launchd). `ctrl_c` alone handles only `SIGINT`, so under
/// `SIGTERM` the kernel's default disposition killed the process with in-flight
/// work still running — the ordered drain spec §7.2 requires was never reached,
/// which is what the plan's P5-03 means by 真实进程信号.
async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};

        let mut interrupt = match signal(SignalKind::interrupt()) {
            Ok(stream) => stream,
            Err(error) => {
                log(&format!("cannot register SIGINT handler: {error}"));
                let _ = tokio::signal::ctrl_c().await;
                return;
            }
        };
        let mut terminate = match signal(SignalKind::terminate()) {
            Ok(stream) => stream,
            Err(error) => {
                log(&format!("cannot register SIGTERM handler: {error}"));
                let _ = interrupt.recv().await;
                return;
            }
        };
        tokio::select! {
            _ = interrupt.recv() => {}
            _ = terminate.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
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
    s3: S3Service,
    ready: Arc<AtomicBool>,
    /// The live instances readiness probes and shutdown closes.
    registry: Arc<DriverRegistry>,
    health_endpoints: bool,
    metrics: Arc<Metrics>,
    /// Body idle timeout, applied to every request body (spec §9.3).
    body_idle_timeout: Option<Duration>,
    /// Total request deadline, applied around the whole S3 call.
    request_timeout: Option<Duration>,
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
                    let registry = self.registry.clone();
                    return Box::pin(async move {
                        if !ready {
                            return Ok(text_response(StatusCode::SERVICE_UNAVAILABLE, "draining"));
                        }
                        // Readiness is per instance, not one process flag: a
                        // replica whose backend is unreachable must leave the
                        // load balancer even while its listener still accepts,
                        // and only the instance knows that (spec §7.2).
                        let ctx = DriverContext::new(RequestId::new("readyz"))
                            .with_timeout(HEALTH_PROBE_TIMEOUT);
                        let health =
                            match tokio::time::timeout(HEALTH_PROBE_TIMEOUT, registry.health(&ctx))
                                .await
                            {
                                Ok(health) => health,
                                Err(_) => {
                                    log("readiness probe timed out waiting for drivers");
                                    return Ok(text_response(
                                        StatusCode::SERVICE_UNAVAILABLE,
                                        "readiness probe timed out",
                                    ));
                                }
                            };
                        let mut unhealthy: Vec<String> = health
                            .into_iter()
                            .filter(|(_, health)| !health.is_serving())
                            .map(|(id, health)| {
                                // The reason is written for an operator and may
                                // name backend detail (a host, a connection
                                // string). `/readyz` is unauthenticated, so the
                                // body names only the instance and the reason
                                // goes to the log.
                                if let DriverHealth::Unhealthy { reason } = &health {
                                    log(&format!(
                                        "driver `{}` is unhealthy: {reason}",
                                        id.as_str()
                                    ));
                                }
                                id.as_str().to_string()
                            })
                            .collect();
                        if unhealthy.is_empty() {
                            return Ok(text_response(StatusCode::OK, "ready"));
                        }
                        unhealthy.sort();
                        Ok(owned_text_response(
                            StatusCode::SERVICE_UNAVAILABLE,
                            format!("unhealthy drivers: {}", unhealthy.join(", ")),
                        ))
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
        let body_idle_timeout = self.body_idle_timeout;
        let request_timeout = self.request_timeout;
        Box::pin(async move {
            metrics.requests.fetch_add(1, Ordering::Relaxed);
            metrics.inflight.fetch_add(1, Ordering::Relaxed);
            let guard = InflightGuard(metrics.clone());
            // The body idle bound wraps the body itself, so a client that stops
            // sending is abandoned without ever putting a timer on a response
            // being written to a slow reader (spec §9.3).
            let (parts, body) = req.into_parts();
            let idled = Arc::new(AtomicBool::new(false));
            let request = Request::from_parts(
                parts,
                S3Body::http_body_unsync(IdleTimeoutBody::new(
                    body,
                    body_idle_timeout,
                    idled.clone(),
                )),
            );
            let call = s3.call(request);
            // The total deadline is the one bound around the whole call. It is
            // off unless configured, so by default it never cuts off a large
            // object.
            let result = match request_timeout {
                Some(limit) => match tokio::time::timeout(limit, call).await {
                    Ok(result) => result,
                    Err(_) => Ok(request_timeout_response()),
                },
                None => call.await,
            };
            let mut response = result.unwrap_or_else(internal_error);
            // A control body (XML) is read by the protocol library, which reports
            // a body error as an internal error; the client's stall must still
            // be a client error (spec §9.1).
            if idled.load(Ordering::Relaxed) {
                response = body_idle_response();
            }
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
    let mut response = Response::new(S3Body::from(Bytes::from_static(body.as_bytes())));
    *response.status_mut() = status;
    response
}

fn owned_text_response(status: StatusCode, body: String) -> HttpResponse {
    let mut response = Response::new(S3Body::from(Bytes::from(body)));
    *response.status_mut() = status;
    response
}

/// The response to a request whose body went idle (spec §9.3).
///
/// The client's stall is not a server fault, so it is reported as
/// `400 InvalidRequest` and counted as a client error.
fn body_idle_response() -> HttpResponse {
    S3Error::with_message(
        S3ErrorCode::InvalidRequest,
        "The request body was idle for longer than the configured body idle timeout",
    )
    .to_http_response()
    .unwrap_or_else(|_| text_response(StatusCode::BAD_REQUEST, "request body idle"))
}

/// The response to a request that outlived the configured total deadline.
///
/// `RequestTimeout` is the S3 code for a request cut off while still in flight:
/// a client-visible timeout, not a server fault.
fn request_timeout_response() -> HttpResponse {
    S3Error::with_message(
        S3ErrorCode::RequestTimeout,
        "The request took longer than the configured total deadline",
    )
    .to_http_response()
    .unwrap_or_else(|_| text_response(StatusCode::REQUEST_TIMEOUT, "request timeout"))
}

fn internal_error(_e: HttpError) -> HttpResponse {
    let mut response = Response::new(S3Body::from(Bytes::from_static(b"internal error")));
    *response.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
    response
}
