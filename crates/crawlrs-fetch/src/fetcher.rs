//! [`Fetcher`] impl backed by `wreq`.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use crawlrs_core::{
    CanonicalUrl, Error, FetchRequest, FetchResponse, Fetcher, ProxyOutcome, RedirectHop, Result,
};
use futures::StreamExt;
use wreq::tls::session::{Key, TlsSession, TlsSessionCache};
use wreq::{Client, redirect};
use wreq_util::Emulation;

use crate::config::WreqFetcherConfig;

/// `TlsSessionCache` impl that discards every session BoringSSL hands
/// us. wreq's default LRU cache keeps each server's parsed X.509 chain
/// (held inside the cached `SslSession`); the outer host-map is not
/// globally capped, so under random emulation across many unique hosts
/// the cache grows linearly with (host, profile) tuples. With
/// `pool_max_idle_per_host = 0` we don't reuse TLS connections anyway,
/// so PSK resumption can't pay off; dropping the session lets the
/// parsed certs fall out of scope at end-of-handshake.
#[derive(Default)]
struct NoopTlsSessionCache;

impl TlsSessionCache for NoopTlsSessionCache {
    fn put(&self, _key: Key, _session: TlsSession) {}
    fn pop(&self, _key: &Key) -> Option<TlsSession> {
        None
    }
}

pub struct WreqFetcher {
    client: Client,
    config: WreqFetcherConfig,
}

impl WreqFetcher {
    pub fn new(config: WreqFetcherConfig) -> Result<Self> {
        let mut client_builder = Client::builder()
            .connect_timeout(config.connect_timeout)
            .read_timeout(config.read_timeout)
            .timeout(config.default_timeout)
            .tcp_keepalive(Duration::from_secs(30))
            .tcp_nodelay(true)
            .pool_max_idle_per_host(config.pool_max_idle_per_host)
            .redirect(redirect::Policy::limited(config.max_redirects))
            .tls_session_cache(NoopTlsSessionCache);

        // Pinned profile -> lock it on the client so the connection
        // pool can fully reuse. Unpinned -> leave the client neutral;
        // `fetch()` attaches a fresh `Emulation::random()` per request
        // for fingerprint diversity. Clone here because `Emulation` no
        // longer implements `Copy` upstream and `config` is moved into
        // `Self` below.
        if let Some(emulation) = config.emulation.clone() {
            client_builder = client_builder.emulation(emulation);
        }

        if let Some(user_agent_override) = &config.user_agent {
            client_builder = client_builder.user_agent(user_agent_override.as_str());
        }

        if let Some(ca_pem_bytes) = config.proxy.trusted_ca_pem() {
            let cert_store =
                wreq::tls::trust::CertStore::from_pem_stack(ca_pem_bytes).map_err(|err| {
                    Error::Fetch(format!("invalid CA PEM from proxy resolver: {err}"))
                })?;
            client_builder = client_builder.tls_cert_store(cert_store);
        }

        let client = client_builder
            .build()
            .map_err(|err| Error::Fetch(format!("wreq client build failed: {err}")))?;

        Ok(Self { client, config })
    }

    pub fn config(&self) -> &WreqFetcherConfig {
        &self.config
    }
}

#[async_trait]
impl Fetcher for WreqFetcher {
    #[tracing::instrument(
        skip(self, request),
        fields(url = %request.url, timeout_ms = request.timeout.as_millis() as u64)
    )]
    async fn fetch(&self, request: FetchRequest) -> Result<FetchResponse> {
        let kind_label = crate::classify::fetch_kind(&request.url);
        let proxy_selection = self.config.proxy.resolve(&request).await?;
        let started_at = chrono::Utc::now();
        let start_instant = Instant::now();

        let mut request_builder = self
            .client
            .get(request.url.as_str())
            .timeout(request.timeout);

        // No pinned profile -> roll a fresh random emulation per
        // request. The client-level fingerprint stays unset so the
        // per-request override fully governs TLS / HTTP/2 / headers
        // for this fetch only. wreq pools connections per-emulation,
        // so a same-emulation hit can still reuse a pooled connection
        // on the next request that happens to land on the same profile.
        if self.config.emulation.is_none() {
            request_builder = request_builder.emulation(Emulation::random());
        }

        for (header_name, header_value) in &request.headers {
            request_builder = request_builder.header(header_name.as_str(), header_value.as_str());
        }
        if let Some(active_proxy) = &proxy_selection {
            let wreq_proxy = wreq::Proxy::all(active_proxy.url.as_str()).map_err(|err| {
                Error::Fetch(format!("invalid proxy url {}: {err}", active_proxy.url))
            })?;
            request_builder = request_builder.proxy(wreq_proxy);
            for (header_name, header_value) in &active_proxy.extra_headers {
                request_builder =
                    request_builder.header(header_name.as_str(), header_value.as_str());
            }
        }

        let result: Result<FetchResponse> = match request_builder.send().await {
            Ok(response) => {
                // Stage split: `request` covers connect + TLS + send +
                // headers (everything wreq did before returning);
                // `body` covers the streaming body read below. Total is
                // emitted separately so dashboards can compare the
                // sum-of-parts to the wall-clock to spot drift.
                let request_elapsed = start_instant.elapsed();
                let body_started_at = Instant::now();

                let status_code = response.status().as_u16();
                let final_uri_string = response.uri().to_string();
                let response_headers = response
                    .headers()
                    .iter()
                    .filter_map(|(header_name, header_value)| {
                        header_value.to_str().ok().map(|value_str| {
                            (header_name.as_str().to_string(), value_str.to_string())
                        })
                    })
                    .collect::<HashMap<_, _>>();
                let content_type_label = crate::classify::content_type(
                    response_headers.get("content-type").map(String::as_str),
                );
                let redirect_history = response.extensions().get::<redirect::History>().cloned();

                // Pre-check Content-Length when the server provided it.
                // Cheap reject for adversarial servers that advertise a
                // huge body. For unknown-length / chunked responses we
                // still rely on the streaming guard below.
                if let Some(advertised_len) = response.content_length()
                    && advertised_len > self.config.max_body_bytes
                {
                    return Err(Error::Fetch(format!(
                        "response body content-length {advertised_len} exceeds cap {}",
                        self.config.max_body_bytes
                    )));
                }

                let response_body = read_body(response, self.config.max_body_bytes).await?;
                let body_elapsed = body_started_at.elapsed();
                metrics::histogram!(
                    crate::metrics::FETCH_STAGE_SECONDS,
                    "kind" => kind_label,
                    "stage" => crate::classify::STAGE_REQUEST,
                )
                .record(request_elapsed.as_secs_f64());
                metrics::histogram!(
                    crate::metrics::FETCH_STAGE_SECONDS,
                    "kind" => kind_label,
                    "stage" => crate::classify::STAGE_BODY,
                )
                .record(body_elapsed.as_secs_f64());
                metrics::histogram!(
                    crate::metrics::FETCH_BODY_BYTES,
                    "kind" => kind_label,
                    "content_type" => content_type_label,
                )
                .record(response_body.len() as f64);

                let final_url =
                    CanonicalUrl::parse(&final_uri_string).unwrap_or_else(|_| request.url.clone());
                let redirect_chain: smallvec::SmallVec<[RedirectHop; 4]> = redirect_history
                    .map(|history| {
                        history
                            .into_iter()
                            .map(|history_entry| RedirectHop {
                                from: CanonicalUrl::parse(&history_entry.previous.to_string())
                                    .unwrap_or_else(|_| request.url.clone()),
                                to: CanonicalUrl::parse(&history_entry.uri.to_string())
                                    .unwrap_or_else(|_| request.url.clone()),
                                status: history_entry.status.as_u16(),
                            })
                            .collect()
                    })
                    .unwrap_or_default();

                if let Some(active_proxy) = &proxy_selection {
                    let proxy_outcome = classify_outcome(status_code);
                    self.config.proxy.report(active_proxy, proxy_outcome).await;
                }

                Ok(FetchResponse {
                    url: final_url,
                    status: status_code,
                    headers: Box::new(response_headers),
                    body: response_body,
                    redirect_chain,
                    fetched_at: started_at,
                    duration: start_instant.elapsed(),
                })
            }
            Err(send_error) => {
                let proxy_outcome = if send_error.is_timeout() {
                    ProxyOutcome::Timeout
                } else {
                    ProxyOutcome::NetworkError
                };
                if let Some(active_proxy) = &proxy_selection {
                    self.config.proxy.report(active_proxy, proxy_outcome).await;
                }
                Err(Error::Fetch(format!("{send_error}")))
            }
        };

        if let Ok(resp) = &result {
            metrics::histogram!(
                crate::metrics::FETCH_SECONDS,
                "kind" => kind_label,
            )
            .record(resp.duration.as_secs_f64());
            metrics::histogram!(
                crate::metrics::FETCH_STAGE_SECONDS,
                "kind" => kind_label,
                "stage" => crate::classify::STAGE_TOTAL,
            )
            .record(resp.duration.as_secs_f64());
            metrics::counter!(
                crate::metrics::FETCH_RESPONSE_TOTAL,
                "status_class" => crate::classify::status_class(resp.status),
            )
            .increment(1);
        }
        result
    }
}

/// Map an HTTP status code to a proxy-health verdict. `403`/`429`
/// indicate the proxy itself was burned on the destination
/// (anti-bot challenge / rate-limit attribution); everything else
/// is treated as success from the proxy's perspective even if it's
/// a 5xx - the proxy did its job, the upstream just returned what
/// it returned.
fn classify_outcome(status: u16) -> ProxyOutcome {
    match status {
        403 | 429 => ProxyOutcome::Banned,
        _ => ProxyOutcome::Success,
    }
}

/// Read the response body via `bytes_stream`, aborting once
/// cumulative bytes cross `cap`. Bounded memory regardless of whether
/// the server advertised a Content-Length - chunked-transfer
/// responses can otherwise stream forever, and a content-length
/// pre-check (done by the caller) only catches honest servers.
///
/// Pre-sizes the accumulator from the Content-Length header when one
/// is present and within `cap`, clamping above; this avoids the
/// 4 -> 8 -> 16 -> ... realloc chain on every fetch (the standard
/// growth strategy doubles capacity, freeing each intermediate
/// allocation, which both costs CPU and fragments the heap).
async fn read_body(response: wreq::Response, cap: u64) -> Result<Bytes> {
    let preset = response
        .headers()
        .get(wreq::header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok())
        .map(|n| n.min(cap) as usize)
        .unwrap_or(0);
    let mut stream = response.bytes_stream();
    let mut buf = BytesMut::with_capacity(preset);
    while let Some(chunk_result) = stream.next().await {
        let chunk = chunk_result.map_err(|err| Error::Fetch(format!("body read failed: {err}")))?;
        if (buf.len() as u64).saturating_add(chunk.len() as u64) > cap {
            return Err(Error::Fetch(format!(
                "response body exceeds cap of {cap} bytes during streaming"
            )));
        }
        buf.extend_from_slice(&chunk);
    }
    Ok(buf.freeze())
}
