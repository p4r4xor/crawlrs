//! [`Fetcher`] impl backed by `wreq`.

use std::collections::HashMap;
use std::time::Instant;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use crawlrs_core::{
    CanonicalUrl, Error, FetchRequest, FetchResponse, Fetcher, ProxyOutcome, RedirectHop, Result,
};
use futures::StreamExt;
use wreq::{Client, redirect};

use crate::config::WreqFetcherConfig;

pub struct WreqFetcher {
    client: Client,
    config: WreqFetcherConfig,
}

impl WreqFetcher {
    pub fn new(config: WreqFetcherConfig) -> Result<Self> {
        let mut client_builder = Client::builder()
            .emulation(config.emulation)
            .connect_timeout(config.connect_timeout)
            .timeout(config.default_timeout)
            .pool_max_idle_per_host(config.pool_max_idle_per_host)
            .redirect(redirect::Policy::limited(config.max_redirects));

        if let Some(user_agent_override) = &config.user_agent {
            client_builder = client_builder.user_agent(user_agent_override.as_str());
        }

        if let Some(ca_pem_bytes) = config.proxy.trusted_ca_pem() {
            let cert_store = wreq::tls::CertStore::from_pem_stack(ca_pem_bytes).map_err(|err| {
                Error::Fetch(format!("invalid CA PEM from proxy resolver: {err}"))
            })?;
            client_builder = client_builder.cert_store(cert_store);
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
    async fn fetch(&self, request: FetchRequest) -> Result<FetchResponse> {
        let kind_label = crate::metrics::fetch_kind_label(&request.url);
        let proxy_selection = self.config.proxy.resolve(&request).await?;
        let started_at = chrono::Utc::now();
        let start_instant = Instant::now();

        let mut request_builder = self
            .client
            .get(request.url.as_str())
            .timeout(request.timeout);

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

                let final_url =
                    CanonicalUrl::parse(&final_uri_string).unwrap_or_else(|_| request.url.clone());
                let redirect_chain = redirect_history
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
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();

                if let Some(active_proxy) = &proxy_selection {
                    let proxy_outcome = classify_outcome(status_code);
                    self.config.proxy.report(active_proxy, proxy_outcome).await;
                }

                Ok(FetchResponse {
                    url: final_url,
                    status: status_code,
                    headers: response_headers,
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
            metrics::counter!(
                crate::metrics::FETCH_RESPONSE_TOTAL,
                "status_class" => crate::metrics::status_class_label(resp.status),
            )
            .increment(1);
            metrics::histogram!(crate::metrics::FETCH_BODY_BYTES).record(resp.body.len() as f64);
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
async fn read_body(response: wreq::Response, cap: u64) -> Result<Bytes> {
    let mut stream = response.bytes_stream();
    let mut buf = BytesMut::new();
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
