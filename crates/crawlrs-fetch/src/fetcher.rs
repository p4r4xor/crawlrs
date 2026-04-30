//! [`Fetcher`] impl backed by `wreq`.

use std::collections::HashMap;
use std::time::Instant;

use async_trait::async_trait;
use crawlrs_core::{
    CanonicalUrl, Error, FetchRequest, FetchResponse, Fetcher, ProxyOutcome, RedirectHop, Result,
};
use wreq::{Client, redirect};

use crate::config::WreqFetcherConfig;

pub struct WreqFetcher {
    client: Client,
    config: WreqFetcherConfig,
}

impl WreqFetcher {
    pub fn new(config: WreqFetcherConfig) -> Result<Self> {
        let mut client_builder = Client::builder()
            .emulation(config.emulation.clone())
            .connect_timeout(config.connect_timeout)
            .timeout(config.default_timeout)
            .pool_max_idle_per_host(config.pool_max_idle_per_host)
            .redirect(redirect::Policy::limited(config.max_redirects));

        if let Some(user_agent_override) = &config.user_agent {
            client_builder = client_builder.user_agent(user_agent_override.as_str());
        }

        if let Some(ca_pem_bytes) = config.proxy.trusted_ca_pem() {
            let cert_store = wreq::tls::CertStore::from_pem_stack(ca_pem_bytes)
                .map_err(|err| Error::Fetch(format!("invalid CA PEM from proxy resolver: {err}")))?;
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

        let send_result = request_builder.send().await;

        match send_result {
            Ok(response) => {
                let status_code = response.status().as_u16();
                let final_uri_string = response.uri().to_string();
                let response_headers = response
                    .headers()
                    .iter()
                    .filter_map(|(header_name, header_value)| {
                        header_value
                            .to_str()
                            .ok()
                            .map(|value_str| (header_name.as_str().to_string(), value_str.to_string()))
                    })
                    .collect::<HashMap<_, _>>();
                let redirect_history = response
                    .extensions()
                    .get::<redirect::History>()
                    .cloned();
                let response_body = response
                    .bytes()
                    .await
                    .map_err(|err| Error::Fetch(format!("body read failed: {err}")))?;

                let final_url = CanonicalUrl::parse(&final_uri_string)
                    .unwrap_or_else(|_| request.url.clone());
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
                    let proxy_outcome = classify_proxy_outcome_from_status(status_code);
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
        }
    }
}

fn classify_proxy_outcome_from_status(status: u16) -> ProxyOutcome {
    match status {
        403 | 429 => ProxyOutcome::Banned,
        _ => ProxyOutcome::Success,
    }
}
