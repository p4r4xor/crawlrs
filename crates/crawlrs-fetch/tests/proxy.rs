//! Tests for the built-in `ProxyResolver` implementations:
//! `NoProxyResolver`, `EnvProxyResolver`, `GatewayProxyResolver`.

#![allow(unsafe_code)] // env::set_var/remove_var are unsafe under Rust 2024

use std::collections::HashMap;

use crawlrs_core::{CanonicalUrl, FetchRequest, ProxyResolver};
use crawlrs_fetch::{EnvProxyResolver, GatewayProxyResolver, NoProxyResolver};

fn req() -> FetchRequest {
    FetchRequest::new(CanonicalUrl::parse("https://example.test/").unwrap())
}

#[tokio::test]
async fn no_proxy_returns_none() {
    let r = NoProxyResolver;
    assert!(r.resolve(&req()).await.unwrap().is_none());
}

#[tokio::test]
async fn env_resolver_reads_https_proxy() {
    // SAFETY: tests share process env; restore the prior value after.
    let prev = std::env::var("HTTPS_PROXY").ok();
    unsafe {
        std::env::set_var("HTTPS_PROXY", "http://proxy.example:8080");
    }
    let r = EnvProxyResolver::new();
    let sel = r.resolve(&req()).await.unwrap();
    assert_eq!(sel.unwrap().url, "http://proxy.example:8080");
    unsafe {
        match prev {
            Some(v) => std::env::set_var("HTTPS_PROXY", v),
            None => std::env::remove_var("HTTPS_PROXY"),
        }
    }
}

#[tokio::test]
async fn gateway_returns_url_and_headers() {
    let r = GatewayProxyResolver::new("http://gw.example:8123").with_header_fn(|_req| {
        let mut h = HashMap::new();
        h.insert("x-hma-algorithm".into(), "random".into());
        h
    });
    let sel = r.resolve(&req()).await.unwrap().unwrap();
    assert_eq!(sel.url, "http://gw.example:8123");
    assert_eq!(sel.extra_headers.get("x-hma-algorithm").unwrap(), "random");
}

#[tokio::test]
async fn gateway_exposes_ca_pem() {
    let pem = b"-----BEGIN CERTIFICATE-----\nfake\n-----END CERTIFICATE-----\n";
    let r = GatewayProxyResolver::new("http://gw:1").with_ca_pem(pem.to_vec());
    assert_eq!(r.trusted_ca_pem().unwrap(), pem);
}
