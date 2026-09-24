#![cfg(not(target_arch = "wasm32"))]
#![cfg(not(feature = "rustls-no-provider"))]
mod support;
use support::server;

use reqwest::middleware::BoxFuture;
use reqwest::{Middleware, Next, Request, Response};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

struct Header(&'static str, &'static str);

impl Middleware for Header {
    fn handle<'a>(&'a self, mut req: Request, next: Next) -> BoxFuture<'a, reqwest::Result<Response>> {
        Box::pin(async move {
            // Keep a header set by an earlier middleware.
            if !req.headers().contains_key(self.0) {
                req.headers_mut().insert(self.0, self.1.parse().unwrap());
            }
            next.run(req).await
        })
    }
}

#[tokio::test]
async fn middleware_runs_in_the_order_added() {
    let server = server::http(move |req| async move {
        assert_eq!(req.headers()["x-token"], "first");
        http::Response::default()
    });

    let client = reqwest::Client::builder()
        .middleware(Header("x-token", "first"))
        .middleware(Header("x-token", "second"))
        .build()
        .unwrap();
    let resp = client.get(format!("http://{}", server.addr())).send().await.unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
}

/// Resends once after a 401, as an auth middleware that refreshes a token does.
struct RetryUnauthorized;

impl Middleware for RetryUnauthorized {
    fn handle<'a>(&'a self, req: Request, next: Next) -> BoxFuture<'a, reqwest::Result<Response>> {
        Box::pin(async move {
            let retry = req.try_clone().ok_or_else(|| reqwest::Error::middleware("body not cloneable"))?;
            let resp = next.clone().run(req).await?;
            if resp.status() == reqwest::StatusCode::UNAUTHORIZED {
                return next.run(retry).await;
            }
            Ok(resp)
        })
    }
}

#[tokio::test]
async fn middleware_can_send_a_request_twice() {
    let calls = Arc::new(AtomicUsize::new(0));
    let seen = calls.clone();
    let server = server::http(move |_req| {
        let calls = calls.clone();
        async move {
            let status = match calls.fetch_add(1, Ordering::SeqCst) {
                0 => http::StatusCode::UNAUTHORIZED,
                _ => http::StatusCode::OK,
            };
            http::Response::builder().status(status).body(Default::default()).unwrap()
        }
    });

    let client = reqwest::Client::builder()
        .middleware(RetryUnauthorized)
        .build()
        .unwrap();
    let resp = client
        .post(format!("http://{}", server.addr()))
        .body("payload")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    assert_eq!(seen.load(Ordering::SeqCst), 2);
}

struct Reject;

impl Middleware for Reject {
    fn handle<'a>(&'a self, _req: Request, _next: Next) -> BoxFuture<'a, reqwest::Result<Response>> {
        Box::pin(async { Err(reqwest::Error::middleware("rejected")) })
    }
}

#[tokio::test]
async fn middleware_error_is_returned_without_sending() {
    let client = reqwest::Client::builder().middleware(Reject).build().unwrap();
    let err = client.get("http://127.0.0.1:1").send().await.unwrap_err();
    assert!(err.is_middleware());
}
