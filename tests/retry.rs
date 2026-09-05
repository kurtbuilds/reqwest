#![cfg(not(target_arch = "wasm32"))]
#![cfg(not(feature = "rustls-no-provider"))]
mod support;
use support::server;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[tokio::test]
async fn other_status_retries_are_opt_in() {
    let _ = env_logger::try_init();
    let cnt = Arc::new(AtomicUsize::new(0));
    let seen = cnt.clone();
    let server = server::http(move |_req| {
        let cnt = cnt.clone();
        async move {
            if cnt.fetch_add(1, Ordering::Relaxed) == 0 {
                http::Response::builder()
                    .status(http::StatusCode::BAD_GATEWAY)
                    .body(Default::default())
                    .unwrap()
            } else {
                http::Response::default()
            }
        }
    });

    let url = format!("http://{}", server.addr());
    let resp = reqwest::Client::builder()
        .build()
        .unwrap()
        .get(url)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), http::StatusCode::BAD_GATEWAY);
    assert_eq!(seen.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn default_retries_429_at_most_three_total_attempts() {
    let _ = env_logger::try_init();
    let cnt = Arc::new(AtomicUsize::new(0));
    let seen = cnt.clone();
    let server = server::http(move |_req| {
        let cnt = cnt.clone();
        async move {
            cnt.fetch_add(1, Ordering::Relaxed);
            http::Response::builder()
                .status(http::StatusCode::TOO_MANY_REQUESTS)
                .body(Default::default())
                .unwrap()
        }
    });

    let url = format!("http://{}", server.addr());
    let resp = reqwest::Client::builder()
        .build()
        .unwrap()
        .get(url)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), http::StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(seen.load(Ordering::Relaxed), 3);
}

#[tokio::test]
async fn default_retries_503() {
    let _ = env_logger::try_init();
    let cnt = Arc::new(AtomicUsize::new(0));
    let seen = cnt.clone();
    let server = server::http(move |_req| {
        let cnt = cnt.clone();
        async move {
            if cnt.fetch_add(1, Ordering::Relaxed) == 0 {
                http::Response::builder()
                    .status(http::StatusCode::SERVICE_UNAVAILABLE)
                    .body(Default::default())
                    .unwrap()
            } else {
                http::Response::default()
            }
        }
    });

    let url = format!("http://{}", server.addr());
    let resp = reqwest::Client::builder()
        .build()
        .unwrap()
        .get(url)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), http::StatusCode::OK);
    assert_eq!(seen.load(Ordering::Relaxed), 2);
}

#[tokio::test]
async fn default_does_not_retry_post_without_idempotency_key() {
    let _ = env_logger::try_init();
    let cnt = Arc::new(AtomicUsize::new(0));
    let seen = cnt.clone();
    let server = server::http(move |_req| {
        let cnt = cnt.clone();
        async move {
            cnt.fetch_add(1, Ordering::Relaxed);
            http::Response::builder()
                .status(http::StatusCode::TOO_MANY_REQUESTS)
                .body(Default::default())
                .unwrap()
        }
    });

    let url = format!("http://{}", server.addr());
    let resp = reqwest::Client::builder()
        .build()
        .unwrap()
        .post(url)
        .body("not automatically replayed")
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), http::StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(seen.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn default_retries_post_with_idempotency_key() {
    let _ = env_logger::try_init();
    let cnt = Arc::new(AtomicUsize::new(0));
    let seen = cnt.clone();
    let server = server::http(move |_req| {
        let cnt = cnt.clone();
        async move {
            if cnt.fetch_add(1, Ordering::Relaxed) == 0 {
                http::Response::builder()
                    .status(http::StatusCode::TOO_MANY_REQUESTS)
                    .body(Default::default())
                    .unwrap()
            } else {
                http::Response::default()
            }
        }
    });

    let url = format!("http://{}", server.addr());
    let resp = reqwest::Client::builder()
        .build()
        .unwrap()
        .post(url)
        .header("idempotency-key", "one-logical-operation")
        .body("safe to replay")
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), http::StatusCode::OK);
    assert_eq!(seen.load(Ordering::Relaxed), 2);
}

#[tokio::test]
async fn max_attempts_counts_the_initial_request() {
    let _ = env_logger::try_init();
    let cnt = Arc::new(AtomicUsize::new(0));
    let seen = cnt.clone();
    let server = server::http(move |_req| {
        let cnt = cnt.clone();
        async move {
            cnt.fetch_add(1, Ordering::Relaxed);
            http::Response::builder()
                .status(http::StatusCode::TOO_MANY_REQUESTS)
                .body(Default::default())
                .unwrap()
        }
    });

    let url = format!("http://{}", server.addr());
    let resp = reqwest::Client::builder()
        .retry(reqwest::retry::standard().max_attempts(2))
        .build()
        .unwrap()
        .get(url)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), http::StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(seen.load(Ordering::Relaxed), 2);
}

#[tokio::test]
async fn custom_classifier_adds_to_status_classifier() {
    let _ = env_logger::try_init();
    let cnt = Arc::new(AtomicUsize::new(0));
    let seen = cnt.clone();
    let server = server::http(move |_req| {
        let cnt = cnt.clone();
        async move {
            if cnt.fetch_add(1, Ordering::Relaxed) == 0 {
                http::Response::builder()
                    .status(http::StatusCode::BAD_GATEWAY)
                    .body(Default::default())
                    .unwrap()
            } else {
                http::Response::default()
            }
        }
    });

    let policy = reqwest::retry::custom()
        .retry_on_status(http::StatusCode::BAD_GATEWAY)
        .classify_fn(|req_rep| req_rep.success())
        .backoff(Duration::ZERO);
    let url = format!("http://{}", server.addr());
    let resp = reqwest::Client::builder()
        .retry(policy)
        .build()
        .unwrap()
        .get(url)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), http::StatusCode::OK);
    assert_eq!(seen.load(Ordering::Relaxed), 2);
}

#[tokio::test]
async fn retries_matching_status() {
    let _ = env_logger::try_init();
    let cnt = Arc::new(AtomicUsize::new(0));
    let server = server::http(move |_req| {
        let cnt = cnt.clone();
        async move {
            if cnt.fetch_add(1, Ordering::Relaxed) == 0 {
                http::Response::builder()
                    .status(http::StatusCode::SERVICE_UNAVAILABLE)
                    .body(Default::default())
                    .unwrap()
            } else {
                http::Response::default()
            }
        }
    });

    let scope = server.addr().ip().to_string();
    let retries =
        reqwest::retry::for_host(scope).retry_on_status(http::StatusCode::SERVICE_UNAVAILABLE);

    let url = format!("http://{}", server.addr());
    let resp = reqwest::Client::builder()
        .retry(retries)
        .build()
        .unwrap()
        .get(url)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), http::StatusCode::OK);
}

#[tokio::test]
async fn status_retries_apply_only_in_scope() {
    let _ = env_logger::try_init();
    let cnt = Arc::new(AtomicUsize::new(0));
    let seen = cnt.clone();
    let server = server::http(move |_req| {
        let cnt = cnt.clone();
        async move {
            if cnt.fetch_add(1, Ordering::Relaxed) == 0 {
                http::Response::builder()
                    .status(http::StatusCode::SERVICE_UNAVAILABLE)
                    .body(Default::default())
                    .unwrap()
            } else {
                http::Response::default()
            }
        }
    });

    let retries = reqwest::retry::for_host("example.com")
        .retry_on_status(http::StatusCode::SERVICE_UNAVAILABLE);

    let url = format!("http://{}", server.addr());
    let resp = reqwest::Client::builder()
        .retry(retries)
        .build()
        .unwrap()
        .get(url)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), http::StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(seen.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn retries_apply_in_scope() {
    let _ = env_logger::try_init();
    let cnt = Arc::new(AtomicUsize::new(0));
    let server = server::http(move |_req| {
        let cnt = cnt.clone();
        async move {
            if cnt.fetch_add(1, Ordering::Relaxed) == 0 {
                // first req is bad
                http::Response::builder()
                    .status(http::StatusCode::SERVICE_UNAVAILABLE)
                    .body(Default::default())
                    .unwrap()
            } else {
                http::Response::default()
            }
        }
    });

    let scope = server.addr().ip().to_string();
    let retries = reqwest::retry::for_host(scope).classify_fn(|req_rep| {
        if req_rep.status() == Some(http::StatusCode::SERVICE_UNAVAILABLE) {
            req_rep.retryable()
        } else {
            req_rep.success()
        }
    });

    let url = format!("http://{}", server.addr());
    let resp = reqwest::Client::builder()
        .retry(retries)
        .build()
        .unwrap()
        .get(url)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
}

#[tokio::test]
async fn retries_apply_to_any_host() {
    let _ = env_logger::try_init();
    let cnt = Arc::new(AtomicUsize::new(0));
    let seen = cnt.clone();
    let server = server::http(move |_req| {
        let cnt = cnt.clone();
        async move {
            if cnt.fetch_add(1, Ordering::Relaxed) == 0 {
                http::Response::builder()
                    .status(http::StatusCode::SERVICE_UNAVAILABLE)
                    .body(Default::default())
                    .unwrap()
            } else {
                http::Response::default()
            }
        }
    });

    let retries = reqwest::retry::any_host()
        .retry_on_status(http::StatusCode::SERVICE_UNAVAILABLE)
        .backoff(Duration::ZERO);

    let url = format!("http://{}", server.addr());
    let resp = reqwest::Client::builder()
        .retry(retries)
        .build()
        .unwrap()
        .get(url)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), http::StatusCode::OK);
    assert_eq!(seen.load(Ordering::Relaxed), 2);
}

#[tokio::test]
async fn backoff_spaces_out_retries() {
    let _ = env_logger::try_init();
    let cnt = Arc::new(AtomicUsize::new(0));
    let seen = cnt.clone();
    let server = server::http(move |_req| {
        let cnt = cnt.clone();
        async move {
            if cnt.fetch_add(1, Ordering::Relaxed) < 2 {
                http::Response::builder()
                    .status(http::StatusCode::SERVICE_UNAVAILABLE)
                    .body(Default::default())
                    .unwrap()
            } else {
                http::Response::default()
            }
        }
    });

    let retries = reqwest::retry::any_host()
        .retry_on_status(http::StatusCode::SERVICE_UNAVAILABLE)
        .backoff_fn(|_attempt| Duration::from_millis(100));

    let url = format!("http://{}", server.addr());
    let start = Instant::now();
    let resp = reqwest::Client::builder()
        .retry(retries)
        .build()
        .unwrap()
        .get(url)
        .send()
        .await
        .unwrap();
    let elapsed = start.elapsed();

    assert_eq!(resp.status(), http::StatusCode::OK);
    assert_eq!(seen.load(Ordering::Relaxed), 3);
    assert!(
        elapsed >= Duration::from_millis(200),
        "two retries should have waited 100ms each, took {elapsed:?}"
    );
}

#[tokio::test]
async fn zero_backoff_retries_immediately() {
    let _ = env_logger::try_init();
    let cnt = Arc::new(AtomicUsize::new(0));
    let server = server::http(move |_req| {
        let cnt = cnt.clone();
        async move {
            if cnt.fetch_add(1, Ordering::Relaxed) < 2 {
                http::Response::builder()
                    .status(http::StatusCode::SERVICE_UNAVAILABLE)
                    .body(Default::default())
                    .unwrap()
            } else {
                http::Response::default()
            }
        }
    });

    let retries = reqwest::retry::any_host()
        .retry_on_status(http::StatusCode::SERVICE_UNAVAILABLE)
        .backoff(Duration::ZERO);

    let url = format!("http://{}", server.addr());
    let start = Instant::now();
    let resp = reqwest::Client::builder()
        .retry(retries)
        .build()
        .unwrap()
        .get(url)
        .send()
        .await
        .unwrap();
    let elapsed = start.elapsed();

    assert_eq!(resp.status(), http::StatusCode::OK);
    assert!(elapsed < Duration::from_millis(100), "took {elapsed:?}");
}

#[tokio::test]
async fn honors_retry_after() {
    let _ = env_logger::try_init();
    let cnt = Arc::new(AtomicUsize::new(0));
    let seen = cnt.clone();
    let server = server::http(move |_req| {
        let cnt = cnt.clone();
        async move {
            if cnt.fetch_add(1, Ordering::Relaxed) == 0 {
                http::Response::builder()
                    .status(http::StatusCode::TOO_MANY_REQUESTS)
                    .header("retry-after", "1")
                    .body(Default::default())
                    .unwrap()
            } else {
                http::Response::default()
            }
        }
    });

    // Backoff alone would retry immediately; the header should win.
    let retries = reqwest::retry::any_host()
        .retry_on_status(http::StatusCode::TOO_MANY_REQUESTS)
        .backoff(Duration::ZERO);

    let url = format!("http://{}", server.addr());
    let start = Instant::now();
    let resp = reqwest::Client::builder()
        .retry(retries)
        .build()
        .unwrap()
        .get(url)
        .send()
        .await
        .unwrap();
    let elapsed = start.elapsed();

    assert_eq!(resp.status(), http::StatusCode::OK);
    assert_eq!(seen.load(Ordering::Relaxed), 2);
    assert!(
        elapsed >= Duration::from_millis(900),
        "should have waited about a second, took {elapsed:?}"
    );
}

#[tokio::test]
async fn retry_after_past_the_deadline_gives_up() {
    let _ = env_logger::try_init();
    let cnt = Arc::new(AtomicUsize::new(0));
    let seen = cnt.clone();
    let server = server::http(move |_req| {
        let cnt = cnt.clone();
        async move {
            cnt.fetch_add(1, Ordering::Relaxed);
            http::Response::builder()
                .status(http::StatusCode::TOO_MANY_REQUESTS)
                .header("retry-after", "30")
                .body(Default::default())
                .unwrap()
        }
    });

    let retries = reqwest::retry::any_host().retry_on_status(http::StatusCode::TOO_MANY_REQUESTS);

    let url = format!("http://{}", server.addr());
    let start = Instant::now();
    let resp = reqwest::Client::builder()
        .retry(retries)
        // waiting 30s wouldn't fit, so the 429 should come straight back
        // instead of the request timing out
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap()
        .get(url)
        .send()
        .await
        .unwrap();
    let elapsed = start.elapsed();

    assert_eq!(resp.status(), http::StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(seen.load(Ordering::Relaxed), 1);
    assert!(elapsed < Duration::from_secs(1), "took {elapsed:?}");
}

#[cfg(feature = "http2")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn default_retries_have_a_limit() {
    let _ = env_logger::try_init();

    let cnt = Arc::new(AtomicUsize::new(0));
    let seen = cnt.clone();
    let server = server::http_with_config(
        move |req| {
            let cnt = cnt.clone();
            async move {
                assert_eq!(req.version(), http::Version::HTTP_2);
                cnt.fetch_add(1, Ordering::Relaxed);
                // refused forever
                Err(h2::Error::from(h2::Reason::REFUSED_STREAM))
            }
        },
        |_| {},
    );

    let client = reqwest::Client::builder()
        .http2_prior_knowledge()
        .build()
        .unwrap();

    let url = format!("http://{}", server.addr());

    let _err = client.get(url).send().await.unwrap_err();
    assert_eq!(seen.load(Ordering::Relaxed), 3);
}

#[cfg(feature = "http2")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn never_disables_transient_transport_retries() {
    let _ = env_logger::try_init();

    let cnt = Arc::new(AtomicUsize::new(0));
    let seen = cnt.clone();
    let server = server::http_with_config(
        move |req| {
            let cnt = cnt.clone();
            async move {
                assert_eq!(req.version(), http::Version::HTTP_2);
                cnt.fetch_add(1, Ordering::Relaxed);
                Err(h2::Error::from(h2::Reason::REFUSED_STREAM))
            }
        },
        |_| {},
    );

    let client = reqwest::Client::builder()
        .http2_prior_knowledge()
        .retry(reqwest::retry::never())
        .build()
        .unwrap();

    let url = format!("http://{}", server.addr());
    let _err = client.get(url).send().await.unwrap_err();

    assert_eq!(seen.load(Ordering::Relaxed), 1);
}

#[cfg(feature = "http2")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn transport_and_status_retries_share_one_attempt_limit() {
    let _ = env_logger::try_init();

    let cnt = Arc::new(AtomicUsize::new(0));
    let seen = cnt.clone();
    let server = server::http_with_config(
        move |req| {
            let cnt = cnt.clone();
            async move {
                assert_eq!(req.version(), http::Version::HTTP_2);
                if cnt.fetch_add(1, Ordering::Relaxed) == 0 {
                    Err(h2::Error::from(h2::Reason::REFUSED_STREAM))
                } else {
                    Ok(http::Response::builder()
                        .status(http::StatusCode::TOO_MANY_REQUESTS)
                        .body(Default::default())
                        .unwrap())
                }
            }
        },
        |_| {},
    );

    let client = reqwest::Client::builder()
        .http2_prior_knowledge()
        .retry(reqwest::retry::standard().max_attempts(3))
        .build()
        .unwrap();

    let url = format!("http://{}", server.addr());
    let resp = client.get(url).send().await.unwrap();

    assert_eq!(resp.status(), http::StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(seen.load(Ordering::Relaxed), 3);
}

// NOTE: using the default "current_thread" runtime here would cause the test to
// fail, because the only thread would block until `panic_rx` receives a
// notification while the client needs to be driven to get the graceful shutdown
// done.
#[cfg(feature = "http2")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn highly_concurrent_requests_to_http2_server_with_low_max_concurrent_streams() {
    let client = reqwest::Client::builder()
        .http2_prior_knowledge()
        .build()
        .unwrap();

    let server = server::http_with_config(
        move |req| async move {
            assert_eq!(req.version(), http::Version::HTTP_2);
            Ok::<_, std::convert::Infallible>(http::Response::default())
        },
        |builder| {
            builder.http2().max_concurrent_streams(1);
        },
    );

    let url = format!("http://{}", server.addr());

    let futs = (0..100).map(|_| {
        let client = client.clone();
        let url = url.clone();
        async move {
            let res = client.get(&url).send().await.unwrap();
            assert_eq!(res.status(), reqwest::StatusCode::OK);
        }
    });
    futures_util::future::join_all(futs).await;
}

#[cfg(feature = "http2")]
#[tokio::test]
async fn highly_concurrent_requests_to_slow_http2_server_with_low_max_concurrent_streams() {
    use support::delay_server;

    let client = reqwest::Client::builder()
        .http2_prior_knowledge()
        .build()
        .unwrap();

    let server = delay_server::Server::new(
        move |req| async move {
            assert_eq!(req.version(), http::Version::HTTP_2);
            http::Response::default()
        },
        |http| {
            http.http2().max_concurrent_streams(1);
        },
        std::time::Duration::from_secs(2),
    )
    .await;

    let url = format!("http://{}", server.addr());

    let futs = (0..100).map(|_| {
        let client = client.clone();
        let url = url.clone();
        async move {
            let res = client.get(&url).send().await.unwrap();
            assert_eq!(res.status(), reqwest::StatusCode::OK);
        }
    });
    futures_util::future::join_all(futs).await;

    server.shutdown().await;
}

#[tokio::test]
async fn deadline_survives_a_redirect() {
    let _ = env_logger::try_init();
    let cnt = Arc::new(AtomicUsize::new(0));
    let seen = cnt.clone();
    let server = server::http(move |req| {
        let cnt = cnt.clone();
        async move {
            if req.uri().path() == "/from" {
                return http::Response::builder()
                    .status(http::StatusCode::FOUND)
                    .header("location", "/to")
                    .body(Default::default())
                    .unwrap();
            }
            cnt.fetch_add(1, Ordering::Relaxed);
            http::Response::builder()
                .status(http::StatusCode::TOO_MANY_REQUESTS)
                .header("retry-after", "20")
                .body(Default::default())
                .unwrap()
        }
    });

    let retries = reqwest::retry::any_host().retry_on_status(http::StatusCode::TOO_MANY_REQUESTS);

    let url = format!("http://{}/from", server.addr());
    let start = Instant::now();
    let resp = reqwest::Client::builder()
        .retry(retries)
        // 20s of Retry-After doesn't fit, even on the hop after the redirect
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap()
        .get(url)
        .send()
        .await
        .unwrap();
    let elapsed = start.elapsed();

    assert_eq!(resp.status(), http::StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(seen.load(Ordering::Relaxed), 1);
    assert!(elapsed < Duration::from_secs(1), "took {elapsed:?}");
}
