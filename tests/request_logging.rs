#![cfg(not(target_arch = "wasm32"))]
#![cfg(not(feature = "rustls-no-provider"))]

mod support;
use support::server;

use std::sync::Arc;
use std::sync::Mutex;

#[derive(Clone, Debug)]
struct CaptureLogger {
    records: Arc<Mutex<Vec<String>>>,
}

impl CaptureLogger {
    fn new() -> Self {
        Self {
            records: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn records(&self) -> String {
        self.records.lock().unwrap().join("\n")
    }
}

impl reqwest::logging::LoggerFormatter for CaptureLogger {
    fn log_request(&self, request: reqwest::logging::RequestLog<'_>) {
        let mut records = self.records.lock().unwrap();
        records.push(format!(
            "request {} {} {:?}",
            request.method(),
            request.uri(),
            request.version()
        ));
        for (name, value) in request.headers() {
            records.push(format!("> {name}: {}", value.to_str().unwrap()));
        }
    }

    fn log_response(&self, response: reqwest::logging::ResponseLog<'_>) {
        let mut records = self.records.lock().unwrap();
        records.push(format!(
            "response {} {:?} {}",
            response.url(),
            response.version(),
            response.status()
        ));
        for (name, value) in response.headers() {
            records.push(format!("< {name}: {}", value.to_str().unwrap()));
        }
    }

    fn log_error(&self, error: reqwest::logging::ErrorLog<'_>) {
        self.records
            .lock()
            .unwrap()
            .push(format!("error {} {}", error.url(), error.error()));
    }
}

#[tokio::test]
async fn request_logger_accepts_custom_formatter_and_skips_bodies() {
    let server = server::http(move |_req| async move {
        http::Response::builder()
            .header(http::header::CONTENT_TYPE, "text/plain")
            .body("response body".into())
            .unwrap()
    });

    let url = format!("http://{}", server.addr());
    let formatter = CaptureLogger::new();
    let client = reqwest::Client::builder()
        .request_logger(reqwest::logging::Logger::custom(formatter.clone()))
        .build()
        .unwrap();

    let body = client
        .post(url)
        .header(http::header::CONTENT_TYPE, "text/plain")
        .body("request body")
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();

    assert_eq!(body, "response body");

    let records = formatter.records();
    assert!(records.contains("request POST http://"));
    assert!(records.contains("> content-type: text/plain"));
    assert!(records.contains("response http://"));
    assert!(records.contains("HTTP/1.1 200 OK"));
    assert!(records.contains("< content-type: text/plain"));
    assert!(!records.contains("request body"));
    assert!(!records.contains("response body"));
}
