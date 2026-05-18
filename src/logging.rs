//! Request and response logging.
//!
//! Use [`ClientBuilder::request_logger()`] to enable logging for a client.
//!
//! [`ClientBuilder::request_logger()`]: crate::ClientBuilder::request_logger

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{ready, Context, Poll};

use pin_project_lite::pin_project;
use tower::Service;

use crate::async_impl::body::Body;

/// Request and response logger configuration.
///
/// The default logger writes request and response lines and headers to stdout
/// with [`println!`]. Body logging is intentionally not included.
///
/// Header logs can include credentials or other sensitive values. Only enable
/// this for traffic that is safe to log.
#[derive(Clone, Debug)]
pub struct Logger {
    formatter: Arc<dyn LoggerFormatter>,
}

impl Default for Logger {
    fn default() -> Self {
        Self::new()
    }
}

impl Logger {
    /// Create a new request and response logger.
    pub fn new() -> Self {
        Self::custom(PrintlnFormatter)
    }

    /// Create a logger with a custom formatter.
    pub fn custom(formatter: impl LoggerFormatter) -> Self {
        Self {
            formatter: Arc::new(formatter),
        }
    }
}

/// Formats request logger events.
pub trait LoggerFormatter: Send + Sync + fmt::Debug + 'static {
    /// Log a request before it is sent.
    fn log_request(&self, request: RequestLog<'_>);

    /// Log a response after response headers are received.
    fn log_response(&self, response: ResponseLog<'_>);

    /// Log a request error.
    fn log_error(&self, error: ErrorLog<'_>);
}

/// Request metadata passed to [`LoggerFormatter`].
#[derive(Clone, Copy, Debug)]
pub struct RequestLog<'a> {
    method: &'a http::Method,
    uri: &'a http::Uri,
    version: http::Version,
    headers: &'a http::HeaderMap,
}

impl<'a> RequestLog<'a> {
    fn new(req: &'a http::Request<Body>) -> Self {
        Self {
            method: req.method(),
            uri: req.uri(),
            version: req.version(),
            headers: req.headers(),
        }
    }

    /// The request method.
    pub fn method(&self) -> &'a http::Method {
        self.method
    }

    /// The request URI.
    pub fn uri(&self) -> &'a http::Uri {
        self.uri
    }

    /// The HTTP version.
    pub fn version(&self) -> http::Version {
        self.version
    }

    /// The request headers.
    pub fn headers(&self) -> &'a http::HeaderMap {
        self.headers
    }
}

/// Response metadata passed to [`LoggerFormatter`].
#[derive(Clone, Copy, Debug)]
pub struct ResponseLog<'a> {
    url: &'a str,
    version: http::Version,
    status: http::StatusCode,
    headers: &'a http::HeaderMap,
}

impl<'a> ResponseLog<'a> {
    fn new<B>(url: &'a str, res: &'a http::Response<B>) -> Self {
        Self {
            url,
            version: res.version(),
            status: res.status(),
            headers: res.headers(),
        }
    }

    /// The request URL this response belongs to.
    pub fn url(&self) -> &'a str {
        self.url
    }

    /// The HTTP version.
    pub fn version(&self) -> http::Version {
        self.version
    }

    /// The response status.
    pub fn status(&self) -> http::StatusCode {
        self.status
    }

    /// The response headers.
    pub fn headers(&self) -> &'a http::HeaderMap {
        self.headers
    }
}

/// Error metadata passed to [`LoggerFormatter`].
#[derive(Clone, Copy, Debug)]
pub struct ErrorLog<'a> {
    url: &'a str,
    error: &'a crate::Error,
}

impl<'a> ErrorLog<'a> {
    fn new(url: &'a str, error: &'a crate::Error) -> Self {
        Self { url, error }
    }

    /// The request URL this error belongs to.
    pub fn url(&self) -> &'a str {
        self.url
    }

    /// The error.
    pub fn error(&self) -> &'a crate::Error {
        self.error
    }
}

#[derive(Debug)]
struct PrintlnFormatter;

impl LoggerFormatter for PrintlnFormatter {
    fn log_request(&self, request: RequestLog<'_>) {
        println!(
            ">>> Request:\n> {} {} {:?}",
            request.method(),
            request.uri(),
            request.version()
        );
        if !request.headers().is_empty() {
            println!("{}", headers_to_string(request.headers(), '>'));
        }
    }

    fn log_response(&self, response: ResponseLog<'_>) {
        println!(
            "<<< Response to {}:\n< {:?} {}",
            response.url(),
            response.version(),
            response.status()
        );
        if !response.headers().is_empty() {
            println!("{}", headers_to_string(response.headers(), '<'));
        }
    }

    fn log_error(&self, error: ErrorLog<'_>) {
        println!("<<< Response to {}:\n{}", error.url(), error.error());
    }
}

#[derive(Clone, Debug)]
pub(crate) struct LoggerService<S> {
    inner: S,
    logger: Option<Logger>,
}

impl<S> LoggerService<S> {
    pub(crate) fn new(inner: S, logger: Option<Logger>) -> Self {
        Self { inner, logger }
    }
}

impl<S, ResBody> Service<http::Request<Body>> for LoggerService<S>
where
    S: Service<http::Request<Body>, Response = http::Response<ResBody>, Error = crate::Error>,
{
    type Response = http::Response<ResBody>;
    type Error = crate::Error;
    type Future = LoggerFuture<S::Future>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: http::Request<Body>) -> Self::Future {
        let logger = self.logger.clone();
        let url = req.uri().to_string();

        if let Some(logger) = &logger {
            logger.formatter.log_request(RequestLog::new(&req));
        }

        LoggerFuture {
            inner: self.inner.call(req),
            logger,
            url,
        }
    }
}

pin_project! {
    pub(crate) struct LoggerFuture<F> {
        #[pin]
        inner: F,
        logger: Option<Logger>,
        url: String,
    }
}

impl<F, B> Future for LoggerFuture<F>
where
    F: Future<Output = Result<http::Response<B>, crate::Error>>,
{
    type Output = Result<http::Response<B>, crate::Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        match ready!(this.inner.poll(cx)) {
            Ok(res) => {
                if let Some(logger) = this.logger.as_ref() {
                    logger
                        .formatter
                        .log_response(ResponseLog::new(this.url, &res));
                }
                Poll::Ready(Ok(res))
            }
            Err(err) => {
                if let Some(logger) = this.logger.as_ref() {
                    logger.formatter.log_error(ErrorLog::new(this.url, &err));
                }
                Poll::Ready(Err(err))
            }
        }
    }
}

fn headers_to_string(headers: &http::HeaderMap, dir: char) -> String {
    headers
        .iter()
        .map(|(name, value)| {
            format!(
                "{dir} {name}: {}",
                value
                    .to_str()
                    .map(std::borrow::Cow::Borrowed)
                    .unwrap_or_else(|_| std::borrow::Cow::Owned(format!("{:?}", value.as_bytes())))
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}
