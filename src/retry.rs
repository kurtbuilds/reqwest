//! Retry requests
//!
//! A `Client` has the ability to retry requests, by sending additional copies
//! to the server if a response is considered retryable.
//!
//! The [`Builder`] makes it easier to configure what requests to retry, along
//! with including best practices by default, such as a retry budget.
//!
//! # Defaults
//!
//! A `Client` uses [`standard()`] retry behavior by default. It separates
//! evidence about the failure from whether the request can be repeated:
//!
//! - If the protocol confirms the request was not processed, any method can
//!   retry immediately without spending the retry budget.
//! - If processing is unknown, a temporary transport interruption or HTTP
//!   429/503 response can retry only for an idempotent method or a request
//!   with an `Idempotency-Key`. These retries use backoff and the retry budget.
//! - Other failures, including builder and certificate-validation errors,
//!   do not retry. A timeout is not evidence that a request was unprocessed.
//!
//! An idempotency key is the caller's declaration that the server supports
//! deduplication for this request. Custom classifiers can override the standard
//! conditions; their caller is responsible for deciding whether replay is safe.
//!
//! Every retry requires a replayable body and an in-scope request, and respects
//! the shared attempt limit and request deadline. The standard limit is three
//! total attempts. Backoff is exponential with jitter and honors `Retry-After`.
//! Providing a specific retry policy replaces the entire standard policy.
//! Use [`never()`] to disable all retries.
//!
//! HTTP response status retries can be added to a policy with
//! [`Builder::retry_on_status()`] or [`Builder::retry_on_statuses()`].
//!
//! The standard and custom policy builders include a retry budget that permits
//! 20% extra requests to be sent.
//!
//! # Scope
//!
//! A retry budget's history is always tracked per-host, so that a failing
//! host can never consume the retry allowance of a healthy one.
//!
//! The [`standard()`] and [`custom()`] policies apply to every request the
//! client makes. Use [`for_host()`] when only one host of several should be
//! retried at all.
//!
//! # Backoff
//!
//! Retries are spaced out with an exponential backoff, jittered to avoid
//! synchronizing many clients. [`Builder::backoff()`] sets the base delay,
//! which doubles with each attempt. A base of [`Duration::ZERO`] retries
//! immediately.
//!
//! If a retryable response includes a `Retry-After` header, it is honored,
//! as long as it doesn't exceed the request's remaining
//! [timeout][crate::ClientBuilder::timeout()]. If it would, the request is
//! not retried, and the response is returned as-is.
//!
//! # Classifiers
//!
//! Advanced custom policies can add a classifier that determines if a request
//! should be retried. Knowledge of the destination server's behavior is
//! required to make a safe classifier. **Requests should not be retried** if
//! the server cannot safely handle the same request twice, or if it causes
//! side effects.
//!
//! Some common properties to check include if the request method is
//! idempotent, or if the response status code indicates a transient error.

use std::sync::Arc;
use std::time::{Duration, Instant};

use tower::retry::budget::Budget as _;

#[cfg(docsrs)]
pub use classify::ReqRep;

/// Builder to configure retries
///
/// Most clients should start with [`standard()`]. Use [`custom()`] to build a
/// policy from no retry conditions, or [`for_host()`] for a host-specific
/// custom policy.
#[derive(Debug)]
pub struct Builder {
    backoff: backoff::Backoff,
    budget: Option<f32>,
    classifier: classify::Classifier,
    max_retries_per_request: u32,
    retry_unprocessed_requests: bool,
    scope: scope::Scoped,
}

/// The internal type that we convert the builder into, that implements
/// tower::retry::Policy privately.
#[derive(Clone, Debug)]
pub(crate) struct Policy {
    backoff: backoff::Backoff,
    budget: Option<budget::Budgets>,
    classifier: classify::Classifier,
    // Confirmed unprocessed requests retry immediately without advancing backoff.
    backoff_retry_cnt: u32,
    max_retries_per_request: u32,
    // All retry kinds share this limit.
    total_retry_cnt: u32,
    retry_unprocessed_requests: bool,
    scope: scope::Scoped,
}

/// A deadline for a request, inserted as an extension by the `Client`.
///
/// The retry policy uses this to avoid sleeping longer than the request has
/// left to live.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Deadline(pub(crate) Instant);

/// Create the standard retry policy used by a `Client` by default.
///
/// Retries temporary failures for repeatable requests, and confirmed
/// unprocessed requests for any method. See the module's default-policy rules.
pub fn standard() -> Builder {
    let mut builder = Builder::with_scope(scope::Scoped::Unscoped);
    builder.classifier = classify::Classifier::Standard;
    builder.retry_unprocessed_requests = true;
    builder
}

/// Create an empty retry policy that applies to requests to any host.
///
/// Add retry conditions with methods such as [`Builder::retry_on_status()`].
pub fn custom() -> Builder {
    Builder::with_scope(scope::Scoped::Unscoped)
}

/// Create an empty retry policy that applies to requests to any host.
///
/// Retry budgets are still tracked per-host, so a failing host cannot consume
/// the retry allowance of a healthy one.
#[doc(hidden)]
pub fn any_host() -> Builder {
    custom()
}

/// Create an empty retry policy scoped to one host.
///
/// To provide a scope that isn't a closure, use the more general
/// [`Builder::scoped()`].
pub fn for_host<S>(host: S) -> Builder
where
    S: for<'a> PartialEq<&'a str> + Send + Sync + 'static,
{
    scoped(move |req| host == req.uri().host().unwrap_or(""))
}

/// Create a retry policy that will never retry any request.
///
/// This disables both HTTP response retries and safe transport retries.
pub fn never() -> Builder {
    let mut builder = custom().no_budget();
    builder.max_retries_per_request = 0;
    builder
}

fn scoped<F>(func: F) -> Builder
where
    F: Fn(&Req) -> bool + Send + Sync + 'static,
{
    Builder::scoped(scope::ScopeFn(func))
}

// ===== impl Builder =====

impl Builder {
    /// Create a scoped retry policy.
    ///
    /// For a more convenient constructor, see [`for_host()`].
    pub fn scoped(scope: impl scope::Scope) -> Self {
        Self::with_scope(scope::Scoped::Dyn(Arc::new(scope)))
    }

    fn with_scope(scope: scope::Scoped) -> Self {
        Self {
            backoff: backoff::Backoff::default(),
            budget: Some(0.2),
            classifier: classify::Classifier::Never,
            max_retries_per_request: 2, // on top of the original
            retry_unprocessed_requests: false,
            scope,
        }
    }

    /// Set the base delay to space out retries.
    ///
    /// Each attempt waits about twice as long as the one before it, starting
    /// from `base`, with jitter applied so that many clients retrying at once
    /// don't synchronize. Passing [`Duration::ZERO`] retries immediately.
    ///
    /// Default is currently 100 milliseconds.
    ///
    /// # Example
    ///
    /// ```rust
    /// # use std::time::Duration;
    /// # fn with_builder(builder: reqwest::retry::Builder) -> reqwest::retry::Builder {
    /// builder.backoff(Duration::from_millis(50))
    /// # }
    /// ```
    pub fn backoff(mut self, base: Duration) -> Self {
        self.backoff = backoff::Backoff::Exponential(base);
        self
    }

    /// Provide a function to determine how long to wait before a retry.
    ///
    /// The function is called with the number of retries already made for
    /// this request, starting at `0`. Use this when the exponential backoff
    /// of [`backoff()`][Self::backoff()] isn't the schedule you want. Note
    /// that no jitter is applied to the returned duration.
    ///
    /// A `Retry-After` header, if present, still takes precedence when it
    /// asks to wait longer than the returned duration.
    ///
    /// # Example
    ///
    /// ```rust
    /// # use std::time::Duration;
    /// # fn with_builder(builder: reqwest::retry::Builder) -> reqwest::retry::Builder {
    /// // a fixed 500ms between attempts
    /// builder.backoff_fn(|_attempt| Duration::from_millis(500))
    /// # }
    /// ```
    pub fn backoff_fn<F>(mut self, func: F) -> Self
    where
        F: Fn(u32) -> Duration + Send + Sync + 'static,
    {
        self.backoff = backoff::Backoff::Fn(Arc::new(func));
        self
    }

    /// Set no retry budget.
    ///
    /// Sets that no budget will be enforced. This could also be considered
    /// to be an infinite budget.
    ///
    /// This is NOT recommended. Disabling the budget can make your system more
    /// susceptible to retry storms.
    pub fn no_budget(mut self) -> Self {
        self.budget = None;
        self
    }

    /// Sets the max extra load the budget will allow.
    ///
    /// Think of the amount of requests your client generates, and how much
    /// load that puts on the server. This option configures as a percentage
    /// how much extra load is allowed via retries.
    ///
    /// For example, if you send 1,000 requests per second, setting a maximum
    /// extra load value of `0.3` would allow 300 more requests per second
    /// in retries. A value of `2.5` would allow 2,500 more requests.
    ///
    /// # Panics
    ///
    /// The `extra_percent` value must be within reasonable values for a
    /// percentage. This method will panic if it is less than `0.0`, or greater
    /// than `1000.0`.
    pub fn max_extra_load(mut self, extra_percent: f32) -> Self {
        assert!(extra_percent >= 0.0);
        assert!(extra_percent <= 1000.0);
        self.budget = Some(extra_percent);
        self
    }

    // pub fn max_replay_body

    /// Set the max retries allowed per request.
    ///
    /// For each logical (initial) request, only retry up to `max` times.
    ///
    /// This value is used in combination with a token budget that is applied
    /// to all requests. Even if the budget would allow more requests, this
    /// limit will prevent. Likewise, the budget may prevent retrying up to
    /// `max` times. This setting prevents a single request from consuming
    /// the entire budget.
    ///
    /// Default is currently 2 retries.
    #[doc(hidden)]
    pub fn max_retries_per_request(mut self, max: u32) -> Self {
        self.max_retries_per_request = max;
        self
    }

    /// Set the maximum number of total attempts per request.
    ///
    /// The initial request counts as the first attempt. The standard policy
    /// makes at most three attempts.
    ///
    /// # Panics
    ///
    /// Panics if `max` is zero.
    #[track_caller]
    pub fn max_attempts(mut self, max: u32) -> Self {
        assert!(max > 0, "max attempts must be at least 1");
        self.max_retries_per_request = max - 1;
        self
    }

    /// Retry protocol failures that confirm the request was not processed.
    ///
    /// These are failures where the server or protocol indicates that the
    /// request was not processed, so sending it again is safe. This behavior
    /// is included in [`standard()`].
    pub fn retry_on_transient_transport_errors(mut self) -> Self {
        self.retry_unprocessed_requests = true;
        self
    }

    /// Retry requests that fail with a low-level protocol NACK.
    ///
    /// This is the legacy name for [`retry_on_transient_transport_errors()`].
    ///
    /// [`retry_on_transient_transport_errors()`]: Self::retry_on_transient_transport_errors()
    #[doc(hidden)]
    pub fn retry_on_protocol_nacks(self) -> Self {
        self.retry_on_transient_transport_errors()
    }

    /// Retry responses with this HTTP status code.
    ///
    /// This adds to any previously configured retry classifiers. Status-code
    /// retries apply to every request in the policy's scope, so prefer
    /// [`classify_fn()`] when retrying the same request twice may not be safe
    /// for all methods.
    ///
    /// [`classify_fn()`]: Self::classify_fn()
    pub fn retry_on_status(self, status: http::StatusCode) -> Self {
        self.retry_on_statuses([status])
    }

    /// Retry responses with any of these HTTP status codes.
    ///
    /// This adds to any previously configured retry classifiers. Status-code
    /// retries apply to every request in the policy's scope, so prefer
    /// [`classify_fn()`] when retrying the same request twice may not be safe
    /// for all methods.
    ///
    /// # Example
    ///
    /// ```rust
    /// # fn with_builder(builder: reqwest::retry::Builder) -> reqwest::retry::Builder {
    /// builder.retry_on_statuses([
    ///     http::StatusCode::TOO_MANY_REQUESTS,
    ///     http::StatusCode::SERVICE_UNAVAILABLE,
    /// ])
    /// # }
    /// ```
    ///
    /// [`classify_fn()`]: Self::classify_fn()
    pub fn retry_on_statuses<I>(mut self, statuses: I) -> Self
    where
        I: IntoIterator<Item = http::StatusCode>,
    {
        let statuses: Vec<_> = statuses.into_iter().collect();
        if !statuses.is_empty() {
            self.classifier = self
                .classifier
                .or(classify::Classifier::StatusCodes(statuses.into()));
        }
        self
    }

    /// Add a classifier that determines if a request should be retried.
    ///
    /// This classifier is combined with any retry conditions already on the
    /// builder.
    ///
    /// # Example
    ///
    /// ```rust
    /// # fn with_builder(builder: reqwest::retry::Builder) -> reqwest::retry::Builder {
    /// builder.classify_fn(|req_rep| {
    ///     match (req_rep.method(), req_rep.status()) {
    ///         (&http::Method::GET, Some(http::StatusCode::SERVICE_UNAVAILABLE)) => {
    ///             req_rep.retryable()
    ///         },
    ///         _ => req_rep.success()
    ///     }
    /// })
    /// # }
    /// ```
    pub fn classify_fn<F>(self, func: F) -> Self
    where
        F: Fn(classify::ReqRep<'_>) -> classify::Action + Send + Sync + 'static,
    {
        self.classify(classify::ClassifyFn(func))
    }

    /// Add a classifier that determines if a request should be retried.
    ///
    /// This classifier is combined with any retry conditions already on the
    /// builder.
    pub fn classify(mut self, classifier: impl classify::Classify) -> Self {
        self.classifier = self
            .classifier
            .or(classify::Classifier::Dyn(Arc::new(classifier)));
        self
    }

    pub(crate) fn into_policy(self) -> Policy {
        Policy {
            backoff: self.backoff,
            budget: self.budget.map(budget::Budgets::new),
            classifier: self.classifier,
            backoff_retry_cnt: 0,
            max_retries_per_request: self.max_retries_per_request,
            total_retry_cnt: 0,
            retry_unprocessed_requests: self.retry_unprocessed_requests,
            scope: self.scope,
        }
    }
}

impl Default for Builder {
    fn default() -> Self {
        standard()
    }
}

// ===== internal ======

type Req = http::Request<crate::async_impl::body::Body>;

#[derive(Clone, Copy, Debug)]
enum RetryKind {
    Backoff,
    Immediate,
}

impl Policy {
    /// How long to wait before the next attempt, or `None` if waiting that
    /// long would outlive the request anyway.
    fn delay<B>(&self, req: &Req, res: Option<&http::Response<B>>) -> Option<Duration> {
        let backoff = self.backoff.delay(self.backoff_retry_cnt);

        // A server that told us when to come back knows better than we do,
        // but never let it talk us into retrying sooner than the backoff.
        let delay = match res.and_then(|res| retry_after::parse(res.headers())) {
            Some(retry_after) => backoff.max(retry_after),
            None => backoff,
        };

        let room = match req.extensions().get::<Deadline>() {
            Some(deadline) => deadline.0.checked_duration_since(Instant::now()),
            None => return Some(delay),
        };

        // Sleeping past the deadline would just turn a real response into a
        // timeout error. Give up now and let the caller see what we have.
        match room {
            Some(room) if delay < room => Some(delay),
            _ => {
                log::debug!("retry delay of {delay:?} would outlive the request, not retrying");
                None
            }
        }
    }

    fn retry_kind<B>(
        &self,
        req: &Req,
        result: &crate::Result<http::Response<B>>,
    ) -> Option<RetryKind> {
        let req_rep = classify::ReqRep::new(req, result);
        if self.retry_unprocessed_requests
            && req_rep.transport_failure == Some(TransportFailure::NotProcessed)
        {
            Some(RetryKind::Immediate)
        } else if let classify::Action::Retryable = self.classifier.classify(req_rep) {
            Some(RetryKind::Backoff)
        } else {
            None
        }
    }
}

impl<B> tower::retry::Policy<Req, http::Response<B>, crate::Error> for Policy {
    type Future = Delay;

    fn retry(
        &mut self,
        req: &mut Req,
        result: &mut crate::Result<http::Response<B>>,
    ) -> Option<Self::Future> {
        if req
            .extensions()
            .get::<Deadline>()
            .map(|deadline| deadline.0 <= Instant::now())
            .unwrap_or(false)
        {
            return None;
        }
        match self.retry_kind(req, result) {
            None => {
                log::trace!("shouldn't retry!");
                if let Some(ref budget) = self.budget {
                    budget.get(req).deposit();
                }
                None
            }
            Some(kind) => {
                log::trace!("could retry!");

                // Checked before sleeping, and before spending budget, since
                // `clone_request()` isn't consulted until after the delay.
                if self.total_retry_cnt >= self.max_retries_per_request {
                    log::trace!("max_retries_per_request hit");
                    return None;
                }

                let delay = match kind {
                    RetryKind::Backoff => self.delay(req, result.as_ref().ok())?,
                    RetryKind::Immediate => Duration::ZERO,
                };

                let within_budget = match kind {
                    RetryKind::Backoff => self
                        .budget
                        .as_ref()
                        .map(|b| b.get(req).withdraw())
                        .unwrap_or(true),
                    RetryKind::Immediate => true,
                };

                if within_budget {
                    self.total_retry_cnt += 1;
                    if let RetryKind::Backoff = kind {
                        self.backoff_retry_cnt += 1;
                    }
                    Some(Delay::new(delay))
                } else {
                    log::debug!("retryable but could not withdraw from budget");
                    None
                }
            }
        }
    }

    fn clone_request(&mut self, req: &Req) -> Option<Req> {
        if !self.scope.applies_to(req) {
            return None;
        }
        if self.total_retry_cnt >= self.max_retries_per_request {
            log::trace!("max_retries_per_request hit");
            return None;
        }
        let body = req.body().try_clone()?;
        let mut new = http::Request::new(body);
        *new.method_mut() = req.method().clone();
        *new.uri_mut() = req.uri().clone();
        *new.version_mut() = req.version();
        *new.headers_mut() = req.headers().clone();
        *new.extensions_mut() = req.extensions().clone();

        Some(new)
    }
}

/// The future awaited by `tower::retry` before the next attempt.
pub(crate) enum Delay {
    Ready,
    Sleep(std::pin::Pin<Box<tokio::time::Sleep>>),
}

impl Delay {
    fn new(dur: Duration) -> Self {
        if dur.is_zero() {
            Self::Ready
        } else {
            Self::Sleep(Box::pin(tokio::time::sleep(dur)))
        }
    }
}

impl std::future::Future for Delay {
    type Output = ();

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        match self.get_mut() {
            Self::Ready => std::task::Poll::Ready(()),
            Self::Sleep(sleep) => sleep.as_mut().poll(cx),
        }
    }
}

impl std::fmt::Debug for Delay {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Delay")
    }
}

/// What the transport tells us, independently of the request's method.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TransportFailure {
    /// The protocol confirms that this request was not processed.
    NotProcessed,
    /// A temporary failure; the request may already have taken effect.
    Interrupted,
    /// No evidence that retrying will help.
    Other,
}

/// Inspect the source chain once; wrappers do not determine retry safety.
fn classify_transport_failure(err: &crate::Error) -> TransportFailure {
    use std::error::Error as _;
    use std::io::ErrorKind;

    if !err.is_request() {
        return TransportFailure::Other;
    }
    let mut failure = TransportFailure::Other;
    let mut source = err.source();
    while let Some(cause) = source {
        #[cfg(feature = "http2")]
        if let Some(h2) = cause.downcast_ref::<h2::Error>() {
            // h2 delivers remote GOAWAY errors to streams above the last processed ID.
            let rejected = h2.is_remote()
                && ((h2.is_go_away() && h2.reason() == Some(h2::Reason::NO_ERROR))
                    || (h2.is_reset() && h2.reason() == Some(h2::Reason::REFUSED_STREAM)));
            if rejected {
                return TransportFailure::NotProcessed;
            }
        }
        #[cfg(feature = "http3")]
        if matches!(
            cause.downcast_ref::<h3::error::ConnectionError>(),
            Some(h3::error::ConnectionError::Timeout { .. })
        ) {
            // A timeout does not establish whether the server processed the request.
            failure = TransportFailure::Interrupted;
        }
        if cause.is::<crate::error::TimedOut>() {
            failure = TransportFailure::Interrupted;
        }
        if let Some(io) = cause.downcast_ref::<std::io::Error>() {
            if matches!(
                io.kind(),
                ErrorKind::ConnectionRefused
                    | ErrorKind::ConnectionReset
                    | ErrorKind::ConnectionAborted
                    | ErrorKind::NotConnected
                    | ErrorKind::BrokenPipe
                    | ErrorKind::TimedOut
                    | ErrorKind::UnexpectedEof
            ) {
                failure = TransportFailure::Interrupted;
            }
        }
        if let Some(hyper) = cause.downcast_ref::<hyper::Error>() {
            if hyper.is_closed() || hyper.is_incomplete_message() || hyper.is_timeout() {
                failure = TransportFailure::Interrupted;
            }
        }
        source = cause.source();
    }
    failure
}

mod backoff {
    use std::sync::Arc;
    use std::time::Duration;

    /// Anything past this and the deadline check will reject the delay
    /// anyway. Keeps the shift and the jitter math from overflowing.
    const MAX_DOUBLINGS: u32 = 20;

    #[derive(Clone)]
    pub(super) enum Backoff {
        Exponential(Duration),
        Fn(Arc<dyn Fn(u32) -> Duration + Send + Sync>),
    }

    impl Default for Backoff {
        fn default() -> Self {
            Self::Exponential(Duration::from_millis(100))
        }
    }

    impl Backoff {
        pub(super) fn delay(&self, attempt: u32) -> Duration {
            match self {
                Self::Exponential(base) => {
                    if base.is_zero() {
                        return Duration::ZERO;
                    }
                    let factor = 1u32 << attempt.min(MAX_DOUBLINGS);
                    jitter(base.saturating_mul(factor))
                }
                Self::Fn(func) => func(attempt),
            }
        }
    }

    impl std::fmt::Debug for Backoff {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                Self::Exponential(base) => f.debug_tuple("Exponential").field(base).finish(),
                Self::Fn(_) => f.write_str("Backoff"),
            }
        }
    }

    /// Spread the delay over the second half of the interval, so that clients
    /// that all failed at the same moment don't all come back at the same
    /// moment.
    fn jitter(delay: Duration) -> Duration {
        let half = delay / 2;
        // The random value is uniform over the whole u64 range, so the shift
        // lands the product uniformly in `[0, half)`.
        let extra = (half.as_nanos() * u128::from(crate::util::fast_random())) >> 64;
        half + Duration::from_nanos(extra as u64)
    }
}

mod budget {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use tower::retry::budget::TpsBudget;

    /// Enough for any sane client, and a bound on how much a client crawling
    /// arbitrary hosts can accumulate.
    const MAX_HOSTS: usize = 512;

    /// A retry budget per host.
    ///
    /// Budgets are deliberately not shared between hosts, so that one failing
    /// host can't spend the retry allowance of the others.
    #[derive(Clone)]
    pub(super) struct Budgets {
        extra_percent: f32,
        hosts: Arc<Mutex<HashMap<Box<str>, Arc<TpsBudget>>>>,
    }

    impl Budgets {
        pub(super) fn new(extra_percent: f32) -> Self {
            Self {
                extra_percent,
                hosts: Arc::new(Mutex::new(HashMap::new())),
            }
        }

        pub(super) fn get(&self, req: &super::Req) -> Arc<TpsBudget> {
            let host = req
                .uri()
                .authority()
                .map(|authority| authority.as_str())
                .unwrap_or("");

            let mut hosts = self.hosts.lock().unwrap_or_else(|e| e.into_inner());

            if let Some(budget) = hosts.get(host) {
                return budget.clone();
            }

            // Budgets are a rolling window anyways, so forgetting the history
            // of a client that talks to this many hosts is no great loss.
            if hosts.len() >= MAX_HOSTS {
                log::debug!("retry budgets tracked for over {MAX_HOSTS} hosts, resetting");
                hosts.clear();
            }

            let budget = Arc::new(TpsBudget::new(
                Duration::from_secs(10),
                10,
                self.extra_percent,
            ));
            hosts.insert(host.into(), budget.clone());
            budget
        }
    }

    impl std::fmt::Debug for Budgets {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("Budgets")
                .field("extra_percent", &self.extra_percent)
                .finish()
        }
    }
}

mod retry_after {
    use std::time::{Duration, SystemTime};

    /// Parse a `Retry-After` header into how long to wait from now.
    ///
    /// Returns `None` if the header is absent, malformed, or names a time
    /// that has already passed.
    pub(super) fn parse(headers: &http::HeaderMap) -> Option<Duration> {
        let value = headers.get(http::header::RETRY_AFTER)?.to_str().ok()?;
        let value = value.trim();

        // delay-seconds
        if let Ok(secs) = value.parse::<u64>() {
            return Some(Duration::from_secs(secs));
        }

        // HTTP-date
        http_date(value)?.duration_since(SystemTime::now()).ok()
    }

    /// Parse an IMF-fixdate, the format RFC 9110 requires servers to send.
    ///
    /// `Sun, 06 Nov 1994 08:49:37 GMT`
    fn http_date(s: &str) -> Option<SystemTime> {
        if s.len() != 29 || !s.is_char_boundary(5) {
            return None;
        }
        let (weekday, rest) = s.split_at(5);
        if !weekday.ends_with(", ") || !rest.ends_with(" GMT") {
            return None;
        }

        let day: i64 = num(rest.get(0..2)?)?;
        let month: i64 = match rest.get(3..6)? {
            "Jan" => 1,
            "Feb" => 2,
            "Mar" => 3,
            "Apr" => 4,
            "May" => 5,
            "Jun" => 6,
            "Jul" => 7,
            "Aug" => 8,
            "Sep" => 9,
            "Oct" => 10,
            "Nov" => 11,
            "Dec" => 12,
            _ => return None,
        };
        let year: i64 = num(rest.get(7..11)?)?;
        let hour: i64 = num(rest.get(12..14)?)?;
        let min: i64 = num(rest.get(15..17)?)?;
        let sec: i64 = num(rest.get(18..20)?)?;

        if hour > 23 || min > 59 || sec > 60 || day == 0 || day > 31 {
            return None;
        }

        let secs = days_from_civil(year, month, day) * 86_400 + hour * 3600 + min * 60 + sec;
        // Anything before the epoch is in the past, which we ignore anyway.
        Some(SystemTime::UNIX_EPOCH + Duration::from_secs(u64::try_from(secs).ok()?))
    }

    fn num(s: &str) -> Option<i64> {
        if s.bytes().all(|b| b.is_ascii_digit()) {
            s.parse().ok()
        } else {
            None
        }
    }

    /// Days since 1970-01-01, from a proleptic Gregorian date.
    ///
    /// From Howard Hinnant's `days_from_civil`.
    fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
        let y = if m <= 2 { y - 1 } else { y };
        let era = if y >= 0 { y } else { y - 399 } / 400;
        let yoe = y - era * 400; // [0, 399]
        let mp = (m + 9) % 12; // Mar = 0
        let doy = (153 * mp + 2) / 5 + d - 1; // [0, 365]
        let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy; // [0, 146096]
        era * 146_097 + doe - 719_468
    }
}

// sealed types and traits on purpose while exploring design space
mod scope {
    pub trait Scope: Send + Sync + 'static {
        fn applies_to(&self, req: &super::Req) -> bool;
    }

    // I think scopes likely make the most sense being to hosts.
    // If that's the case, then it should probably be easiest to check for
    // the host. Perhaps also considering the ability to add more things
    // to scope off in the future...

    // For Future Whoever: making a blanket impl for any closure sounds nice,
    // but it causes inference issues at the call site. Every closure would
    // need to include `: ReqRep` in the arguments.
    //
    // An alternative is to make things like `ScopeFn`. Slightly more annoying,
    // but also more forwards-compatible. :shrug:

    pub struct ScopeFn<F>(pub(super) F);

    impl<F> Scope for ScopeFn<F>
    where
        F: Fn(&super::Req) -> bool + Send + Sync + 'static,
    {
        fn applies_to(&self, req: &super::Req) -> bool {
            (self.0)(req)
        }
    }

    #[derive(Clone)]
    pub(super) enum Scoped {
        Unscoped,
        Dyn(std::sync::Arc<dyn Scope>),
    }

    impl Scoped {
        pub(super) fn applies_to(&self, req: &super::Req) -> bool {
            let ret = match self {
                Self::Unscoped => true,
                Self::Dyn(s) => s.applies_to(req),
            };
            log::trace!("retry in scope: {ret}");
            ret
        }
    }

    impl std::fmt::Debug for Scoped {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                Self::Unscoped => f.write_str("Unscoped"),
                Self::Dyn(_) => f.write_str("Scoped"),
            }
        }
    }
}

// sealed types and traits on purpose while exploring design space
mod classify {
    pub trait Classify: Send + Sync + 'static {
        fn classify(&self, req_rep: ReqRep<'_>) -> Action;
    }

    // For Future Whoever: making a blanket impl for any closure sounds nice,
    // but it causes inference issues at the call site. Every closure would
    // need to include `: ReqRep` in the arguments.
    //
    // An alternative is to make things like `ClassifyFn`. Slightly more
    // annoying, but also more forwards-compatible. :shrug:
    pub struct ClassifyFn<F>(pub(super) F);

    impl<F> Classify for ClassifyFn<F>
    where
        F: Fn(ReqRep<'_>) -> Action + Send + Sync + 'static,
    {
        fn classify(&self, req_rep: ReqRep<'_>) -> Action {
            (self.0)(req_rep)
        }
    }

    /// A request/response result to inspect for possible retries.
    ///
    /// This is passed to a `classify` function.
    #[derive(Clone, Copy, Debug)]
    pub struct ReqRep<'a> {
        request: &'a super::Req,
        response: Result<http::StatusCode, &'a crate::Error>,
        pub(super) transport_failure: Option<super::TransportFailure>,
    }

    impl<'a> ReqRep<'a> {
        pub(super) fn new<B>(
            request: &'a super::Req,
            result: &'a Result<http::Response<B>, crate::Error>,
        ) -> Self {
            Self {
                request,
                response: result.as_ref().map(|response| response.status()),
                transport_failure: result.as_ref().err().map(super::classify_transport_failure),
            }
        }

        /// Access the request method.
        pub fn method(&self) -> &http::Method {
            self.request.method()
        }

        /// Access the request URI.
        pub fn uri(&self) -> &http::Uri {
            self.request.uri()
        }

        /// Access the response status, if it did not error.
        pub fn status(&self) -> Option<http::StatusCode> {
            self.response.ok()
        }

        /// Access the error, if a response was not received.
        pub fn error(&self) -> Option<&(dyn std::error::Error + 'static)> {
            self.response.as_ref().err().map(|e| &**e as _)
        }

        /// Classify this attempt as retryable.
        pub fn retryable(self) -> Action {
            Action::Retryable
        }

        /// Classify this attempt as success.
        ///
        /// Even if it was a domain error, a "success" means it will not retry.
        pub fn success(self) -> Action {
            Action::Success
        }

        fn is_status_code(&self, statuses: &[http::StatusCode]) -> bool {
            self.status()
                .map(|status| statuses.contains(&status))
                .unwrap_or(false)
        }

        /// Repetition has the same intended effect, even if the first attempt succeeded.
        fn is_repeatable(&self) -> bool {
            self.method().is_idempotent() || self.request.headers().contains_key("idempotency-key")
        }

        fn is_standard_retry(&self) -> bool {
            let temporary = match self.response {
                Ok(status) => matches!(
                    status,
                    http::StatusCode::TOO_MANY_REQUESTS | http::StatusCode::SERVICE_UNAVAILABLE
                ),
                Err(_) => self.transport_failure == Some(super::TransportFailure::Interrupted),
            };
            temporary && self.is_repeatable()
        }
    }

    #[must_use]
    #[derive(Debug)]
    pub enum Action {
        Success,
        Retryable,
    }

    #[derive(Clone)]
    pub(super) enum Classifier {
        Never,
        Standard,
        StatusCodes(std::sync::Arc<[http::StatusCode]>),
        Dyn(std::sync::Arc<dyn Classify>),
        Any(Vec<Classifier>),
    }

    impl Classifier {
        pub(super) fn or(self, other: Self) -> Self {
            match (self, other) {
                (Self::Never, other) => other,
                (this, Self::Never) => this,
                (Self::Any(mut all), Self::Any(mut other)) => {
                    all.append(&mut other);
                    Self::Any(all)
                }
                (Self::Any(mut all), other) => {
                    all.push(other);
                    Self::Any(all)
                }
                (this, Self::Any(mut other)) => {
                    other.insert(0, this);
                    Self::Any(other)
                }
                (this, other) => Self::Any(vec![this, other]),
            }
        }

        pub(super) fn classify(&self, req_rep: ReqRep<'_>) -> Action {
            match self {
                Self::Never => Action::Success,
                Self::Standard => {
                    if req_rep.is_standard_retry() {
                        Action::Retryable
                    } else {
                        Action::Success
                    }
                }
                Self::StatusCodes(statuses) => {
                    if req_rep.is_status_code(statuses) {
                        Action::Retryable
                    } else {
                        Action::Success
                    }
                }
                Self::Dyn(c) => c.classify(req_rep),
                Self::Any(classifiers) => {
                    for classifier in classifiers {
                        if let Action::Retryable = classifier.classify(req_rep) {
                            return Action::Retryable;
                        }
                    }
                    Action::Success
                }
            }
        }
    }

    impl std::fmt::Debug for Classifier {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                Self::Never => f.write_str("Never"),
                Self::Standard => f.write_str("Standard"),
                Self::StatusCodes(_) => f.write_str("StatusCodes"),
                Self::Dyn(_) => f.write_str("Classifier"),
                Self::Any(classifiers) => f.debug_tuple("Any").field(classifiers).finish(),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(method: http::Method) -> Req {
        http::Request::builder()
            .method(method)
            .uri("https://example.com/")
            .body(crate::async_impl::body::Body::empty())
            .unwrap()
    }

    #[test]
    fn standard_retries_repeatable_requests_after_transport_interruptions() {
        use std::io::ErrorKind;

        let policy = standard().into_policy();
        for kind in [
            ErrorKind::ConnectionRefused,
            ErrorKind::ConnectionReset,
            ErrorKind::ConnectionAborted,
            ErrorKind::NotConnected,
            ErrorKind::BrokenPipe,
            ErrorKind::TimedOut,
            ErrorKind::UnexpectedEof,
        ] {
            // Check a nested source, as in a connector error wrapped by hyper.
            let error = crate::error::request(crate::error::request(std::io::Error::from(kind)));
            let result: crate::Result<http::Response<()>> = Err(error);
            for method in [
                http::Method::GET,
                http::Method::HEAD,
                http::Method::PUT,
                http::Method::DELETE,
                http::Method::OPTIONS,
                http::Method::TRACE,
            ] {
                assert!(matches!(
                    policy.retry_kind(&request(method), &result),
                    Some(RetryKind::Backoff)
                ));
            }
            for method in [http::Method::POST, http::Method::PATCH] {
                let mut req = request(method);
                assert!(policy.retry_kind(&req, &result).is_none());
                req.headers_mut()
                    .insert("idempotency-key", "key".parse().unwrap());
                assert!(matches!(
                    policy.retry_kind(&req, &result),
                    Some(RetryKind::Backoff)
                ));
            }
            for builder in [custom(), never()] {
                assert!(builder
                    .into_policy()
                    .retry_kind(&request(http::Method::GET), &result)
                    .is_none());
            }
        }
    }

    #[test]
    fn standard_does_not_retry_permanent_or_non_request_errors() {
        let policy = standard().into_policy();
        let req = request(http::Method::GET);
        for error in [
            crate::error::request(std::io::Error::from(std::io::ErrorKind::InvalidData)),
            crate::error::request(std::io::Error::from(std::io::ErrorKind::PermissionDenied)),
            crate::error::request("unrecognized error"),
            crate::error::builder(std::io::Error::from(std::io::ErrorKind::ConnectionReset)),
            crate::error::body(std::io::Error::from(std::io::ErrorKind::ConnectionReset)),
            crate::error::decode(std::io::Error::from(std::io::ErrorKind::ConnectionReset)),
        ] {
            let result: crate::Result<http::Response<()>> = Err(error);
            assert!(policy.retry_kind(&req, &result).is_none());
        }
        #[cfg(feature = "__rustls")]
        {
            let error = crate::error::request(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                rustls::Error::InvalidCertificate(rustls::CertificateError::UnknownIssuer),
            ));
            let result: crate::Result<http::Response<()>> = Err(error);
            assert!(policy.retry_kind(&req, &result).is_none());
        }
    }

    #[test]
    fn retries_respect_attempt_limit_and_deadline_even_without_backoff() {
        use tower::retry::Policy as _;

        let mut policy = standard().no_budget().backoff(Duration::ZERO).into_policy();
        let mut req = request(http::Method::GET);
        let mut result: crate::Result<http::Response<()>> =
            Err(crate::error::request(crate::error::TimedOut));
        assert!(policy.retry(&mut req, &mut result).is_some());
        assert!(policy.retry(&mut req, &mut result).is_some());
        assert!(policy.retry(&mut req, &mut result).is_none());

        for backoff in [Duration::ZERO, Duration::from_millis(100)] {
            let mut policy = standard().backoff(backoff).into_policy();
            req.extensions_mut().insert(Deadline(Instant::now()));
            assert!(policy.retry(&mut req, &mut result).is_none());
        }
    }

    fn header(value: &str) -> http::HeaderMap {
        let mut headers = http::HeaderMap::new();
        headers.insert(http::header::RETRY_AFTER, value.parse().unwrap());
        headers
    }

    #[test]
    fn retry_after_delay_seconds() {
        assert_eq!(
            retry_after::parse(&header("120")),
            Some(Duration::from_secs(120))
        );
        assert_eq!(
            retry_after::parse(&header(" 3 ")),
            Some(Duration::from_secs(3))
        );
        assert_eq!(retry_after::parse(&header("0")), Some(Duration::ZERO));
    }

    #[test]
    fn retry_after_http_date() {
        let future = std::time::SystemTime::now() + Duration::from_secs(60 * 60);
        let secs = future
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        // Only the date matters here, so build one the parser should agree
        // with: 2038-01-19 03:14:07 GMT.
        let parsed = retry_after::parse(&header("Tue, 19 Jan 2038 03:14:07 GMT"))
            .expect("a date that far out is still in the future");
        let expected = Duration::from_secs(2_147_483_647 - secs + 60 * 60);
        // Within a second of the computed distance.
        assert!(
            parsed.abs_diff(expected) < Duration::from_secs(2),
            "{parsed:?} vs {expected:?}"
        );
    }

    #[test]
    fn retry_after_past_date_is_ignored() {
        assert_eq!(
            retry_after::parse(&header("Sun, 06 Nov 1994 08:49:37 GMT")),
            None
        );
    }

    #[test]
    fn retry_after_garbage_is_ignored() {
        assert_eq!(retry_after::parse(&http::HeaderMap::new()), None);
        assert_eq!(retry_after::parse(&header("soon")), None);
        assert_eq!(retry_after::parse(&header("-5")), None);
        assert_eq!(retry_after::parse(&header("1.5")), None);
        assert_eq!(
            retry_after::parse(&header("Tue, 19 Xxx 2038 03:14:07 GMT")),
            None
        );
        assert_eq!(
            retry_after::parse(&header("Tue, 19 Jan 2038 33:14:07 GMT")),
            None
        );
        assert_eq!(
            retry_after::parse(&header("Tue, 19 Jan 2038 03:14:07")),
            None
        );
    }

    #[test]
    fn exponential_doubles_within_jitter() {
        let backoff = backoff::Backoff::Exponential(Duration::from_millis(100));
        for attempt in 0..5 {
            let expected = Duration::from_millis(100 << attempt);
            for _ in 0..100 {
                let delay = backoff.delay(attempt);
                assert!(
                    delay >= expected / 2 && delay <= expected,
                    "attempt {attempt}: {delay:?} outside [{:?}, {expected:?}]",
                    expected / 2
                );
            }
        }
    }

    #[test]
    fn exponential_jitter_varies() {
        let backoff = backoff::Backoff::Exponential(Duration::from_secs(1));
        let first = backoff.delay(0);
        assert!(
            (0..100).any(|_| backoff.delay(0) != first),
            "jitter is constant"
        );
    }

    #[test]
    fn zero_base_never_delays() {
        let backoff = backoff::Backoff::Exponential(Duration::ZERO);
        for attempt in 0..40 {
            assert_eq!(backoff.delay(attempt), Duration::ZERO);
        }
    }

    #[test]
    fn configured_backoff_is_not_capped_without_a_request_timeout() {
        let policy = custom()
            .backoff_fn(|_| Duration::from_secs(60))
            .into_policy();
        let request = http::Request::new(crate::async_impl::body::Body::empty());

        assert_eq!(
            policy.delay::<()>(&request, None),
            Some(Duration::from_secs(60))
        );
    }

    #[test]
    fn huge_attempt_counts_dont_overflow() {
        let backoff = backoff::Backoff::Exponential(Duration::from_secs(1));
        for attempt in [30, 31, 32, 64, u32::MAX] {
            let _ = backoff.delay(attempt);
        }
    }
}
