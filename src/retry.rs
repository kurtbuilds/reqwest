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
//! A `Client` uses [`standard()`] retry behavior by default. It retries
//! `429 Too Many Requests` and `503 Service Unavailable` responses for
//! idempotent requests, or requests with an `Idempotency-Key` header. It also
//! transparently retries failures where the server indicates that it did not
//! process the request.
//!
//! The standard policy makes at most three total attempts, uses exponential
//! backoff with jitter, honors `Retry-After`, and includes a per-host retry
//! budget. Providing a specific retry policy replaces the entire standard
//! policy. Use [`never()`] to disable all retries.
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
    retry_transient_transport_errors: bool,
    scope: scope::Scoped,
}

/// The internal type that we convert the builder into, that implements
/// tower::retry::Policy privately.
#[derive(Clone, Debug)]
pub(crate) struct Policy {
    backoff: backoff::Backoff,
    budget: Option<budget::Budgets>,
    classifier: classify::Classifier,
    // Safe transport retries are immediate, so they don't advance backoff.
    application_retry_cnt: u32,
    max_retries_per_request: u32,
    // All retry kinds share this limit.
    total_retry_cnt: u32,
    retry_transient_transport_errors: bool,
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
/// This retries `429 Too Many Requests` and `503 Service Unavailable`
/// responses for idempotent requests, or requests with an `Idempotency-Key`
/// header. It also retries safe transient transport failures.
pub fn standard() -> Builder {
    let mut builder = Builder::with_scope(scope::Scoped::Unscoped);
    builder.classifier = classify::Classifier::Standard;
    builder.retry_transient_transport_errors = true;
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
            retry_transient_transport_errors: false,
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

    /// Retry safe transient transport failures.
    ///
    /// These are failures where the server or protocol indicates that the
    /// request was not processed, so sending it again is safe. This behavior
    /// is included in [`standard()`].
    pub fn retry_on_transient_transport_errors(mut self) -> Self {
        self.retry_transient_transport_errors = true;
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
            application_retry_cnt: 0,
            max_retries_per_request: self.max_retries_per_request,
            total_retry_cnt: 0,
            retry_transient_transport_errors: self.retry_transient_transport_errors,
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
    Application,
    TransientTransport,
}

impl Policy {
    /// How long to wait before the next attempt, or `None` if waiting that
    /// long would outlive the request anyway.
    fn delay<B>(&self, req: &Req, res: Option<&http::Response<B>>) -> Option<Duration> {
        let backoff = self.backoff.delay(self.application_retry_cnt);

        // A server that told us when to come back knows better than we do,
        // but never let it talk us into retrying sooner than the backoff.
        let delay = match res.and_then(|res| retry_after::parse(res.headers())) {
            Some(retry_after) => backoff.max(retry_after),
            None => backoff,
        };

        if delay.is_zero() {
            return Some(delay);
        }

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
        if self.retry_transient_transport_errors
            && result
                .as_ref()
                .err()
                .map(is_retryable_error)
                .unwrap_or(false)
        {
            Some(RetryKind::TransientTransport)
        } else if let classify::Action::Retryable = self.classifier.classify(req, result) {
            Some(RetryKind::Application)
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
                    RetryKind::Application => self.delay(req, result.as_ref().ok())?,
                    RetryKind::TransientTransport => Duration::ZERO,
                };

                let within_budget = match kind {
                    RetryKind::Application => self
                        .budget
                        .as_ref()
                        .map(|b| b.get(req).withdraw())
                        .unwrap_or(true),
                    RetryKind::TransientTransport => true,
                };

                if within_budget {
                    self.total_retry_cnt += 1;
                    if let RetryKind::Application = kind {
                        self.application_retry_cnt += 1;
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

fn is_retryable_error(err: &crate::Error) -> bool {
    use std::error::Error as _;

    // pop the reqwest::Error
    let err = if let Some(err) = err.source() {
        err
    } else {
        return false;
    };
    // pop the legacy::Error
    let err = if let Some(err) = err.source() {
        err
    } else {
        return false;
    };

    #[cfg(not(any(feature = "http3", feature = "http2")))]
    let _err = err;

    #[cfg(feature = "http3")]
    if let Some(cause) = err.source() {
        if let Some(err) = cause.downcast_ref::<h3::error::ConnectionError>() {
            log::trace!("determining if HTTP/3 error {err} can be retried");
            // TODO: Does h3 provide an API for checking the error?
            return err.to_string().as_str() == "timeout";
        }
    }

    #[cfg(feature = "http2")]
    if let Some(cause) = err.source() {
        if let Some(err) = cause.downcast_ref::<h2::Error>() {
            // They sent us a graceful shutdown, try with a new connection!
            if err.is_go_away() && err.is_remote() && err.reason() == Some(h2::Reason::NO_ERROR) {
                return true;
            }

            // REFUSED_STREAM was sent from the server, which is safe to retry.
            // https://www.rfc-editor.org/rfc/rfc9113.html#section-8.7-3.2
            if err.is_reset() && err.is_remote() && err.reason() == Some(h2::Reason::REFUSED_STREAM)
            {
                return true;
            }
        }
    }
    false
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
    #[derive(Debug)]
    pub struct ReqRep<'a>(&'a super::Req, Result<http::StatusCode, &'a crate::Error>);

    impl ReqRep<'_> {
        /// Access the request method.
        pub fn method(&self) -> &http::Method {
            self.0.method()
        }

        /// Access the request URI.
        pub fn uri(&self) -> &http::Uri {
            self.0.uri()
        }

        /// Access the response status, if it did not error.
        pub fn status(&self) -> Option<http::StatusCode> {
            self.1.ok()
        }

        /// Access the error, if a response was not received.
        pub fn error(&self) -> Option<&(dyn std::error::Error + 'static)> {
            self.1.as_ref().err().map(|e| &**e as _)
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

        fn is_standard_retry(&self) -> bool {
            let transient_status = matches!(
                self.status(),
                Some(http::StatusCode::TOO_MANY_REQUESTS | http::StatusCode::SERVICE_UNAVAILABLE)
            );
            transient_status
                && (self.method().is_idempotent()
                    || self.0.headers().contains_key("idempotency-key"))
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

        pub(super) fn classify<B>(
            &self,
            req: &super::Req,
            res: &Result<http::Response<B>, crate::Error>,
        ) -> Action {
            let req_rep = ReqRep(req, res.as_ref().map(|r| r.status()));
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
                        if let Action::Retryable = classifier.classify(req, res) {
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
