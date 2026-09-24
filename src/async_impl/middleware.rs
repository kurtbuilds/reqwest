//! Request middleware.
//!
//! A [`Middleware`] sees each request before it is sent and each response
//! after it arrives. It can change the request, send it more than once, or
//! answer without sending it. Add one with
//! [`ClientBuilder::middleware`](super::ClientBuilder::middleware).

use std::future::Future;
use std::pin::Pin;

use super::{Client, Request, Response};

/// A boxed, sendable future, as returned by [`Middleware::handle`].
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Runs around every request that a [`Client`] sends.
///
/// ```
/// use reqwest::{Middleware, Next, Request, Response};
/// use reqwest::middleware::BoxFuture;
///
/// struct Auth(String);
///
/// impl Middleware for Auth {
///     fn handle<'a>(&'a self, mut req: Request, next: Next) -> BoxFuture<'a, reqwest::Result<Response>> {
///         Box::pin(async move {
///             req.headers_mut().insert("authorization", self.0.parse().unwrap());
///             next.run(req).await
///         })
///     }
/// }
/// ```
pub trait Middleware: Send + Sync + 'static {
    /// Handle one request. Call [`Next::run`] to pass it on.
    fn handle<'a>(&'a self, req: Request, next: Next) -> BoxFuture<'a, crate::Result<Response>>;
}

/// The rest of the middleware chain, ending with the actual send.
///
/// `Next` is cheap to clone, so a middleware can send a request more than once.
#[derive(Clone)]
pub struct Next {
    client: Client,
    index: usize,
}

impl Next {
    pub(super) fn new(client: Client) -> Next {
        Next { client, index: 0 }
    }

    /// Pass `req` to the next middleware, or send it if none is left.
    pub fn run(self, req: Request) -> BoxFuture<'static, crate::Result<Response>> {
        Box::pin(async move {
            match self.client.middleware(self.index) {
                Some(middleware) => {
                    let next = Next {
                        client: self.client,
                        index: self.index + 1,
                    };
                    middleware.handle(req, next).await
                }
                None => self.client.execute_without_middleware(req).await,
            }
        })
    }
}

impl std::fmt::Debug for Next {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Next").field("index", &self.index).finish()
    }
}
