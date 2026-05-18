use url::Url;

/// A trait to try to convert some type into a `Url`.
///
/// This trait is "sealed", such that only types within reqwest can
/// implement it.
pub trait IntoUrl: IntoUrlSealed {}

impl IntoUrl for Url {}
impl IntoUrl for String {}
impl<'a> IntoUrl for &'a str {}
impl<'a> IntoUrl for &'a String {}

pub trait IntoUrlSealed {
    // Besides parsing as a valid `Url`, the `Url` must be a valid
    // `http::Uri`, in that it makes sense to use in a network request.
    fn into_url(self) -> crate::Result<Url>;

    fn as_str(&self) -> &str;
}

impl IntoUrlSealed for Url {
    fn into_url(self) -> crate::Result<Url> {
        // With blob url the `self.has_host()` check is always false, so we
        // remove the `blob:` scheme and check again if the url is valid.
        #[cfg(target_arch = "wasm32")]
        if self.scheme() == "blob"
            && self.path().starts_with("http") // Check if the path starts with http or https to avoid validating a `blob:blob:...` url.
            && self.as_str()[5..].into_url().is_ok()
        {
            return Ok(self);
        }

        if self.has_host() {
            Ok(self)
        } else {
            Err(crate::error::url_bad_scheme(self))
        }
    }

    fn as_str(&self) -> &str {
        self.as_ref()
    }
}

impl<'a> IntoUrlSealed for &'a str {
    fn into_url(self) -> crate::Result<Url> {
        Url::parse(self).map_err(crate::error::builder)?.into_url()
    }

    fn as_str(&self) -> &str {
        self
    }
}

impl<'a> IntoUrlSealed for &'a String {
    fn into_url(self) -> crate::Result<Url> {
        (&**self).into_url()
    }

    fn as_str(&self) -> &str {
        self.as_ref()
    }
}

impl IntoUrlSealed for String {
    fn into_url(self) -> crate::Result<Url> {
        (&*self).into_url()
    }

    fn as_str(&self) -> &str {
        self.as_ref()
    }
}

pub(crate) fn into_url_with_base<U: IntoUrl>(url: U, base_url: Option<&Url>) -> crate::Result<Url> {
    match Url::parse(url.as_str()) {
        Ok(url) => url.into_url(),
        Err(url::ParseError::RelativeUrlWithoutBase) => match base_url {
            Some(base_url) => base_url
                .join(url.as_str())
                .map_err(crate::error::builder)?
                .into_url(),
            None => Err(crate::error::builder(
                url::ParseError::RelativeUrlWithoutBase,
            )),
        },
        Err(err) => Err(crate::error::builder(err)),
    }
}

if_hyper! {
    pub(crate) fn try_uri(url: &Url) -> crate::Result<http::Uri> {
        url.as_str()
            .parse()
            .map_err(|_| crate::error::url_invalid_uri(url.clone()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::error::Error;

    #[test]
    fn into_url_file_scheme() {
        let err = "file:///etc/hosts".into_url().unwrap_err();
        assert_eq!(
            err.source().unwrap().to_string(),
            "URL scheme is not allowed"
        );
    }

    #[test]
    fn into_url_blob_scheme() {
        let err = "blob:https://example.com".into_url().unwrap_err();
        assert_eq!(
            err.source().unwrap().to_string(),
            "URL scheme is not allowed"
        );
    }

    #[test]
    fn into_url_with_base_joins_relative_url() {
        let base_url = Url::parse("https://example.com/api/").unwrap();
        let url = into_url_with_base("users", Some(&base_url)).unwrap();

        assert_eq!(url.as_str(), "https://example.com/api/users");
    }

    #[test]
    fn into_url_with_base_prefers_absolute_url() {
        let base_url = Url::parse("https://example.com/api/").unwrap();
        let url = into_url_with_base("https://other.example/users", Some(&base_url)).unwrap();

        assert_eq!(url.as_str(), "https://other.example/users");
    }

    #[test]
    fn into_url_without_base_rejects_relative_url() {
        let err = into_url_with_base("users", None).unwrap_err();

        assert_eq!(
            err.source().unwrap().to_string(),
            "relative URL without a base"
        );
    }

    if_wasm! {
        use wasm_bindgen_test::*;

        #[wasm_bindgen_test]
        fn into_url_blob_scheme_wasm() {
            let url = "blob:http://example.com".into_url().unwrap();

            assert_eq!(url.as_str(), "blob:http://example.com");
        }
    }
}
