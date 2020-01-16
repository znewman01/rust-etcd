use base64::encode;
use http::header::{AUTHORIZATION, CONTENT_TYPE};
use http::request::Builder;
use hyper::client::connect::Connect;
use hyper::client::ResponseFuture;
use hyper::{Body, Client as Hyper, Method, Request, Uri};

use crate::client::BasicAuth;

#[derive(Clone, Debug)]
pub struct HttpClient<C>
where
    C: Clone + Connect + Sync + Send + 'static,
{
    basic_auth: Option<BasicAuth>,
    hyper: Hyper<C>,
}

impl<C> HttpClient<C>
where
    C: Clone + Connect + Sync + Send + 'static,
{
    /// Constructs a new `HttpClient`.
    pub fn new(hyper: Hyper<C>, basic_auth: Option<BasicAuth>) -> Self {
        HttpClient { basic_auth, hyper }
    }

    /// Makes a DELETE request to etcd.
    pub fn delete(&self, uri: Uri) -> ResponseFuture {
        self.request(Method::DELETE, uri)
    }

    /// Makes a GET request to etcd.
    pub fn get(&self, uri: Uri) -> ResponseFuture {
        self.request(Method::GET, uri)
    }

    /// Makes a POST request to etcd.
    pub fn post(&self, uri: Uri, body: String) -> ResponseFuture {
        self.request_with_body(Method::POST, uri, body)
    }

    /// Makes a PUT request to etcd.
    pub fn put(&self, uri: Uri, body: String) -> ResponseFuture {
        self.request_with_body(Method::PUT, uri, body)
    }

    // private

    /// Adds the Authorization HTTP header to a request if a credentials were supplied.
    fn add_auth_header<'a>(&self, request: Builder) -> http::request::Builder {
        if let Some(ref basic_auth) = self.basic_auth {
            let auth = format!("{}:{}", basic_auth.username, basic_auth.password);
            let header_value = format!("Basic {}", encode(&auth));

            request.header(AUTHORIZATION, header_value)
        } else {
            request
        }
    }

    /// Makes a request to etcd.
    fn request(&self, method: Method, uri: Uri) -> ResponseFuture {
        let request = self.add_auth_header(Request::builder().method(method).uri(uri));

        self.hyper.request(request.body(Body::empty()).unwrap())
    }

    /// Makes a request with an HTTP body to etcd.
    fn request_with_body(&self, method: Method, uri: Uri, body: String) -> ResponseFuture {
        let request = self.add_auth_header(
            Request::builder()
                .method(method)
                .uri(uri)
                .header(CONTENT_TYPE, "application/x-www-form-urlencoded"),
        );

        self.hyper.request(request.body(Body::from(body)).unwrap())
    }
}
