use std::fs::File;
use std::io::Read;
use std::ops::Deref;

use etcd::{kv, Client};
use futures::{Future, FutureExt};
use hyper::client::connect::Connect;
use hyper::client::{Client as Hyper, HttpConnector};
use hyper_tls::HttpsConnector;
use native_tls::{Certificate, Identity, TlsConnector};
use tokio::runtime::Runtime;

/// Wrapper around Client that automatically cleans up etcd after each test.
pub struct TestClient<C>
where
    C: Clone + Connect + Sync + Send + 'static,
{
    c: Client<C>,
    run_destructor: bool,
    runtime: Runtime,
}

impl TestClient<HttpConnector> {
    /// Creates a new client for a test.
    #[allow(dead_code)]
    pub fn new() -> TestClient<HttpConnector> {
        TestClient {
            c: Client::new(&["http://etcd:2379"], None).unwrap(),
            run_destructor: true,
            runtime: Runtime::new().expect("failed to create Tokio runtime"),
        }
    }

    /// Creates a new client for a test that will not clean up the key space afterwards.
    #[allow(dead_code)]
    pub fn no_destructor() -> TestClient<HttpConnector> {
        TestClient {
            c: Client::new(&["http://etcd:2379"], None).unwrap(),
            run_destructor: false,
            runtime: Runtime::new().expect("failed to create Tokio runtime"),
        }
    }

    /// Creates a new HTTPS client for a test.
    #[allow(dead_code)]
    pub fn https(use_client_cert: bool) -> TestClient<HttpsConnector<HttpConnector>> {
        let mut ca_cert_file = File::open("/source/tests/ssl/ca.der").unwrap();
        let mut ca_cert_buffer = Vec::new();
        ca_cert_file.read_to_end(&mut ca_cert_buffer).unwrap();

        let mut builder = TlsConnector::builder();
        builder.add_root_certificate(Certificate::from_der(&ca_cert_buffer).unwrap());

        if use_client_cert {
            let mut pkcs12_file = File::open("/source/tests/ssl/client.p12").unwrap();
            let mut pkcs12_buffer = Vec::new();
            pkcs12_file.read_to_end(&mut pkcs12_buffer).unwrap();

            builder.identity(Identity::from_pkcs12(&pkcs12_buffer, "secret").unwrap());
        }

        let https_connector = HttpsConnector::new();

        let hyper = Hyper::builder().build::<_, hyper::Body>(https_connector);

        TestClient {
            c: Client::custom(hyper, &["https://etcdsecure:2379"], None).unwrap(),
            run_destructor: true,
            runtime: Runtime::new().expect("failed to create Tokio runtime"),
        }
    }
}

impl<C> TestClient<C>
where
    C: Clone + Connect + Sync + Send + 'static,
{
    #[allow(dead_code)]
    pub fn run<F, O, E>(&mut self, future: F)
    where
        F: Future<Output = Result<O, E>> + Send + 'static,
        O: Send + 'static,
        E: Send + 'static,
    {
        let _ = self.runtime.block_on(future.map(|_| ()));
    }
}

impl<C> Drop for TestClient<C>
where
    C: Clone + Connect + Sync + Send + 'static,
{
    fn drop(&mut self) {
        if self.run_destructor {
            let future = kv::delete(&self.c, "/test", true).map(|_| ());

            let _ = self.runtime.block_on(future);
        }
    }
}

impl<C> Deref for TestClient<C>
where
    C: Clone + Connect + Sync + Send + 'static,
{
    type Target = Client<C>;

    fn deref(&self) -> &Self::Target {
        &self.c
    }
}
