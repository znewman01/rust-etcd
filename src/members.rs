//! etcd's members API.
//!
//! These API endpoints are used to manage cluster membership.

use std::str::FromStr;

use bytes::buf::BufExt;
use futures::future::ready;
use futures::TryFutureExt;
use hyper::client::connect::Connect;
use hyper::{StatusCode, Uri};
use serde_derive::{Deserialize, Serialize};
use serde_json;

use crate::client::{Client, ClusterInfo, Response};
use crate::error::{ApiError, Error};
use crate::first_ok::first_ok;

/// An etcd server that is a member of a cluster.
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct Member {
    /// An internal identifier for the cluster member.
    pub id: String,
    /// A human-readable name for the cluster member.
    pub name: String,
    /// URLs exposing this cluster member's peer API.
    #[serde(rename = "peerURLs")]
    pub peer_urls: Vec<String>,
    /// URLs exposing this cluster member's client API.
    #[serde(rename = "clientURLs")]
    pub client_urls: Vec<String>,
}

/// The request body for `POST /v2/members` and `PUT /v2/members/:id`.
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
struct PeerUrls {
    /// The peer URLs.
    #[serde(rename = "peerURLs")]
    peer_urls: Vec<String>,
}

/// A small wrapper around `Member` to match the response of `GET /v2/members`.
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
struct ListResponse {
    /// The members.
    members: Vec<Member>,
}

/// Adds a new member to the cluster.
///
/// # Parameters
///
/// * client: A `Client` to use to make the API call.
/// * peer_urls: URLs exposing this cluster member's peer API.
pub async fn add<C>(
    client: &Client<C>,
    peer_urls: Vec<String>,
) -> Result<Response<()>, Vec<Error>>
where
    C: Clone + Connect + Sync + Send,
{
    let peer_urls = PeerUrls { peer_urls };

    let body = match serde_json::to_string(&peer_urls) {
        Ok(body) => body,
        Err(error) => return Err(vec![Error::Serialization(error)]),
    };

    let http_client = client.http_client().clone();

    first_ok(client.endpoints().to_vec(), move |member| {
        let url = build_url(member, "");
        let uri = ready(Uri::from_str(url.as_str()).map_err(Error::from));

        let body = body.clone();
        let http_client = http_client.clone();

        let response = uri.and_then(move |uri| http_client.post(uri, body).map_err(Error::from));

        response.and_then(|response| {
            let status = response.status();
            let cluster_info = ClusterInfo::from(response.headers());
            let body = hyper::body::aggregate(response.into_body())
                .map_ok(BufExt::reader)
                .err_into();

            body.and_then(move |body| async move{
                if status == StatusCode::CREATED {
                    Ok(Response {
                        data: (),
                        cluster_info,
                    })
                } else {
                    match serde_json::from_reader::<_, ApiError>(body) {
                        Ok(error) => Err(Error::Api(error)),
                        Err(error) => Err(Error::Serialization(error)),
                    }
                }
            })
        })
    }).await
}

/// Deletes a member from the cluster.
///
/// # Parameters
///
/// * client: A `Client` to use to make the API call.
/// * id: The unique identifier of the member to delete.
pub async fn delete<C>(
    client: &Client<C>,
    id: String,
) -> Result<Response<()>, Vec<Error>>
where
    C: Clone + Connect + Sync + Send,
{
    let http_client = client.http_client().clone();

    first_ok(client.endpoints().to_vec(), move |member| {
        let url = build_url(member, &format!("/{}", id));
        let uri = ready(Uri::from_str(url.as_str()).map_err(Error::from));

        let http_client = http_client.clone();

        let response = uri.and_then(move |uri| http_client.delete(uri).map_err(Error::from));

        response.and_then(|response| {
            let status = response.status();
            let cluster_info = ClusterInfo::from(response.headers());
            let body = hyper::body::aggregate(response.into_body())
                .map_ok(BufExt::reader)
                .err_into();

            body.and_then(move |body| async move {
                if status == StatusCode::NO_CONTENT {
                    Ok(Response {
                        data: (),
                        cluster_info,
                    })
                } else {
                    match serde_json::from_reader::<_, ApiError>(body) {
                        Ok(error) => Err(Error::Api(error)),
                        Err(error) => Err(Error::Serialization(error)),
                    }
                }
            })
        })
    }).await
}

/// Lists the members of the cluster.
///
/// # Parameters
///
/// * client: A `Client` to use to make the API call.
pub async fn list<C>(
    client: &Client<C>,
) -> Result<Response<Vec<Member>>, Vec<Error>>
where
    C: Clone + Connect + Sync + Send,
{
    let http_client = client.http_client().clone();

    first_ok(client.endpoints().to_vec(), move |member| {
        let url = build_url(member, "");
        let uri = ready(Uri::from_str(url.as_str()).map_err(Error::from));

        let http_client = http_client.clone();

        let response = uri.and_then(move |uri| http_client.get(uri).map_err(Error::from));

        response.and_then(|response| {
            let status = response.status();
            let cluster_info = ClusterInfo::from(response.headers());
            let body = hyper::body::aggregate(response.into_body())
                .map_ok(BufExt::reader)
                .err_into();

            body.and_then(move |body| async move {
                if status == StatusCode::OK {
                    match serde_json::from_reader::<_, ListResponse>(body) {
                        Ok(data) => Ok(Response {
                            data: data.members,
                            cluster_info,
                        }),
                        Err(error) => Err(Error::Serialization(error)),
                    }
                } else {
                    match serde_json::from_reader::<_, ApiError>(body) {
                        Ok(error) => Err(Error::Api(error)),
                        Err(error) => Err(Error::Serialization(error)),
                    }
                }
            })
        })
    }).await
}

/// Updates the peer URLs of a member of the cluster.
///
/// # Parameters
///
/// * client: A `Client` to use to make the API call.
/// * id: The unique identifier of the member to update.
/// * peer_urls: URLs exposing this cluster member's peer API.
pub async fn update<C>(
    client: &Client<C>,
    id: String,
    peer_urls: Vec<String>,
) -> Result<Response<()>, Vec<Error>>
where
    C: Clone + Connect + Sync + Send,
{
    let peer_urls = PeerUrls { peer_urls };

    let body = match serde_json::to_string(&peer_urls) {
        Ok(body) => body,
        Err(error) => return Err(vec![Error::Serialization(error)]),
    };

    let http_client = client.http_client().clone();

    first_ok(client.endpoints().to_vec(), move |member| {
        let url = build_url(member, &format!("/{}", id));
        let uri = ready(Uri::from_str(url.as_str()).map_err(Error::from));

        let body = body.clone();
        let http_client = http_client.clone();

        let response = uri.and_then(move |uri| http_client.put(uri, body).map_err(Error::from));

        response.and_then(|response| {
            let status = response.status();
            let cluster_info = ClusterInfo::from(response.headers());
            let body = hyper::body::aggregate(response.into_body())
                .err_into()
                .map_ok(BufExt::reader);

            body.and_then(move |body| async move{
                if status == StatusCode::NO_CONTENT {
                    Ok(Response {
                        data: (),
                        cluster_info,
                    })
                } else {
                    match serde_json::from_reader::<_, ApiError>(body) {
                        Ok(error) => Err(Error::Api(error)),
                        Err(error) => Err(Error::Serialization(error)),
                    }
                }
            })
        })
    }).await
}

/// Constructs the full URL for an API call.
fn build_url(endpoint: &Uri, path: &str) -> String {
    format!("{}v2/members{}", endpoint, path)
}
