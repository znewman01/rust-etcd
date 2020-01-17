//! etcd's authentication and authorization API.
//!
//! These API endpoints are used to manage users and roles.

use std::str::FromStr;

use futures::{Future, IntoFuture, Stream};
use hyper::client::connect::Connect;
use hyper::{StatusCode, Uri};
use serde_derive::{Deserialize, Serialize};
use serde_json;

use crate::client::{Client, ClusterInfo, Response};
use crate::error::{ApiError, Error};
use crate::first_ok::first_ok;

/// The structure returned by the `GET /v2/auth/enable` endpoint.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
struct AuthStatus {
    /// Whether or not the auth system is enabled.
    pub enabled: bool,
}

/// The type returned when the auth system is successfully enabled or disabled.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub enum AuthChange {
    /// The auth system was successfully enabled or disabled.
    Changed,
    /// The auth system was already in the desired state.
    Unchanged,
}

/// An existing etcd user with a list of their granted roles.
#[derive(Debug, Clone, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct User {
    /// The user's name.
    #[serde(rename = "user")]
    name: String,
    /// The names of roles granted to the user.
    roles: Vec<String>,
}

impl User {
    /// Returns the user's name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the names of the roles granted to the user.
    pub fn role_names(&self) -> &[String] {
        &self.roles
    }
}

/// An existing etcd user with details of granted roles.
#[derive(Debug, Clone, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct UserDetail {
    /// The user's name.
    #[serde(rename = "user")]
    name: String,
    /// Roles granted to the user.
    roles: Vec<Role>,
}

impl UserDetail {
    /// Returns the user's name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the roles granted to the user.
    pub fn roles(&self) -> &[Role] {
        &self.roles
    }
}

/// A list of all users.
#[derive(Debug, Clone, Deserialize, Eq, Hash, PartialEq, Serialize)]
struct Users {
    users: Option<Vec<UserDetail>>,
}

/// Paramters used to create a new etcd user.
#[derive(Debug, Deserialize, Clone, Eq, Hash, PartialEq, Serialize)]
pub struct NewUser {
    /// The user's name.
    #[serde(rename = "user")]
    name: String,
    /// The user's password.
    password: String,
    /// An initial set of roles granted to the user.
    #[serde(skip_serializing_if = "Option::is_none")]
    roles: Option<Vec<String>>,
}

impl NewUser {
    /// Creates a new user.
    pub fn new<N, P>(name: N, password: P) -> Self
    where
        N: Into<String>,
        P: Into<String>,
    {
        NewUser {
            name: name.into(),
            password: password.into(),
            roles: None,
        }
    }

    /// Gets the name of the new user.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Grants a role to the new user.
    pub fn add_role<R>(&mut self, role: R)
    where
        R: Into<String>,
    {
        match self.roles {
            Some(ref mut roles) => roles.push(role.into()),
            None => self.roles = Some(vec![role.into()]),
        }
    }
}

/// Parameters used to update an existing etcd user.
#[derive(Debug, Deserialize, Clone, Eq, Hash, PartialEq, Serialize)]
pub struct UserUpdate {
    /// The user's name.
    #[serde(rename = "user")]
    name: String,
    /// A new password for the user.
    #[serde(skip_serializing_if = "Option::is_none")]
    password: Option<String>,
    /// Roles being granted to the user.
    #[serde(rename = "grant")]
    #[serde(skip_serializing_if = "Option::is_none")]
    grants: Option<Vec<String>>,
    /// Roles being revoked from the user.
    #[serde(rename = "revoke")]
    #[serde(skip_serializing_if = "Option::is_none")]
    revocations: Option<Vec<String>>,
}

impl UserUpdate {
    /// Creates a new `UserUpdate` for the given user.
    pub fn new<N>(name: N) -> Self
    where
        N: Into<String>,
    {
        UserUpdate {
            name: name.into(),
            password: None,
            grants: None,
            revocations: None,
        }
    }

    /// Gets the name of the user being updated.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Updates the user's password.
    pub fn update_password<P>(&mut self, password: P)
    where
        P: Into<String>,
    {
        self.password = Some(password.into());
    }

    /// Grants the given role to the user.
    pub fn grant_role<R>(&mut self, role: R)
    where
        R: Into<String>,
    {
        match self.grants {
            Some(ref mut grants) => grants.push(role.into()),
            None => self.grants = Some(vec![role.into()]),
        }
    }

    /// Revokes the given role from the user.
    pub fn revoke_role<R>(&mut self, role: R)
    where
        R: Into<String>,
    {
        match self.revocations {
            Some(ref mut revocations) => revocations.push(role.into()),
            None => self.revocations = Some(vec![role.into()]),
        }
    }
}

/// An authorization role.
#[derive(Debug, Deserialize, Clone, Eq, Hash, PartialEq, Serialize)]
pub struct Role {
    /// The name of the role.
    #[serde(rename = "role")]
    name: String,
    /// Permissions granted to the role.
    permissions: Permissions,
}

impl Role {
    /// Creates a new role.
    pub fn new<N>(name: N) -> Self
    where
        N: Into<String>,
    {
        Role {
            name: name.into(),
            permissions: Permissions::new(),
        }
    }

    /// Gets the name of the role.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Grants read permission for a key in etcd's key-value store to this role.
    pub fn grant_kv_read_permission<K>(&mut self, key: K)
    where
        K: Into<String>,
    {
        self.permissions.kv.modify_read_permission(key)
    }

    /// Grants write permission for a key in etcd's key-value store to this role.
    pub fn grant_kv_write_permission<K>(&mut self, key: K)
    where
        K: Into<String>,
    {
        self.permissions.kv.modify_write_permission(key)
    }

    /// Returns a list of keys in etcd's key-value store that this role is allowed to read.
    pub fn kv_read_permissions(&self) -> &[String] {
        match self.permissions.kv.read {
            Some(ref read) => read,
            None => &[],
        }
    }

    /// Returns a list of keys in etcd's key-value store that this role is allowed to write.
    pub fn kv_write_permissions(&self) -> &[String] {
        match self.permissions.kv.write {
            Some(ref write) => write,
            None => &[],
        }
    }
}

/// A list of all roles.
#[derive(Debug, Clone, Deserialize, Eq, Hash, PartialEq, Serialize)]
struct Roles {
    roles: Option<Vec<Role>>,
}

/// Parameters used to update an existing authorization role.
#[derive(Debug, Deserialize, Clone, Eq, Hash, PartialEq, Serialize)]
pub struct RoleUpdate {
    /// The name of the role.
    #[serde(rename = "role")]
    name: String,
    /// Permissions being added to the role.
    #[serde(rename = "grant")]
    #[serde(skip_serializing_if = "Option::is_none")]
    grants: Option<Permissions>,
    /// Permissions being removed from the role.
    #[serde(rename = "revoke")]
    #[serde(skip_serializing_if = "Option::is_none")]
    revocations: Option<Permissions>,
}

impl RoleUpdate {
    /// Creates a new `RoleUpdate` for the given role.
    pub fn new<R>(role: R) -> Self
    where
        R: Into<String>,
    {
        RoleUpdate {
            name: role.into(),
            grants: None,
            revocations: None,
        }
    }

    /// Gets the name of the role being updated.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Grants read permission for a key in etcd's key-value store to this role.
    pub fn grant_kv_read_permission<K>(&mut self, key: K)
    where
        K: Into<String>,
    {
        match self.grants {
            Some(ref mut grants) => grants.kv.modify_read_permission(key),
            None => {
                let mut permissions = Permissions::new();
                permissions.kv.modify_read_permission(key);
                self.grants = Some(permissions);
            }
        }
    }

    /// Grants write permission for a key in etcd's key-value store to this role.
    pub fn grant_kv_write_permission<K>(&mut self, key: K)
    where
        K: Into<String>,
    {
        match self.grants {
            Some(ref mut grants) => grants.kv.modify_write_permission(key),
            None => {
                let mut permissions = Permissions::new();
                permissions.kv.modify_write_permission(key);
                self.grants = Some(permissions);
            }
        }
    }

    /// Revokes read permission for a key in etcd's key-value store from this role.
    pub fn revoke_kv_read_permission<K>(&mut self, key: K)
    where
        K: Into<String>,
    {
        match self.revocations {
            Some(ref mut revocations) => revocations.kv.modify_read_permission(key),
            None => {
                let mut permissions = Permissions::new();
                permissions.kv.modify_read_permission(key);
                self.revocations = Some(permissions);
            }
        }
    }

    /// Revokes write permission for a key in etcd's key-value store from this role.
    pub fn revoke_kv_write_permission<K>(&mut self, key: K)
    where
        K: Into<String>,
    {
        match self.revocations {
            Some(ref mut revocations) => revocations.kv.modify_write_permission(key),
            None => {
                let mut permissions = Permissions::new();
                permissions.kv.modify_write_permission(key);
                self.revocations = Some(permissions);
            }
        }
    }
}

/// The access permissions granted to a role.
#[derive(Debug, Deserialize, Clone, Eq, Hash, PartialEq, Serialize)]
struct Permissions {
    /// Permissions for etcd's key-value store.
    kv: Permission,
}

impl Permissions {
    /// Creates a new set of permissions.
    fn new() -> Self {
        Permissions {
            kv: Permission::new(),
        }
    }
}

/// A set of read and write access permissions for etcd resources.
#[derive(Debug, Deserialize, Clone, Eq, Hash, PartialEq, Serialize)]
struct Permission {
    /// Resources allowed to be read.
    #[serde(skip_serializing_if = "Option::is_none")]
    read: Option<Vec<String>>,
    /// Resources allowed to be written.
    #[serde(skip_serializing_if = "Option::is_none")]
    write: Option<Vec<String>>,
}

impl Permission {
    /// Creates a new permission record.
    fn new() -> Self {
        Permission {
            read: None,
            write: None,
        }
    }

    /// Modifies read access to a resource.
    fn modify_read_permission<K>(&mut self, key: K)
    where
        K: Into<String>,
    {
        match self.read {
            Some(ref mut read) => read.push(key.into()),
            None => self.read = Some(vec![key.into()]),
        }
    }

    /// Modifies write access to a resource.
    fn modify_write_permission<K>(&mut self, key: K)
    where
        K: Into<String>,
    {
        match self.write {
            Some(ref mut write) => write.push(key.into()),
            None => self.write = Some(vec![key.into()]),
        }
    }
}

/// Creates a new role.
pub fn create_role<C>(
    client: &Client<C>,
    role: Role,
) -> impl Future<Output = Result<Response<Role>, Vec<Error>>> + Send
where
    C: Clone + Connect + Sync + Send + 'static,
{
    let http_client = client.http_client().clone();

    first_ok(client.endpoints().to_vec(), move |member| {
        let body = serde_json::to_string(&role)
            .map_err(Error::from)
            .into_future();

        let url = build_url(member, &format!("/roles/{}", role.name));
        let uri = Uri::from_str(url.as_str())
            .map_err(Error::from)
            .into_future();

        let params = uri.join(body);

        let http_client = http_client.clone();

        let response =
            params.and_then(move |(uri, body)| http_client.put(uri, body).map_err(Error::from));

        response.and_then(|response| {
            let status = response.status();
            let cluster_info = ClusterInfo::from(response.headers());
            let body = response.into_body().concat2().map_err(Error::from);

            body.and_then(move |ref body| match status {
                StatusCode::OK | StatusCode::CREATED => {
                    match serde_json::from_slice::<Role>(body) {
                        Ok(data) => Ok(Response { data, cluster_info }),
                        Err(error) => Err(Error::Serialization(error)),
                    }
                }
                status => Err(Error::UnexpectedStatus(status)),
            })
        })
    })
}

/// Creates a new user.
pub fn create_user<C>(
    client: &Client<C>,
    user: NewUser,
) -> impl Future<Output = Result<Response<User>, Vec<Error>>> + Send
where
    C: Clone + Connect + Sync + Send + 'static,
{
    let http_client = client.http_client().clone();

    first_ok(client.endpoints().to_vec(), move |member| {
        let body = serde_json::to_string(&user)
            .map_err(Error::from)
            .into_future();

        let url = build_url(member, &format!("/users/{}", user.name));
        let uri = Uri::from_str(url.as_str())
            .map_err(Error::from)
            .into_future();

        let params = uri.join(body);

        let http_client = http_client.clone();

        let response =
            params.and_then(move |(uri, body)| http_client.put(uri, body).map_err(Error::from));

        response.and_then(|response| {
            let status = response.status();
            let cluster_info = ClusterInfo::from(response.headers());
            let body = response.into_body().concat2().map_err(Error::from);

            body.and_then(move |ref body| match status {
                StatusCode::OK | StatusCode::CREATED => {
                    match serde_json::from_slice::<User>(body) {
                        Ok(data) => Ok(Response { data, cluster_info }),
                        Err(error) => Err(Error::Serialization(error)),
                    }
                }
                status => Err(Error::UnexpectedStatus(status)),
            })
        })
    })
}

/// Deletes a role.
pub fn delete_role<C, N>(
    client: &Client<C>,
    name: N,
) -> impl Future<Output = Result<Response<()>, Vec<Error>>> + Send
where
    C: Clone + Connect + Sync + Send + 'static,
    N: Into<String>,
{
    let http_client = client.http_client().clone();
    let name = name.into();

    first_ok(client.endpoints().to_vec(), move |member| {
        let url = build_url(member, &format!("/roles/{}", name));
        let uri = Uri::from_str(url.as_str())
            .map_err(Error::from)
            .into_future();

        let http_client = http_client.clone();

        let response = uri.and_then(move |uri| http_client.delete(uri).map_err(Error::from));

        response.and_then(|response| {
            let status = response.status();
            let cluster_info = ClusterInfo::from(response.headers());

            if status == StatusCode::OK {
                Ok(Response {
                    data: (),
                    cluster_info,
                })
            } else {
                Err(Error::UnexpectedStatus(status))
            }
        })
    })
}

/// Deletes a user.
pub fn delete_user<C, N>(
    client: &Client<C>,
    name: N,
) -> impl Future<Output = Result<Response<()>, Vec<Error>>> + Send
where
    C: Clone + Connect + Sync + Send + 'static,
    N: Into<String>,
{
    let http_client = client.http_client().clone();
    let name = name.into();

    first_ok(client.endpoints().to_vec(), move |member| {
        let url = build_url(member, &format!("/users/{}", name));
        let uri = Uri::from_str(url.as_str())
            .map_err(Error::from)
            .into_future();

        let http_client = http_client.clone();

        let response = uri.and_then(move |uri| http_client.delete(uri).map_err(Error::from));

        response.and_then(|response| {
            let status = response.status();
            let cluster_info = ClusterInfo::from(response.headers());

            if status == StatusCode::OK {
                Ok(Response {
                    data: (),
                    cluster_info,
                })
            } else {
                Err(Error::UnexpectedStatus(status))
            }
        })
    })
}

/// Attempts to disable the auth system.
pub fn disable<C>(
    client: &Client<C>,
) -> impl Future<Output = Result<Response<AuthChange>, Vec<Error>>> + Send
where
    C: Clone + Connect + Sync + Send + 'static,
{
    let http_client = client.http_client().clone();

    first_ok(client.endpoints().to_vec(), move |member| {
        let url = build_url(member, "/enable");
        let uri = Uri::from_str(url.as_str())
            .map_err(Error::from)
            .into_future();

        let http_client = http_client.clone();

        let response = uri.and_then(move |uri| http_client.delete(uri).map_err(Error::from));

        response.and_then(|response| {
            let status = response.status();
            let cluster_info = ClusterInfo::from(response.headers());

            match status {
                StatusCode::OK => Ok(Response {
                    data: AuthChange::Changed,
                    cluster_info,
                }),
                StatusCode::CONFLICT => Ok(Response {
                    data: AuthChange::Unchanged,
                    cluster_info,
                }),
                _ => Err(Error::UnexpectedStatus(status)),
            }
        })
    })
}

/// Attempts to enable the auth system.
pub fn enable<C>(
    client: &Client<C>,
) -> impl Future<Output = Result<Response<AuthChange>, Vec<Error>>> + Send
where
    C: Clone + Connect + Sync + Send + 'static,
{
    let http_client = client.http_client().clone();

    first_ok(client.endpoints().to_vec(), move |member| {
        let url = build_url(member, "/enable");
        let uri = Uri::from_str(url.as_str())
            .map_err(Error::from)
            .into_future();

        let http_client = http_client.clone();

        let response =
            uri.and_then(move |uri| http_client.put(uri, "".to_owned()).map_err(Error::from));

        response.and_then(|response| {
            let status = response.status();
            let cluster_info = ClusterInfo::from(response.headers());

            match status {
                StatusCode::OK => Ok(Response {
                    data: AuthChange::Changed,
                    cluster_info,
                }),
                StatusCode::CONFLICT => Ok(Response {
                    data: AuthChange::Unchanged,
                    cluster_info,
                }),
                _ => return Err(Error::UnexpectedStatus(status)),
            }
        })
    })
}

/// Get a role.
pub fn get_role<C, N>(
    client: &Client<C>,
    name: N,
) -> impl Future<Output = Result<Response<Role>, Vec<Error>>> + Send
where
    C: Clone + Connect + Sync + Send + 'static,
    N: Into<String>,
{
    let http_client = client.http_client().clone();
    let name = name.into();

    first_ok(client.endpoints().to_vec(), move |member| {
        let url = build_url(member, &format!("/roles/{}", name));
        let uri = Uri::from_str(url.as_str())
            .map_err(Error::from)
            .into_future();

        let http_client = http_client.clone();

        let response = uri.and_then(move |uri| http_client.get(uri).map_err(Error::from));

        response.and_then(|response| {
            let status = response.status();
            let cluster_info = ClusterInfo::from(response.headers());
            let body = response.into_body().concat2().map_err(Error::from);

            body.and_then(move |ref body| {
                if status == StatusCode::OK {
                    match serde_json::from_slice::<Role>(body) {
                        Ok(data) => Ok(Response { data, cluster_info }),
                        Err(error) => Err(Error::Serialization(error)),
                    }
                } else {
                    Err(Error::UnexpectedStatus(status))
                }
            })
        })
    })
}

/// Gets all roles.
pub fn get_roles<C>(
    client: &Client<C>,
) -> impl Future<Output = Result<Response<Vec<Role>>, Vec<Error>>> + Send
where
    C: Clone + Connect + Sync + Send + 'static,
{
    let http_client = client.http_client().clone();

    first_ok(client.endpoints().to_vec(), move |member| {
        let url = build_url(member, "/roles");
        let uri = Uri::from_str(url.as_str())
            .map_err(Error::from)
            .into_future();

        let http_client = http_client.clone();

        let response = uri.and_then(move |uri| http_client.get(uri).map_err(Error::from));

        response.and_then(|response| {
            let status = response.status();
            let cluster_info = ClusterInfo::from(response.headers());
            let body = response.into_body().concat2().map_err(Error::from);

            body.and_then(move |ref body| {
                if status == StatusCode::OK {
                    match serde_json::from_slice::<Roles>(body) {
                        Ok(roles) => {
                            let data = roles.roles.unwrap_or_else(|| Vec::with_capacity(0));

                            Ok(Response { data, cluster_info })
                        }
                        Err(error) => Err(Error::Serialization(error)),
                    }
                } else {
                    Err(Error::UnexpectedStatus(status))
                }
            })
        })
    })
}

/// Get a user.
pub fn get_user<C, N>(
    client: &Client<C>,
    name: N,
) -> impl Future<Output = Result<Response<UserDetail>, Vec<Error>>> + Send
where
    C: Clone + Connect + Sync + Send + 'static,
    N: Into<String>,
{
    let http_client = client.http_client().clone();
    let name = name.into();

    first_ok(client.endpoints().to_vec(), move |member| {
        let url = build_url(member, &format!("/users/{}", name));
        let uri = Uri::from_str(url.as_str())
            .map_err(Error::from)
            .into_future();

        let http_client = http_client.clone();

        let response = uri.and_then(move |uri| http_client.get(uri).map_err(Error::from));

        response.and_then(|response| {
            let status = response.status();
            let cluster_info = ClusterInfo::from(response.headers());
            let body = response.into_body().concat2().map_err(Error::from);

            body.and_then(move |ref body| {
                if status == StatusCode::OK {
                    match serde_json::from_slice::<UserDetail>(body) {
                        Ok(data) => Ok(Response { data, cluster_info }),
                        Err(error) => Err(Error::Serialization(error)),
                    }
                } else {
                    Err(Error::UnexpectedStatus(status))
                }
            })
        })
    })
}

/// Gets all users.
pub fn get_users<C>(
    client: &Client<C>,
) -> impl Future<Output = Result<Response<Vec<UserDetail>>, Vec<Error>>> + Send
where
    C: Clone + Connect + Sync + Send + 'static,
{
    let http_client = client.http_client().clone();

    first_ok(client.endpoints().to_vec(), move |member| {
        let url = build_url(member, "/users");
        let uri = Uri::from_str(url.as_str())
            .map_err(Error::from)
            .into_future();

        let http_client = http_client.clone();

        let response = uri.and_then(move |uri| http_client.get(uri).map_err(Error::from));

        response.and_then(|response| {
            let status = response.status();
            let cluster_info = ClusterInfo::from(response.headers());
            let body = response.into_body().concat2().map_err(Error::from);

            body.and_then(move |ref body| {
                if status == StatusCode::OK {
                    match serde_json::from_slice::<Users>(body) {
                        Ok(users) => {
                            let data = users.users.unwrap_or_else(|| Vec::with_capacity(0));

                            Ok(Response { data, cluster_info })
                        }
                        Err(error) => Err(Error::Serialization(error)),
                    }
                } else {
                    Err(Error::UnexpectedStatus(status))
                }
            })
        })
    })
}

/// Determines whether or not the auth system is enabled.
pub fn status<C>(
    client: &Client<C>,
) -> impl Future<Output = Result<Response<bool>, Vec<Error>>> + Send
where
    C: Clone + Connect + Sync + Send + 'static,
{
    let http_client = client.http_client().clone();

    first_ok(client.endpoints().to_vec(), move |member| {
        let url = build_url(member, "/enable");
        let uri = Uri::from_str(url.as_str())
            .map_err(Error::from)
            .into_future();

        let http_client = http_client.clone();

        let response = uri.and_then(move |uri| http_client.get(uri).map_err(Error::from));

        response.and_then(|response| {
            let status = response.status();
            let cluster_info = ClusterInfo::from(response.headers());
            let body = response.into_body().concat2().map_err(Error::from);

            body.and_then(move |ref body| {
                if status == StatusCode::OK {
                    match serde_json::from_slice::<AuthStatus>(body) {
                        Ok(data) => Ok(Response {
                            data: data.enabled,
                            cluster_info,
                        }),
                        Err(error) => Err(Error::Serialization(error)),
                    }
                } else {
                    match serde_json::from_slice::<ApiError>(body) {
                        Ok(error) => Err(Error::Api(error)),
                        Err(error) => Err(Error::Serialization(error)),
                    }
                }
            })
        })
    })
}

/// Updates an existing role.
pub fn update_role<C>(
    client: &Client<C>,
    role: RoleUpdate,
) -> impl Future<Output = Result<Response<Role>, Vec<Error>>> + Send
where
    C: Clone + Connect + Sync + Send + 'static,
{
    let http_client = client.http_client().clone();

    first_ok(client.endpoints().to_vec(), move |member| {
        let body = serde_json::to_string(&role)
            .map_err(Error::from)
            .into_future();

        let url = build_url(member, &format!("/roles/{}", role.name));
        let uri = Uri::from_str(url.as_str())
            .map_err(Error::from)
            .into_future();

        let params = uri.join(body);

        let http_client = http_client.clone();

        let response =
            params.and_then(move |(uri, body)| http_client.put(uri, body).map_err(Error::from));

        response.and_then(|response| {
            let status = response.status();
            let cluster_info = ClusterInfo::from(response.headers());
            let body = response.into_body().concat2().map_err(Error::from);

            body.and_then(move |ref body| {
                if status == StatusCode::OK {
                    match serde_json::from_slice::<Role>(body) {
                        Ok(data) => Ok(Response { data, cluster_info }),
                        Err(error) => Err(Error::Serialization(error)),
                    }
                } else {
                    Err(Error::UnexpectedStatus(status))
                }
            })
        })
    })
}

/// Updates an existing user.
pub fn update_user<C>(
    client: &Client<C>,
    user: UserUpdate,
) -> impl Future<Output = Result<Response<User>, Vec<Error>>> + Send
where
    C: Clone + Connect + Sync + Send + 'static,
{
    let http_client = client.http_client().clone();

    first_ok(client.endpoints().to_vec(), move |member| {
        let body = serde_json::to_string(&user)
            .map_err(Error::from)
            .into_future();

        let url = build_url(member, &format!("/users/{}", user.name));
        let uri = Uri::from_str(url.as_str())
            .map_err(Error::from)
            .into_future();

        let params = uri.join(body);

        let http_client = http_client.clone();

        let response =
            params.and_then(move |(uri, body)| http_client.put(uri, body).map_err(Error::from));

        response.and_then(|response| {
            let status = response.status();
            let cluster_info = ClusterInfo::from(response.headers());
            let body = response.into_body().concat2().map_err(Error::from);

            body.and_then(move |ref body| {
                if status == StatusCode::OK {
                    match serde_json::from_slice::<User>(body) {
                        Ok(data) => Ok(Response { data, cluster_info }),
                        Err(error) => Err(Error::Serialization(error)),
                    }
                } else {
                    Err(Error::UnexpectedStatus(status))
                }
            })
        })
    })
}

/// Constructs the full URL for an API call.
fn build_url(endpoint: &Uri, path: &str) -> String {
    format!("{}v2/auth{}", endpoint, path)
}
