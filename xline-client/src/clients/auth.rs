use std::sync::Arc;

use curp::{client::Client as CurpClient, cmd::ProposeId};
use pbkdf2::{
    password_hash::{rand_core::OsRng, PasswordHasher, SaltString},
    Pbkdf2,
};
use tonic::transport::Channel;
use uuid::Uuid;
use xline::server::{Command, KeyRange};
use xlineapi::{
    AuthDisableResponse, AuthEnableResponse, AuthRoleAddResponse, AuthRoleDeleteResponse,
    AuthRoleGetResponse, AuthRoleGrantPermissionResponse, AuthRoleListResponse,
    AuthRoleRevokePermissionResponse, AuthStatusResponse, AuthUserAddResponse,
    AuthUserChangePasswordResponse, AuthUserDeleteResponse, AuthUserGetResponse,
    AuthUserGrantRoleResponse, AuthUserListResponse, AuthUserRevokeRoleResponse,
    AuthenticateResponse, RequestWithToken, RequestWrapper, ResponseWrapper,
    Type as PermissionType,
};

use crate::{
    error::{ClientError, Result},
    AuthService,
};

/// Client for Auth operations.
#[derive(Clone, Debug)]
pub struct AuthClient {
    /// Name of the AuthClient
    name: String,
    /// The client running the CURP protocol, communicate with all servers.
    curp_client: Arc<CurpClient<Command>>,
    /// The auth RPC client, only communicate with one server at a time
    auth_client: xlineapi::AuthClient<AuthService<Channel>>,
    /// Auth token
    token: Option<String>,
}

impl AuthClient {
    /// New `AuthClient`
    #[inline]
    pub fn new(
        name: String,
        curp_client: Arc<CurpClient<Command>>,
        channel: Channel,
        token: Option<String>,
    ) -> Self {
        Self {
            name,
            curp_client,
            auth_client: xlineapi::AuthClient::new(AuthService::new(
                channel,
                token.as_ref().and_then(|t| t.parse().ok().map(Arc::new)),
            )),
            token,
        }
    }

    /// Enables authentication.
    ///
    /// # Errors
    ///
    /// If request fails to send
    #[inline]
    pub async fn auth_enable(&self) -> Result<AuthEnableResponse> {
        self.handle_req_slow(xlineapi::AuthEnableRequest {}).await
    }

    /// Disables authentication.
    ///
    /// # Errors
    ///
    /// If request fails to send
    #[inline]
    pub async fn auth_disable(&self) -> Result<AuthDisableResponse> {
        self.handle_req_slow(xlineapi::AuthDisableRequest {}).await
    }

    /// Get auth status.
    ///
    /// # Errors
    ///
    /// If request fails to send
    #[inline]
    pub async fn auth_status(&self) -> Result<AuthStatusResponse> {
        self.handle_req_fast(xlineapi::AuthStatusRequest {}).await
    }

    /// Get token through authenticate
    ///
    /// # Errors
    ///
    /// If request fails to send
    #[inline]
    pub async fn authenticate(
        &mut self,
        request: AuthenticateRequest,
    ) -> Result<AuthenticateResponse> {
        Ok(self
            .auth_client
            .authenticate(xlineapi::AuthenticateRequest::from(request))
            .await?
            .into_inner())
    }

    /// Add an user.
    ///
    /// # Errors
    ///
    /// If request fails to send
    #[inline]
    pub async fn user_add(&self, mut request: AuthUserAddRequest) -> Result<AuthUserAddResponse> {
        if request.inner.name.is_empty() {
            return Err(ClientError::InvalidArgs(String::from("user name is empty")));
        }
        let need_password = request
            .inner
            .options
            .as_ref()
            .map_or(true, |o| !o.no_password);
        if need_password && request.inner.password.is_empty() {
            return Err(ClientError::InvalidArgs(String::from(
                "password is required but not provided",
            )));
        }
        let hashed_password = Self::hash_password(request.inner.password.as_bytes());
        request.inner.hashed_password = hashed_password;
        request.inner.password = String::new();
        self.handle_req_slow(request.inner).await
    }

    /// Gets the user info by the user name.
    ///
    /// # Errors
    ///
    /// If request fails to send
    #[inline]
    pub async fn user_get(&self, request: AuthUserGetRequest) -> Result<AuthUserGetResponse> {
        self.handle_req_fast(request.inner).await
    }

    /// Lists all users.
    ///
    /// # Errors
    ///
    /// If request fails to send
    #[inline]
    pub async fn user_list(&self) -> Result<AuthUserListResponse> {
        self.handle_req_fast(xlineapi::AuthUserListRequest {}).await
    }

    /// Deletes the given key from the key-value store.
    ///
    /// # Errors
    ///
    /// If request fails to send
    #[inline]
    pub async fn user_delete(
        &self,
        request: AuthUserDeleteRequest,
    ) -> Result<AuthUserDeleteResponse> {
        self.handle_req_slow(request.inner).await
    }

    /// Change password for an user.
    ///
    /// # Errors
    ///
    /// If request fails to send
    #[inline]
    pub async fn user_change_password(
        &self,
        mut request: AuthUserChangePasswordRequest,
    ) -> Result<AuthUserChangePasswordResponse> {
        if request.inner.password.is_empty() {
            return Err(ClientError::InvalidArgs(String::from("role name is empty")));
        }
        let hashed_password = Self::hash_password(request.inner.password.as_bytes());
        request.inner.hashed_password = hashed_password;
        request.inner.password = String::new();
        self.handle_req_slow(request.inner).await
    }

    /// Grant role for an user.
    ///
    /// # Errors
    ///
    /// If request fails to send
    #[inline]
    pub async fn user_grant_role(
        &self,
        request: AuthUserGrantRoleRequest,
    ) -> Result<AuthUserGrantRoleResponse> {
        self.handle_req_slow(request.inner).await
    }

    /// Revoke role for an user.
    ///
    /// # Errors
    ///
    /// If request fails to send
    #[inline]
    pub async fn user_revoke_role(
        &self,
        request: AuthUserRevokeRoleRequest,
    ) -> Result<AuthUserRevokeRoleResponse> {
        self.handle_req_slow(request.inner).await
    }

    /// Adds role.
    ///
    /// # Errors
    ///
    /// If request fails to send
    #[inline]
    pub async fn role_add(&self, request: AuthRoleAddRequest) -> Result<AuthRoleAddResponse> {
        if request.inner.name.is_empty() {
            return Err(ClientError::InvalidArgs(String::from("role name is empty")));
        }
        self.handle_req_slow(request.inner).await
    }

    /// Gets role.
    ///
    /// # Errors
    ///
    /// If request fails to send
    #[inline]
    pub async fn role_get(&self, request: AuthRoleGetRequest) -> Result<AuthRoleGetResponse> {
        self.handle_req_fast(request.inner).await
    }

    /// Lists role.
    ///
    /// # Errors
    ///
    /// If request fails to send
    #[inline]
    pub async fn role_list(&self) -> Result<AuthRoleListResponse> {
        self.handle_req_fast(xlineapi::AuthRoleListRequest {}).await
    }

    /// Deletes role.
    ///
    /// # Errors
    ///
    /// If request fails to send
    #[inline]
    pub async fn role_delete(
        &self,
        request: AuthRoleDeleteRequest,
    ) -> Result<AuthRoleDeleteResponse> {
        self.handle_req_slow(request.inner).await
    }

    /// Grants role permission.
    ///
    /// # Errors
    ///
    /// If request fails to send
    #[inline]
    pub async fn role_grant_permission(
        &self,
        request: AuthRoleGrantPermissionRequest,
    ) -> Result<AuthRoleGrantPermissionResponse> {
        if request.inner.perm.is_none() {
            return Err(ClientError::InvalidArgs(String::from(
                "Permission not given",
            )));
        }
        self.handle_req_slow(request.inner).await
    }

    /// Revokes role permission.
    ///
    /// # Errors
    ///
    /// If request fails to send
    #[inline]
    pub async fn role_revoke_permission(
        &self,
        request: AuthRoleRevokePermissionRequest,
    ) -> Result<AuthRoleRevokePermissionResponse> {
        self.handle_req_slow(request.inner).await
    }

    /// Send request using fast path
    async fn handle_req_fast<Req: Into<RequestWrapper>, Res: From<ResponseWrapper>>(
        &self,
        request: Req,
    ) -> Result<Res> {
        let propose_id = self.generate_propose_id();
        let request = RequestWithToken::new_with_token(request.into(), self.token.clone());
        let cmd = Command::new(vec![], request, propose_id);
        let cmd_res = self.curp_client.propose(cmd).await?;
        Ok(cmd_res.decode().into())
    }

    /// Send request using fast path
    async fn handle_req_slow<Req: Into<RequestWrapper>, Res: From<ResponseWrapper>>(
        &self,
        request: Req,
    ) -> Result<Res> {
        let propose_id = self.generate_propose_id();
        let request = RequestWithToken::new_with_token(request.into(), self.token.clone());
        let cmd = Command::new(vec![], request, propose_id);
        let (cmd_res, sync_res) = self.curp_client.propose_indexed(cmd).await?;
        let mut res_wrapper = cmd_res.decode();
        res_wrapper.update_revision(sync_res.revision());
        Ok(res_wrapper.into())
    }

    /// Hash password
    fn hash_password(password: &[u8]) -> String {
        let salt = SaltString::generate(&mut OsRng);
        #[allow(clippy::panic)] // This doesn't seems to be fallible
        let hashed_password = Pbkdf2
            .hash_password(password, salt.as_ref())
            .unwrap_or_else(|e| panic!("Failed to hash password: {e}"));
        hashed_password.to_string()
    }

    /// Generate a new `ProposeId`
    fn generate_propose_id(&self) -> ProposeId {
        ProposeId::new(format!("{}-{}", self.name, Uuid::new_v4()))
    }
}

/// Request for `Authenticate`
#[derive(Debug)]
pub struct AuthenticateRequest {
    /// inner request
    inner: xlineapi::AuthenticateRequest,
}

impl AuthenticateRequest {
    /// New `AuthenticateRequest`
    #[inline]
    pub fn new(name: impl Into<String>, password: impl Into<String>) -> Self {
        Self {
            inner: xlineapi::AuthenticateRequest {
                name: name.into(),
                password: password.into(),
            },
        }
    }
}

impl From<AuthenticateRequest> for xlineapi::AuthenticateRequest {
    #[inline]
    fn from(req: AuthenticateRequest) -> Self {
        req.inner
    }
}

/// Request for `Authenticate`
#[derive(Debug, PartialEq)]
pub struct AuthUserAddRequest {
    /// inner request
    inner: xlineapi::AuthUserAddRequest,
}

impl AuthUserAddRequest {
    /// New `AuthUserAddRequest`
    #[inline]
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            inner: xlineapi::AuthUserAddRequest {
                name: name.into(),
                ..Default::default()
            },
        }
    }

    /// Set password.
    #[inline]
    #[must_use]
    pub fn with_pwd(mut self, password: impl Into<String>) -> Self {
        self.inner.password = password.into();
        self
    }

    /// Set no password.
    #[inline]
    #[must_use]
    pub const fn with_no_pwd(mut self) -> Self {
        self.inner.options = Some(xlineapi::UserAddOptions { no_password: true });
        self
    }
}

impl From<AuthUserAddRequest> for xlineapi::AuthUserAddRequest {
    #[inline]
    fn from(req: AuthUserAddRequest) -> Self {
        req.inner
    }
}

/// Request for `AuthUserGet`
#[derive(Debug, PartialEq)]
pub struct AuthUserGetRequest {
    /// inner request
    inner: xlineapi::AuthUserGetRequest,
}

impl AuthUserGetRequest {
    /// New `AuthUserGetRequest`
    #[inline]
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            inner: xlineapi::AuthUserGetRequest { name: name.into() },
        }
    }
}

impl From<AuthUserGetRequest> for xlineapi::AuthUserGetRequest {
    #[inline]
    fn from(req: AuthUserGetRequest) -> Self {
        req.inner
    }
}

/// Request for `AuthUserDelete`
#[derive(Debug, PartialEq)]
pub struct AuthUserDeleteRequest {
    /// inner request
    inner: xlineapi::AuthUserDeleteRequest,
}

impl AuthUserDeleteRequest {
    /// New `AuthUserDeleteRequest`
    #[inline]
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            inner: xlineapi::AuthUserDeleteRequest { name: name.into() },
        }
    }
}

impl From<AuthUserDeleteRequest> for xlineapi::AuthUserDeleteRequest {
    #[inline]
    fn from(req: AuthUserDeleteRequest) -> Self {
        req.inner
    }
}

/// Request for `AuthUserChangePassword`
#[derive(Debug, PartialEq)]
pub struct AuthUserChangePasswordRequest {
    /// inner request
    inner: xlineapi::AuthUserChangePasswordRequest,
}

impl AuthUserChangePasswordRequest {
    /// New `AuthUserChangePasswordRequest`
    #[inline]
    pub fn new(name: impl Into<String>, password: impl Into<String>) -> Self {
        Self {
            inner: xlineapi::AuthUserChangePasswordRequest {
                name: name.into(),
                password: password.into(),
                hashed_password: String::new(),
            },
        }
    }
}

impl From<AuthUserChangePasswordRequest> for xlineapi::AuthUserChangePasswordRequest {
    #[inline]
    fn from(req: AuthUserChangePasswordRequest) -> Self {
        req.inner
    }
}

/// Request for `AuthUserGrantRole`
#[derive(Debug, PartialEq)]
pub struct AuthUserGrantRoleRequest {
    /// inner request
    inner: xlineapi::AuthUserGrantRoleRequest,
}

impl AuthUserGrantRoleRequest {
    /// New `AuthUserGrantRoleRequest`
    #[inline]
    pub fn new(name: impl Into<String>, role: impl Into<String>) -> Self {
        Self {
            inner: xlineapi::AuthUserGrantRoleRequest {
                user: name.into(),
                role: role.into(),
            },
        }
    }
}

impl From<AuthUserGrantRoleRequest> for xlineapi::AuthUserGrantRoleRequest {
    #[inline]
    fn from(req: AuthUserGrantRoleRequest) -> Self {
        req.inner
    }
}

/// Request for `AuthUserRevokeRole`
#[derive(Debug, PartialEq)]
pub struct AuthUserRevokeRoleRequest {
    /// inner request
    inner: xlineapi::AuthUserRevokeRoleRequest,
}

impl AuthUserRevokeRoleRequest {
    /// New `AuthUserRevokeRoleRequest`
    #[inline]
    pub fn new(name: impl Into<String>, role: impl Into<String>) -> Self {
        Self {
            inner: xlineapi::AuthUserRevokeRoleRequest {
                name: name.into(),
                role: role.into(),
            },
        }
    }
}

impl From<AuthUserRevokeRoleRequest> for xlineapi::AuthUserRevokeRoleRequest {
    #[inline]
    fn from(req: AuthUserRevokeRoleRequest) -> Self {
        req.inner
    }
}

/// Request for `AuthRoleAdd`
#[derive(Debug)]
pub struct AuthRoleAddRequest {
    /// inner request
    inner: xlineapi::AuthRoleAddRequest,
}

impl AuthRoleAddRequest {
    /// New `AuthRoleAddRequest`
    #[inline]
    pub fn new(role: impl Into<String>) -> Self {
        Self {
            inner: xlineapi::AuthRoleAddRequest { name: role.into() },
        }
    }
}

impl From<AuthRoleAddRequest> for xlineapi::AuthRoleAddRequest {
    #[inline]
    fn from(req: AuthRoleAddRequest) -> Self {
        req.inner
    }
}

/// Request for `AuthRoleGet`
#[derive(Debug)]
pub struct AuthRoleGetRequest {
    /// inner request
    inner: xlineapi::AuthRoleGetRequest,
}

impl AuthRoleGetRequest {
    /// New `AuthRoleGetRequest`
    #[inline]
    pub fn new(role: impl Into<String>) -> Self {
        Self {
            inner: xlineapi::AuthRoleGetRequest { role: role.into() },
        }
    }
}

impl From<AuthRoleGetRequest> for xlineapi::AuthRoleGetRequest {
    #[inline]
    fn from(req: AuthRoleGetRequest) -> Self {
        req.inner
    }
}

/// Request for `AuthRoleDelete`
#[derive(Debug)]
pub struct AuthRoleDeleteRequest {
    /// inner request
    inner: xlineapi::AuthRoleDeleteRequest,
}

impl AuthRoleDeleteRequest {
    /// New `AuthRoleDeleteRequest`
    #[inline]
    pub fn new(role: impl Into<String>) -> Self {
        Self {
            inner: xlineapi::AuthRoleDeleteRequest { role: role.into() },
        }
    }
}

impl From<AuthRoleDeleteRequest> for xlineapi::AuthRoleDeleteRequest {
    #[inline]
    fn from(req: AuthRoleDeleteRequest) -> Self {
        req.inner
    }
}

/// Request for `AuthRoleGrantPermission`
#[derive(Debug)]
pub struct AuthRoleGrantPermissionRequest {
    /// inner request
    inner: xlineapi::AuthRoleGrantPermissionRequest,
}

impl AuthRoleGrantPermissionRequest {
    /// New `AuthRoleGrantPermissionRequest`
    #[inline]
    pub fn new(role: impl Into<String>, perm: Permission) -> Self {
        Self {
            inner: xlineapi::AuthRoleGrantPermissionRequest {
                name: role.into(),
                perm: Some(perm.into()),
            },
        }
    }
}

impl From<AuthRoleGrantPermissionRequest> for xlineapi::AuthRoleGrantPermissionRequest {
    #[inline]
    fn from(req: AuthRoleGrantPermissionRequest) -> Self {
        req.inner
    }
}

/// Request for `AuthRoleRevokePermission`
#[derive(Debug)]
pub struct AuthRoleRevokePermissionRequest {
    /// inner request
    inner: xlineapi::AuthRoleRevokePermissionRequest,
}

impl AuthRoleRevokePermissionRequest {
    /// Create a new `RoleRevokePermissionOption` from pb role revoke permission.
    #[inline]
    pub fn new(role: impl Into<String>, key: impl Into<Vec<u8>>) -> Self {
        Self {
            inner: xlineapi::AuthRoleRevokePermissionRequest {
                role: role.into(),
                key: key.into(),
                ..Default::default()
            },
        }
    }

    /// Set `key` and `range_end` when with prefix
    #[inline]
    #[must_use]
    pub fn with_prefix(mut self) -> Self {
        if self.inner.key.is_empty() {
            self.inner.key = vec![0];
            self.inner.range_end = vec![0];
        } else {
            self.inner.range_end = KeyRange::get_prefix(&self.inner.key);
        }
        self
    }

    /// Set `key` and `range_end` when with from key
    #[inline]
    #[must_use]
    pub fn with_from_key(mut self) -> Self {
        if self.inner.key.is_empty() {
            self.inner.key = vec![0];
        }
        self.inner.range_end = vec![0];
        self
    }

    /// Set `range_end`
    #[inline]
    #[must_use]
    pub fn with_range_end(mut self, range_end: impl Into<Vec<u8>>) -> Self {
        self.inner.range_end = range_end.into();
        self
    }
}

impl From<AuthRoleRevokePermissionRequest> for xlineapi::AuthRoleRevokePermissionRequest {
    #[inline]
    fn from(req: AuthRoleRevokePermissionRequest) -> Self {
        req.inner
    }
}

/// Role access permission.
#[derive(Debug, Clone)]
pub struct Permission {
    /// The inner Permission
    inner: xlineapi::Permission,
}

impl Permission {
    /// Creates a permission with operation type and key
    #[inline]
    #[must_use]
    pub fn new(perm_type: PermissionType, key: impl Into<Vec<u8>>) -> Self {
        Self {
            inner: xlineapi::Permission {
                perm_type: perm_type.into(),
                key: key.into(),
                ..Default::default()
            },
        }
    }
    /// Set `key` and `range_end` when with prefix
    #[inline]
    #[must_use]
    pub fn with_prefix(mut self) -> Self {
        if self.inner.key.is_empty() {
            self.inner.key = vec![0];
            self.inner.range_end = vec![0];
        } else {
            self.inner.range_end = KeyRange::get_prefix(&self.inner.key);
        }
        self
    }

    /// Set `key` and `range_end` when with from key
    #[inline]
    #[must_use]
    pub fn with_from_key(mut self) -> Self {
        if self.inner.key.is_empty() {
            self.inner.key = vec![0];
        }
        self.inner.range_end = vec![0];
        self
    }

    /// Set `range_end`
    #[inline]
    #[must_use]
    pub fn with_range_end(mut self, range_end: impl Into<Vec<u8>>) -> Self {
        self.inner.range_end = range_end.into();
        self
    }
}

impl From<Permission> for xlineapi::Permission {
    #[inline]
    fn from(perm: Permission) -> Self {
        perm.inner
    }
}
