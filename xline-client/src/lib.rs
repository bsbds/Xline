//! Xline-client
#![deny(
    // The following are allowed by default lints according to
    // https://doc.rust-lang.org/rustc/lints/listing/allowed-by-default.html

    absolute_paths_not_starting_with_crate,
    // box_pointers, async trait must use it
    // elided_lifetimes_in_paths,  // allow anonymous lifetime
    explicit_outlives_requirements,
    keyword_idents,
    macro_use_extern_crate,
    meta_variable_misuse,
    missing_abi,
    missing_copy_implementations,
    missing_debug_implementations,
    missing_docs,
    // must_not_suspend, unstable
    non_ascii_idents,
    // non_exhaustive_omitted_patterns, unstable
    noop_method_call,
    pointer_structural_match,
    rust_2021_incompatible_closure_captures,
    rust_2021_incompatible_or_patterns,
    rust_2021_prefixes_incompatible_syntax,
    rust_2021_prelude_collisions,
    single_use_lifetimes,
    trivial_casts,
    trivial_numeric_casts,
    unreachable_pub,
    unsafe_code,
    unsafe_op_in_unsafe_fn,
    unstable_features,
    // unused_crate_dependencies, the false positive case blocks us
    unused_extern_crates,
    unused_import_braces,
    unused_lifetimes,
    unused_qualifications,
    unused_results,
    variant_size_differences,

    warnings, // treat all warnings as errors

    clippy::all,
    clippy::pedantic,
    clippy::cargo,

    // The followings are selected restriction lints for rust 1.57
    clippy::as_conversions,
    clippy::clone_on_ref_ptr,
    clippy::create_dir,
    clippy::dbg_macro,
    clippy::decimal_literal_representation,
    // clippy::default_numeric_fallback, too verbose when dealing with numbers
    clippy::disallowed_script_idents,
    clippy::else_if_without_else,
    clippy::exhaustive_enums,
    clippy::exhaustive_structs,
    clippy::exit,
    clippy::expect_used,
    clippy::filetype_is_file,
    clippy::float_arithmetic,
    clippy::float_cmp_const,
    clippy::get_unwrap,
    clippy::if_then_some_else_none,
    // clippy::implicit_return, it's idiomatic Rust code.
    clippy::indexing_slicing,
    // clippy::inline_asm_x86_att_syntax, stick to intel syntax
    clippy::inline_asm_x86_intel_syntax,
    clippy::integer_arithmetic,
    // clippy::integer_division, required in the project
    clippy::let_underscore_must_use,
    clippy::lossy_float_literal,
    clippy::map_err_ignore,
    clippy::mem_forget,
    clippy::missing_docs_in_private_items,
    clippy::missing_enforced_import_renames,
    clippy::missing_inline_in_public_items,
    // clippy::mod_module_files, mod.rs file is used
    clippy::modulo_arithmetic,
    clippy::multiple_inherent_impl,
    // clippy::panic, allow in application code
    // clippy::panic_in_result_fn, not necessary as panic is banned
    clippy::pattern_type_mismatch,
    clippy::print_stderr,
    clippy::print_stdout,
    clippy::rc_buffer,
    clippy::rc_mutex,
    clippy::rest_pat_in_fully_bound_structs,
    clippy::same_name_method,
    clippy::self_named_module_files,
    // clippy::shadow_reuse, it’s a common pattern in Rust code
    // clippy::shadow_same, it’s a common pattern in Rust code
    clippy::shadow_unrelated,
    clippy::str_to_string,
    clippy::string_add,
    clippy::string_to_string,
    clippy::todo,
    clippy::unimplemented,
    clippy::unnecessary_self_imports,
    clippy::unneeded_field_pattern,
    // clippy::unreachable, allow unreachable panic, which is out of expectation
    clippy::unwrap_in_result,
    clippy::unwrap_used,
    // clippy::use_debug, debug is allow for debug log
    clippy::verbose_file_reads,
    clippy::wildcard_enum_match_arm,

    // The followings are selected lints from 1.61.0 to 1.67.1
    clippy::as_ptr_cast_mut,
    clippy::derive_partial_eq_without_eq,
    clippy::empty_drop,
    clippy::empty_structs_with_brackets,
    clippy::format_push_string,
    clippy::iter_on_empty_collections,
    clippy::iter_on_single_items,
    clippy::large_include_file,
    clippy::manual_clamp,
    clippy::suspicious_xor_used_as_pow,
    clippy::unnecessary_safety_comment,
    clippy::unnecessary_safety_doc,
    clippy::unused_peekable,
    clippy::unused_rounding,

    // The followings are selected restriction lints from rust 1.68.0 to 1.70.0
    // clippy::allow_attributes, still unstable
    clippy::impl_trait_in_params,
    clippy::let_underscore_untyped,
    clippy::missing_assert_message,
    clippy::multiple_unsafe_ops_per_block,
    clippy::semicolon_inside_block,
    // clippy::semicolon_outside_block, already used `semicolon_inside_block`
    clippy::tests_outside_test_module
)]
#![allow(
    clippy::multiple_crate_versions, // caused by the dependency, can't be fixed
    clippy::module_name_repetitions, // It will be more easy to use for the type name prefixed by module name
)]
#![cfg_attr(
    test,
    allow(
        clippy::indexing_slicing,
        unused_results,
        clippy::unwrap_used,
        clippy::as_conversions,
        clippy::shadow_unrelated,
        clippy::integer_arithmetic
    )
)]
use std::{
    collections::HashMap,
    fmt::Debug,
    sync::Arc,
    task::{Context, Poll},
};

use curp::client::Client as CurpClient;
use http::{header::AUTHORIZATION, HeaderValue, Request};
use tonic::{transport::Channel, Streaming};
use tower::Service;
use utils::config::ClientTimeout;

use xlineapi::{
    AuthDisableResponse, AuthEnableResponse, AuthRoleAddResponse, AuthRoleDeleteResponse,
    AuthRoleGetResponse, AuthRoleGrantPermissionResponse, AuthRoleListResponse,
    AuthRoleRevokePermissionResponse, AuthStatusResponse, AuthUserAddResponse,
    AuthUserChangePasswordResponse, AuthUserDeleteResponse, AuthUserGetResponse,
    AuthUserGrantRoleResponse, AuthUserListResponse, AuthUserRevokeRoleResponse,
    DeleteRangeResponse, LeaseGrantResponse, LeaseKeepAliveResponse, LeaseLeasesResponse,
    LeaseRevokeResponse, LeaseTimeToLiveResponse, LockResponse, PutResponse, RangeResponse,
    SnapshotResponse, TxnResponse, UnlockResponse, WatchResponse,
};

use crate::{
    clients::{
        auth::{
            AuthClient, AuthRoleAddRequest, AuthRoleDeleteRequest, AuthRoleGetRequest,
            AuthRoleGrantPermissionRequest, AuthRoleRevokePermissionRequest, AuthUserAddRequest,
            AuthUserChangePasswordRequest, AuthUserDeleteRequest, AuthUserGetRequest,
            AuthUserGrantRoleRequest, AuthUserRevokeRoleRequest,
        },
        cluster::ClusterClient,
        election::ElectionClient,
        kv::{DeleteRangeRequest, KvClient, PutRequest, RangeRequest, Txn},
        lease::{
            LeaseClient, LeaseGrantRequest, LeaseKeepAliveRequest, LeaseKeeper, LeaseRevokeRequest,
            LeaseTimeToLiveRequest,
        },
        lock::{LockClient, LockRequest, UnlockRequest},
        maintenance::MaintenanceClient,
        watch::{WatchClient, WatchRequest, Watcher},
    },
    error::{ClientError, Result},
};

/// Clients
pub mod clients;
/// Error.
pub mod error;
/// Lease Id generator
pub mod lease_gen;

/// Xline client
#[derive(Clone, Debug)]
pub struct Client {
    /// kv client
    kv: KvClient,
    /// lease client
    lease: LeaseClient,
    /// lock client
    lock: LockClient,
    /// auth clinet
    auth: AuthClient,
    /// maintenance client
    maintenance: MaintenanceClient,
    /// watch client
    watch: WatchClient,
    /// cluster client
    cluster: ClusterClient,
    /// election client
    election: ElectionClient,
}

impl Client {
    /// New `Client`
    ///
    /// # Errors
    ///
    /// If `Self::build_channel` fails.
    #[inline]
    pub async fn connect(
        all_members: HashMap<String, String>,
        options: ClientOptions,
    ) -> Result<Self> {
        let name = String::from("client");
        let channel = Self::build_channel(&all_members).await?;
        let curp_client = Arc::new(CurpClient::new(None, all_members, options.curp_timeout).await);
        let id_gen = Arc::new(lease_gen::LeaseIdGenerator::new());

        let token = {
            match options.user {
                Some((username, password)) => {
                    let mut tmp_auth = AuthClient::new(
                        name.clone(),
                        Arc::clone(&curp_client),
                        channel.clone(),
                        None,
                    );
                    let resp = tmp_auth
                        .authenticate(clients::auth::AuthenticateRequest::new(username, password))
                        .await?;

                    Some(resp.token)
                }
                None => None,
            }
        };

        let kv = KvClient::new(name.clone(), Arc::clone(&curp_client), token.clone());
        let lease = LeaseClient::new(
            name.clone(),
            Arc::clone(&curp_client),
            channel.clone(),
            token.clone(),
            Arc::clone(&id_gen),
        );
        let lock = LockClient::new(
            name.clone(),
            Arc::clone(&curp_client),
            channel.clone(),
            token.clone(),
            id_gen,
        );
        let auth = AuthClient::new(name.clone(), curp_client, channel.clone(), token.clone());
        let maintenance = MaintenanceClient::new(channel.clone(), token.clone());
        let watch = WatchClient::new(channel, token);
        let cluster = ClusterClient::new();
        let election = ElectionClient::new();

        Ok(Self {
            kv,
            lease,
            lock,
            auth,
            maintenance,
            watch,
            cluster,
            election,
        })
    }

    /// Build a tonic load balancing channel.
    async fn build_channel(all_members: &HashMap<String, String>) -> Result<Channel> {
        let (channel, tx) = Channel::balance_channel(64);

        for mut addr in all_members.values().cloned() {
            if !addr.starts_with("http://") {
                addr.insert_str(0, "http://");
            }
            let endpoint = Channel::builder(
                addr.parse()
                    .map_err(|_e| ClientError::InvalidArgs(String::from("Invalid uri")))?,
            );

            tx.send(tower::discover::Change::Insert(
                endpoint.uri().clone(),
                endpoint,
            ))
            .await
            .unwrap_or_else(|_| unreachable!("The channel will not closed"));
        }

        Ok(channel)
    }

    /// Gets a KV client.
    #[inline]
    #[must_use]
    pub fn kv_client(&self) -> KvClient {
        self.kv.clone()
    }

    /// Gets a lease client.
    #[inline]
    #[must_use]
    pub fn lease_client(&self) -> LeaseClient {
        self.lease.clone()
    }

    /// Gets a lock client.
    #[inline]
    #[must_use]
    pub fn lock_client(&self) -> LockClient {
        self.lock.clone()
    }

    /// Gets a auth client.
    #[inline]
    #[must_use]
    pub fn auth_client(&self) -> AuthClient {
        self.auth.clone()
    }

    /// Gets a watch client.
    #[inline]
    #[must_use]
    pub fn watch_client(&self) -> WatchClient {
        self.watch.clone()
    }

    /// Gets a maintenance client.
    #[inline]
    #[must_use]
    pub fn maintenance_client(&self) -> MaintenanceClient {
        self.maintenance.clone()
    }

    /// Gets a cluster client.
    #[inline]
    #[must_use]
    pub fn cluster_client(&self) -> ClusterClient {
        self.cluster.clone()
    }

    /// Gets a election client.
    #[inline]
    #[must_use]
    pub fn election_client(&self) -> ElectionClient {
        self.election.clone()
    }

    /// Put the given key into the key-value store.
    /// A put request increments the revision of the key-value store
    /// and generates one event in the event history.
    ///
    /// # Errors
    ///
    /// If kv client failed to send request
    #[inline]
    pub async fn put(&mut self, request: PutRequest) -> Result<PutResponse> {
        self.kv.put(request).await
    }

    /// Gets the key from the key-value store.
    ///
    /// # Errors
    ///
    /// If kv client failed to send request
    #[inline]
    pub async fn range(&mut self, request: RangeRequest) -> Result<RangeResponse> {
        self.kv.range(request).await
    }

    /// Deletes the given key from the key-value store.
    ///
    /// # Errors
    ///
    /// If kv client failed to send request
    #[inline]
    pub async fn delete(&mut self, request: DeleteRangeRequest) -> Result<DeleteRangeResponse> {
        self.kv.delete(request).await
    }

    /// Processes multiple operations in a single transaction.
    /// A txn request increments the revision of the key-value store
    /// and generates events with the same revision for every completed operation.
    /// It is not allowed to modify the same key several times within one txn.
    ///
    /// # Errors
    ///
    /// If kv client failed to send request
    #[inline]
    pub async fn txn(&mut self, request: Txn) -> Result<TxnResponse> {
        self.kv.txn(request).await
    }

    /// Watches for events happening or that have happened. Both input and output
    /// are streams; the input stream is for creating and canceling watcher and the output
    /// stream sends events. The entire event history can be watched starting from the
    /// last compaction revision.
    ///
    /// # Errors
    ///
    /// If watch client failed to send request
    #[inline]
    pub async fn watch(
        &mut self,
        reuqest: WatchRequest,
    ) -> Result<(Watcher, Streaming<WatchResponse>)> {
        self.watch.watch(reuqest).await
    }

    /// Creates a lease which expires if the server does not receive a keepAlive
    /// within a given time to live period. All keys attached to the lease will be expired and
    /// deleted if the lease expires. Each expired key generates a delete event in the event history.
    ///
    /// # Errors
    ///
    /// If lease client failed to send request
    #[inline]
    pub async fn lease_grant(&mut self, request: LeaseGrantRequest) -> Result<LeaseGrantResponse> {
        self.lease.grant(request).await
    }

    /// Revokes a lease. All keys attached to the lease will expire and be deleted.
    ///
    /// # Errors
    ///
    /// If lease client failed to send request
    #[inline]
    pub async fn lease_revoke(
        &mut self,
        request: LeaseRevokeRequest,
    ) -> Result<LeaseRevokeResponse> {
        self.lease.revoke(request).await
    }

    /// Keeps the lease alive by streaming keep alive requests from the client
    /// to the server and streaming keep alive responses from the server to the client.
    ///
    /// # Errors
    ///
    /// If lease client failed to send request
    #[inline]
    pub async fn lease_keep_alive(
        &mut self,
        request: LeaseKeepAliveRequest,
    ) -> Result<(LeaseKeeper, Streaming<LeaseKeepAliveResponse>)> {
        self.lease.keep_alive(request).await
    }

    /// Retrieves lease information.
    ///
    /// # Errors
    ///
    /// If lease client failed to send request
    #[inline]
    pub async fn lease_time_to_live(
        &mut self,
        request: LeaseTimeToLiveRequest,
    ) -> Result<LeaseTimeToLiveResponse> {
        self.lease.time_to_live(request).await
    }

    /// Lists all existing leases.
    ///
    /// # Errors
    ///
    /// If lease client failed to send request
    #[inline]
    pub async fn leases(&mut self) -> Result<LeaseLeasesResponse> {
        self.lease.leases().await
    }

    /// Lock acquires a distributed shared lock on a given named lock.
    /// On success, it will return a unique key that exists so long as the
    /// lock is held by the caller. This key can be used in conjunction with
    /// transactions to safely ensure updates to etcd only occur while holding
    /// lock ownership. The lock is held until Unlock is called on the key or the
    /// lease associate with the owner expires.
    ///
    /// # Errors
    ///
    /// If lock client failed to send request
    #[inline]
    pub async fn lock(&mut self, request: LockRequest) -> Result<LockResponse> {
        self.lock.lock(request).await
    }

    /// Unlock takes a key returned by Lock and releases the hold on lock. The
    /// next Lock caller waiting for the lock will then be woken up and given
    /// ownership of the lock.
    ///
    /// # Errors
    ///
    /// If lock client failed to send request
    #[inline]
    pub async fn unlock(&mut self, request: UnlockRequest) -> Result<UnlockResponse> {
        self.lock.unlock(request).await
    }

    /// Enables authentication.
    ///
    /// # Errors
    ///
    /// If auth client failed to send request
    #[inline]
    pub async fn auth_enable(&mut self) -> Result<AuthEnableResponse> {
        self.auth.auth_enable().await
    }

    /// Disables authentication.
    ///
    /// # Errors
    ///
    /// If auth client failed to send request
    #[inline]
    pub async fn auth_disable(&mut self) -> Result<AuthDisableResponse> {
        self.auth.auth_disable().await
    }

    /// Show authentication status.
    ///
    /// # Errors
    ///
    /// If auth client failed to send request
    #[inline]
    pub async fn auth_status(&mut self) -> Result<AuthStatusResponse> {
        self.auth.auth_status().await
    }

    /// Adds role.
    ///
    /// # Errors
    ///
    /// If auth client failed to send request
    #[inline]
    pub async fn role_add(&mut self, request: AuthRoleAddRequest) -> Result<AuthRoleAddResponse> {
        self.auth.role_add(request).await
    }

    /// Deletes role.
    ///
    /// # Errors
    ///
    /// If auth client failed to send request
    #[inline]
    pub async fn role_delete(
        &mut self,
        request: AuthRoleDeleteRequest,
    ) -> Result<AuthRoleDeleteResponse> {
        self.auth.role_delete(request).await
    }

    /// Gets role.
    ///
    /// # Errors
    ///
    /// If auth client failed to send request
    #[inline]
    pub async fn role_get(&mut self, request: AuthRoleGetRequest) -> Result<AuthRoleGetResponse> {
        self.auth.role_get(request).await
    }

    /// Lists role.
    ///
    /// # Errors
    ///
    /// If auth client failed to send request
    #[inline]
    pub async fn role_list(&mut self) -> Result<AuthRoleListResponse> {
        self.auth.role_list().await
    }

    /// Grants role permission.
    ///
    /// # Errors
    ///
    /// If auth client failed to send request
    #[inline]
    pub async fn role_grant_permission(
        &mut self,
        request: AuthRoleGrantPermissionRequest,
    ) -> Result<AuthRoleGrantPermissionResponse> {
        self.auth.role_grant_permission(request).await
    }

    /// Revokes role permission.
    ///
    /// # Errors
    ///
    /// If auth client failed to send request
    #[inline]
    pub async fn role_revoke_permission(
        &mut self,
        request: AuthRoleRevokePermissionRequest,
    ) -> Result<AuthRoleRevokePermissionResponse> {
        self.auth.role_revoke_permission(request).await
    }

    /// Add an user.
    ///
    /// # Errors
    ///
    /// If auth client failed to send request
    #[inline]
    pub async fn user_add(&mut self, request: AuthUserAddRequest) -> Result<AuthUserAddResponse> {
        self.auth.user_add(request).await
    }

    /// Gets the user info by the user name.
    ///
    /// # Errors
    ///
    /// If auth client failed to send request
    #[inline]
    pub async fn user_get(&mut self, request: AuthUserGetRequest) -> Result<AuthUserGetResponse> {
        self.auth.user_get(request).await
    }

    /// Lists all users.
    ///
    /// # Errors
    ///
    /// If auth client failed to send request
    #[inline]
    pub async fn user_list(&mut self) -> Result<AuthUserListResponse> {
        self.auth.user_list().await
    }

    /// Deletes the given key from the key-value store.
    ///
    /// # Errors
    ///
    /// If auth client failed to send request
    #[inline]
    pub async fn user_delete(
        &mut self,
        request: AuthUserDeleteRequest,
    ) -> Result<AuthUserDeleteResponse> {
        self.auth.user_delete(request).await
    }

    /// Change password for an user.
    ///
    /// # Errors
    ///
    /// If auth client failed to send request
    #[inline]
    pub async fn user_change_password(
        &mut self,
        request: AuthUserChangePasswordRequest,
    ) -> Result<AuthUserChangePasswordResponse> {
        self.auth.user_change_password(request).await
    }

    /// Grant role for an user.
    ///
    /// # Errors
    ///
    /// If auth client failed to send request
    #[inline]
    pub async fn user_grant_role(
        &mut self,
        request: AuthUserGrantRoleRequest,
    ) -> Result<AuthUserGrantRoleResponse> {
        self.auth.user_grant_role(request).await
    }

    /// Revoke role for an user.
    ///
    /// # Errors
    ///
    /// If auth client failed to send request
    #[inline]
    pub async fn user_revoke_role(
        &mut self,
        request: AuthUserRevokeRoleRequest,
    ) -> Result<AuthUserRevokeRoleResponse> {
        self.auth.user_revoke_role(request).await
    }

    /// Gets a snapshot of the entire backend from a member over a stream to a client.
    ///
    /// # Errors
    ///
    /// If maintenance client failed to send request
    #[inline]
    pub async fn snapshot(&mut self) -> Result<Streaming<SnapshotResponse>> {
        self.maintenance.snapshot().await
    }
}

/// Options for a client connection
#[derive(Clone, Debug, Default)]
pub struct ClientOptions {
    /// User is a pair values of name and password
    user: Option<(String, String)>,
    /// Timeout settings for the curp client
    curp_timeout: ClientTimeout,
}

impl ClientOptions {
    /// Create a new `ClientOptions`
    #[inline]
    #[must_use]
    pub fn new(user: Option<(String, String)>, curp_timeout: ClientTimeout) -> Self {
        Self { user, curp_timeout }
    }

    /// Get `user`
    #[inline]
    #[must_use]
    pub fn user(&self) -> Option<(String, String)> {
        self.user.clone()
    }

    /// Get `curp_timeout`
    #[inline]
    #[must_use]
    pub fn curp_timeout(&self) -> ClientTimeout {
        self.curp_timeout
    }
}

/// Authentication service.
#[derive(Debug, Clone)]
pub struct AuthService<S> {
    /// A `Service` trait object
    inner: S,
    /// Auth token
    token: Option<Arc<HeaderValue>>,
}

impl<S> AuthService<S> {
    /// Create a new `AuthService`
    #[inline]
    pub fn new(inner: S, token: Option<Arc<HeaderValue>>) -> Self {
        Self { inner, token }
    }
}

impl<S, Body, Response> Service<Request<Body>> for AuthService<S>
where
    S: Service<Request<Body>, Response = Response>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    #[inline]
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<std::result::Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    #[inline]
    fn call(&mut self, mut request: Request<Body>) -> Self::Future {
        if let Some(token) = self.token.as_ref() {
            let _: Option<HeaderValue> = request
                .headers_mut()
                .insert(AUTHORIZATION, token.as_ref().clone());
        }

        self.inner.call(request)
    }
}
