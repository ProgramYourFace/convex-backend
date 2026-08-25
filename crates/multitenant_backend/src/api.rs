//! `ApplicationApi` over N instances.
//!
//! This is the cheapest seam in the whole design, because `ApplicationApi` was
//! already written for a multi-tenant implementor: every one of its methods
//! takes `host: &ResolvedHostname` as its first argument, and the single-tenant
//! `impl ApplicationApi for Application<RT>` names that parameter `_host` and
//! ignores it. All the migrated routes go through
//! `RouterState { api: Arc<dyn ApplicationApi>, .. }`, so replacing that one
//! `Arc` replaces the routing for the sync worker, the public HTTP API, file
//! storage and HTTP actions at once.
//!
//! Every method is the same three lines — look the instance up by
//! `host.deployment_name`, delegate, pass `host` through unchanged so the
//! callee sees exactly what it would have seen — so they are written once, by
//! `multitenant_api!`, from a signature list. That list is the ONLY thing that
//! has to change when the trait gains a method, and forgetting is a compile
//! error rather than a route that silently stops working.
//!
//! ## Why every delegation is written `ApplicationApi::method(&app, ..)`
//!
//! `Application<RT>` has INHERENT methods sharing a name with several of the
//! trait's (`authenticate`, `store_file`, `get_file`, `get_file_range`) that
//! take entirely different arguments. Rust method resolution always prefers an
//! inherent method over a trait one, so `app.authenticate(host, ..)` resolves
//! to the inherent one and fails to compile. Fully qualifying EVERY call — not
//! only the ones that collide today — is what stops a future inherent method
//! from re-introducing the problem in a method that currently looks fine.
//!
//! ## The lookup failure is a 404
//!
//! Not a panic, and not a fallback to some default instance. It is genuinely
//! reachable: an instance can be retired between the pre-routing resolution and
//! the moment a long-running request reaches this table. The sync worker's
//! reconnect logic depends on "no such deployment" being distinguishable from
//! "the deployment broke", so it must not be a 500 either.

use std::{
    collections::HashMap,
    ops::Bound,
    sync::Arc,
    time::Duration,
};

use application::{
    api::{
        ApplicationApi,
        ExecuteQueryTimestamp,
        SubscriptionClient,
    },
    Application,
    FunctionError,
    FunctionReturn,
    RedactedActionError,
    RedactedActionReturn,
    RedactedMutationError,
    RedactedMutationReturn,
    RedactedQueryReturn,
};
use arc_swap::ArcSwap;
use async_trait::async_trait;
use bytes::Bytes;
use common::{
    components::{
        CanonicalizedComponentFunctionPath,
        ComponentId,
        ExportPath,
    },
    http::ResolvedHostname,
    types::{
        ConvexOrigin,
        FunctionCaller,
        QueryInvocation,
        RepeatableTimestamp,
    },
    RequestContext,
    RequestId,
};
use errors::ErrorMetadata;
use file_storage::FileStream;
use futures::stream::BoxStream;
use headers::{
    ContentLength,
    ContentType,
};
use keybroker::Identity;
use local_backend::LocalAppState;
use model::{
    file_storage::FileStorageId,
    session_requests::types::SessionRequestIdentifier,
};
use runtime::prod::ProdRuntime;
use sync_types::{
    types::SerializedArgs,
    AuthenticationToken,
    SerializedQueryJournal,
};
use udf::{
    HttpActionRequest,
    HttpActionResponseStreamer,
};
use value::{
    sha256::Sha256Digest,
    DeveloperDocumentId,
};

/// Dispatches every `ApplicationApi` call to the instance named by the
/// request's resolved hostname.
///
/// Holds the same `ArcSwap` the router state does, so an instance-set change is
/// visible here on the next call with no synchronisation between the two.
pub struct MultitenantApplicationApi {
    instances: Arc<ArcSwap<HashMap<String, LocalAppState>>>,
}

impl MultitenantApplicationApi {
    pub fn new(instances: Arc<ArcSwap<HashMap<String, LocalAppState>>>) -> Self {
        Self { instances }
    }

    fn lookup(&self, host: &ResolvedHostname) -> anyhow::Result<Application<ProdRuntime>> {
        self.instances
            .load()
            .get(&host.deployment_name)
            .map(|app| app.application.clone())
            .ok_or_else(|| {
                anyhow::anyhow!(ErrorMetadata::not_found(
                    "InstanceNotFound",
                    format!(
                        "Instance {} is not hosted on this backend.",
                        host.deployment_name
                    ),
                ))
            })
    }
}

/// Writes the whole `impl`, with one `look up, then delegate` method per
/// signature in `delegated`.
///
/// Each entry is the trait's own signature with the leading
/// `&self, host: &ResolvedHostname` elided, so adding a trait method is one
/// line here and omitting it is a "not all trait items implemented" error.
///
/// The macro emits the `#[async_trait]` attribute itself rather than sitting
/// inside an already-attributed `impl`. An attribute macro runs BEFORE the
/// `macro_rules!` invocations in its input are expanded, so an `#[async_trait]`
/// written outside would see this call as one opaque item, leave the generated
/// `async fn`s un-rewritten, and produce a "lifetime parameters do not match
/// the trait declaration" error on every single method.
macro_rules! multitenant_api {
    (
        delegated {$(
            fn $name:ident($($arg:ident : $ty:ty),* $(,)?) -> $ret:ty;
        )*}
        $($rest:item)*
    ) => {
        #[async_trait]
        impl ApplicationApi for MultitenantApplicationApi {
            $(
                async fn $name(
                    &self,
                    host: &ResolvedHostname,
                    $($arg: $ty),*
                ) -> $ret {
                    let app = self.lookup(host)?;
                    ApplicationApi::$name(&app, host $(, $arg)*).await
                }
            )*
            $($rest)*
        }
    };
}

multitenant_api! {
    delegated {
        fn authenticate(
            request_context: RequestContext,
            auth_token: AuthenticationToken,
        ) -> anyhow::Result<Identity>;

        fn execute_public_query(
            request_context: RequestContext,
            identity: Identity,
            path: ExportPath,
            args: SerializedArgs,
            caller: FunctionCaller,
            ts: ExecuteQueryTimestamp,
            journal: Option<SerializedQueryJournal>,
            invocation: Option<QueryInvocation>,
        ) -> anyhow::Result<RedactedQueryReturn>;

        fn execute_admin_query(
            request_context: RequestContext,
            identity: Identity,
            path: CanonicalizedComponentFunctionPath,
            args: SerializedArgs,
            caller: FunctionCaller,
            ts: ExecuteQueryTimestamp,
            journal: Option<SerializedQueryJournal>,
            invocation: Option<QueryInvocation>,
        ) -> anyhow::Result<RedactedQueryReturn>;

        fn execute_public_mutation(
            request_context: RequestContext,
            identity: Identity,
            path: ExportPath,
            args: SerializedArgs,
            caller: FunctionCaller,
            mutation_identifier: Option<SessionRequestIdentifier>,
            mutation_queue_length: Option<usize>,
        ) -> anyhow::Result<Result<RedactedMutationReturn, RedactedMutationError>>;

        fn execute_admin_mutation(
            request_context: RequestContext,
            identity: Identity,
            path: CanonicalizedComponentFunctionPath,
            args: SerializedArgs,
            caller: FunctionCaller,
            mutation_identifier: Option<SessionRequestIdentifier>,
            mutation_queue_length: Option<usize>,
        ) -> anyhow::Result<Result<RedactedMutationReturn, RedactedMutationError>>;

        fn execute_public_action(
            request_context: RequestContext,
            identity: Identity,
            path: ExportPath,
            args: SerializedArgs,
            caller: FunctionCaller,
        ) -> anyhow::Result<Result<RedactedActionReturn, RedactedActionError>>;

        fn execute_admin_action(
            request_context: RequestContext,
            identity: Identity,
            path: CanonicalizedComponentFunctionPath,
            args: SerializedArgs,
            caller: FunctionCaller,
        ) -> anyhow::Result<Result<RedactedActionReturn, RedactedActionError>>;

        fn execute_http_action(
            request_context: RequestContext,
            http_request_metadata: HttpActionRequest,
            identity: Identity,
            caller: FunctionCaller,
            response_streamer: HttpActionResponseStreamer,
        ) -> anyhow::Result<()>;

        fn execute_any_function(
            request_context: RequestContext,
            identity: Identity,
            path: CanonicalizedComponentFunctionPath,
            args: SerializedArgs,
            caller: FunctionCaller,
        ) -> anyhow::Result<Result<FunctionReturn, FunctionError>>;

        fn latest_timestamp(request_id: RequestId) -> anyhow::Result<RepeatableTimestamp>;

        fn check_store_file_authorization(
            request_id: RequestId,
            token: &str,
            validity: Duration,
        ) -> anyhow::Result<ComponentId>;

        fn store_file(
            request_id: RequestId,
            origin: ConvexOrigin,
            component: ComponentId,
            content_length: Option<ContentLength>,
            content_type: Option<ContentType>,
            expected_sha256: Option<Sha256Digest>,
            body: BoxStream<'_, anyhow::Result<Bytes>>,
        ) -> anyhow::Result<DeveloperDocumentId>;

        fn get_file_range(
            request_id: RequestId,
            origin: ConvexOrigin,
            component: ComponentId,
            file_storage_id: FileStorageId,
            range: (Bound<u64>, Bound<u64>),
        ) -> anyhow::Result<FileStream>;

        fn get_file(
            request_id: RequestId,
            origin: ConvexOrigin,
            component: ComponentId,
            file_storage_id: FileStorageId,
        ) -> anyhow::Result<FileStream>;

        fn subscription_client() -> anyhow::Result<Box<dyn SubscriptionClient>>;
    }

    /// The single-tenant implementation returns a constant `0`, because one
    /// backend is one partition. Here it is a stable hash of the instance name,
    /// so the per-partition metrics that consume it separate the tenants
    /// sharing a process instead of collapsing them into one bucket.
    async fn partition_id(&self, host: &ResolvedHostname) -> anyhow::Result<u64> {
        // Existence check first, so an unrouted request reports the same error
        // here as everywhere else rather than a plausible-looking number.
        self.lookup(host)?;
        Ok(partition_id_for(&host.deployment_name))
    }
}

/// FNV-1a 64. Deliberately not `DefaultHasher`: this value appears in metrics
/// and must be stable across restarts and across processes, which a
/// `RandomState`-seeded hasher explicitly is not.
fn partition_id_for(deployment_name: &str) -> u64 {
    const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;
    let mut hash = OFFSET;
    for byte in deployment_name.as_bytes() {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(PRIME);
    }
    hash
}

#[cfg(test)]
mod tests {
    use common::http::RequestDestination;
    use errors::ErrorMetadataAnyhowExt;

    use super::*;

    #[test]
    fn partition_ids_are_stable_and_distinct() {
        // Pinned: a change here silently re-buckets every metric.
        assert_eq!(partition_id_for(""), 0xcbf2_9ce4_8422_2325);
        assert_eq!(partition_id_for("cell-01"), partition_id_for("cell-01"));
        assert_ne!(
            partition_id_for("i-0068a1f39c2b4d5e6f708192"),
            partition_id_for("i-0068a1f4a1b2c3d4e5f60718")
        );
    }

    #[test]
    fn lookup_on_an_unhosted_instance_is_not_found() {
        let api = MultitenantApplicationApi::new(Arc::new(ArcSwap::from_pointee(HashMap::new())));
        let host = ResolvedHostname {
            deployment_name: "i-0068a1f39c2b4d5e6f708192".to_owned(),
            destination: RequestDestination::ConvexCloud,
        };
        // Matched rather than `unwrap_err`, which would need `Debug` on the Ok
        // type — and `Application` does not implement it.
        let Err(err) = api.lookup(&host) else {
            panic!("an empty instance map must not resolve anything");
        };
        // Must be a 404, not a 500: an unrouted request is a client-visible
        // "no such deployment", and the sync worker's reconnect logic depends
        // on the distinction.
        assert_eq!(err.short_msg(), "InstanceNotFound");
    }
}
