//! The two state impls that make `local_backend::router::router` serve N
//! instances.
//!
//! `router` is generic over its state `S` with exactly two bounds, and this
//! module satisfies both:
//!
//! * `LocalAppState: FromMtState<MultitenantState>` — the ~90 legacy `/api/**`
//!   handlers extract `MtState<LocalAppState>`, and `FromMtState` (unlike
//!   axum's `FromRef`) is handed the request's `Parts`. That is exactly the
//!   hook a multi-instance host needs: the app is a function of the REQUEST,
//!   not of the router.
//! * `RouterState: FromRef<MultitenantState>` — the migrated routes (the sync
//!   worker, the public HTTP API, file storage, HTTP actions) go through
//!   `RouterState { api: Arc<dyn ApplicationApi>, .. }`, and `ApplicationApi`
//!   is already a multi-tenant interface: every method takes `host:
//!   &ResolvedHostname`. So the state itself is constant, and
//!   [`crate::api::MultitenantApplicationApi`] does the per-request dispatch.
//!
//! DO NOT WRITE `impl FromRef<MultitenantState> for LocalAppState`. There is
//! exactly one upstream impl of `FromMtState` — a blanket
//! `impl<Outer, T: FromRef<Outer>> FromMtState<Outer> for T` — and the impl
//! below does not overlap it only because nothing makes
//! `LocalAppState: FromRef<MultitenantState>` hold. Adding it would both create
//! a genuine coherence conflict and, if it somehow compiled, silently pin every
//! legacy route to one instance.

use axum::extract::FromRef;
use common::http::{
    extract::FromMtState,
    HttpResponseError,
    ResolvedHostname,
};
use errors::ErrorMetadata;
use http::request::Parts;
use local_backend::{
    LocalAppState,
    RouterState,
};

use crate::{
    naming,
    MultitenantState,
};

impl FromMtState<MultitenantState> for LocalAppState {
    fn from_request_parts(
        parts: &mut Parts,
        state: &MultitenantState,
    ) -> impl Future<Output = Result<Self, HttpResponseError>> + Send {
        std::future::ready(resolve(parts, state))
    }
}

impl FromRef<MultitenantState> for RouterState {
    fn from_ref(state: &MultitenantState) -> Self {
        RouterState {
            api: state.api.clone(),
            runtime: state.runtime.clone(),
            subscription_reconnect_rate_limiter: None,
        }
    }
}

fn resolve(parts: &Parts, state: &MultitenantState) -> Result<LocalAppState, HttpResponseError> {
    let instance = instance_name(parts, &state.instance_header).ok_or_else(|| {
        HttpResponseError::from(anyhow::anyhow!(ErrorMetadata::not_found(
            "InstanceNotFound",
            "This request could not be resolved to an instance on this backend.",
        )))
    })?;
    state.lookup(&instance).ok_or_else(|| {
        // Reachable without any bug: the instance source can retire an instance
        // between the pre-routing middleware and the handler's extraction.
        HttpResponseError::from(anyhow::anyhow!(ErrorMetadata::not_found(
            "InstanceNotFound",
            format!("Instance {instance} is not hosted on this backend."),
        )))
    })
}

/// The resolved instance for this request.
///
/// Normally the pre-routing middleware has already inserted a
/// `ResolvedHostname` extension and this is a map lookup. The header fallback
/// exists for the paths that do not go through that middleware — the site-proxy
/// hop, and any in-process caller that builds a request directly. It
/// deliberately does NOT re-parse the `Host` header: doing so would reintroduce
/// the fail-closed logic that [`crate::host`] owns in a second place, and the
/// two would drift.
fn instance_name(parts: &Parts, header: &str) -> Option<String> {
    if let Some(resolved) = parts.extensions.get::<ResolvedHostname>() {
        return Some(resolved.deployment_name.clone());
    }
    parts
        .headers
        .get(header)
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .filter(|name| naming::is_valid_instance_name(name))
        .map(str::to_owned)
}

#[cfg(test)]
mod tests {
    use common::http::RequestDestination;

    use super::*;
    use crate::host::DEFAULT_INSTANCE_HEADER;

    const INST: &str = "i-0068a1f39c2b4d5e6f708192";
    const OTHER: &str = "i-0068a1f4a1b2c3d4e5f60718";

    fn parts(headers: &[(&str, &str)], resolved: Option<&str>) -> Parts {
        let mut builder = http::Request::builder().uri("/api/instance_name");
        for (k, v) in headers {
            builder = builder.header(*k, *v);
        }
        let mut request = builder.body(()).unwrap();
        if let Some(name) = resolved {
            request.extensions_mut().insert(ResolvedHostname {
                deployment_name: name.to_owned(),
                destination: RequestDestination::ConvexCloud,
            });
        }
        request.into_parts().0
    }

    #[test]
    fn prefers_the_injected_extension() {
        let p = parts(&[(DEFAULT_INSTANCE_HEADER, OTHER)], Some(INST));
        assert_eq!(
            instance_name(&p, DEFAULT_INSTANCE_HEADER).as_deref(),
            Some(INST)
        );
    }

    #[test]
    fn falls_back_to_the_header() {
        let p = parts(&[(DEFAULT_INSTANCE_HEADER, INST)], None);
        assert_eq!(
            instance_name(&p, DEFAULT_INSTANCE_HEADER).as_deref(),
            Some(INST)
        );
    }

    #[test]
    fn never_resolves_from_a_bare_host_header() {
        // Host resolution lives in exactly one place. If this ever returns
        // something, two implementations of the fail-closed rule exist and one
        // of them will drift.
        let p = parts(
            &[("host", "i-0068a1f39c2b4d5e6f708192.cell-01.api.example")],
            None,
        );
        assert_eq!(instance_name(&p, DEFAULT_INSTANCE_HEADER), None);
    }

    #[test]
    fn a_malformed_header_is_ignored() {
        for bad in ["", "Foo", "../../etc", "a b"] {
            let p = parts(&[(DEFAULT_INSTANCE_HEADER, bad)], None);
            assert_eq!(instance_name(&p, DEFAULT_INSTANCE_HEADER), None, "{bad}");
        }
    }
}
