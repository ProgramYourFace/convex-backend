//! Instance resolution: which of the N hosted instances is this request for?
//!
//! This runs as PRE-ROUTING middleware
//! (`ConvexHttpService::serve_with_middleware`) and inserts an
//! `axum::Extension<ResolvedHostname>`. `ExtractResolvedHostname` checks that
//! extension FIRST, before its own hostname regex and before its `CONVEX_SITE`
//! fallback, so host resolution needs no change anywhere else — this simply
//! answers the question before it is asked.
//!
//! ## Resolution order (normative)
//!
//! 1. `X-Convex-Instance: <name>` — the selector for callers with no
//!    per-instance hostname to use.
//! 2. else the wildcard Host `<label>.<group>.(api|site).<base>` — the label is
//!    the instance. This is how public browser traffic arrives.
//! 3. else the BARE group Host `<group>.(api|site).<base>` — resolves to the
//!    instance named after the group, i.e. an adopted single-tenant deployment.
//!    A wildcard DNS record and a wildcard certificate both match exactly one
//!    label and do NOT match the bare parent, so this is a separate case here
//!    for the same reason it is a separate DNS record.
//! 4. else 404 `{"error":"unknown_instance"}`. FAIL CLOSED.
//! 5. if (1) and (2)/(3) both resolve and DISAGREE → 400
//!    `{"error":"instance_conflict"}`.
//! 6. if the resolved name is not currently hosted → 404
//!    `{"error":"unknown_instance","instance":"…"}`.
//!
//! ## Why a header and not only a hostname
//!
//! The callers that cannot use a per-instance hostname are the ones inside the
//! deployment: a sidecar reaching the backend over pod loopback, or a service
//! whose in-cluster address is a DNS name that cannot take an extra leading
//! label (`<instance>.<service>.<ns>.svc` does not resolve). Forcing them onto
//! public hostnames would hairpin internal traffic out through the ingress and
//! back.
//!
//! ## Why the header must not be trusted from outside
//!
//! Rule 5 is the trust boundary, not the ingress. A public request that reached
//! a host rule matched it by definition, so its `Host` always resolves to an
//! instance, so a client-supplied header is always either redundant or a 400 —
//! it can never select a co-tenant. Stripping the header at the ingress is
//! worthwhile defence in depth, but the conflict rule is what makes it safe
//! without one.
//!
//! Addressing a foreign instance would not by itself be an authorization
//! bypass — each instance authenticates independently against its own derived
//! deployment secret — but a selector must not be attacker-controlled anyway.

use std::{
    collections::HashMap,
    sync::Arc,
};

use axum::{
    body::Body,
    response::{
        IntoResponse,
        Response,
    },
};
use common::http::{
    RequestDestination,
    ResolvedHostname,
};
use http::StatusCode;

use crate::naming;

/// The default in-cluster instance selector. Lowercase because
/// `http::HeaderMap` lookups are case-insensitive but this constant is also
/// compared as written by whatever sets it.
pub const DEFAULT_INSTANCE_HEADER: &str = "x-convex-instance";

/// `/http` and everything under it is an HTTP action, i.e. the `.site` surface.
///
/// The host runs ONE resolving listener. The site port is `dev_site_proxy`, a
/// forwarder that rewrites the URI to `<api>/http{uri}` and preserves the
/// headers — so a site request is resolved here, once, after the hop, with both
/// `Host` and the selector header intact. Deriving the destination from the
/// (already rewritten) path is therefore equivalent to deriving it from the
/// listener, without a second middleware stack.
const SITE_PATH_PREFIX: &str = "/http";

/// The set of instance names hosted right now.
///
/// Indirected through a closure so the middleware does not depend on the whole
/// host state, which makes resolution unit-testable without a runtime, a
/// database or V8.
#[derive(Clone)]
pub struct Hosted(Arc<dyn Fn(&str) -> bool + Send + Sync>);

impl Hosted {
    pub fn new(f: impl Fn(&str) -> bool + Send + Sync + 'static) -> Self {
        Self(Arc::new(f))
    }

    /// A fixed set, for tests.
    #[cfg(test)]
    pub fn fixed(names: &[&str]) -> Self {
        let owned: Vec<String> = names.iter().map(|n| (*n).to_owned()).collect();
        Self::new(move |name| owned.iter().any(|n| n == name))
    }

    fn contains(&self, name: &str) -> bool {
        (self.0)(name)
    }
}

/// Builds the [`Hosted`] predicate over the live instance map.
pub fn hosted_from_map<T: Send + Sync + 'static>(
    instances: Arc<arc_swap::ArcSwap<HashMap<String, T>>>,
) -> Hosted {
    Hosted::new(move |name| instances.load().contains_key(name))
}

/// Why a request could not be routed to an instance.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResolveError {
    /// No selector resolved, or the resolved name is not hosted here.
    UnknownInstance { instance: Option<String> },
    /// The header and the `Host` header named different instances.
    Conflict { header: String, host: String },
}

impl ResolveError {
    fn body(&self) -> String {
        // The shapes are fixed and part of the contract clients and smoke tests
        // match on. The VALUES are attacker-controlled — an unresolved instance
        // name is echoed back verbatim — so they go through `serde_json`, which
        // is the crate's existing escaper, rather than a second one here.
        match self {
            ResolveError::UnknownInstance { instance: None } => {
                serde_json::json!({ "error": "unknown_instance" })
            },
            ResolveError::UnknownInstance {
                instance: Some(name),
            } => serde_json::json!({ "error": "unknown_instance", "instance": name }),
            ResolveError::Conflict { header, host } => {
                serde_json::json!({ "error": "instance_conflict", "header": header, "host": host })
            },
        }
        .to_string()
    }

    fn status(&self) -> StatusCode {
        match self {
            ResolveError::UnknownInstance { .. } => StatusCode::NOT_FOUND,
            ResolveError::Conflict { .. } => StatusCode::BAD_REQUEST,
        }
    }
}

impl IntoResponse for ResolveError {
    fn into_response(self) -> Response {
        (
            self.status(),
            [(http::header::CONTENT_TYPE, "application/json")],
            self.body(),
        )
            .into_response()
    }
}

/// Everything the middleware needs to answer "which instance?".
///
/// Cheap to clone — the middleware closure is cloned per connection: the
/// strings are `Arc<str>` and the hosted-set predicate reads the same `ArcSwap`
/// the supervisor publishes to, so resolution always sees the current set
/// without a lock.
#[derive(Clone)]
pub struct HostResolver {
    group: Arc<str>,
    header: Arc<str>,
    /// `.<group>.api.<base>` — precomputed so the hot path is two
    /// `strip_suffix` comparisons and no allocation.
    api_suffix: Arc<str>,
    /// `.<group>.site.<base>`
    site_suffix: Arc<str>,
    /// `<group>.api.<base>` (the bare group host)
    bare_api_host: Arc<str>,
    /// `<group>.site.<base>`
    bare_site_host: Arc<str>,
    hosted: Hosted,
}

impl HostResolver {
    pub fn new(group: &str, base_domain: &str, header: &str, hosted: Hosted) -> Self {
        Self {
            group: group.into(),
            header: header.to_ascii_lowercase().into(),
            api_suffix: format!(".{group}.api.{base_domain}").into(),
            site_suffix: format!(".{group}.site.{base_domain}").into(),
            bare_api_host: format!("{group}.api.{base_domain}").into(),
            bare_site_host: format!("{group}.site.{base_domain}").into(),
            hosted,
        }
    }

    /// Steps 1-6 of the resolution order. Pure: no I/O, no runtime.
    pub fn resolve(
        &self,
        headers: &http::HeaderMap,
        path: &str,
    ) -> Result<ResolvedHostname, ResolveError> {
        let from_header = headers
            .get(&*self.header)
            .and_then(|v| v.to_str().ok())
            .map(str::trim)
            .filter(|v| !v.is_empty())
            .map(str::to_owned);
        let from_host = headers
            .get(http::header::HOST)
            .and_then(|v| v.to_str().ok())
            .and_then(|host| self.instance_from_host(host));

        let name = match (&from_header, &from_host) {
            // Step 5: an explicit selector that contradicts the hostname is a
            // client error, never a silent win for either side.
            (Some(h), Some(u)) if h != u => {
                return Err(ResolveError::Conflict {
                    header: h.clone(),
                    host: u.clone(),
                });
            },
            // Step 1.
            (Some(h), _) => h.clone(),
            // Steps 2 and 3.
            (None, Some(u)) => u.clone(),
            // Step 4: FAIL CLOSED. Never fall through to the `CONVEX_SITE`
            // default, which would land an unrouted request on one arbitrary
            // tenant.
            (None, None) => return Err(ResolveError::UnknownInstance { instance: None }),
        };

        // Validate before the hosted-set lookup, so a hostile name never
        // reaches a map key, a log line, or (via a later admit) a file path.
        if !naming::is_valid_instance_name(&name) {
            return Err(ResolveError::UnknownInstance {
                instance: Some(name),
            });
        }
        // Step 6.
        if !self.hosted.contains(&name) {
            return Err(ResolveError::UnknownInstance {
                instance: Some(name),
            });
        }

        Ok(ResolvedHostname {
            deployment_name: name,
            destination: destination_for_path(path),
        })
    }

    /// Steps 2 and 3: the instance named by a `Host` header, if any.
    ///
    /// Accepts both the `.api.` and the `.site.` families: which one a request
    /// used says nothing about which instance it is for, and the `.site` family
    /// is what arrives after the site-proxy hop.
    fn instance_from_host(&self, host: &str) -> Option<String> {
        // Strip the port. These are DNS names, never IPv6 literals, so a plain
        // split on ':' is safe.
        let host = host.split(':').next().unwrap_or(host);
        // Tolerate the legal, rare fully-qualified trailing dot.
        let host = host.strip_suffix('.').unwrap_or(host);
        if host.eq_ignore_ascii_case(&self.bare_api_host)
            || host.eq_ignore_ascii_case(&self.bare_site_host)
        {
            return Some(self.group.to_string());
        }
        for suffix in [&self.api_suffix, &self.site_suffix] {
            if host.len() > suffix.len() {
                let (label, rest) = host.split_at(host.len() - suffix.len());
                if rest.eq_ignore_ascii_case(suffix) && !label.contains('.') {
                    return Some(label.to_ascii_lowercase());
                }
            }
        }
        None
    }
}

/// `destination` is presentational — nothing routes on it — but getting it
/// right costs one comparison.
fn destination_for_path(path: &str) -> RequestDestination {
    if path == SITE_PATH_PREFIX || path.starts_with("/http/") {
        RequestDestination::ConvexSite
    } else {
        RequestDestination::ConvexCloud
    }
}

/// The pre-routing middleware: the request with an
/// `Extension<ResolvedHostname>` attached, or the rejection response.
///
/// `ConvexHttpService::serve_with_middleware` takes an
/// `FnMut(Request<Body>) -> Future<Output = Result<Request<Body>, Rejection>>`,
/// which is exactly this shape.
pub type ResolveOutcome = std::future::Ready<Result<http::Request<Body>, ResolveError>>;

pub fn inject_resolved_hostname(
    resolver: HostResolver,
    // The `FnMut` bound is written LAST deliberately: `impl FnMut(A) -> B + Clone`
    // does not parse (E0178), because the `+ Clone` would bind to the return
    // type `B` rather than to the `impl` bound list.
) -> impl Clone + Send + Sync + 'static + FnMut(http::Request<Body>) -> ResolveOutcome {
    move |mut req: http::Request<Body>| {
        let outcome = resolver.resolve(req.headers(), req.uri().path());
        std::future::ready(match outcome {
            Ok(resolved) => {
                req.extensions_mut().insert(resolved);
                Ok(req)
            },
            Err(e) => {
                // One line per rejection is acceptable: these are 4xx, and a
                // flood of them is exactly what an operator needs to see. An
                // instance name is not a secret.
                tracing::debug!("rejecting request: {e:?}");
                Err(e)
            },
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const GROUP: &str = "cell-01";
    const BASE: &str = "127.0.0.1.nip.io";
    const INST: &str = "i-0068a1f39c2b4d5e6f708192";
    const OTHER: &str = "i-0068a1f4a1b2c3d4e5f60718";
    /// The path an ordinary API request carries.
    const API_PATH: &str = "/api/1.0.0/sync";
    /// The path the site proxy rewrites an HTTP-action request to.
    const SITE_PATH: &str = "/http/webhook";

    fn resolver() -> HostResolver {
        HostResolver::new(
            GROUP,
            BASE,
            DEFAULT_INSTANCE_HEADER,
            Hosted::fixed(&[GROUP, INST, OTHER]),
        )
    }

    fn headers(pairs: &[(&str, &str)]) -> http::HeaderMap {
        let mut map = http::HeaderMap::new();
        for (k, v) in pairs {
            map.insert(
                http::HeaderName::from_bytes(k.as_bytes()).unwrap(),
                http::HeaderValue::from_str(v).unwrap(),
            );
        }
        map
    }

    #[test]
    fn header_wins() {
        // The loopback case: a sidecar posts to http://localhost with no
        // meaningful Host at all and names the instance in the header.
        let resolved = resolver()
            .resolve(
                &headers(&[("host", "localhost:3211"), (DEFAULT_INSTANCE_HEADER, INST)]),
                SITE_PATH,
            )
            .unwrap();
        assert_eq!(resolved.deployment_name, INST);
    }

    #[test]
    fn a_custom_header_name_is_honoured_case_insensitively() {
        let r = HostResolver::new(GROUP, BASE, "X-AA-Instance", Hosted::fixed(&[INST]));
        let resolved = r
            .resolve(&headers(&[("x-aa-instance", INST)]), API_PATH)
            .unwrap();
        assert_eq!(resolved.deployment_name, INST);
        // ...and the default name is then NOT a selector.
        assert!(r
            .resolve(&headers(&[(DEFAULT_INSTANCE_HEADER, INST)]), API_PATH)
            .is_err());
    }

    #[test]
    fn wildcard_host_resolves_the_label() {
        let r = resolver();
        for host in [
            format!("{INST}.{GROUP}.api.{BASE}"),
            format!("{INST}.{GROUP}.api.{BASE}:443"),
            format!("{INST}.{GROUP}.site.{BASE}"),
            // Host headers are case-insensitive.
            format!("{INST}.{GROUP}.api.{BASE}").to_uppercase(),
            // A fully-qualified name with the trailing root dot.
            format!("{INST}.{GROUP}.api.{BASE}."),
        ] {
            let resolved = r.resolve(&headers(&[("host", &host)]), API_PATH).unwrap();
            assert_eq!(resolved.deployment_name, INST, "host {host}");
        }
    }

    #[test]
    fn the_bare_group_host_resolves_the_adopted_instance() {
        // What keeps a deployment that predates this host working with no
        // client change: a wildcard record matches exactly one label and does
        // not match its bare parent, so the bare host is its own record and
        // must land on the group-named instance.
        let r = resolver();
        for host in [
            format!("{GROUP}.api.{BASE}"),
            format!("{GROUP}.site.{BASE}"),
        ] {
            let resolved = r.resolve(&headers(&[("host", &host)]), API_PATH).unwrap();
            assert_eq!(resolved.deployment_name, GROUP, "host {host}");
        }
    }

    #[test]
    fn destination_follows_the_path_not_the_hostname() {
        // A `.site.` Host on an API path is still ConvexCloud, and an `.api.`
        // Host on the proxied `/http/` path is ConvexSite: the site proxy
        // rewrites the URI and preserves the Host, so after the hop the path is
        // the only honest signal.
        let r = resolver();
        let site_host = headers(&[("host", &format!("{INST}.{GROUP}.site.{BASE}"))]);
        let api_host = headers(&[("host", &format!("{INST}.{GROUP}.api.{BASE}"))]);
        assert_eq!(
            r.resolve(&site_host, API_PATH).unwrap().destination,
            RequestDestination::ConvexCloud
        );
        assert_eq!(
            r.resolve(&api_host, SITE_PATH).unwrap().destination,
            RequestDestination::ConvexSite
        );
        assert_eq!(
            r.resolve(&api_host, "/http").unwrap().destination,
            RequestDestination::ConvexSite
        );
        // `/httpfoo` is not an HTTP action.
        assert_eq!(
            r.resolve(&api_host, "/httpfoo").unwrap().destination,
            RequestDestination::ConvexCloud
        );
    }

    #[test]
    fn no_selector_is_404_even_when_convex_site_is_set() {
        // THE FAIL-CLOSED REGRESSION GUARD. `ExtractResolvedHostname` falls
        // back to $CONVEX_SITE; if this middleware ever fell through instead of
        // rejecting, every unrouted request would land silently on whichever
        // instance that variable names. `MultitenantConfig::from_env` refuses to
        // boot with it set, and this asserts resolution does not consult it
        // either way.
        // SAFETY: single-threaded test process; the variable is removed below.
        unsafe { std::env::set_var("CONVEX_SITE", INST) };
        let err = resolver()
            .resolve(&headers(&[("host", "localhost:3210")]), API_PATH)
            .unwrap_err();
        assert_eq!(err, ResolveError::UnknownInstance { instance: None });
        assert_eq!(err.status(), StatusCode::NOT_FOUND);
        assert_eq!(err.body(), r#"{"error":"unknown_instance"}"#);
        // SAFETY: as above.
        unsafe { std::env::remove_var("CONVEX_SITE") };
    }

    #[test]
    fn no_headers_at_all_is_404() {
        assert_eq!(
            resolver()
                .resolve(&http::HeaderMap::new(), API_PATH)
                .unwrap_err(),
            ResolveError::UnknownInstance { instance: None }
        );
    }

    #[test]
    fn conflicting_selectors_are_400() {
        let err = resolver()
            .resolve(
                &headers(&[
                    ("host", &format!("{INST}.{GROUP}.api.{BASE}")),
                    (DEFAULT_INSTANCE_HEADER, OTHER),
                ]),
                API_PATH,
            )
            .unwrap_err();
        assert_eq!(
            err,
            ResolveError::Conflict {
                header: OTHER.to_owned(),
                host: INST.to_owned(),
            }
        );
        assert_eq!(err.status(), StatusCode::BAD_REQUEST);
        assert_eq!(
            err.body(),
            r#"{"error":"instance_conflict","header":"i-0068a1f4a1b2c3d4e5f60718","host":"i-0068a1f39c2b4d5e6f708192"}"#
        );
    }

    #[test]
    fn agreeing_selectors_are_fine() {
        let resolved = resolver()
            .resolve(
                &headers(&[
                    ("host", &format!("{INST}.{GROUP}.api.{BASE}")),
                    (DEFAULT_INSTANCE_HEADER, INST),
                ]),
                API_PATH,
            )
            .unwrap();
        assert_eq!(resolved.deployment_name, INST);
    }

    #[test]
    fn an_unhosted_name_is_404_and_is_echoed() {
        let err = resolver()
            .resolve(
                &headers(&[(DEFAULT_INSTANCE_HEADER, "i-0068deadbeefdeadbeefdead")]),
                API_PATH,
            )
            .unwrap_err();
        assert_eq!(
            err,
            ResolveError::UnknownInstance {
                instance: Some("i-0068deadbeefdeadbeefdead".to_owned())
            }
        );
        assert_eq!(
            err.body(),
            r#"{"error":"unknown_instance","instance":"i-0068deadbeefdeadbeefdead"}"#
        );
    }

    #[test]
    fn a_malformed_name_never_reaches_the_hosted_set() {
        let r = resolver();
        for bad in ["../../etc/passwd", "Foo", "a\"b", "1abc", &"a".repeat(80)] {
            let err = r
                .resolve(&headers(&[(DEFAULT_INSTANCE_HEADER, bad)]), API_PATH)
                .unwrap_err();
            assert!(
                matches!(err, ResolveError::UnknownInstance { instance: Some(_) }),
                "{bad}"
            );
        }
        // ...and the echo is JSON-escaped.
        let err = r
            .resolve(&headers(&[(DEFAULT_INSTANCE_HEADER, "a\\b\"c")]), API_PATH)
            .unwrap_err();
        assert_eq!(
            err.body(),
            r#"{"error":"unknown_instance","instance":"a\\b\"c"}"#
        );
    }

    #[test]
    fn a_foreign_or_multi_label_host_does_not_resolve() {
        let r = resolver();
        for host in [
            // Another group's wildcard.
            format!("{INST}.cell-02.api.{BASE}"),
            // Two labels where one is allowed — a wildcard certificate and a
            // wildcard DNS record both match exactly one label, and so must we.
            format!("a.{INST}.{GROUP}.api.{BASE}"),
            // Neither api nor site.
            format!("{INST}.{GROUP}.admin.{BASE}"),
            // A different base domain.
            format!("{INST}.{GROUP}.api.evil.example"),
            // Suffix only: no label in front.
            format!(".{GROUP}.api.{BASE}"),
        ] {
            assert_eq!(
                r.resolve(&headers(&[("host", &host)]), API_PATH)
                    .unwrap_err(),
                ResolveError::UnknownInstance { instance: None },
                "host {host}"
            );
        }
    }

    #[test]
    fn an_empty_header_falls_through_to_the_host() {
        // An ingress that clears the header sends it empty, which must behave
        // exactly like an absent header.
        let resolved = resolver()
            .resolve(
                &headers(&[
                    ("host", &format!("{INST}.{GROUP}.api.{BASE}")),
                    (DEFAULT_INSTANCE_HEADER, ""),
                ]),
                API_PATH,
            )
            .unwrap();
        assert_eq!(resolved.deployment_name, INST);
    }

    #[test]
    fn hosted_tracks_the_live_map() {
        let map: Arc<arc_swap::ArcSwap<HashMap<String, u8>>> =
            Arc::new(arc_swap::ArcSwap::from_pointee(HashMap::new()));
        let hosted = hosted_from_map(map.clone());
        assert!(!hosted.contains(INST));
        map.store(Arc::new(HashMap::from([(INST.to_owned(), 1u8)])));
        assert!(hosted.contains(INST));
        map.store(Arc::new(HashMap::new()));
        assert!(!hosted.contains(INST));
    }
}
