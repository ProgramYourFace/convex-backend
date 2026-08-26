//! Cell-wide schema rollout: check everywhere, then commit everywhere.
//!
//! ## The problem this exists for
//!
//! A revision reaches N tenants by running `convex deploy` against each one,
//! and the expensive half of a deploy — `analyze` — executes the modules in the
//! V8 isolate pool that is SHARED with serving traffic. At three tenants that
//! is a hiccup. At three hundred it is an hour of degraded service, because the
//! deploys do not run in parallel so much as queue against the pool.
//!
//! Worse, it is not atomic. Tenant 200's schema can fail validation against its
//! own documents after tenants 1..199 have already been migrated, and there is
//! no going back — the fleet is left straddling two revisions with no operation
//! that returns it to one.
//!
//! ## What this adds
//!
//! Two phases, and the split is the point.
//!
//! * `POST /api/cell/schema/precheck` submits the schema to every hosted
//!   instance as PENDING and reports what each one's own `SchemaWorker` made of
//!   it. Nothing is activated. A tenant whose documents violate the new schema
//!   says so here, while the fleet is still uniformly on the old revision.
//! * `POST /api/cell/schema/commit` activates the validated schema on every
//!   instance. Called only once every cell in the cluster has reported
//!   `allValidated`.
//!
//! The coordinator that fans this across cells lives outside this process and
//! owns the cluster-wide decision; a cell only ever answers for its own
//! instances. That keeps the blast radius of this endpoint to one pod and means
//! the two-phase protocol is auditable from the control plane's side.
//!
//! ## Why this is cheap where `convex deploy` is expensive
//!
//! Schema validation walks documents; it does not run modules. It is database
//! work on the per-instance store rather than CPU work in the shared isolate
//! pool, so N instances validating concurrently contend for IO and the block
//! cache — not for the resource that serves queries.
//!
//! ## What this deliberately does NOT do
//!
//! It does not push modules. A schema rollout and a code rollout are different
//! operations with different failure modes, and conflating them is what makes
//! the current path un-rollbackable. Modules still go through `push_config` per
//! instance; this endpoint answers the question that gates whether that should
//! happen at all.
//!
//! ## Access
//!
//! Gated on `MULTITENANT_ADMIN_TOKEN`. **When that is unset the routes are not
//! mounted at all** — absence is the safe default, and a cell that was never
//! configured for fleet operations does not answer them with a 401 that invites
//! guessing. The token authorises a cell-wide operation, so it is a different
//! credential from any instance's admin key and must not be an instance's admin
//! key.

use std::{
    collections::HashMap,
    sync::Arc,
    time::Duration,
};

use application::deploy_config::ModuleJson;
use arc_swap::ArcSwap;
use axum::{
    extract::{
        Request,
        State,
    },
    middleware::{
        self,
        Next,
    },
    routing::{
        get,
        post,
    },
    Json,
    Router,
};
use common::{
    bootstrap_model::schema::{
        SchemaMetadata,
        SchemaState,
    },
    http::HttpResponseError,
    schemas::DatabaseSchema,
};
use database::SchemaModel;
use errors::ErrorMetadata;
use futures::future::join_all;
use json_trait::JsonForm as _;
use keybroker::Identity;
use local_backend::LocalAppState;
use serde::{
    Deserialize,
    Serialize,
};
use sha2::{
    Digest,
    Sha256,
};
use value::TableNamespace;

use crate::naming;

/// How long to wait for an instance's `SchemaWorker` to move a submitted schema
/// off `Pending`.
///
/// Validation walks the tenant's documents, so this scales with that tenant's
/// data rather than with the fleet's size. A timeout is reported as its own
/// outcome rather than as a failure: "we do not know yet" and "this tenant's
/// data violates the schema" are different answers, and a coordinator must not
/// treat the first as the second.
const VALIDATION_TIMEOUT: Duration = Duration::from_secs(300);
const VALIDATION_POLL: Duration = Duration::from_millis(250);

#[derive(Clone)]
pub struct FleetState {
    pub instances: Arc<ArcSwap<HashMap<String, LocalAppState>>>,
    pub group: Arc<str>,
    pub token: Arc<str>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PrecheckRequest {
    /// The bundled `schema.js`, exactly as `prepare_schema` takes it.
    pub bundle: ModuleJson,
    /// Restrict to these instances. Omitted means every hosted instance, which
    /// is the normal case — a subset is for retrying stragglers.
    pub instances: Option<Vec<String>>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CommitRequest {
    pub instances: Option<Vec<String>>,
    /// The `schemaFingerprint` the precheck returned. REQUIRED.
    ///
    /// Without it, commit activates "whatever this instance last validated",
    /// and an ABANDONED precheck poisons the next rollout: a fleet-wide `no`
    /// still leaves `validated` schemas behind on the instances that passed,
    /// and the next commit would activate one of those instead of the schema it
    /// was called for. Learned from watching exactly that state after a refused
    /// rollout. Binding commit to the fingerprint makes the two phases one
    /// operation.
    pub schema_fingerprint: String,
}

/// SHA-256 of the schema's canonical JSON — the same string `SchemaMetadata`
/// stores, so a fingerprint computed here and one recomputed from a stored
/// schema agree by construction.
fn fingerprint(schema: &DatabaseSchema) -> anyhow::Result<String> {
    // `json_serialize` consumes the schema; clone rather than thread ownership
    // through, since this runs once per instance per rollout.
    Ok(naming::hex_encode(&Sha256::digest(
        schema.clone().json_serialize()?.as_bytes(),
    )))
}

/// One instance's answer.
///
/// `state` is deliberately the schema's own vocabulary rather than a boolean:
/// a coordinator that cannot tell `failed` from `timedOut` will eventually
/// commit a rollout it should have abandoned.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct InstanceOutcome {
    pub instance: String,
    pub state: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub table_name: Option<String>,
    /// The fingerprint of the schema this instance currently has ACTIVE.
    ///
    /// Reported by `GET /api/cell/instances` only, and it is what makes drift
    /// checkable as a pure read. `state: "active"` alone says an active schema
    /// exists, not WHICH one — so a cell built from a stale checkout, or one
    /// that finished provisioning during a rollout, is indistinguishable from a
    /// converged cell without it. A coordinator comparing this against the
    /// fingerprint it last committed sees the divergence without submitting
    /// anything.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fingerprint: Option<String>,
}

impl InstanceOutcome {
    fn ok(instance: &str, state: &str) -> Self {
        Self {
            instance: instance.to_owned(),
            state: state.to_owned(),
            error: None,
            table_name: None,
            fingerprint: None,
        }
    }

    fn failed(instance: &str, state: &str, error: String) -> Self {
        Self {
            instance: instance.to_owned(),
            state: state.to_owned(),
            error: Some(error),
            table_name: None,
            fingerprint: None,
        }
    }

    fn with_fingerprint(mut self, fingerprint: Option<String>) -> Self {
        self.fingerprint = fingerprint;
        self
    }

    fn is_active(&self) -> bool {
        self.state == "active"
    }

    fn is_validated(&self) -> bool {
        // `active` counts: an instance that already holds this schema is
        // converged, not a straggler, and a retry after a partial commit must
        // not read it as a failure.
        self.state == "validated" || self.state == "active"
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FleetResponse {
    pub cell: String,
    /// The schema this call acted on. Feed it back to `commit`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema_fingerprint: Option<String>,
    pub results: Vec<InstanceOutcome>,
    /// True only when EVERY instance asked for reported a good state. The
    /// coordinator's gate: commit across the cluster only when every cell says
    /// true.
    pub all_ok: bool,
    /// Instances that were asked for but are not hosted here. Reported rather
    /// than ignored: a coordinator working from a stale roster would otherwise
    /// read "every instance I know of passed" from a run that skipped them.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub unknown: Vec<String>,
}

pub fn router(state: FleetState) -> Router {
    Router::new()
        .route("/api/cell/instances", get(list_instances))
        .route("/api/cell/schema/precheck", post(precheck))
        .route("/api/cell/schema/commit", post(commit))
        // AUTH AS A LAYER, not as the first line of each handler.
        //
        // A handler's body runs only after axum has run its extractors, so an
        // in-handler check means an unauthenticated caller gets its whole body
        // buffered and deserialized into a `ModuleJson` before being told no.
        // These routes are mounted beside `/version`, ahead of the concurrency
        // limiter, so nothing else bounds how many of those are in flight.
        // A layer rejects on headers alone, before the body is read.
        .layer(middleware::from_fn_with_state(
            state.clone(),
            |State(state): State<FleetState>, request: Request, next: Next| async move {
                authorize(&state, request.headers())?;
                Ok::<_, HttpResponseError>(next.run(request).await)
            },
        ))
        .with_state(state)
}

fn authorize(state: &FleetState, headers: &http::HeaderMap) -> Result<(), HttpResponseError> {
    let presented = headers
        .get(http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .unwrap_or("");
    // Constant-time compare: this is a bearer secret, and a length-or-prefix
    // oracle on it is worth avoiding for the cost of one crate call.
    let ok = presented.len() == state.token.len()
        && presented
            .as_bytes()
            .iter()
            .zip(state.token.as_bytes())
            .fold(0u8, |acc, (a, b)| acc | (a ^ b))
            == 0;
    if ok {
        Ok(())
    } else {
        Err(anyhow::anyhow!(ErrorMetadata::unauthenticated(
            "CellAdminTokenInvalid",
            "this endpoint requires the cell admin token",
        ))
        .into())
    }
}

/// Resolve the requested names against what is actually hosted.
fn select(
    state: &FleetState,
    requested: Option<Vec<String>>,
) -> (Vec<(String, LocalAppState)>, Vec<String>) {
    let hosted = state.instances.load();
    match requested {
        None => {
            let mut all: Vec<_> = hosted
                .iter()
                .map(|(name, app)| (name.clone(), app.clone()))
                .collect();
            all.sort_by(|a, b| a.0.cmp(&b.0));
            (all, vec![])
        },
        Some(names) => {
            let mut found = vec![];
            let mut unknown = vec![];
            for name in names {
                match hosted.get(&name) {
                    Some(app) => found.push((name, app.clone())),
                    None => unknown.push(name),
                }
            }
            found.sort_by(|a, b| a.0.cmp(&b.0));
            (found, unknown)
        },
    }
}

async fn list_instances(
    State(state): State<FleetState>,
) -> Result<Json<FleetResponse>, HttpResponseError> {
    let (selected, unknown) = select(&state, None);
    let results = join_all(
        selected
            .into_iter()
            .map(|(name, app)| async move { current_state(&name, &app).await }),
    )
    .await;
    let all_ok = all_ok(&results, &[], InstanceOutcome::is_active);
    Ok(Json(FleetResponse {
        cell: state.group.to_string(),
        schema_fingerprint: None,
        results,
        all_ok,
        unknown,
    }))
}

async fn current_state(name: &str, app: &LocalAppState) -> InstanceOutcome {
    match read_state(app).await {
        Ok((s, fp)) => InstanceOutcome::ok(name, &s).with_fingerprint(fp),
        Err(e) => InstanceOutcome::failed(name, "error", format!("{e:#}")),
    }
}

/// The instance's current schema state, and — whenever one is ACTIVE — that
/// schema's fingerprint.
///
/// The active fingerprint is read even when the reported state is `validated`
/// or `pending`: those states describe a rollout IN FLIGHT, while the active
/// schema is what the instance is serving right now, which is the thing drift
/// is measured against.
async fn read_state(app: &LocalAppState) -> anyhow::Result<(String, Option<String>)> {
    let mut tx = app.application.begin(Identity::system()).await?;
    let mut model = SchemaModel::new(&mut tx, TableNamespace::root_component());
    let active = match model.get_by_state(SchemaState::Active).await? {
        Some((_, schema)) => Some(fingerprint(&schema)?),
        None => None,
    };
    if model.get_by_state(SchemaState::Validated).await?.is_some() {
        return Ok(("validated".to_owned(), active));
    }
    if model.get_by_state(SchemaState::Pending).await?.is_some() {
        return Ok(("pending".to_owned(), active));
    }
    if active.is_some() {
        return Ok(("active".to_owned(), active));
    }
    Ok(("none".to_owned(), None))
}

/// Phase one. Submit the schema everywhere, activate nowhere.
async fn precheck(
    State(state): State<FleetState>,
    Json(req): Json<PrecheckRequest>,
) -> Result<Json<FleetResponse>, HttpResponseError> {
    let (selected, unknown) = select(&state, req.instances);
    if selected.is_empty() {
        return Ok(Json(FleetResponse {
            cell: state.group.to_string(),
            schema_fingerprint: None,
            results: vec![],
            all_ok: false,
            unknown,
        }));
    }
    // Evaluate the schema module ONCE for the whole cell.
    //
    // `evaluate_schema` runs schema.js and nothing else — it reads no tenant
    // data — so the result is a pure function of the bundle and is identical on
    // every instance. Evaluating per instance would burn N V8 evaluations in
    // the shared isolate pool to compute N copies of the same value, which is
    // the exact cost this endpoint exists to avoid.
    let schema = selected[0]
        .1
        .application
        .evaluate_schema(req.bundle.try_into()?)
        .await
        // A bundle that will not evaluate is the CALLER's problem, and this is
        // the endpoint whose entire job is to report problems — so say what
        // went wrong rather than returning an opaque 500 that redacts it.
        //
        // But ONLY re-label an error that has no metadata of its own. This runs
        // in the shared isolate pool under one instance's capacity share, so it
        // can fail for reasons that have nothing to do with the bundle —
        // `PerClientWorkerOverloaded` most of all. Blanket-labelling those
        // `SchemaEvaluationFailed` tells the coordinator the schema is broken,
        // and `bad-bundle` is in its DO-NOT-RETRY set: a transient overload on
        // one tenant would abort the whole cluster's rollout and refuse to
        // retry it.
        .map_err(|e| {
            if e.downcast_ref::<ErrorMetadata>().is_some() {
                return e;
            }
            anyhow::anyhow!(ErrorMetadata::bad_request(
                "SchemaEvaluationFailed",
                format!("could not evaluate the schema bundle: {e:#}"),
            ))
        })?;

    let fp = fingerprint(&schema).map_err(HttpResponseError::from)?;

    let results = join_all(selected.into_iter().map(|(name, app)| {
        let schema = schema.clone();
        async move {
            match precheck_one(&app, schema).await {
                Ok(outcome) => outcome_from_state(&name, outcome),
                Err(e) => InstanceOutcome::failed(&name, "error", format!("{e:#}")),
            }
        }
    }))
    .await;
    let all_ok = all_ok(&results, &unknown, InstanceOutcome::is_validated);
    Ok(Json(FleetResponse {
        cell: state.group.to_string(),
        schema_fingerprint: Some(fp),
        results,
        all_ok,
        unknown,
    }))
}

/// The cell-level verdict a coordinator reads as `allOk`.
///
/// `!results.is_empty()` is the load-bearing clause, not defensive noise:
/// `[].iter().all(..)` is TRUE, so without it a cell that selected no instance
/// reports that everything passed on a run that evaluated nothing — and a
/// coordinator would commit a rollout against a cell it never checked. Every
/// endpoint answers by this one rule so the field means the same thing on all
/// of them.
fn all_ok(
    results: &[InstanceOutcome],
    unknown: &[String],
    ok: fn(&InstanceOutcome) -> bool,
) -> bool {
    !results.is_empty() && unknown.is_empty() && results.iter().all(ok)
}

fn outcome_from_state(name: &str, state: SchemaState) -> InstanceOutcome {
    match state {
        SchemaState::Validated => InstanceOutcome::ok(name, "validated"),
        SchemaState::Active => InstanceOutcome::ok(name, "active"),
        SchemaState::Pending => InstanceOutcome::ok(name, "timedOut"),
        SchemaState::Failed { error, table_name } => InstanceOutcome {
            instance: name.to_owned(),
            state: "failed".to_owned(),
            error: Some(error),
            table_name,
            fingerprint: None,
        },
        SchemaState::Overwritten => InstanceOutcome::ok(name, "overwritten"),
    }
}

async fn precheck_one(app: &LocalAppState, schema: DatabaseSchema) -> anyhow::Result<SchemaState> {
    let mut tx = app.application.begin(Identity::system()).await?;
    let namespace = TableNamespace::root_component();
    let (schema_id, submitted) = SchemaModel::new(&mut tx, namespace)
        .submit_pending(schema)
        .await?;
    // Deliberately no index preparation here. `prepare_schema` builds new and
    // mutated indexes because it is on the deploy path; a precheck must leave
    // the instance exactly as it found it apart from the pending schema row,
    // so that a fleet-wide "no" costs nothing to abandon.
    app.application.commit(tx, "fleet_precheck").await?;

    // Already terminal — the schema is byte-identical to one this instance
    // holds, so there is nothing to wait for.
    if !matches!(submitted, SchemaState::Pending) {
        return Ok(submitted);
    }

    // The instance's own SchemaWorker walks its documents and moves the row off
    // Pending. Poll rather than subscribe: this runs once per rollout, not per
    // request, and a subscription would outlive the request it belongs to.
    let deadline = std::time::Instant::now() + VALIDATION_TIMEOUT;
    loop {
        let mut tx = app.application.begin(Identity::system()).await?;
        let doc = tx.get(schema_id).await?;
        let state = match doc {
            Some(doc) => {
                let meta: SchemaMetadata = doc.into_value().into_value().try_into()?;
                meta.state
            },
            // The row is gone, which means something else overwrote it — a
            // concurrent push. Report it rather than looping forever.
            None => return Ok(SchemaState::Overwritten),
        };
        if !matches!(state, SchemaState::Pending) {
            return Ok(state);
        }
        if std::time::Instant::now() >= deadline {
            return Ok(SchemaState::Pending);
        }
        tokio::time::sleep(VALIDATION_POLL).await;
    }
}

/// Phase two. Activate the schema each instance already validated.
///
/// Takes no schema id: an id is per-instance, so shipping one across the fleet
/// would be meaningless. Each instance activates the schema IT validated, which
/// is by construction the one the precheck submitted.
async fn commit(
    State(state): State<FleetState>,
    Json(req): Json<CommitRequest>,
) -> Result<Json<FleetResponse>, HttpResponseError> {
    let (selected, unknown) = select(&state, req.instances);
    let wanted = req.schema_fingerprint;

    // CHECK EVERY INSTANCE BEFORE ACTIVATING ANY.
    //
    // Without this pass the cell activates instance by instance and stops at
    // the first refusal, which leaves the ones already done on the new schema
    // and the rest on the old — the straddled fleet this whole endpoint exists
    // to avoid. Worse, it is reachable from a plausible operator mistake:
    // committing the fingerprint of a rollout that FAILED elsewhere in the
    // fleet. The fingerprint matches, so a per-instance guard alone waves it
    // through on every instance that happened to validate.
    //
    // A cell cannot know what other cells decided, but it can refuse to be
    // half-migrated itself. Checking first makes the cell an all-or-nothing
    // participant, which is what lets the coordinator treat cells as units.
    let readiness = join_all(selected.iter().map(|(name, app)| {
        let wanted = wanted.clone();
        async move {
            match check_ready(app, &wanted).await {
                Ok(state) => InstanceOutcome::ok(name, &state),
                Err(e) => InstanceOutcome::failed(name, "notReady", format!("{e:#}")),
            }
        }
    }))
    .await;
    if !unknown.is_empty() || readiness.iter().any(|r| r.state == "notReady") {
        return Ok(Json(FleetResponse {
            cell: state.group.to_string(),
            schema_fingerprint: Some(wanted),
            results: readiness,
            all_ok: false,
            unknown,
        }));
    }

    let results = join_all(selected.into_iter().map(|(name, app)| {
        let wanted = wanted.clone();
        async move {
            match commit_one(&app, &wanted).await {
                Ok(state) => InstanceOutcome::ok(&name, &state),
                Err(e) => InstanceOutcome::failed(&name, "error", format!("{e:#}")),
            }
        }
    }))
    .await;
    let all_ok = all_ok(&results, &unknown, InstanceOutcome::is_active);
    Ok(Json(FleetResponse {
        cell: state.group.to_string(),
        schema_fingerprint: Some(wanted),
        results,
        all_ok,
        unknown,
    }))
}

/// Would `commit_one` succeed on this instance? Same checks, no writes.
async fn check_ready(app: &LocalAppState, wanted: &str) -> anyhow::Result<String> {
    let mut tx = app.application.begin(Identity::system()).await?;
    let mut model = SchemaModel::new(&mut tx, TableNamespace::root_component());
    if let Some((_, active)) = model.get_by_state(SchemaState::Active).await?
        && fingerprint(&active)? == wanted
    {
        return Ok("active".to_owned());
    }
    let Some((_, validated)) = model.get_by_state(SchemaState::Validated).await? else {
        anyhow::bail!(ErrorMetadata::bad_request(
            "NoValidatedSchema",
            "this instance has no validated schema to activate; run the precheck first",
        ));
    };
    let found = fingerprint(&validated)?;
    anyhow::ensure!(
        found == wanted,
        ErrorMetadata::bad_request(
            "SchemaFingerprintMismatch",
            format!(
                "this instance has a validated schema ({found}) that is not the one being \
                 committed ({wanted}); re-run the precheck",
            ),
        )
    );
    Ok("ready".to_owned())
}

async fn commit_one(app: &LocalAppState, wanted: &str) -> anyhow::Result<String> {
    let mut tx = app.application.begin(Identity::system()).await?;
    let namespace = TableNamespace::root_component();
    let mut model = SchemaModel::new(&mut tx, namespace);

    // Already converged on the schema being committed: idempotent, and the
    // normal answer on a retry after a partial commit.
    if let Some((_, active)) = model.get_by_state(SchemaState::Active).await?
        && fingerprint(&active)? == wanted
    {
        return Ok("active".to_owned());
    }

    let Some((id, validated)) = model.get_by_state(SchemaState::Validated).await? else {
        anyhow::bail!(ErrorMetadata::bad_request(
            "NoValidatedSchema",
            "this instance has no validated schema to activate; run the precheck first",
        ));
    };
    // THE GUARD. Activate only the schema that was prechecked — never whatever
    // happens to be sitting in Validated, which may be the residue of a rollout
    // that was refused elsewhere in the fleet.
    let found = fingerprint(&validated)?;
    anyhow::ensure!(
        found == wanted,
        ErrorMetadata::bad_request(
            "SchemaFingerprintMismatch",
            format!(
                "this instance has a validated schema ({found}) that is not the one being \
                 committed ({wanted}); re-run the precheck",
            ),
        )
    );
    model.mark_active(id).await?;
    app.application.commit(tx, "fleet_commit").await?;
    Ok("active".to_owned())
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        sync::Arc,
    };

    use arc_swap::ArcSwap;

    use super::*;

    fn state(token: &str) -> FleetState {
        FleetState {
            instances: Arc::new(ArcSwap::from_pointee(HashMap::new())),
            group: "cell-01".into(),
            token: token.into(),
        }
    }

    fn bearer(value: &str) -> http::HeaderMap {
        let mut headers = http::HeaderMap::new();
        headers.insert(
            http::header::AUTHORIZATION,
            http::HeaderValue::from_str(value).unwrap(),
        );
        headers
    }

    const TOKEN: &str = "0123456789abcdef0123456789abcdef";

    #[test]
    fn the_verdict_is_false_when_nothing_was_evaluated() {
        // THE WHOLE POINT of the rule. `[].iter().all(..)` is true, so an empty
        // cell would otherwise report a clean pass on a run that checked
        // nothing, and a coordinator would commit against it.
        assert!(!all_ok(&[], &[], InstanceOutcome::is_validated));
        assert!(!all_ok(&[], &[], InstanceOutcome::is_active));
    }

    #[test]
    fn an_unknown_instance_falsifies_the_verdict() {
        // A coordinator working from a stale roster must not read "everything I
        // asked about passed" out of a run that silently skipped some of them.
        let results = [InstanceOutcome::ok("i-01", "validated")];
        assert!(all_ok(&results, &[], InstanceOutcome::is_validated));
        assert!(!all_ok(
            &results,
            &["i-02".to_owned()],
            InstanceOutcome::is_validated
        ));
    }

    #[test]
    fn active_counts_as_validated_but_not_the_reverse() {
        // A re-run after a partial commit must read an already-active instance
        // as converged, not as a straggler. Commit is the strict direction:
        // `validated` is not yet activated.
        let active = [InstanceOutcome::ok("i-01", "active")];
        let validated = [InstanceOutcome::ok("i-01", "validated")];
        assert!(all_ok(&active, &[], InstanceOutcome::is_validated));
        assert!(all_ok(&active, &[], InstanceOutcome::is_active));
        assert!(all_ok(&validated, &[], InstanceOutcome::is_validated));
        assert!(!all_ok(&validated, &[], InstanceOutcome::is_active));
    }

    #[test]
    fn every_schema_state_maps_to_the_vocabulary_the_coordinator_classifies() {
        // The coordinator's `classifyOutcome` splits these into ok / retryable /
        // hard, and treats anything it does not recognise as HARD. A state that
        // silently renamed here would abort every rollout in the fleet.
        let cases = [
            (SchemaState::Validated, "validated"),
            (SchemaState::Active, "active"),
            (SchemaState::Pending, "timedOut"),
            (SchemaState::Overwritten, "overwritten"),
        ];
        for (schema_state, expected) in cases {
            let outcome = outcome_from_state("i-01", schema_state);
            assert_eq!(outcome.state, expected);
            assert!(outcome.error.is_none());
        }
    }

    #[test]
    fn a_failed_state_carries_the_error_and_the_table() {
        // This is the ONLY channel by which an operator learns which document
        // violated the schema; dropping either field turns a precise refusal
        // into "something went wrong somewhere in the fleet".
        let outcome = outcome_from_state(
            "i-01",
            SchemaState::Failed {
                error: "document does not match".to_owned(),
                table_name: Some("deviceLocations".to_owned()),
            },
        );
        assert_eq!(outcome.state, "failed");
        assert_eq!(outcome.error.as_deref(), Some("document does not match"));
        assert_eq!(outcome.table_name.as_deref(), Some("deviceLocations"));
    }

    #[test]
    fn authorize_accepts_only_the_exact_bearer_token() {
        let st = state(TOKEN);
        assert!(authorize(&st, &bearer(&format!("Bearer {TOKEN}"))).is_ok());
        for bad in [
            "",
            "Bearer ",
            &format!("Bearer {TOKEN}x"),
            &format!("Bearer {}", &TOKEN[..TOKEN.len() - 1]),
            &format!("bearer {TOKEN}"), // the scheme is case-sensitive here
            TOKEN,                      // no scheme at all
        ] {
            assert!(
                authorize(&st, &bearer(bad)).is_err(),
                "accepted {bad:?}, which is not the token"
            );
        }
        assert!(
            authorize(&st, &http::HeaderMap::new()).is_err(),
            "accepted a request with no Authorization header"
        );
    }

    #[test]
    fn selecting_names_a_cell_does_not_host_reports_them_rather_than_skipping() {
        let st = state(TOKEN);
        let (found, unknown) = select(&st, Some(vec!["i-02".to_owned(), "i-01".to_owned()]));
        assert!(found.is_empty());
        // Order is the caller's, which is what makes the message readable.
        assert_eq!(unknown, vec!["i-02".to_owned(), "i-01".to_owned()]);
    }

    #[test]
    fn selecting_nothing_on_an_empty_cell_yields_nothing_rather_than_erroring() {
        // The empty-cell path: warm pool stock with no tenant yet. It must be a
        // clean no-op here, and `all_ok` above is what stops it reading as a
        // pass.
        let st = state(TOKEN);
        let (found, unknown) = select(&st, None);
        assert!(found.is_empty());
        assert!(unknown.is_empty());
    }
}
