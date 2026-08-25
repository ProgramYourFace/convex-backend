//! Where the set of instances to host comes from.
//!
//! The host is a follower, never the placement authority: something else
//! decides which tenants live here, and this module's only job is to observe
//! that decision and publish it. Three shapes cover the deployments that exist:
//! a static list in the environment (one node, or a test), a JSON file on disk
//! (a projected config map, or a hand-edited roster), and an HTTP control
//! plane.
//!
//! ## The failure contract, verbatim
//!
//! EVICTION IS EXPRESSED BY ABSENCE. An instance leaves by not being in the
//! set, which makes a poller's error handling load-bearing: on ANY failure —
//! network, 5xx, 401, malformed JSON, an unreadable file — the poller keeps
//! serving the LAST KNOWN GOOD set from memory and retries with full-jitter
//! backoff. It never exits the process, and it NEVER treats a failed read as
//! "all instances removed". A naive poller that published an empty set on a 500
//! would tear down every tenant on the host.
//!
//! Errors are logged at most once a minute. A host whose control plane is down
//! for an hour should produce sixty log lines, not a hundred thousand.

use std::{
    collections::BTreeMap,
    path::{
        Path,
        PathBuf,
    },
    time::{
        Duration,
        Instant,
    },
};

use common::runtime::Runtime;
use runtime::prod::ProdRuntime;
use serde::Deserialize;
use sync_types::backoff::Backoff;
use tokio::sync::watch;

use crate::{
    config::SourceConfig,
    naming,
};

const INITIAL_BACKOFF: Duration = Duration::from_secs(1);
const MAX_BACKOFF: Duration = Duration::from_secs(30);
/// At most one error line per minute, however fast the poll runs.
const ERROR_LOG_INTERVAL: Duration = Duration::from_secs(60);
/// A roster is a few KiB; a request that takes longer than this is not going to
/// succeed on this tick.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

/// An instance's lifecycle state, as the source sees it.
///
/// There is deliberately no `Retired`: a retired instance is expressed by
/// ABSENCE, which is exactly how a poller learns to stop hosting it.
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum InstanceStatus {
    /// Reserved by placement, not yet live. These ARE admitted — that is how
    /// they become live.
    Provisioning,
    #[default]
    Ready,
    /// Being wound down. Still hosted: draining means "serve what is in
    /// flight", and the entry disappears when it is done.
    Draining,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SourceInstance {
    pub name: String,
    #[serde(default)]
    pub status: InstanceStatus,
}

/// The document both the file and the HTTP source parse.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RosterDocument {
    /// Optional, and checked when present: a roster that names a different
    /// group is a misconfiguration (the wrong file mounted, the wrong URL) and
    /// acting on it would host somebody else's tenants.
    #[serde(default)]
    group: Option<String>,
    #[serde(default)]
    version: Option<String>,
    instances: Vec<SourceInstance>,
}

/// A validated instance set, sorted by name and free of duplicates and bad
/// names.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Roster {
    /// An opaque version, echoed back as `If-None-Match` by the HTTP source.
    pub version: String,
    pub instances: Vec<SourceInstance>,
}

impl Roster {
    pub fn names(&self) -> impl Iterator<Item = &str> {
        self.instances.iter().map(|i| i.name.as_str())
    }
}

/// Drops entries the host must not act on and imposes a canonical order.
///
/// A name from here becomes a directory under the data root, an origin, and a
/// `KeyBroker` identity. The source is a trust boundary even when it is
/// trusted: a bug on the other side must not turn into a path traversal here.
/// Bad entries are DROPPED, not fatal — one malformed row must not stop the
/// other tenants from being hosted.
fn sanitize(instances: Vec<SourceInstance>) -> Vec<SourceInstance> {
    let mut by_name: BTreeMap<String, SourceInstance> = BTreeMap::new();
    for instance in instances {
        if !naming::is_valid_instance_name(&instance.name) {
            tracing::warn!(
                "instance source lists an unusable instance name {:?}; dropping it",
                instance.name
            );
            continue;
        }
        if let Some(previous) = by_name.insert(instance.name.clone(), instance) {
            tracing::warn!(
                "instance source lists {} more than once; keeping the last",
                previous.name
            );
        }
    }
    by_name.into_values().collect()
}

fn parse_roster(group: &str, body: &str) -> anyhow::Result<Roster> {
    let doc: RosterDocument = serde_json::from_str(body)?;
    if let Some(named) = &doc.group {
        anyhow::ensure!(
            named == group,
            "instance source is for group {named:?}, not {group:?}"
        );
    }
    let instances = sanitize(doc.instances);
    Ok(Roster {
        // Fall back to a content-derived version so an unversioned source still
        // produces a stable value; the supervisor only ever compares rosters,
        // never versions, so this is for logging and `If-None-Match`.
        version: doc
            .version
            .unwrap_or_else(|| format!("{}", instances.len())),
        instances,
    })
}

/// Publishes each new instance set on a `watch` channel.
///
/// A `watch` rather than an mpsc is the right shape: the supervisor only ever
/// cares about the LATEST set, and a slow reconcile must not build a queue of
/// stale ones behind it.
pub struct InstanceSource {
    config: SourceConfig,
    group: String,
    interval: Duration,
    runtime: ProdRuntime,
    client: reqwest::Client,
}

impl InstanceSource {
    pub fn new(
        runtime: ProdRuntime,
        group: &str,
        config: SourceConfig,
        interval: Duration,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            config,
            group: group.to_owned(),
            interval,
            runtime,
            client: reqwest::Client::builder()
                .timeout(REQUEST_TIMEOUT)
                .build()?,
        })
    }

    /// The set as it stands right now, without starting the poll loop.
    ///
    /// Read once at boot so the process reconciles before it starts serving,
    /// rather than 404ing every request for one poll interval.
    pub async fn read_once(&self) -> anyhow::Result<Roster> {
        self.read(None).await.map(|outcome| match outcome {
            Fetched::Changed(roster) => roster,
            // A conditional request is never sent on the first read.
            Fetched::Unchanged => Roster::default(),
        })
    }

    /// Runs the poll loop until the process shuts down, publishing to `tx`.
    ///
    /// A static source has nothing to poll, so this returns immediately after
    /// the caller's initial read — the set is fixed for the life of the
    /// process.
    pub async fn run(self, tx: watch::Sender<Roster>) {
        if matches!(self.config, SourceConfig::Static { .. }) {
            tracing::info!("instance source is static; not polling");
            return;
        }
        let mut backoff = Backoff::new(INITIAL_BACKOFF, MAX_BACKOFF);
        let mut last_error_log: Option<Instant> = None;
        let mut version: Option<String> = None;
        loop {
            self.runtime.wait(self.interval).await;
            match self.read(version.as_deref()).await {
                Ok(Fetched::Unchanged) => {
                    backoff.reset();
                },
                Ok(Fetched::Changed(roster)) => {
                    backoff.reset();
                    last_error_log = None;
                    version = Some(roster.version.clone());
                    // `send_if_modified` rather than `send`: a source that
                    // re-serves an identical set every tick must not wake the
                    // supervisor, which would re-plan (and re-log) for nothing.
                    tx.send_if_modified(|current| {
                        if *current == roster {
                            false
                        } else {
                            *current = roster;
                            true
                        }
                    });
                },
                Err(e) => {
                    // THE CONTRACT: keep the last known good set. Never publish
                    // an empty one, which the supervisor would read as "unload
                    // every tenant".
                    let should_log =
                        last_error_log.is_none_or(|logged| logged.elapsed() >= ERROR_LOG_INTERVAL);
                    if should_log {
                        last_error_log = Some(Instant::now());
                        tracing::error!(
                            "could not read the instance source; continuing with the last known \
                             set of {} instance(s): {e:#}",
                            tx.borrow().instances.len(),
                        );
                    }
                    // Bound in its own statement: `Runtime::rng` returns a
                    // `Box<dyn RngCore>`, which is not `Send`, and as a
                    // temporary in the `wait` argument it would live across the
                    // await and make this whole future unspawnable.
                    let delay = backoff.fail(&mut self.runtime.rng());
                    self.runtime.wait(delay).await;
                },
            }
        }
    }

    async fn read(&self, version: Option<&str>) -> anyhow::Result<Fetched> {
        match &self.config {
            SourceConfig::Static { names } => Ok(Fetched::Changed(Roster {
                version: "static".to_owned(),
                instances: sanitize(
                    names
                        .iter()
                        .map(|name| SourceInstance {
                            name: name.clone(),
                            status: InstanceStatus::Ready,
                        })
                        .collect(),
                ),
            })),
            SourceConfig::File { path } => self.read_file(path).map(Fetched::Changed),
            SourceConfig::Http { url, bearer } => {
                self.read_http(url, bearer.as_deref(), version).await
            },
        }
    }

    fn read_file(&self, path: &Path) -> anyhow::Result<Roster> {
        let body = std::fs::read_to_string(path)
            .map_err(|e| anyhow::anyhow!("could not read {}: {e}", path.display()))?;
        parse_roster(&self.group, &body)
            .map_err(|e| anyhow::anyhow!("{} is not a valid instance roster: {e}", path.display()))
    }

    async fn read_http(
        &self,
        url: &str,
        bearer: Option<&str>,
        version: Option<&str>,
    ) -> anyhow::Result<Fetched> {
        let mut request = self.client.get(url);
        if let Some(token) = bearer {
            request = request.bearer_auth(token);
        }
        if let Some(version) = version {
            request = request.header(http::header::IF_NONE_MATCH, version);
        }
        let response = request.send().await?;
        if response.status() == http::StatusCode::NOT_MODIFIED {
            return Ok(Fetched::Unchanged);
        }
        let status = response.status();
        let body = response.text().await?;
        anyhow::ensure!(
            status.is_success(),
            // The body can contain anything the control plane chose to say, so
            // it is truncated rather than logged whole.
            "instance source returned {status}: {}",
            body.chars().take(256).collect::<String>()
        );
        parse_roster(&self.group, &body).map(Fetched::Changed)
    }
}

enum Fetched {
    Changed(Roster),
    Unchanged,
}

/// The file shape, for documentation and for tests.
pub fn example_roster_file() -> &'static str {
    r#"{
  "group": "cell-01",
  "version": "3",
  "instances": [
    { "name": "cell-01", "status": "ready" },
    { "name": "i-0068a1f39c2b4d5e6f708192", "status": "ready" }
  ]
}"#
}

/// The path a `File` source reads, for error messages.
pub fn source_description(config: &SourceConfig) -> String {
    match config {
        SourceConfig::File { path } => format!("file {}", PathBuf::from(path).display()),
        SourceConfig::Http { url, .. } => format!("control plane {url}"),
        SourceConfig::Static { names } => format!("a static list of {} instance(s)", names.len()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_the_documented_file_shape() {
        let roster = parse_roster("cell-01", example_roster_file()).unwrap();
        assert_eq!(roster.version, "3");
        assert_eq!(
            roster.names().collect::<Vec<_>>(),
            vec!["cell-01", "i-0068a1f39c2b4d5e6f708192"]
        );
    }

    #[test]
    fn status_defaults_to_ready_and_all_states_are_hosted() {
        // Every listed state is hosted: provisioning becomes live by being
        // admitted, and draining means "serve what is in flight". Only absence
        // unloads.
        let roster = parse_roster(
            "cell-01",
            r#"{"instances":[
                {"name":"a-one","status":"provisioning"},
                {"name":"a-two","status":"draining"},
                {"name":"a-three"}
            ]}"#,
        )
        .unwrap();
        assert_eq!(roster.instances.len(), 3);
        assert_eq!(
            roster.instances[0].status,
            InstanceStatus::Provisioning,
            "sorted by name, so a-one is first"
        );
        assert_eq!(roster.instances[2].status, InstanceStatus::Ready);
    }

    #[test]
    fn a_roster_for_another_group_is_rejected_whole() {
        // Not sanitised away one row at a time: the whole document is for
        // somebody else, and acting on any of it would host their tenants.
        let err = parse_roster("cell-01", r#"{"group":"cell-02","instances":[]}"#).unwrap_err();
        assert!(err.to_string().contains("cell-02"), "{err}");
    }

    #[test]
    fn bad_names_are_dropped_not_fatal() {
        let roster = parse_roster(
            "cell-01",
            r#"{"instances":[
                {"name":"../../etc/passwd"},
                {"name":"Uppercase"},
                {"name":"a-good-one"}
            ]}"#,
        )
        .unwrap();
        // One malformed row must not stop the other tenants from being hosted.
        assert_eq!(roster.names().collect::<Vec<_>>(), vec!["a-good-one"]);
    }

    #[test]
    fn duplicates_collapse_and_order_is_canonical() {
        let roster = parse_roster(
            "cell-01",
            r#"{"instances":[
                {"name":"b-two"},
                {"name":"a-one"},
                {"name":"a-one","status":"draining"}
            ]}"#,
        )
        .unwrap();
        assert_eq!(roster.names().collect::<Vec<_>>(), vec!["a-one", "b-two"]);
        assert_eq!(roster.instances[0].status, InstanceStatus::Draining);
    }

    #[test]
    fn malformed_json_is_an_error_not_an_empty_roster() {
        // The distinction the whole failure contract rests on: an error keeps
        // the last known good set, an empty roster unloads every tenant.
        assert!(parse_roster("cell-01", "not json").is_err());
        assert!(parse_roster("cell-01", "{}").is_err());
        // ...whereas a genuinely empty list IS an empty roster.
        assert!(parse_roster("cell-01", r#"{"instances":[]}"#)
            .unwrap()
            .instances
            .is_empty());
    }
}
