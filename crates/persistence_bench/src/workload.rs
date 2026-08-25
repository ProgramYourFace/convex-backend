//! A workload modelled on aa-app's device-location ingest path.
//!
//! Read from `convex/deviceTraits/locations.ts` and `convex/schema.ts`. Per
//! location event the real path does:
//!
//! 1. Two **run-length-encoding neighbour reads** on
//!    `deviceLocations.by_device_timestamp` — the preceding fix
//!    (`lte(timestamp).order(desc).first()`) and the following one
//!    (`gte(timestamp).order(asc).first()`). These decide whether the new fix
//!    extends an existing cluster or starts a new one, and they also serve as
//!    the exact-duplicate guard.
//! 2. Either an **insert** into `deviceLocations` (the vehicle moved) or a
//!    **patch** of the neighbouring row (an RLE merge — parked, so the
//!    cluster's `stillDuration` and `mergedCount` grow instead).
//! 3. A **`deviceLatestLocations` upsert**: read the device's single row
//!    through `by_device`, then replace it — the "where is every device right
//!    now" spine.
//!
//! That shape matters more than the row count. It is read-modify-write against
//! a hot per-device row, not an append-only stream, so it exercises exactly the
//! version-shadowing case that a pure-insert benchmark misses: one key in
//! `by_device` accumulating a new version on every single event.
//!
//! Physical rows per event, counting Convex's implicit `by_id` and
//! `by_creation_time`:
//!
//! | table                   | indexes | rows per write |
//! |-------------------------|---------|----------------|
//! | `deviceLocations`       | 3       | 1 doc + 3      |
//! | `deviceLatestLocations` | 4       | 1 doc + 4      |
//!
//! so nine physical rows per event, against three index reads.

use anyhow::Context as _;
use common::{
    bootstrap_model::index::{
        database_index::IndexedFields,
        IndexMetadata,
    },
    query::{
        IndexRange,
        IndexRangeExpression,
        Order,
        Query,
    },
    runtime::Runtime,
    types::{
        GenericIndexName,
        IndexDescriptor,
        IndexName,
        MaybeValue,
    },
};
use database::{
    IndexModel,
    ResolvedQuery,
    Transaction,
    UserFacingModel,
};
use value::{
    ConvexObject,
    ConvexValue,
    FieldPath,
    PendingValue,
    ResolvedDocumentId,
    TableName,
    TableNamespace,
};

pub const LOCATIONS_TABLE: &str = "deviceLocations";
pub const LATEST_TABLE: &str = "deviceLatestLocations";

/// Default fleet size. Events per device — `events / devices` — is the knob
/// that decides how hot each key is: it sets how many versions accumulate on a
/// device's `by_device` spine row, and whether a fix has a previous fix to
/// RLE-merge into at all. A fleet larger than the event count never merges.
pub const DEFAULT_DEVICES: u64 = 512;

fn field(path: &str) -> anyhow::Result<FieldPath> {
    path.parse().context("bad field path")
}

/// Create the two tables and their user indexes, already enabled.
///
/// `add_system_index` is used rather than `add_application_index` because the
/// latter forces a new index through the backfilling state and an async worker;
/// here the tables are empty, so an index can start out enabled.
pub async fn create_schema<RT: Runtime>(tx: &mut Transaction<RT>) -> anyhow::Result<()> {
    let locations: TableName = LOCATIONS_TABLE.parse()?;
    let latest: TableName = LATEST_TABLE.parse()?;

    // deviceLocations.by_device_timestamp — the RLE neighbour lookups.
    IndexModel::new(tx)
        .add_system_index(
            TableNamespace::Global,
            IndexMetadata::new_enabled(
                GenericIndexName::new(
                    locations.clone(),
                    IndexDescriptor::new("by_device_timestamp")?,
                )?,
                IndexedFields::try_from(vec![field("deviceId")?, field("timestamp")?])?,
                None,
            ),
        )
        .await?;

    // deviceLatestLocations.by_device — the one-row-per-device spine lookup,
    // and by_geohash, which has no reader today but is still written on every
    // upsert.
    IndexModel::new(tx)
        .add_system_index(
            TableNamespace::Global,
            IndexMetadata::new_enabled(
                GenericIndexName::new(latest.clone(), IndexDescriptor::new("by_device")?)?,
                IndexedFields::try_from(vec![field("deviceId")?])?,
                None,
            ),
        )
        .await?;
    IndexModel::new(tx)
        .add_system_index(
            TableNamespace::Global,
            IndexMetadata::new_enabled(
                GenericIndexName::new(latest, IndexDescriptor::new("by_geohash")?)?,
                IndexedFields::try_from(vec![field("geohash")?])?,
                None,
            ),
        )
        .await?;
    Ok(())
}

/// One GPS fix, shaped like `schemas/device.location.ts`.
pub fn location_document(
    device: u64,
    timestamp: f64,
    merged_count: i64,
) -> anyhow::Result<ConvexObject> {
    let mut map = std::collections::BTreeMap::new();
    let mut put = |k: &str, v: ConvexValue| -> anyhow::Result<()> {
        map.insert(k.parse()?, v);
        Ok(())
    };
    put(
        "deviceId",
        ConvexValue::try_from(format!("device-{device:06}"))?,
    )?;
    put("timestamp", ConvexValue::Float64(timestamp))?;
    put(
        "latitude",
        ConvexValue::Float64(37.0 + (device as f64 % 1000.0) / 10_000.0),
    )?;
    put(
        "longitude",
        ConvexValue::Float64(-122.0 + (timestamp % 1000.0) / 10_000.0),
    )?;
    put(
        "geohash",
        ConvexValue::try_from(format!("9q8yy{:03}", device % 1000))?,
    )?;
    put("altitude", ConvexValue::Float64(30.0))?;
    put(
        "speed",
        ConvexValue::Float64((timestamp as u64 % 90) as f64),
    )?;
    put(
        "heading",
        ConvexValue::Float64((timestamp as u64 % 360) as f64),
    )?;
    put("speedLimit", ConvexValue::Float64(80.0))?;
    put("pointAccuracy", ConvexValue::Float64(4.5))?;
    put("engineOn", ConvexValue::Boolean(true))?;
    put("fuelRate", ConvexValue::Float64(7.25))?;
    put("fuelCounter", ConvexValue::Float64(timestamp % 100_000.0))?;
    put("fuelCounterEpoch", ConvexValue::Float64(1.0))?;
    put("stillDuration", ConvexValue::Float64(0.0))?;
    put("mergedCount", ConvexValue::Int64(merged_count))?;
    Ok(ConvexObject::try_from(map)?)
}

/// The `deviceLatestLocations` spine row for a device.
pub fn latest_document(device: u64, timestamp: f64) -> anyhow::Result<ConvexObject> {
    let mut map = std::collections::BTreeMap::new();
    let mut put = |k: &str, v: ConvexValue| -> anyhow::Result<()> {
        map.insert(k.parse()?, v);
        Ok(())
    };
    put(
        "deviceId",
        ConvexValue::try_from(format!("device-{device:06}"))?,
    )?;
    put("timestamp", ConvexValue::Float64(timestamp))?;
    put(
        "latitude",
        ConvexValue::Float64(37.0 + (device as f64 % 1000.0) / 10_000.0),
    )?;
    put(
        "longitude",
        ConvexValue::Float64(-122.0 + (timestamp % 1000.0) / 10_000.0),
    )?;
    put(
        "geohash",
        ConvexValue::try_from(format!("9q8yy{:03}", device % 1000))?,
    )?;
    put(
        "speed",
        ConvexValue::Float64((timestamp as u64 % 90) as f64),
    )?;
    put("engineOn", ConvexValue::Boolean(true))?;
    put("effectiveTime", ConvexValue::Float64(timestamp))?;
    Ok(ConvexObject::try_from(map)?)
}

/// One bound of the RLE neighbour lookup: the nearest fix for `device` on the
/// given side of `timestamp`.
async fn neighbour<RT: Runtime>(
    tx: &mut Transaction<RT>,
    device: u64,
    timestamp: f64,
    order: Order,
) -> anyhow::Result<Option<ResolvedDocumentId>> {
    let bound = MaybeValue(Some(ConvexValue::Float64(timestamp)));
    let range = vec![
        IndexRangeExpression::Eq(
            field("deviceId")?,
            MaybeValue(Some(ConvexValue::try_from(format!("device-{device:06}"))?)),
        ),
        match order {
            Order::Desc => IndexRangeExpression::Lte(field("timestamp")?, bound),
            Order::Asc => IndexRangeExpression::Gte(field("timestamp")?, bound),
        },
    ];
    let query = Query::index_range(IndexRange {
        index_name: IndexName::new(
            LOCATIONS_TABLE.parse()?,
            IndexDescriptor::new("by_device_timestamp")?,
        )?,
        range,
        order,
    });
    let mut resolved = ResolvedQuery::new(tx, TableNamespace::Global, query)?;
    Ok(resolved.next(tx, Some(1)).await?.map(|d| d.id()))
}

/// "Where is this device right now" — the dashboard read, one indexed lookup
/// through `deviceLatestLocations.by_device`.
pub async fn latest_for_device<RT: Runtime>(
    tx: &mut Transaction<RT>,
    device: u64,
) -> anyhow::Result<Option<ResolvedDocumentId>> {
    latest_row(tx, device).await
}

/// The device's current spine row, if it has one.
async fn latest_row<RT: Runtime>(
    tx: &mut Transaction<RT>,
    device: u64,
) -> anyhow::Result<Option<ResolvedDocumentId>> {
    let query = Query::index_range(IndexRange {
        index_name: IndexName::new(LATEST_TABLE.parse()?, IndexDescriptor::new("by_device")?)?,
        range: vec![IndexRangeExpression::Eq(
            field("deviceId")?,
            MaybeValue(Some(ConvexValue::try_from(format!("device-{device:06}"))?)),
        )],
        order: Order::Asc,
    });
    let mut resolved = ResolvedQuery::new(tx, TableNamespace::Global, query)?;
    Ok(resolved.next(tx, Some(1)).await?.map(|d| d.id()))
}

/// What one event did, for reporting.
#[derive(Default, Debug, Clone, Copy)]
pub struct EventStats {
    pub inserts: usize,
    pub merges: usize,
    pub reads: usize,
}

impl EventStats {
    pub fn add(&mut self, other: EventStats) {
        self.inserts += other.inserts;
        self.merges += other.merges;
        self.reads += other.reads;
    }
}

/// Apply one location event the way `ingestLocationImpl` does.
///
/// `merge` decides whether this fix extends the previous cluster (a parked
/// vehicle) or starts a new row (a moving one). The real code decides from
/// drift radius and speed; the ratio is what matters for the storage shape, so
/// it is a parameter here.
pub async fn apply_location_event<RT: Runtime>(
    tx: &mut Transaction<RT>,
    device: u64,
    timestamp: f64,
    merge: bool,
) -> anyhow::Result<EventStats> {
    let mut stats = EventStats::default();

    // 1. The two RLE neighbour reads.
    let previous = neighbour(tx, device, timestamp, Order::Desc).await?;
    let _following = neighbour(tx, device, timestamp, Order::Asc).await?;
    stats.reads += 2;

    // 2. Extend the previous cluster, or start a new one.
    let locations: TableName = LOCATIONS_TABLE.parse()?;
    match (merge, &previous) {
        (true, Some(previous)) => {
            // An RLE merge rewrites the neighbouring row in place, which is a
            // new version of an existing document rather than a new document.
            let merged = location_document(device, timestamp, 2)?;
            UserFacingModel::new(tx, TableNamespace::Global)
                .replace((*previous).into(), PendingValue::from(merged))
                .await?;
            stats.merges += 1;
        },
        _ => {
            UserFacingModel::new(tx, TableNamespace::Global)
                .insert(locations, location_document(device, timestamp, 1)?)
                .await?;
            stats.inserts += 1;
        },
    }

    // 3. The latest-location spine upsert: read then replace, or first insert.
    let spine = latest_row(tx, device).await?;
    stats.reads += 1;
    let row = latest_document(device, timestamp)?;
    match spine {
        Some(id) => {
            UserFacingModel::new(tx, TableNamespace::Global)
                .replace(id.into(), PendingValue::from(row))
                .await?;
        },
        None => {
            UserFacingModel::new(tx, TableNamespace::Global)
                .insert(LATEST_TABLE.parse()?, row)
                .await?;
        },
    }

    Ok(stats)
}
