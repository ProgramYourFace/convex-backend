use std::{
    collections::HashMap,
    path::PathBuf,
    sync::Arc,
};

use anyhow::Context as _;
use clusters::{
    persistence_args_from_cluster_url,
    DbDriverTag,
    PersistenceArgs,
};
use common::{
    knobs::DATABASE_USE_PREPARED_STATEMENTS,
    persistence::{
        Persistence,
        PersistenceReader,
    },
    runtime::Runtime,
    shutdown::ShutdownSignal,
};
use mysql::{
    ConvexMySqlPool,
    MySqlOptions,
    MySqlReaderOptions,
};
use postgres::{
    PostgresOptions,
    PostgresPersistence,
    PostgresReaderOptions,
};
use rocksdb_persistence::RocksDbPersistence;
use sqlite::SqlitePersistence;
use tokio_postgres::config::TargetSessionAttrs;

#[derive(Copy, Clone, Debug)]
pub struct ConnectPersistenceFlags {
    pub require_ssl: bool,
    pub allow_read_only: bool,
    pub skip_index_creation: bool,
}

pub enum PersistenceSeed<RT: Runtime> {
    Sqlite {
        db_spec: String,
    },
    RocksDb {
        path: PathBuf,
    },
    Postgres {
        config: tokio_postgres::Config,
        options: PostgresOptions,
    },
    MySql {
        pool: Arc<ConvexMySqlPool<RT>>,
        db_name: String,
        options: MySqlOptions,
    },
}

pub fn persistence_seed<RT: Runtime>(
    db: DbDriverTag,
    db_spec: &str,
    flags: ConnectPersistenceFlags,
    deployment_name: &str,
    runtime: RT,
) -> anyhow::Result<PersistenceSeed<RT>> {
    match db {
        DbDriverTag::Sqlite => Ok(PersistenceSeed::Sqlite {
            db_spec: db_spec.to_owned(),
        }),
        // An embedded store is addressed by a filesystem path, so there is no
        // cluster URL to parse and no lease to acquire.
        DbDriverTag::RocksDb => Ok(PersistenceSeed::RocksDb {
            path: PathBuf::from(db_spec),
        }),
        DbDriverTag::Postgres(version)
        | DbDriverTag::MySql(version)
        | DbDriverTag::MySqlMultitenant(version) => {
            let args = persistence_args_from_cluster_url(
                deployment_name,
                db_spec.parse()?,
                db,
                flags.require_ssl,
                true, /* require_leader */
            )?;
            match args {
                PersistenceArgs::Postgres {
                    mut url,
                    schema,
                    multitenant,
                } => {
                    let options = PostgresOptions {
                        allow_read_only: flags.allow_read_only,
                        version,
                        schema,
                        instance_name: deployment_name.into(),
                        multitenant,
                        skip_index_creation: flags.skip_index_creation,
                    };
                    // tokio-postgres forbids unknown query parameters, so we need to filter out
                    // `search_path` which is our "hack" for propagating the target schema name
                    // to the persistence layer
                    let query = url
                        .query_pairs()
                        .filter(|(k, _)| k != "search_path")
                        .map(|(k, v)| (k.into_owned(), v.into_owned()))
                        .collect::<HashMap<_, _>>();
                    let url = url.query_pairs_mut().clear().extend_pairs(query).finish();
                    Ok(PersistenceSeed::Postgres {
                        config: url
                            .as_str()
                            .parse()
                            .context("invalid postgres connection url")?,
                        options,
                    })
                },
                PersistenceArgs::MySql {
                    url,
                    db_name,
                    multitenant,
                    require_leader,
                } => {
                    let options = MySqlOptions {
                        allow_read_only: flags.allow_read_only,
                        version,
                        multitenant,
                        instance_name: deployment_name.into(),
                    };
                    Ok(PersistenceSeed::MySql {
                        pool: Arc::new(ConvexMySqlPool::new(
                            &url,
                            *DATABASE_USE_PREPARED_STATEMENTS,
                            require_leader,
                            Some(runtime),
                        )?),
                        db_name,
                        options,
                    })
                },
            }
        },
        _ => unreachable!(),
    }
}

pub async fn connect_persistence<RT: Runtime>(
    db: DbDriverTag,
    db_spec: &str,
    flags: ConnectPersistenceFlags,
    deployment_name: &str,
    runtime: RT,
    shutdown_signal: ShutdownSignal,
) -> anyhow::Result<Arc<dyn Persistence>> {
    match persistence_seed(db, db_spec, flags, deployment_name, runtime)? {
        PersistenceSeed::Sqlite { db_spec } => {
            let persistence = Arc::new(SqlitePersistence::new(&db_spec)?);
            tracing::info!("Connected to SQLite at {db_spec}");
            Ok(persistence as Arc<dyn Persistence>)
        },
        PersistenceSeed::RocksDb { path } => {
            // Postgres fences a deployment with a `read_only` row that every
            // write checks. An embedded store has no equivalent, and silently
            // ignoring the request would let a migration or an import run
            // against a database the operator believes is frozen.
            anyhow::ensure!(
                !flags.allow_read_only,
                "the RocksDB backend has no read-only mode: it cannot fence writes the way the \
                 relational backends do with their `read_only` row. Stop the writer instead.",
            );
            // The same signal the relational backends raise on lease loss. An
            // embedded engine has no lease, but it does latch read-only on a
            // background error — a full disk, a corrupt SST — after which every
            // write fails and nothing crashes. Without somewhere to report
            // that, the process serves and fails mutations indefinitely.
            let persistence = Arc::new(RocksDbPersistence::open_with(
                &path,
                rocksdb_persistence::OpenOptions {
                    shutdown: Some(shutdown_signal),
                    background: true,
                    ..rocksdb_persistence::OpenOptions::default()
                },
            )?);
            tracing::info!("Opened RocksDB at {}", path.display());
            Ok(persistence as Arc<dyn Persistence>)
        },
        PersistenceSeed::Postgres {
            mut config,
            options,
        } => {
            config.target_session_attrs(TargetSessionAttrs::ReadWrite);
            let pool = PostgresPersistence::create_pool(config)?;
            let persistence =
                Arc::new(PostgresPersistence::with_pool(pool, options, shutdown_signal).await?);
            tracing::info!("Connected to Postgres database: {}", deployment_name);
            Ok(persistence)
        },
        PersistenceSeed::MySql {
            pool,
            db_name,
            options,
        } => {
            let persistence =
                mysql::connect_persistence(pool, db_name.clone(), options, shutdown_signal).await?;
            tracing::info!("Connected to MySQL database: {}", db_name);
            Ok(persistence)
        },
    }
}

pub async fn connect_persistence_reader<RT: Runtime>(
    db: DbDriverTag,
    db_spec: &str,
    require_ssl: bool,
    db_should_be_leader: bool,
    deployment_name: &str,
    runtime: RT,
) -> anyhow::Result<Arc<dyn PersistenceReader>> {
    match persistence_seed(
        db,
        db_spec,
        ConnectPersistenceFlags {
            require_ssl,
            allow_read_only: true,
            skip_index_creation: false,
        },
        deployment_name,
        runtime,
    )? {
        PersistenceSeed::Sqlite { db_spec } => {
            Ok(Arc::new(SqlitePersistence::new(&db_spec)?) as Arc<dyn PersistenceReader>)
        },
        PersistenceSeed::RocksDb { path } => {
            // A RocksDB directory has one writer, so a standalone reader opens
            // a secondary instance beside it rather than the primary itself.
            //
            // The secondary's scratch directory is created and owned inside
            // the backend, so it is removed when the reader using it is
            // dropped rather than accumulating one per reader in TMPDIR.
            Ok(RocksDbPersistence::new_secondary(&path)?.reader())
        },
        PersistenceSeed::Postgres { config, options } => {
            let options = PostgresReaderOptions {
                version: options.version,
                schema: options.schema,
                instance_name: options.instance_name,
                multitenant: options.multitenant,
            };
            Ok(Arc::new(
                PostgresPersistence::new_reader(
                    PostgresPersistence::create_pool(config)
                        .context("failed to create postgres pool")?,
                    options,
                )
                .await?,
            ))
        },
        PersistenceSeed::MySql {
            pool,
            db_name,
            options,
        } => {
            let options = MySqlReaderOptions {
                db_should_be_leader,
                version: options.version,
                multitenant: options.multitenant,
                instance_name: options.instance_name,
            };
            mysql::connect_persistence_reader(pool, db_name, options)
        },
    }
}

pub async fn set_read_only<RT: Runtime>(
    db: DbDriverTag,
    db_spec: &str,
    flags: ConnectPersistenceFlags,
    instance_name: &str,
    runtime: RT,
    read_only: bool,
) -> anyhow::Result<()> {
    match persistence_seed(db, db_spec, flags, instance_name, runtime)? {
        PersistenceSeed::Postgres { config, options } => {
            let pool = PostgresPersistence::create_pool(config)?;
            PostgresPersistence::set_read_only(pool, options, read_only).await?;
            Ok(())
        },
        PersistenceSeed::MySql {
            pool,
            db_name,
            options,
        } => {
            mysql::set_persistence_read_only(pool, db_name, options, read_only).await?;
            Ok(())
        },
        _ => anyhow::bail!("unsupported persistence type: {db:?}"),
    }
}
