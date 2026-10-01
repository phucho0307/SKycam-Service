use anyhow::Result;
use bson::doc;
use mongodb::{options::IndexOptions, Client, Database, IndexModel};

use crate::config::Config;

#[derive(Clone)]
pub struct Db {
    pub database: Database,
}

impl Db {
    pub async fn connect(cfg: &Config) -> Result<Self> {
        let client = Client::with_uri_str(&cfg.mongodb_uri).await?;
        let database = client.database(&cfg.mongodb_db);
        database.run_command(doc! { "ping": 1 }).await?;
        tracing::info!(db = %cfg.mongodb_db, "connected to MongoDB");
        ensure_indexes(&database).await;
        Ok(Self { database })
    }
}

/// One index this service relies on, and the query it exists for.
struct Spec {
    collection: &'static str,
    name: &'static str,
    model: IndexModel,
    /// Kept next to the definition so an index can never outlive the reason for
    /// it without someone noticing.
    serves: &'static str,
}

fn specs() -> Vec<Spec> {
    let named = |name: &str| IndexOptions::builder().name(name.to_string());
    vec![
        Spec {
            collection: "frames",
            name: "frames_captured_at_desc",
            model: IndexModel::builder()
                .keys(doc! { "captured_at": -1 })
                .options(named("frames_captured_at_desc").build())
                .build(),
            // GET /frames/latest (polled every 3s by every open browser) and
            // GET /frames?from&to&limit. Both sort on captured_at and take the top
            // N, so the index hands back rows already in order: one key read for
            // "latest" instead of reading and sorting the whole collection.
            serves: "GET /frames/latest, GET /frames",
        },
        Spec {
            collection: "telemetry",
            name: "telemetry_recorded_at_desc",
            model: IndexModel::builder()
                .keys(doc! { "recorded_at": -1 })
                .options(named("telemetry_recorded_at_desc").build())
                .build(),
            serves: "GET /telemetry",
        },
        Spec {
            collection: "settings",
            name: "settings_device_id_unique",
            model: IndexModel::builder()
                .keys(doc! { "device_id": 1 })
                .options(named("settings_device_id_unique").unique(true).build())
                .build(),
            // Read by every camera before every exposure, ~43,200 times a day per
            // site. The collection is tiny, so speed is not the point: unique is.
            // PUT /settings upserts on device_id, and two concurrent upserts with
            // no unique index can both insert, leaving the camera with two
            // settings documents and find_one returning whichever comes first.
            serves: "GET /settings, PUT /settings (upsert)",
        },
        Spec {
            collection: "frames",
            name: "frames_cloud_score",
            model: IndexModel::builder()
                .keys(doc! { "cloud_score": 1 })
                .options(named("frames_cloud_score").build())
                .build(),
            // Not a read-API query: the detection worker's scan for unscored
            // frames, find({cloud_score: {$exists: false}}), every 3 seconds. It
            // lives here because it full-scans the same collection the read API
            // serves from, and competes with it for the same mongod.
            //
            // Not a partial index, although that would be smaller: MongoDB does
            // not allow {$exists: false} in a partialFilterExpression. A plain
            // index works because a missing field is indexed as null, so the
            // query scans only the null bucket.
            serves: "detect worker scan for unscored frames",
        },
        Spec {
            collection: "alarms",
            name: "alarms_frame_kind_unique",
            model: IndexModel::builder()
                .keys(doc! { "frame_id": 1, "kind": 1 })
                .options(named("alarms_frame_kind_unique").unique(true).build())
                .build(),
            // One alarm per frame per kind. With task_acks_late, a detect worker
            // that dies after writing an alarm gets the task redelivered; without
            // this the same cloud raises two alarms.
            serves: "alarm idempotency",
        },
    ]
}

/// Create the indexes this service's queries depend on. Idempotent: creating an
/// index that already exists with the same definition is a no-op, so this runs
/// on every start.
///
/// A failure is logged and **not** fatal. Every query still returns correct
/// results without its index, only slower, so refusing to start would turn a
/// performance problem into an outage. The likeliest failure is a unique index
/// meeting duplicates that already exist, and a crash-looping pod helps nobody
/// fix that.
async fn ensure_indexes(db: &Database) {
    for spec in specs() {
        let coll = db.collection::<bson::Document>(spec.collection);
        match coll.create_index(spec.model).await {
            Ok(_) => tracing::info!(
                collection = spec.collection,
                index = spec.name,
                serves = spec.serves,
                "index ensured"
            ),
            Err(e) => tracing::error!(
                collection = spec.collection,
                index = spec.name,
                serves = spec.serves,
                error = %e,
                "index creation failed; queries still work but may scan"
            ),
        }
    }
}
