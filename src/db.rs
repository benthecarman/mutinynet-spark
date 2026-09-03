use std::{path::PathBuf, sync::Arc};

use serde_json::Value;
use tokio::sync::Mutex;

/// SQLite persistence for everything the SSP must survive restarts with:
/// auth challenges/sessions, user requests, LN preimages, payment states.
/// Single file at `<SSP_DATA_DIR>/ssp.sqlite` (volume-mount in compose).
#[derive(Clone)]
pub struct Db {
    inner: Arc<Mutex<rusqlite::Connection>>,
}

impl Db {
    pub fn open(data_dir: &str) -> Result<Self, String> {
        std::fs::create_dir_all(data_dir).map_err(|e| e.to_string())?;
        let path: PathBuf = [data_dir, "ssp.sqlite"].iter().collect();
        let conn = rusqlite::Connection::open(path).map_err(|e| e.to_string())?;
        conn.execute_batch(
            "PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000; PRAGMA foreign_keys=ON;",
        )
        .map_err(|e| e.to_string())?;
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS challenges(identity TEXT PRIMARY KEY, protected TEXT NOT NULL, issued_at TEXT NOT NULL);
             CREATE TABLE IF NOT EXISTS sessions(token TEXT PRIMARY KEY, identity TEXT NOT NULL, valid_until TEXT NOT NULL);
             CREATE TABLE IF NOT EXISTS requests(id TEXT PRIMARY KEY, kind TEXT NOT NULL, owner TEXT NOT NULL, created_at TEXT NOT NULL, payload TEXT NOT NULL);
             CREATE TABLE IF NOT EXISTS transfers(spark_id TEXT PRIMARY KEY, request_id TEXT NOT NULL, kind TEXT NOT NULL, status TEXT NOT NULL);
             CREATE TABLE IF NOT EXISTS preimages(hash TEXT PRIMARY KEY, preimage TEXT NOT NULL);
             CREATE TABLE IF NOT EXISTS payments(id TEXT PRIMARY KEY, status TEXT NOT NULL);
             CREATE TABLE IF NOT EXISTS static_quotes(txid TEXT NOT NULL, vout INTEGER NOT NULL, credit INTEGER NOT NULL, signature TEXT NOT NULL, created_at TEXT NOT NULL, PRIMARY KEY(txid, vout));",
        )
        .map_err(|e| e.to_string())?;
        // Lightweight migrations for DBs created by older builds: probe for
        // the column, ALTER when missing.
        if conn
            .prepare("SELECT issued_at FROM challenges LIMIT 0")
            .is_err()
        {
            conn.execute(
                "ALTER TABLE challenges ADD COLUMN issued_at TEXT NOT NULL DEFAULT '1970-01-01T00:00:00+00:00'",
                [],
            )
            .map_err(|e| e.to_string())?;
        }
        if conn.prepare("SELECT owner FROM transfers LIMIT 0").is_err() {
            conn.execute(
                "ALTER TABLE transfers ADD COLUMN owner TEXT NOT NULL DEFAULT ''",
                [],
            )
            .map_err(|e| e.to_string())?;
        }
        if conn
            .prepare("SELECT idempotency_key FROM requests LIMIT 0")
            .is_err()
        {
            conn.execute("ALTER TABLE requests ADD COLUMN idempotency_key TEXT", [])
                .map_err(|e| e.to_string())?;
            conn.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_requests_owner_idem ON requests(owner, idempotency_key) WHERE idempotency_key IS NOT NULL AND idempotency_key <> ''",
                [],
            )
            .map_err(|e| e.to_string())?;
        }
        Ok(Self {
            inner: Arc::new(Mutex::new(conn)),
        })
    }

    async fn with<T, F>(&self, f: F) -> Result<T, String>
    where
        F: FnOnce(&rusqlite::Connection) -> rusqlite::Result<T> + Send,
        T: Send + 'static,
    {
        // rusqlite is blocking I/O: keep it off async workers.
        tokio::task::block_in_place(|| {
            let conn = self.inner.blocking_lock();
            f(&conn).map_err(|e| e.to_string())
        })
    }

    // ---- challenges (single-use, 5-minute expiry) ----
    pub async fn save_challenge(
        &self,
        identity: &str,
        protected: &str,
        now: &str,
    ) -> Result<(), String> {
        self.with(|c| {
            c.execute(
                "INSERT INTO challenges(identity,protected,issued_at) VALUES(?1,?2,?3)
                 ON CONFLICT(identity) DO UPDATE SET protected=excluded.protected, issued_at=excluded.issued_at",
                (identity, protected, now),
            )
            .map(|_| ())
        })
        .await
    }

    /// Atomically consume a challenge: returns true only if the stored
    /// challenge matches and is younger than `max_age_secs`. Always deletes.
    pub async fn consume_challenge(
        &self,
        identity: &str,
        protected: &str,
        now_epoch_secs: i64,
        max_age_secs: i64,
    ) -> Result<bool, String> {
        let conn = self.inner.lock().await;
        let row: Option<(String, String)> = conn
            .query_row(
                "SELECT protected, issued_at FROM challenges WHERE identity=?1",
                (identity,),
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .map(Some)
            .or_else(|e| match e {
                rusqlite::Error::QueryReturnedNoRows => Ok(None),
                e => Err(e),
            })
            .map_err(|e: rusqlite::Error| e.to_string())?;
        conn.execute("DELETE FROM challenges WHERE identity=?1", (identity,))
            .map_err(|e: rusqlite::Error| e.to_string())?;
        let Some((stored, issued_at)) = row else {
            return Ok(false);
        };
        if stored != protected {
            return Ok(false);
        }
        let issued = chrono::DateTime::parse_from_rfc3339(&issued_at)
            .map(|d| d.timestamp())
            .unwrap_or(0);
        Ok(now_epoch_secs - issued <= max_age_secs)
    }

    pub async fn prune_challenges(&self, older_than_rfc3339: &str) -> Result<(), String> {
        self.with(|c| {
            c.execute(
                "DELETE FROM challenges WHERE issued_at<?1",
                (older_than_rfc3339,),
            )
            .map(|_| ())
        })
        .await
    }

    // ---- sessions ----
    pub async fn save_session(
        &self,
        token: &str,
        identity: &str,
        valid_until: &str,
    ) -> Result<(), String> {
        self.with(|c| {
            c.execute(
                "INSERT INTO sessions(token,identity,valid_until) VALUES(?1,?2,?3)",
                (token, identity, valid_until),
            )
            .map(|_| ())
        })
        .await
    }

    pub async fn session_owner(
        &self,
        token: &str,
        now_rfc3339: &str,
    ) -> Result<Option<String>, String> {
        self.with(|c| {
            c.query_row(
                "SELECT identity FROM sessions WHERE token=?1 AND valid_until>?2",
                (token, now_rfc3339),
                |r| r.get(0),
            )
            .map(Some)
            .or_else(|e| match e {
                rusqlite::Error::QueryReturnedNoRows => Ok(None),
                e => Err(e),
            })
        })
        .await
    }

    // ---- requests (owner-scoped) ----
    pub async fn insert_request(
        &self,
        id: &str,
        kind: &str,
        owner: &str,
        created_at: &str,
        payload: &Value,
        idempotency_key: Option<&str>,
    ) -> Result<(), String> {
        let payload = serde_json::to_string(payload).map_err(|e| e.to_string())?;
        self.with(|c| {
            c.execute(
                "INSERT INTO requests(id,kind,owner,created_at,payload,idempotency_key) VALUES(?1,?2,?3,?4,?5,?6)",
                (id, kind, owner, created_at, payload, idempotency_key),
            )
            .map(|_| ())
        })
        .await
    }

    pub async fn get_request(&self, id: &str, owner: &str) -> Result<Option<Value>, String> {
        self.with(|c| {
            c.query_row(
                "SELECT id,kind,owner,created_at,payload FROM requests WHERE id=?1 AND owner=?2",
                (id, owner),
                |r| {
                    let payload: String = r.get(4)?;
                    Ok(serde_json::json!({
                        "id": r.get::<_, String>(0)?,
                        "type": r.get::<_, String>(1)?,
                        "owner_identity_pubkey": r.get::<_, String>(2)?,
                        "created_at": r.get::<_, String>(3)?,
                        "payload": serde_json::from_str::<Value>(&payload).unwrap_or(Value::Null),
                    }))
                },
            )
            .map(Some)
            .or_else(|e| match e {
                rusqlite::Error::QueryReturnedNoRows => Ok(None),
                e => Err(e),
            })
        })
        .await
    }

    /// Idempotency: an in-flight/completed request with the same owner+key.
    pub async fn find_by_idempotency(
        &self,
        owner: &str,
        key: &str,
    ) -> Result<Option<Value>, String> {
        if key.is_empty() {
            return Ok(None);
        }
        self.with(|c| {
            c.query_row(
                "SELECT id,kind,owner,created_at,payload FROM requests WHERE owner=?1 AND idempotency_key=?2 ORDER BY created_at DESC LIMIT 1",
                (owner, key),
                |r| {
                    let payload: String = r.get(4)?;
                    Ok(serde_json::json!({
                        "id": r.get::<_, String>(0)?,
                        "type": r.get::<_, String>(1)?,
                        "owner_identity_pubkey": r.get::<_, String>(2)?,
                        "created_at": r.get::<_, String>(3)?,
                        "payload": serde_json::from_str::<Value>(&payload).unwrap_or(Value::Null),
                    }))
                },
            )
            .map(Some)
            .or_else(|e| match e {
                rusqlite::Error::QueryReturnedNoRows => Ok(None),
                e => Err(e),
            })
        })
        .await
    }

    pub async fn prune_expired_sessions(&self, now_rfc3339: &str) -> Result<(), String> {
        self.with(|c| {
            c.execute("DELETE FROM sessions WHERE valid_until<=?1", (now_rfc3339,))
                .map(|_| ())
        })
        .await
    }

    // ---- transfers (owner-scoped) ----
    pub async fn insert_transfer(
        &self,
        spark_id: &str,
        request_id: &str,
        kind: &str,
        status: &str,
        owner: &str,
    ) -> Result<(), String> {
        self.with(|c| {
            c.execute(
                "INSERT INTO transfers(spark_id,request_id,kind,status,owner) VALUES(?1,?2,?3,?4,?5)
                 ON CONFLICT(spark_id) DO UPDATE SET status=excluded.status WHERE transfers.owner=excluded.owner",
                (spark_id, request_id, kind, status, owner),
            )
            .map(|_| ())
        })
        .await
    }

    /// Single query, capped row count.
    pub async fn transfers_for(&self, ids: &[String], owner: &str) -> Result<Vec<Value>, String> {
        const MAX_IDS: usize = 500;
        if ids.is_empty() {
            return Ok(vec![]);
        }
        let ids: Vec<&String> = ids.iter().take(MAX_IDS).collect();
        let placeholders = ids.iter().map(|_| "?").collect::<Vec<_>>().join(",");
        // rusqlite params must be homogeneous: build owned Vec<Value-ish> via params_from_iter.
        let sql = format!(
            "SELECT spark_id,request_id,kind,status FROM transfers WHERE owner=? AND spark_id IN ({placeholders})"
        );
        self.with(move |c| {
            let mut stmt = c.prepare(&sql)?;
            let mut params: Vec<&dyn rusqlite::ToSql> = vec![&owner];
            for id in &ids {
                params.push(id);
            }
            let rows = stmt.query_map(rusqlite::params_from_iter(params), |r| {
                Ok(serde_json::json!({
                    "spark_id": r.get::<_, String>(0)?,
                    "user_request_id": r.get::<_, String>(1)?,
                    "type": r.get::<_, String>(2)?,
                    "status": r.get::<_, String>(3)?,
                }))
            })?;
            rows.collect::<rusqlite::Result<Vec<Value>>>()
        })
        .await
    }

    // ---- static deposit quotes (one row per UTXO) ----
    pub async fn record_static_quote(
        &self,
        txid: &str,
        vout: u32,
        credit: u64,
        signature: &str,
        created_at: &str,
    ) -> Result<(), String> {
        self.with(|c| {
            c.execute(
                "INSERT INTO static_quotes(txid,vout,credit,signature,created_at) VALUES(?1,?2,?3,?4,?5)
                 ON CONFLICT(txid,vout) DO UPDATE SET credit=excluded.credit, signature=excluded.signature, created_at=excluded.created_at",
                (txid, vout, credit as i64, signature, created_at),
            )
            .map(|_| ())
        })
        .await
    }
    // ---- preimages ----
    pub async fn save_preimage(&self, hash: &str, preimage: &str) -> Result<(), String> {
        self.with(|c| {
            c.execute(
                "INSERT INTO preimages(hash,preimage) VALUES(?1,?2)
                 ON CONFLICT(hash) DO UPDATE SET preimage=excluded.preimage",
                (hash, preimage),
            )
            .map(|_| ())
        })
        .await
    }

    pub async fn get_preimage(&self, hash: &str) -> Result<Option<String>, String> {
        self.with(|c| {
            c.query_row(
                "SELECT preimage FROM preimages WHERE hash=?1",
                (hash,),
                |r| r.get(0),
            )
            .map(Some)
            .or_else(|e| match e {
                rusqlite::Error::QueryReturnedNoRows => Ok(None),
                e => Err(e),
            })
        })
        .await
    }

    // ---- payments ----
    pub async fn set_payment(&self, id: &str, status: &str) -> Result<(), String> {
        self.with(|c| {
            c.execute(
                "INSERT INTO payments(id,status) VALUES(?1,?2)
                 ON CONFLICT(id) DO UPDATE SET status=excluded.status",
                (id, status),
            )
            .map(|_| ())
        })
        .await
    }

    pub async fn payment_status(&self, id: &str) -> Result<String, String> {
        let found: Option<String> = self
            .with(|c| {
                c.query_row("SELECT status FROM payments WHERE id=?1", (id,), |r| {
                    r.get(0)
                })
                .map(Some)
                .or_else(|e| match e {
                    rusqlite::Error::QueryReturnedNoRows => Ok(None),
                    e => Err(e),
                })
            })
            .await?;
        Ok(found.unwrap_or_else(|| "UNKNOWN".to_string()))
    }
}
