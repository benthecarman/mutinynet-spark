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
             CREATE TABLE IF NOT EXISTS preimages(hash TEXT PRIMARY KEY, preimage TEXT NOT NULL, owner TEXT NOT NULL DEFAULT '', created_at TEXT NOT NULL DEFAULT '1970-01-01T00:00:00+00:00');
             CREATE TABLE IF NOT EXISTS payments(id TEXT PRIMARY KEY, status TEXT NOT NULL);
             CREATE TABLE IF NOT EXISTS receive_payments(hash TEXT PRIMARY KEY, status TEXT NOT NULL);
             CREATE TABLE IF NOT EXISTS static_quotes(txid TEXT NOT NULL, vout INTEGER NOT NULL, credit INTEGER NOT NULL, signature TEXT NOT NULL, created_at TEXT NOT NULL, claimed INTEGER NOT NULL DEFAULT 0, PRIMARY KEY(txid, vout));",
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
        if conn.prepare("SELECT owner FROM preimages LIMIT 0").is_err() {
            conn.execute(
                "ALTER TABLE preimages ADD COLUMN owner TEXT NOT NULL DEFAULT ''",
                [],
            )
            .map_err(|e| e.to_string())?;
        }
        if conn
            .prepare("SELECT created_at FROM preimages LIMIT 0")
            .is_err()
        {
            conn.execute(
                "ALTER TABLE preimages ADD COLUMN created_at TEXT NOT NULL DEFAULT '1970-01-01T00:00:00+00:00'",
                [],
            )
            .map_err(|e| e.to_string())?;
        }
        if conn
            .prepare("SELECT claimed FROM static_quotes LIMIT 0")
            .is_err()
        {
            conn.execute(
                "ALTER TABLE static_quotes ADD COLUMN claimed INTEGER NOT NULL DEFAULT 0",
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
        let row: Option<(String, String)> = self
            .with(|conn| {
                let row = conn
                    .query_row(
                        "SELECT protected, issued_at FROM challenges WHERE identity=?1",
                        (identity,),
                        |r| Ok((r.get(0)?, r.get(1)?)),
                    )
                    .map(Some)
                    .or_else(|e| match e {
                        rusqlite::Error::QueryReturnedNoRows => Ok(None),
                        e => Err(e),
                    })?;
                conn.execute("DELETE FROM challenges WHERE identity=?1", (identity,))?;
                Ok(row)
            })
            .await?;
        let Some((stored, issued_at)) = row else {
            return Ok(false);
        };
        if stored != protected {
            return Ok(false);
        }
        let issued = chrono::DateTime::parse_from_rfc3339(&issued_at)
            .map(|d| d.timestamp())
            .unwrap_or(0);
        let age = now_epoch_secs.saturating_sub(issued);
        Ok(issued <= now_epoch_secs && age <= max_age_secs)
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

    pub async fn lightning_send_for_payment(
        &self,
        payment_id: &str,
    ) -> Result<Option<(String, String)>, String> {
        self.with(|c| {
            c.query_row(
                "SELECT owner,
                        json_extract(payload, '$.user_outbound_transfer_external_id')
                 FROM requests
                 WHERE kind='LIGHTNING_SEND'
                   AND json_extract(payload, '$.payment_id')=?1
                 LIMIT 1",
                (payment_id,),
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .map(Some)
            .or_else(|error| match error {
                rusqlite::Error::QueryReturnedNoRows => Ok(None),
                error => Err(error),
            })
        })
        .await
    }

    pub async fn lightning_receive_for_hash(
        &self,
        payment_hash: &str,
    ) -> Result<Option<(String, String, u64)>, String> {
        self.with(|c| {
            c.query_row(
                "SELECT id, owner, json_extract(payload, '$.amount_sats')
                 FROM requests
                 WHERE kind='LIGHTNING_RECEIVE'
                   AND json_extract(payload, '$.payment_hash')=?1
                 LIMIT 1",
                (payment_hash,),
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .map(Some)
            .or_else(|error| match error {
                rusqlite::Error::QueryReturnedNoRows => Ok(None),
                error => Err(error),
            })
        })
        .await
    }

    pub async fn transfer_for_request(
        &self,
        request_id: &str,
        owner: &str,
    ) -> Result<Option<String>, String> {
        self.with(|c| {
            c.query_row(
                "SELECT spark_id FROM transfers
                 WHERE request_id=?1 AND owner=?2
                 LIMIT 1",
                (request_id, owner),
                |row| row.get(0),
            )
            .map(Some)
            .or_else(|error| match error {
                rusqlite::Error::QueryReturnedNoRows => Ok(None),
                error => Err(error),
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
        let changed = self
            .with(|c| {
                c.execute(
                "INSERT INTO transfers(spark_id,request_id,kind,status,owner) VALUES(?1,?2,?3,?4,?5)
                 ON CONFLICT(spark_id) DO UPDATE SET status=excluded.status WHERE transfers.owner=excluded.owner",
                (spark_id, request_id, kind, status, owner),
            )
            })
            .await?;
        if changed == 1 {
            Ok(())
        } else {
            Err("transfer id belongs to another owner".to_string())
        }
    }

    /// Single query, capped row count.
    pub async fn transfers_for(&self, ids: &[String], owner: &str) -> Result<Vec<Value>, String> {
        const MAX_IDS: usize = 500;
        if ids.is_empty() {
            return Ok(vec![]);
        }
        if ids.len() > MAX_IDS {
            return Err(format!("at most {MAX_IDS} transfer ids are allowed"));
        }
        let ids: Vec<&String> = ids.iter().collect();
        let placeholders = ids.iter().map(|_| "?").collect::<Vec<_>>().join(",");
        // rusqlite params must be homogeneous: build owned Vec<Value-ish> via params_from_iter.
        let sql = format!(
            "SELECT t.spark_id,t.request_id,t.kind,t.status,r.payload
             FROM transfers t
             LEFT JOIN requests r ON r.id=t.request_id AND r.owner=t.owner
             WHERE t.owner=? AND t.spark_id IN ({placeholders})"
        );
        self.with(move |c| {
            let mut stmt = c.prepare(&sql)?;
            let mut params: Vec<&dyn rusqlite::ToSql> = vec![&owner];
            for id in &ids {
                params.push(id);
            }
            let rows = stmt.query_map(rusqlite::params_from_iter(params), |r| {
                let payload = r
                    .get::<_, Option<String>>(4)?
                    .and_then(|value| serde_json::from_str::<Value>(&value).ok())
                    .unwrap_or(Value::Null);
                let total_amount_sats = payload
                    .get("total_amount_sats")
                    .or_else(|| payload.get("amount_sats"))
                    .and_then(Value::as_u64)
                    .unwrap_or(0);
                Ok(serde_json::json!({
                    "spark_id": r.get::<_, String>(0)?,
                    "user_request_id": r.get::<_, String>(1)?,
                    "type": r.get::<_, String>(2)?,
                    "status": r.get::<_, String>(3)?,
                    "total_amount_sats": total_amount_sats,
                }))
            })?;
            rows.collect::<rusqlite::Result<Vec<Value>>>()
        })
        .await
    }

    pub async fn request_id_for_transfer(
        &self,
        spark_id: &str,
        owner: &str,
    ) -> Result<Option<String>, String> {
        self.with(|c| {
            c.query_row(
                "SELECT request_id FROM transfers WHERE spark_id=?1 AND owner=?2",
                (spark_id, owner),
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

    // ---- static deposit quotes (one row per UTXO) ----
    pub async fn record_static_quote(
        &self,
        txid: &str,
        vout: u32,
        credit: u64,
        signature: &str,
        created_at: &str,
    ) -> Result<Option<String>, String> {
        self.with(|c| {
            let existing = c
                .query_row(
                    "SELECT credit,signature,claimed FROM static_quotes WHERE txid=?1 AND vout=?2",
                    (txid, vout),
                    |row| {
                        Ok((
                            row.get::<_, i64>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, i64>(2)?,
                        ))
                    },
                )
                .map(Some)
                .or_else(|error| match error {
                    rusqlite::Error::QueryReturnedNoRows => Ok(None),
                    error => Err(error),
                })?;
            if let Some((stored_credit, stored_signature, claimed)) = existing {
                return Ok(
                    (stored_credit == credit as i64 && claimed == 0).then_some(stored_signature)
                );
            }
            c.execute(
                "INSERT INTO static_quotes(txid,vout,credit,signature,created_at)
                 VALUES(?1,?2,?3,?4,?5)",
                (txid, vout, credit as i64, signature, created_at),
            )?;
            Ok(Some(signature.to_string()))
        })
        .await
    }

    pub async fn consume_static_quote(
        &self,
        txid: &str,
        vout: u32,
        signature: &str,
    ) -> Result<bool, String> {
        self.with(|c| {
            c.execute(
                "UPDATE static_quotes SET claimed=1
                 WHERE txid=?1 AND vout=?2 AND signature=?3 AND claimed=0",
                (txid, vout, signature),
            )
            .map(|changed| changed == 1)
        })
        .await
    }
    // ---- preimages ----
    pub async fn save_preimage(
        &self,
        hash: &str,
        preimage: &str,
        owner: &str,
        created_at: &str,
    ) -> Result<(), String> {
        self.with(|c| {
            c.execute(
                "INSERT INTO preimages(hash,preimage,owner,created_at) VALUES(?1,?2,?3,?4)
                 ON CONFLICT(hash) DO UPDATE SET preimage=excluded.preimage, owner=excluded.owner, created_at=excluded.created_at",
                (hash, preimage, owner, created_at),
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

    pub async fn get_preimage_for_owner(
        &self,
        hash: &str,
        owner: &str,
    ) -> Result<Option<String>, String> {
        self.with(|c| {
            c.query_row(
                "SELECT preimage FROM preimages WHERE hash=?1 AND owner=?2",
                (hash, owner),
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

    pub async fn delete_preimage(&self, hash: &str) -> Result<(), String> {
        self.with(|c| {
            c.execute("DELETE FROM preimages WHERE hash=?1", (hash,))
                .map(|_| ())
        })
        .await
    }

    pub async fn prune_orphan_preimages(&self, older_than_rfc3339: &str) -> Result<(), String> {
        self.with(|c| {
            c.execute(
                "DELETE FROM preimages
                 WHERE created_at<?1
                   AND NOT EXISTS (
                     SELECT 1 FROM requests
                     WHERE kind='LIGHTNING_RECEIVE'
                       AND json_extract(payload, '$.payment_hash')=preimages.hash
                   )",
                (older_than_rfc3339,),
            )
            .map(|_| ())
        })
        .await
    }

    pub async fn expired_receive_hashes(&self, now_epoch: i64) -> Result<Vec<String>, String> {
        self.with(|c| {
            let mut stmt = c.prepare(
                "SELECT r.created_at,r.payload
                 FROM requests r
                 LEFT JOIN receive_payments p
                   ON p.hash=json_extract(r.payload, '$.payment_hash')
                 WHERE r.kind='LIGHTNING_RECEIVE'
                   AND COALESCE(p.status, '') NOT IN ('TRANSFER_COMPLETED','HTLC_FAILED')",
            )?;
            let rows =
                stmt.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))?;
            let mut expired = Vec::new();
            for row in rows {
                let (created_at, payload) = row?;
                let created = chrono::DateTime::parse_from_rfc3339(&created_at)
                    .map(|value| value.timestamp())
                    .unwrap_or(i64::MAX);
                let payload: Value = serde_json::from_str(&payload).unwrap_or(Value::Null);
                let expiry = payload
                    .get("expiry_secs")
                    .and_then(Value::as_u64)
                    .unwrap_or(86_400);
                if created.saturating_add(expiry.min(i64::MAX as u64) as i64) <= now_epoch {
                    if let Some(hash) = payload.get("payment_hash").and_then(Value::as_str) {
                        expired.push(hash.to_string());
                    }
                }
            }
            Ok(expired)
        })
        .await
    }

    pub async fn has_receive_request(&self, hash: &str, owner: &str) -> Result<bool, String> {
        self.with(|c| {
            c.query_row(
                "SELECT EXISTS(
                    SELECT 1 FROM requests
                    WHERE kind='LIGHTNING_RECEIVE' AND owner=?1
                      AND json_extract(payload, '$.payment_hash')=?2
                )",
                (owner, hash),
                |r| r.get(0),
            )
        })
        .await
    }

    pub async fn set_receive_status(&self, hash: &str, status: &str) -> Result<(), String> {
        self.with(|c| {
            c.execute(
                "INSERT INTO receive_payments(hash,status) VALUES(?1,?2)
                 ON CONFLICT(hash) DO UPDATE SET status=excluded.status",
                (hash, status),
            )
            .map(|_| ())
        })
        .await
    }

    pub async fn receive_status(&self, hash: &str) -> Result<String, String> {
        let found: Option<String> = self
            .with(|c| {
                c.query_row(
                    "SELECT status FROM receive_payments WHERE hash=?1",
                    (hash,),
                    |r| r.get(0),
                )
                .map(Some)
                .or_else(|e| match e {
                    rusqlite::Error::QueryReturnedNoRows => Ok(None),
                    e => Err(e),
                })
            })
            .await?;
        Ok(found.unwrap_or_else(|| "INVOICE_CREATED".to_string()))
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

#[cfg(test)]
mod tests {
    use super::*;

    fn test_db() -> (Db, PathBuf) {
        let dir = std::env::temp_dir().join(format!("mutinynet-ssp-{}", uuid::Uuid::new_v4()));
        (Db::open(dir.to_str().unwrap()).unwrap(), dir)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn challenge_is_single_use() {
        let (db, dir) = test_db();
        let now = chrono::Utc::now();
        db.save_challenge("owner", "challenge", &now.to_rfc3339())
            .await
            .unwrap();

        assert!(db
            .consume_challenge("owner", "challenge", now.timestamp(), 300)
            .await
            .unwrap());
        assert!(!db
            .consume_challenge("owner", "challenge", now.timestamp(), 300)
            .await
            .unwrap());
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn challenge_rejects_unknown_expired_and_future_values() {
        let (db, dir) = test_db();
        let now = chrono::Utc::now();

        assert!(!db
            .consume_challenge("owner", "never-issued", now.timestamp(), 300)
            .await
            .unwrap());

        db.save_challenge(
            "owner",
            "expired",
            &(now - chrono::Duration::seconds(301)).to_rfc3339(),
        )
        .await
        .unwrap();
        assert!(!db
            .consume_challenge("owner", "expired", now.timestamp(), 300)
            .await
            .unwrap());

        db.save_challenge(
            "owner",
            "future",
            &(now + chrono::Duration::seconds(1)).to_rfc3339(),
        )
        .await
        .unwrap();
        assert!(!db
            .consume_challenge("owner", "future", now.timestamp(), 300)
            .await
            .unwrap());

        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn minted_preimage_is_owner_scoped() {
        let (db, dir) = test_db();
        db.save_preimage("hash", "preimage", "alice", "now")
            .await
            .unwrap();

        assert_eq!(
            db.get_preimage_for_owner("hash", "alice").await.unwrap(),
            Some("preimage".to_string())
        );
        assert_eq!(
            db.get_preimage_for_owner("hash", "bob").await.unwrap(),
            None
        );
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn lightning_receive_links_request_and_transfer() {
        let (db, dir) = test_db();
        db.insert_request(
            "request",
            "LIGHTNING_RECEIVE",
            "owner",
            "now",
            &serde_json::json!({"payment_hash": "hash", "amount_sats": 1234}),
            None,
        )
        .await
        .unwrap();

        assert_eq!(
            db.lightning_receive_for_hash("hash").await.unwrap(),
            Some(("request".to_string(), "owner".to_string(), 1234))
        );
        assert_eq!(
            db.transfer_for_request("request", "owner").await.unwrap(),
            None
        );

        db.insert_transfer(
            "transfer",
            "request",
            "LIGHTNING_RECEIVE",
            "TRANSFER_COMPLETED",
            "owner",
        )
        .await
        .unwrap();
        assert_eq!(
            db.transfer_for_request("request", "owner").await.unwrap(),
            Some("transfer".to_string())
        );
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn static_quote_is_retryable_but_claims_once() {
        let (db, dir) = test_db();
        let txid = "00".repeat(32);
        assert_eq!(
            db.record_static_quote(&txid, 0, 100_000, "first", "now")
                .await
                .unwrap(),
            Some("first".to_string())
        );
        assert_eq!(
            db.record_static_quote(&txid, 0, 100_000, "retry", "later")
                .await
                .unwrap(),
            Some("first".to_string())
        );
        assert!(db.consume_static_quote(&txid, 0, "first").await.unwrap());
        assert!(!db.consume_static_quote(&txid, 0, "first").await.unwrap());
        assert_eq!(
            db.record_static_quote(&txid, 0, 100_000, "after", "later")
                .await
                .unwrap(),
            None
        );
        std::fs::remove_dir_all(dir).unwrap();
    }
}
