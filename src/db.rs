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
            "CREATE TABLE IF NOT EXISTS challenges(identity TEXT PRIMARY KEY, protected TEXT NOT NULL);
             CREATE TABLE IF NOT EXISTS sessions(token TEXT PRIMARY KEY, identity TEXT NOT NULL, valid_until TEXT NOT NULL);
             CREATE TABLE IF NOT EXISTS requests(id TEXT PRIMARY KEY, kind TEXT NOT NULL, owner TEXT NOT NULL, created_at TEXT NOT NULL, payload TEXT NOT NULL);
             CREATE TABLE IF NOT EXISTS transfers(spark_id TEXT PRIMARY KEY, request_id TEXT NOT NULL, kind TEXT NOT NULL, status TEXT NOT NULL);
             CREATE TABLE IF NOT EXISTS preimages(hash TEXT PRIMARY KEY, preimage TEXT NOT NULL);
             CREATE TABLE IF NOT EXISTS payments(id TEXT PRIMARY KEY, status TEXT NOT NULL);",
        )
        .map_err(|e| e.to_string())?;
        Ok(Self {
            inner: Arc::new(Mutex::new(conn)),
        })
    }

    async fn with<T, F>(&self, f: F) -> Result<T, String>
    where
        F: FnOnce(&rusqlite::Connection) -> rusqlite::Result<T> + Send,
        T: Send,
    {
        let conn = self.inner.lock().await;
        f(&conn).map_err(|e| e.to_string())
    }

    // ---- challenges ----
    pub async fn save_challenge(&self, identity: &str, protected: &str) -> Result<(), String> {
        self.with(|c| {
            c.execute(
                "INSERT INTO challenges(identity,protected) VALUES(?1,?2)
                 ON CONFLICT(identity) DO UPDATE SET protected=excluded.protected",
                (identity, protected),
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

    // ---- requests ----
    pub async fn insert_request(
        &self,
        id: &str,
        kind: &str,
        owner: &str,
        created_at: &str,
        payload: &Value,
    ) -> Result<(), String> {
        let payload = serde_json::to_string(payload).map_err(|e| e.to_string())?;
        self.with(|c| {
            c.execute(
                "INSERT INTO requests(id,kind,owner,created_at,payload) VALUES(?1,?2,?3,?4,?5)",
                (id, kind, owner, created_at, payload),
            )
            .map(|_| ())
        })
        .await
    }

    pub async fn get_request(&self, id: &str) -> Result<Option<Value>, String> {
        self.with(|c| {
            c.query_row(
                "SELECT id,kind,owner,created_at,payload FROM requests WHERE id=?1",
                (id,),
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

    // ---- transfers ----
    pub async fn insert_transfer(
        &self,
        spark_id: &str,
        request_id: &str,
        kind: &str,
        status: &str,
    ) -> Result<(), String> {
        self.with(|c| {
            c.execute(
                "INSERT INTO transfers(spark_id,request_id,kind,status) VALUES(?1,?2,?3,?4)
                 ON CONFLICT(spark_id) DO UPDATE SET status=excluded.status",
                (spark_id, request_id, kind, status),
            )
            .map(|_| ())
        })
        .await
    }

    pub async fn transfers_for(&self, ids: &[String]) -> Result<Vec<Value>, String> {
        if ids.is_empty() {
            return Ok(vec![]);
        }
        let conn = self.inner.lock().await;
        let mut out = Vec::new();
        for id in ids {
            let row: Option<Value> = conn
                .query_row(
                    "SELECT spark_id,request_id,kind,status FROM transfers WHERE spark_id=?1",
                    (id,),
                    |r| {
                        Ok(serde_json::json!({
                            "spark_id": r.get::<_, String>(0)?,
                            "user_request_id": r.get::<_, String>(1)?,
                            "type": r.get::<_, String>(2)?,
                            "status": r.get::<_, String>(3)?,
                        }))
                    },
                )
                .map(Some)
                .or_else(|e| match e {
                    rusqlite::Error::QueryReturnedNoRows => Ok(None),
                    e => Err(e),
                })
                .map_err(|e: rusqlite::Error| e.to_string())?;
            if let Some(row) = row {
                out.push(row);
            }
        }
        Ok(out)
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
