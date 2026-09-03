use axum::http::HeaderMap;
use base64::{engine::general_purpose::STANDARD as B64, Engine};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::{auth, ldk::LdkBackend, AppState, GraphqlRequest};

/// Dispatch a GraphQL document to the matching SSP resolver.
/// Operation names mirror spark-sdk `SspClient` methods (client.ts) and the
/// `ssp_rc_schema.graphql` schema (Query/Mutation sections).
///
/// Response shapes must use the RAW schema field names: the SDK sends
/// aliased fragments (`foo_bar: foo`) and `*FromJson` reads the alias keys.
/// Missing inner fields surface as `undefined`, so every resolver below
/// returns the full field set its fragment requests.
pub async fn dispatch(
    state: AppState,
    headers: &HeaderMap,
    op: &str,
    req: &GraphqlRequest,
) -> Result<Value, String> {
    let v = &req.variables;
    // Most mutations nest under `input`.
    let input = v.get("input").cloned().unwrap_or_else(|| v.clone());
    let now = chrono::Utc::now().to_rfc3339();

    match op {
        // ---- auth (no session needed) ----
        "GetChallenge" | "get_challenge" => {
            let pk = input
                .get("public_key")
                .and_then(|x| x.as_str())
                .or_else(|| v.get("public_key").and_then(|x| x.as_str()))
                .unwrap_or("");
            let protected = auth::get_challenge(&state, pk).await?;
            Ok(json!({ "get_challenge": {
                "__typename": "GetChallengeOutput",
                "protected_challenge": protected,
            }}))
        }
        "VerifyChallenge" | "verify_challenge" => {
            let obj = input;
            let pk = str_of(&obj, "identity_public_key");
            let chal = str_of(&obj, "protected_challenge");
            let sig = str_of(&obj, "signature");
            let (token, valid_until) = auth::verify_challenge(&state, &pk, &chal, &sig).await?;
            Ok(json!({ "verify_challenge": {
                "__typename": "VerifyChallengeOutput",
                "session_token": token,
                "valid_until": valid_until.to_rfc3339(),
            }}))
        }
        // ---- fee estimates ----
        "LeavesSwapFeeEstimate" | "leaves_swap_fee_estimate" => {
            let fee = state.config.fee_flat_sats_swap;
            Ok(json!({ "leaves_swap_fee_estimate": {
                "fee_estimate": {
                    "original_value": fee,
                    "original_unit": "SATOSHI",
                }
            }}))
        }
        "LightningSendFeeEstimate" | "lightning_send_fee_estimate" => {
            let inv = str_of(&input, "encoded_invoice");
            let amt = opt_num(&input, "amount_sats");
            let msat = state.ldk.fee_estimate_msat(&inv, amt).await;
            Ok(json!({ "lightning_send_fee_estimate": {
                "fee_estimate": {
                    "original_value": msat / 1000,
                    "original_unit": "SATOSHI",
                }
            }}))
        }
        "CoopExitFeeEstimates" | "coop_exit_fee_estimates" => Ok(json!({
            "coop_exit_fee_estimates": {
                "slow": {"fee_sats": 500}, "medium": {"fee_sats": 1000}, "fast": {"fee_sats": 2000},
            }
        })),
        "CoopExitFeeQuote" | "coop_exit_fee_quote" => {
            let id = Uuid::new_v4().to_string();
            let sats = |v: u64| json!({"original_value": v, "original_unit": "SATOSHI"});
            Ok(json!({ "coop_exit_fee_quote": {
                "quote": {
                    "__typename": "CoopExitFeeQuote",
                    "id": id,
                    "created_at": now,
                    "updated_at": now,
                    "network": state.config.network,
                    "total_amount": sats(0),
                    "user_fee_fast": sats(2000),
                    "user_fee_medium": sats(1000),
                    "user_fee_slow": sats(500),
                    "l1_broadcast_fee_fast": sats(800),
                    "l1_broadcast_fee_medium": sats(500),
                    "l1_broadcast_fee_slow": sats(300),
                }
            }}))
        }
        // Paginated user-request history for the session wallet.
        "FetchCurrentUserToUserRequestsConnection"
        | "fetch_current_user_to_user_requests_connection" => {
            let _ = auth::require_session(&state, headers).await?;
            Ok(json!({ "current_user": {
                "user_requests": {
                    "__typename": "SparkWalletUserToUserRequestsConnection",
                    "count": 0,
                    "page_info": {
                        "__typename": "PageInfo",
                        "has_next_page": false,
                        "has_previous_page": false,
                        "start_cursor": null,
                        "end_cursor": null,
                    },
                    "entities": [],
                }
            }}))
        }
        // ---- lightning receive (quote is stateless+signed; receive persists request) ----
        "LightningReceiveQuote" | "lightning_receive_quote" => {
            let amount = num_of(&input, "amount_sats");
            let network = str_of(&input, "network");
            let network = if network.is_empty() {
                state.config.network.clone()
            } else {
                network
            };
            let transfer_id = Uuid::new_v4().to_string();
            // TODO(live): serialize a real TransferManifest proto (see
            // protos/spark + docs/LDK_GAPS.md). The SDK proto-decodes this on
            // receive, so LN-receive e2e needs the real manifest + key-signed
            // issuer_signature. Base64 JSON keeps the shape for now.
            let manifest = json!({
                "transfer_id": transfer_id,
                "amount_sats": amount,
                "network": network,
                "ssp_identity_pubkey": state.ssp_pubkey_hex,
            });
            let serialized = B64.encode(serde_json::to_vec(&manifest).unwrap());
            let sig = sign_with_ssp(&state, &serialized).await?;
            Ok(json!({ "lightning_receive_quote": {
                "issued_quote": {
                    "serialized_manifest": serialized,
                    "issuer_signature": sig,
                },
                "attribution_status": "NO_PARTNER_JWT",
            }}))
        }
        "RequestLightningReceive" | "request_lightning_receive" => {
            let owner = auth::require_session(&state, headers).await?;
            let amount = num_of(&input, "amount_sats");
            let hash = str_of(&input, "payment_hash");
            let memo = str_of(&input, "memo");
            let expiry = opt_num(&input, "expiry_secs").unwrap_or(86400) as u32;
            let inv = state
                .ldk
                .create_invoice(amount, &hash, &memo, expiry)
                .await
                .map_err(|e| format!("ldk create_invoice: {e}"))?;
            let rec = store_request(
                &state,
                "LIGHTNING_RECEIVE",
                &owner,
                &now,
                json!({"amount_sats": amount, "payment_hash": hash,
                       "invoice": inv.invoice, "network": state.config.network}),
            )
            .await?;
            let req_id = rec["id"].as_str().unwrap_or("").to_string();
            Ok(json!({ "request_lightning_receive": {
                "request": {
                    "__typename": "LightningReceiveRequest",
                    "id": req_id,
                    "created_at": now,
                    "updated_at": now,
                    "network": state.config.network,
                    "invoice": {
                        "__typename": "Invoice",
                        "encoded_invoice": inv.invoice,
                        "bitcoin_network": state.config.network,
                        "payment_hash": hash,
                        "amount": {"original_value": amount, "original_unit": "SATOSHI"},
                        "created_at": now,
                        "expires_at": null,
                        "memo": memo,
                    },
                    "status": "CREATED",
                }
            }}))
        }
        // ---- lightning send ----
        "RequestLightningSend" | "request_lightning_send" => {
            let owner = auth::require_session(&state, headers).await?;
            let inv = str_of(&input, "encoded_invoice");
            let amt = opt_num(&input, "amount_sats");
            let idem = str_of(&input, "idempotency_key");
            let ext_id = str_of(&input, "user_outbound_transfer_external_id");
            let pay = state.ldk.pay_invoice(&inv, amt).await;
            let rec = store_request(
                &state,
                "LIGHTNING_SEND",
                &owner,
                &now,
                json!({"encoded_invoice": inv, "amount_sats": amt,
                       "idempotency_key": idem,
                       "payment_id": pay.payment_id, "status": pay.status,
                       "network": state.config.network,
                       "user_outbound_transfer_external_id": ext_id}),
            )
            .await?;
            state
                .db
                .insert_transfer(
                    &ext_id_or_new(&ext_id),
                    rec["id"].as_str().unwrap_or(""),
                    "PREIMAGE_SWAP",
                    &pay.status,
                )
                .await?;
            // Send only inits: status stays INITIATED until SubscribeEvents
            // reports finality (see LdkBackend::apply_ln_event).
            Ok(json!({ "request_lightning_send": {
                "request": {
                    "__typename": "LightningSendRequest",
                    "id": rec["id"],
                    "created_at": now,
                    "updated_at": now,
                    "network": state.config.network,
                    "encoded_invoice": inv,
                    "fee": {"original_value": 0, "original_unit": "SATOSHI"},
                    "idempotency_key": idem,
                    "status": "LIGHTNING_PAYMENT_INITIATED",
                }
            }}))
        }
        // ---- swaps (SDK mutation name is RequestSwap / field request_swap) ----
        "RequestSwap" | "request_swap" => {
            let owner = auth::require_session(&state, headers).await?;
            let total = num_of(&input, "total_amount_sats");
            // The sidecar serves fills from exact leaves only. If IT needs a
            // swap, its ladder is depleted: fail fast so no leaves lock
            // SO-side in a recursive swap. Top up the ladder instead.
            if !state.config.sidecar_identity_pubkey.is_empty()
                && owner == state.config.sidecar_identity_pubkey
            {
                return Err("NEEDS_TOPUP: sidecar ladder depleted, top up liquidity".to_string());
            }
            if state.config.max_swap_total_sats > 0 && total > state.config.max_swap_total_sats {
                return Err(format!(
                    "swap total {total} exceeds operator cap {}",
                    state.config.max_swap_total_sats
                ));
            }
            let fee = num_of(&input, "fee_sats");
            let ext_id = str_of(&input, "user_outbound_transfer_external_id");
            let network = state.config.network.clone();
            // Target list (rc schema) or scalar (dated schema).
            let targets: Vec<u64> = match input.get("target_amount_sats") {
                Some(Value::Array(a)) => a.iter().filter_map(|e| e.as_u64()).collect(),
                Some(v) => v.as_u64().map(|t| vec![t]).unwrap_or_default(),
                None => vec![],
            };
            let target: u64 = targets.iter().sum();
            // Funded sidecar completes the swap with a real SO transfer.
            // Without it swapLeaves stays empty and the SDK rejects the swap.
            let (inbound_id, swap_leaves) =
                match fill_swap_via_sidecar(&state, &owner, &targets, total).await {
                    Ok(fill) => fill,
                    Err(e) => {
                        tracing::warn!("swap sidecar unavailable ({e}); returning unfillable stub");
                        (Uuid::new_v4().to_string(), vec![])
                    }
                };
            let rec = store_request(
                &state,
                "LEAVES_SWAP",
                &owner,
                &now,
                json!({"total_amount_sats": total, "target_amount_sats": target,
                       "inbound_transfer_spark_id": inbound_id}),
            )
            .await?;
            let rid = rec["id"].as_str().unwrap_or("").to_string();
            state
                .db
                .insert_transfer(&inbound_id, &rid, "COUNTER_SWAP", "CREATED")
                .await?;
            if !ext_id.is_empty() {
                state
                    .db
                    .insert_transfer(&ext_id, &rid, "TRANSFER", "CREATED")
                    .await?;
            }
            let field = "request_swap";
            let mut top = serde_json::Map::with_capacity(1);
            top.insert(
                field.to_string(),
                json!({
                "request": {
                    "__typename": "LeavesSwapRequest",
                    "id": rec["id"],
                    "created_at": now,
                    "updated_at": now,
                    "network": network,
                    "status": "CREATED",
                    "total_amount": {"original_value": total, "original_unit": "SATOSHI"},
                    "target_amount": {"original_value": target, "original_unit": "SATOSHI"},
                    "fee": {"original_value": fee, "original_unit": "SATOSHI"},
                    "inbound_transfer": {
                        "__typename": "Transfer",
                        "total_amount": {"original_value": total, "original_unit": "SATOSHI"},
                        "spark_id": inbound_id,
                        "user_request": {"id": rec["id"]},
                    },
                    "swap_leaves": swap_leaves,
                    "expires_at": null,
                }
                }),
            );
            Ok(Value::Object(top))
        }
        // ---- static deposits (SDK uses static_deposit_quote only) ----
        "StaticDepositQuote" | "static_deposit_quote" => {
            let _ = auth::require_session(&state, headers).await?;
            let txid = str_of(&input, "transaction_id");
            let vout = num_of(&input, "output_index");
            let network = str_of(&input, "network");
            let network = if network.is_empty() {
                state.config.network.clone()
            } else {
                network
            };
            // FAKE credit: fixed amount, must stay <= the real UTXO value or
            // the SO rejects the claim (totalAmount > utxo.Amount check).
            // TODO(live): look up the UTXO via bitcoind/esplora and apply fees.
            let credit: u64 = 100_000;
            let payload = format!("{txid}:{vout}:{credit}");
            let sig = sign_with_ssp(&state, &payload).await?;
            let quote = json!({
                "__typename": "StaticDepositQuoteOutput",
                "transaction_id": txid, "output_index": vout,
                "network": network,
                "credit_amount_sats": credit, "signature": sig,
            });
            Ok(json!({ "static_deposit_quote": quote }))
        }
        // SDK ClaimStaticDeposit mutation only (no fixed-amount variant in SspClient).
        "ClaimStaticDeposit" | "claim_static_deposit" => {
            let owner = auth::require_session(&state, headers).await?;
            store_request(&state, "CLAIM_STATIC_DEPOSIT", &owner, &now, input.clone()).await?;
            // ClaimStaticDepositOutputFragment selects only transfer_id.
            Ok(json!({ "claim_static_deposit": {
                "__typename": "ClaimStaticDepositOutput",
                "transfer_id": Uuid::new_v4().to_string(),
            }}))
        }
        "CreateInstantStaticDepositQuote" | "create_instant_static_deposit_quote" => {
            let _ = auth::require_session(&state, headers).await?;
            Ok(json!({ "create_instant_static_deposit_quote": {
                "quote": {"id": Uuid::new_v4().to_string(), "status": "CREATED"},
            }}))
        }
        "CreateClaimInstantStaticDeposit" | "create_claim_instant_static_deposit" => {
            let owner = auth::require_session(&state, headers).await?;
            let rec = store_request(
                &state,
                "CLAIM_INSTANT_STATIC_DEPOSIT",
                &owner,
                &now,
                input.clone(),
            )
            .await?;
            Ok(json!({ "create_claim_instant_static_deposit": {
                "claim": {"id": rec["id"], "status": "CREATED"},
            }}))
        }
        // ---- coop exit ----
        "RequestCoopExit" | "request_coop_exit" => {
            let owner = auth::require_session(&state, headers).await?;
            let exit_speed = str_of(&input, "exit_speed");
            let exit_speed = if exit_speed.is_empty() {
                "MEDIUM".to_string()
            } else {
                exit_speed
            };
            let rec = store_request(&state, "COOP_EXIT", &owner, &now, input.clone()).await?;
            let req_id = rec["id"].as_str().unwrap_or("").to_string();
            Ok(json!({ "request_coop_exit": {
                "request": {
                    "__typename": "CoopExitRequest",
                    "id": req_id,
                    "created_at": now,
                    "updated_at": now,
                    "network": state.config.network,
                    "fee": {"original_value": 1000, "original_unit": "SATOSHI"},
                    "l1_broadcast_fee": {"original_value": 500, "original_unit": "SATOSHI"},
                    "fee_quote": null,
                    "exit_speed": exit_speed,
                    "status": "CREATED",
                    "expires_at": null,
                    "raw_connector_transaction": "",
                    "raw_coop_exit_transaction": "",
                    "coop_exit_txid": format!("00{}", Uuid::new_v4().simple()),
                }
            }}))
        }
        "CompleteCoopExit" | "complete_coop_exit" => {
            let _ = auth::require_session(&state, headers).await?;
            Ok(json!({ "complete_coop_exit": {
                "request": {"id": str_of(&input, "user_outbound_transfer_external_id"),
                            "status": "COMPLETED"}
            }}))
        }
        // ---- reads ----
        // SDK Transfers query only. user_request is null for transfers the SSP
        // did not participate in (client reads userRequest?.__typename, null-safe).
        "Transfers" | "transfers" => {
            let _ = auth::require_session(&state, headers).await?;
            let ids = ids_of(&input, v);
            let rows = state.db.transfers_for(&ids).await?;
            let map_row = |t: &Value| {
                json!({
                    "__typename": "Transfer",
                    "total_amount": {"original_value": 0, "original_unit": "SATOSHI"},
                    "spark_id": t.get("spark_id").cloned().unwrap_or(Value::Null),
                    "user_request": null,
                })
            };
            let list: Vec<Value> = rows.iter().map(map_row).collect();
            Ok(json!({ "transfers": list }))
        }
        "UserRequest" | "user_request" => {
            let _ = auth::require_session(&state, headers).await?;
            let rid = str_of(&input, "request_id");
            let found = state.db.get_request(&rid).await?;
            match found {
                Some(rec) => Ok(json!({ "user_request": user_request_union(&state, &rec).await })),
                None => Ok(json!({ "user_request": null })),
            }
        }
        "WalletWebhooks" | "wallet_webhooks" | "ListSparkWalletWebhooks" => {
            Ok(json!({ "wallet_webhooks": { "webhooks": [] } }))
        }
        "RegisterWalletWebhook" | "register_wallet_webhook" => Ok(json!({
            "register_wallet_webhook": { "webhook_id": Uuid::new_v4().to_string() }
        })),
        "DeleteWalletWebhook" | "delete_wallet_webhook" => Ok(json!({
            "delete_wallet_webhook": { "success": true }
        })),
        _ => Err(format!("unsupported SSP operation: {op}")),
    }
}

/// Rewrite canonical (schema-named) response keys to the aliased names the
/// SDK's generated documents request (`alias: field`).
///
/// Real GraphQL servers return data keyed by alias; our resolvers return raw
/// schema names. This pass collects `alias: field` pairs from the query text
/// (argument lists stripped first so `name: $var` pairs don't pollute the map)
/// and copies `field` -> `alias` on every object that has `field`.
/// Extra keys are harmless: each `*FromJson` reads only its own aliases.
pub fn apply_query_aliases(data: &mut Value, query: &str) -> Vec<(String, String)> {
    let aliases = collect_aliases(query);
    apply_aliases_to_value(data, &aliases);
    aliases
}

fn collect_aliases(query: &str) -> Vec<(String, String)> {
    // Strip balanced (...) argument lists (they contain `name: value` pairs
    // that are NOT selection aliases).
    let mut stripped = String::with_capacity(query.len());
    let mut depth = 0usize;
    for ch in query.chars() {
        match ch {
            '(' => depth += 1,
            ')' if depth > 0 => depth -= 1,
            _ if depth == 0 => stripped.push(ch),
            _ => {}
        }
    }
    let mut out = Vec::new();
    let bytes = stripped.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        // match ident : ident
        if bytes[i].is_ascii_alphabetic() || bytes[i] == b'_' {
            let start = i;
            while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
                i += 1;
            }
            let name = &stripped[start..i];
            let mut j = i;
            while j < bytes.len() && bytes[j].is_ascii_whitespace() {
                j += 1;
            }
            if j < bytes.len() && bytes[j] == b':' {
                j += 1;
                while j < bytes.len() && bytes[j].is_ascii_whitespace() {
                    j += 1;
                }
                let fstart = j;
                if j < bytes.len() && (bytes[j].is_ascii_alphabetic() || bytes[j] == b'_') {
                    while j < bytes.len() && (bytes[j].is_ascii_alphanumeric() || bytes[j] == b'_')
                    {
                        j += 1;
                    }
                    let field = &stripped[fstart..j];
                    // skip `__typename` (bare, no alias) and fragment spreads
                    if name != "__typename" && name != field {
                        out.push((name.to_string(), field.to_string()));
                    }
                    i = j;
                    continue;
                }
            }
            continue;
        }
        i += 1;
    }
    out
}

fn apply_aliases_to_value(v: &mut Value, aliases: &[(String, String)]) {
    match v {
        Value::Object(map) => {
            for (alias, field) in aliases {
                if let Some(val) = map.get(field).cloned() {
                    if !map.contains_key(alias) {
                        map.insert(alias.clone(), val);
                    }
                }
            }
            for val in map.values_mut() {
                apply_aliases_to_value(val, aliases);
            }
        }
        Value::Array(arr) => {
            for val in arr.iter_mut() {
                apply_aliases_to_value(val, aliases);
            }
        }
        _ => {}
    }
}

/// Build the exact UserRequest union member for a stored request record.
/// Kinds map to GraphQL types: LIGHTNING_SEND->LightningSendRequest,
/// LIGHTNING_RECEIVE->LightningReceiveRequest, LEAVES_SWAP->LeavesSwapRequest,
/// COOP_EXIT->CoopExitRequest, CLAIM_STATIC_DEPOSIT->ClaimStaticDeposit.
/// Send status is refreshed from the payment tracker (event-driven).
async fn user_request_union(state: &AppState, rec: &Value) -> Value {
    let kind = rec.get("type").and_then(|v| v.as_str()).unwrap_or("");
    let id = rec.get("id").cloned().unwrap_or(Value::Null);
    let created = rec
        .get("created_at")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let p = rec.get("payload").cloned().unwrap_or(json!({}));
    let net = p
        .get("network")
        .and_then(|v| v.as_str())
        .unwrap_or(&state.config.network)
        .to_string();
    let sats = |v: u64| json!({"original_value": v, "original_unit": "SATOSHI"});
    match kind {
        "LIGHTNING_SEND" => {
            let pid = p.get("payment_id").and_then(|v| v.as_str()).unwrap_or("");
            let status = match state.ldk.payment_status(pid).await.as_str() {
                "SUCCEEDED" => "LIGHTNING_PAYMENT_SUCCEEDED",
                "FAILED" => "LIGHTNING_PAYMENT_FAILED",
                _ => "LIGHTNING_PAYMENT_INITIATED",
            };
            json!({
                "__typename": "LightningSendRequest",
                "id": id, "created_at": created, "updated_at": created,
                "network": net,
                "encoded_invoice": p.get("encoded_invoice").cloned().unwrap_or(Value::Null),
                "fee": sats(0),
                "idempotency_key": p.get("idempotency_key").cloned().unwrap_or(Value::Null),
                "status": status,
            })
        }
        "LIGHTNING_RECEIVE" => {
            let amount = p.get("amount_sats").and_then(|v| v.as_u64()).unwrap_or(0);
            json!({
                "__typename": "LightningReceiveRequest",
                "id": id, "created_at": created, "updated_at": created,
                "network": net,
                "invoice": {
                    "__typename": "Invoice",
                    "encoded_invoice": p.get("invoice").cloned().unwrap_or(Value::Null),
                    "bitcoin_network": net,
                    "payment_hash": p.get("payment_hash").cloned().unwrap_or(Value::Null),
                    "amount": sats(amount),
                    "created_at": created, "expires_at": null, "memo": null,
                },
                "status": "CREATED",
            })
        }
        "LEAVES_SWAP" => {
            let total = p
                .get("total_amount_sats")
                .and_then(|v| v.as_u64())
                .unwrap_or(0);
            let target = p
                .get("target_amount_sats")
                .and_then(|v| v.as_u64())
                .unwrap_or(0);
            let inbound = p
                .get("inbound_transfer_spark_id")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            json!({
                "__typename": "LeavesSwapRequest",
                "id": id, "created_at": created, "updated_at": created,
                "network": net, "status": "CREATED",
                "total_amount": sats(total), "target_amount": sats(target),
                "fee": sats(0),
                "inbound_transfer": {
                    "__typename": "Transfer",
                    "total_amount": sats(total),
                    "spark_id": inbound,
                    "user_request": {"id": id},
                },
                "swap_leaves": [], "expires_at": null,
            })
        }
        "COOP_EXIT" => json!({
            "__typename": "CoopExitRequest",
            "id": id, "created_at": created, "updated_at": created,
            "network": net,
            "fee": sats(1000), "l1_broadcast_fee": sats(500),
            "fee_quote": null, "exit_speed": "MEDIUM", "status": "CREATED",
            "expires_at": null,
            "raw_connector_transaction": "", "raw_coop_exit_transaction": "",
            "coop_exit_txid": null,
        }),
        "CLAIM_STATIC_DEPOSIT" => json!({
            "__typename": "ClaimStaticDeposit",
            "id": id, "created_at": created, "updated_at": created,
            "network": net,
            "credit_amount": sats(p.get("credit_amount_sats").and_then(|v| v.as_u64()).unwrap_or(0)),
            "max_fee": null, "status": "CREATED",
            "transaction_id": p.get("transaction_id").cloned().unwrap_or(Value::Null),
            "output_index": p.get("output_index").cloned().unwrap_or(Value::Null),
            "bitcoin_network": net, "transfer_spark_id": null,
        }),
        _ => Value::Null,
    }
}

/// Insert a user-request row into sqlite and return the record shape that
/// `user_request_union` reads: {id, type, created_at, payload}.
async fn store_request(
    state: &AppState,
    kind: &str,
    owner: &str,
    now: &str,
    payload: Value,
) -> Result<Value, String> {
    let id = Uuid::new_v4().to_string();
    state
        .db
        .insert_request(&id, kind, owner, now, &payload)
        .await?;
    Ok(json!({
        "id": id, "type": kind,
        "owner_identity_pubkey": owner, "created_at": now,
        "payload": payload,
    }))
}

/// Ask the funded sidecar wallet to send SUM(targets) to the session owner.
/// Returns (real SO inbound transfer id, swap leaves). The SDK only
/// null-checks swapLeaves; the user claims the inbound transfer from the SO.
async fn fill_swap_via_sidecar(
    state: &AppState,
    owner_identity_pubkey: &str,
    targets: &[u64],
    total_amount_sats: u64,
) -> Result<(String, Vec<Value>), String> {
    if state.config.sidecar_url.is_empty() {
        return Err("SIDECAR_URL unset".to_string());
    }
    if targets.is_empty() {
        return Err("no targets".to_string());
    }
    let client = reqwest::Client::builder()
        .build()
        .map_err(|e| e.to_string())?;
    let mut req = client
        .post(format!("{}/swap-fill", state.config.sidecar_url))
        .json(&serde_json::json!({
            "ownerIdentityPubkey": owner_identity_pubkey,
            "targetAmountsSats": targets,
            "totalAmountSats": total_amount_sats,
            "idempotencyKey": Uuid::new_v4().to_string(),
        }));
    if !state.config.sidecar_token.is_empty() {
        req = req.bearer_auth(&state.config.sidecar_token);
    }
    let fill: Value = req
        .send()
        .await
        .map_err(|e| e.to_string())?
        .json()
        .await
        .map_err(|e| e.to_string())?;
    if let Some(err) = fill.get("error").and_then(|e| e.as_str()) {
        return Err(err.to_string());
    }
    let inbound = fill
        .get("inboundTransferSparkId")
        .and_then(|v| v.as_str())
        .ok_or("sidecar: missing inboundTransferSparkId")?
        .to_string();
    let leaves = fill
        .get("swapLeaves")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .map(|l| {
            json!({
                "leaf_id": l.get("leaf_id").cloned().unwrap_or(Value::Null),
                "raw_unsigned_refund_transaction": "",
                "adaptor_signed_signature": "",
                "direct_raw_unsigned_refund_transaction": "",
                "direct_adaptor_signed_signature": "",
                "direct_from_cpfp_raw_unsigned_refund_transaction": "",
                "direct_from_cpfp_adaptor_signed_signature": "",
            })
        })
        .collect();
    Ok((inbound, leaves))
}

fn str_of(v: &Value, k: &str) -> String {
    v.get(k)
        .and_then(|x| {
            if let Some(s) = x.as_str() {
                Some(s.to_string())
            } else {
                Some(x.to_string().trim_matches('"').to_string())
            }
        })
        .unwrap_or_default()
}
fn num_of(v: &Value, k: &str) -> u64 {
    v.get(k)
        .and_then(|x| x.as_u64().or_else(|| x.as_str()?.parse().ok()))
        .unwrap_or(0)
}
fn opt_num(v: &Value, k: &str) -> Option<u64> {
    v.get(k)
        .and_then(|x| x.as_u64().or_else(|| x.as_str()?.parse().ok()))
}
fn ids_of(input: &Value, root: &Value) -> Vec<String> {
    for v in [input, root] {
        if let Some(a) = v.get("transfer_spark_ids").and_then(|x| x.as_array()) {
            return a
                .iter()
                .filter_map(|x| x.as_str().map(|s| s.to_string()))
                .collect();
        }
        if let Some(a) = v.get("transferSparkIds").and_then(|x| x.as_array()) {
            return a
                .iter()
                .filter_map(|x| x.as_str().map(|s| s.to_string()))
                .collect();
        }
        if let Some(s) = v.get("transfer_spark_id").and_then(|x| x.as_str()) {
            return vec![s.to_string()];
        }
    }
    vec![]
}
fn ext_id_or_new(ext: &str) -> String {
    if ext.is_empty() {
        Uuid::new_v4().to_string()
    } else {
        ext.to_string()
    }
}

/// SSP signature: ECDSA (DER, hex) over sha256(message) with the SSP identity
/// key. When a swap sidecar is configured, the sidecar wallet OWNS the SSP
/// identity and signs via its public wallet API (one consistent identity for
/// receiving swap outbounds and signing quotes). Otherwise the resolved local
/// key signs.
async fn sign_with_ssp(state: &AppState, message: &str) -> Result<String, String> {
    if !state.config.sidecar_url.is_empty() {
        return sign_via_sidecar(state, message).await;
    }
    use secp256k1::{Message, Secp256k1, SecretKey};
    let digest = Sha256::digest(message.as_bytes());
    let secret = SecretKey::from_slice(
        &hex::decode(state.ssp_secret_hex.trim()).map_err(|e| e.to_string())?,
    )
    .map_err(|e| e.to_string())?;
    let secp = Secp256k1::new();
    let sig = secp.sign_ecdsa(&Message::from_digest(*digest.as_ref()), &secret);
    Ok(hex::encode(sig.serialize_der()))
}

async fn sign_via_sidecar(state: &AppState, message: &str) -> Result<String, String> {
    let mut req = reqwest::Client::new()
        .post(format!("{}/sign", state.config.sidecar_url))
        .json(&serde_json::json!({ "message": message }));
    if !state.config.sidecar_token.is_empty() {
        req = req.bearer_auth(&state.config.sidecar_token);
    }
    let resp: Value = req
        .send()
        .await
        .map_err(|e| e.to_string())?
        .json()
        .await
        .map_err(|e| e.to_string())?;
    resp.get("signature")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .ok_or_else(|| {
            format!(
                "sidecar sign failed: {}",
                resp.get("error")
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown")
            )
        })
}
