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
            let msat = crate::backend(&state)
                .await
                .fee_estimate_msat(&inv, amt)
                .await;
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
            let _ = auth::require_session(&state, headers).await?;
            let amount = num_of(&input, "amount_sats");
            validate_sats(amount)?;
            let network = str_of(&input, "network");
            let network = if network.is_empty() {
                state.config.network.clone()
            } else {
                network
            };
            validate_network(&state, &network)?;
            let transfer_id = Uuid::new_v4().to_string();
            // The SDK quote flow needs a protobuf TransferManifest. This JSON
            // manifest keeps the GraphQL response shape but is not accepted as
            // a production receive quote.
            let manifest = json!({
                "transfer_id": transfer_id,
                "amount_sats": amount,
                "network": network,
                "ssp_identity_pubkey": crate::ssp_identity(&state).await?,
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
            validate_sats(amount)?;
            let requested_network = str_of(&input, "network");
            if !requested_network.is_empty() {
                validate_network(&state, &requested_network)?;
            }
            let hash = str_of(&input, "payment_hash").to_lowercase();
            if hash.len() != 64 || !hash.bytes().all(|b| b.is_ascii_hexdigit()) {
                return Err("payment_hash must be 32 bytes hex".to_string());
            }
            let memo = str_of(&input, "memo");
            let expiry = u32::try_from(opt_num(&input, "expiry_secs").unwrap_or(86_400))
                .map_err(|_| "expiry_secs is out of range".to_string())?;
            if expiry == 0 {
                return Err("expiry_secs must be positive".to_string());
            }
            let inv = crate::backend(&state)
                .await
                .create_invoice(amount, &hash, &memo, expiry)
                .await
                .map_err(|e| format!("ldk create_invoice: {e}"))?;
            // SSP-owned preimage (minted via mint_preimage): split into FROST
            // shares, encrypt per operator, store via the coordinator with
            // owner = SSP identity (attestor == holder, SO's own rule).
            // Wallet-owned hashes skip this (preimage unknown here).
            if state
                .db
                .get_preimage_for_owner(&hash, &owner)
                .await?
                .is_some()
            {
                if let Err(e) = store_preimage_shares(&state, &hash, &inv.invoice).await {
                    let _ = crate::backend(&state).await.fail_hold(&hash).await;
                    return Err(e);
                }
            }
            let rec = store_request(
                &state,
                "LIGHTNING_RECEIVE",
                &owner,
                &now,
                json!({"amount_sats": amount, "payment_hash": hash,
                       "invoice": inv.invoice, "network": state.config.network,
                       "expiry_secs": expiry}),
                None,
            )
            .await?;
            state
                .db
                .set_receive_status(&hash, "INVOICE_CREATED")
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
                    "status": "INVOICE_CREATED",
                    "transfer": null,
                    "receiver_identity_public_key": owner,
                }
            }}))
        }
        // ---- lightning send ----
        "RequestLightningSend" | "request_lightning_send" => {
            let owner = auth::require_session(&state, headers).await?;
            let inv = str_of(&input, "encoded_invoice");
            let amt = opt_num(&input, "amount_sats");
            let ext_id = str_of(&input, "user_outbound_transfer_external_id");
            if ext_id.is_empty() {
                return Err("user_outbound_transfer_external_id is required".to_string());
            }
            let explicit_idem = str_of(&input, "idempotency_key");
            let idem = if explicit_idem.is_empty() {
                ext_id.clone()
            } else {
                explicit_idem
            };
            let _send_guard = state.send_lock.lock().await;
            // Idempotency: a retry with the same key returns the stored
            // request (with live status) instead of paying twice. The lock
            // makes the lookup and payment one process-local critical section.
            if let Some(rec) = state.db.find_by_idempotency(&owner, &idem).await? {
                let payload = rec.get("payload").cloned().unwrap_or(Value::Null);
                if payload.get("encoded_invoice").and_then(Value::as_str) != Some(inv.as_str())
                    || payload
                        .get("user_outbound_transfer_external_id")
                        .and_then(Value::as_str)
                        != Some(ext_id.as_str())
                    || payload.get("amount_sats").and_then(Value::as_u64) != amt
                {
                    return Err("idempotency key was already used for another payment".to_string());
                }
                return send_response_from_record(&state, &rec, &now).await;
            }
            crate::backend(&state)
                .await
                .verify_lightning_send_funding(&owner, &ext_id, &inv, amt)
                .await?;
            let pay = crate::backend(&state).await.pay_invoice(&inv, amt).await;
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
                Some(idem.as_str()),
            )
            .await?;
            state
                .db
                .insert_transfer(
                    &ext_id_or_new(&ext_id),
                    rec["id"].as_str().unwrap_or(""),
                    "PREIMAGE_SWAP",
                    &pay.status,
                    &owner,
                )
                .await?;
            return send_response_from_record(&state, &rec, &now).await;
        }
        // ---- swaps (SDK mutation name is RequestSwap / field request_swap) ----
        "RequestSwap" | "request_swap" => {
            let owner = auth::require_session(&state, headers).await?;
            let total = num_of(&input, "total_amount_sats");
            if total == 0 {
                return Err("swap total must be positive".to_string());
            }
            // The embedded wallet serves fills from exact leaves only. If it
            // needs a swap, its ladder is depleted. Fail before a recursive
            // swap can lock SSP leaves on the operators.
            if let Ok(resolved) = crate::ssp_identity(&state).await {
                if !resolved.is_empty() && owner == resolved {
                    return Err("NEEDS_TOPUP: SSP ladder depleted, top up liquidity".to_string());
                }
            }
            if state.config.max_swap_total_sats > 0 && total > state.config.max_swap_total_sats {
                return Err(format!(
                    "swap total {total} exceeds operator cap {}",
                    state.config.max_swap_total_sats
                ));
            }
            // Fee is server-side (what leaves_swap_fee_estimate quotes);
            // client input is ignored so a forged fee changes nothing.
            let fee = state.config.fee_flat_sats_swap;
            let ext_id = str_of(&input, "user_outbound_transfer_external_id");
            if ext_id.is_empty() {
                return Err("user_outbound_transfer_external_id is required".to_string());
            }
            let adaptor_pubkey = str_of(&input, "adaptor_pubkey");
            if adaptor_pubkey.len() != 66
                || !adaptor_pubkey.bytes().all(|byte| byte.is_ascii_hexdigit())
            {
                return Err("adaptor_pubkey must be a compressed public key".to_string());
            }
            let network = state.config.network.clone();
            // Target list (rc schema) or scalar (dated schema).
            let targets: Vec<u64> = match input.get("target_amount_sats") {
                Some(Value::Array(a)) => a.iter().filter_map(|e| e.as_u64()).collect(),
                Some(v) => v.as_u64().map(|t| vec![t]).unwrap_or_default(),
                None => vec![],
            };
            let target = targets.iter().try_fold(0u64, |sum, value| {
                sum.checked_add(*value)
                    .ok_or_else(|| "target amount overflow".to_string())
            })?;
            let payout_total = total
                .checked_sub(fee)
                .ok_or_else(|| "swap fee exceeds total".to_string())?;
            if target > payout_total {
                return Err("target amounts plus fee exceed swap total".to_string());
            }
            let fill = state
                .spark
                .fill_swap(
                    &owner,
                    &ext_id,
                    &adaptor_pubkey,
                    &targets,
                    total,
                    payout_total,
                )
                .await?;
            let inbound_id = fill.transfer_id;
            let swap_leaves = fill
                .leaf_ids
                .into_iter()
                .map(|leaf_id| {
                    json!({
                        "leaf_id": leaf_id,
                        "raw_unsigned_refund_transaction": "",
                        "adaptor_signed_signature": "",
                        "direct_raw_unsigned_refund_transaction": "",
                        "direct_adaptor_signed_signature": "",
                        "direct_from_cpfp_raw_unsigned_refund_transaction": "",
                        "direct_from_cpfp_adaptor_signed_signature": "",
                    })
                })
                .collect::<Vec<_>>();
            let rec = store_request(
                &state,
                "LEAVES_SWAP",
                &owner,
                &now,
                json!({"total_amount_sats": total, "target_amount_sats": target,
                       "fee_sats": fee,
                       "inbound_transfer_spark_id": inbound_id}),
                None,
            )
            .await?;
            let rid = rec["id"].as_str().unwrap_or("").to_string();
            state
                .db
                .insert_transfer(&inbound_id, &rid, "COUNTER_SWAP", "CREATED", &owner)
                .await?;
            if !ext_id.is_empty() {
                state
                    .db
                    .insert_transfer(&ext_id, &rid, "TRANSFER", "CREATED", &owner)
                    .await?;
            }
            Ok(json!({ "request_swap": {
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
            }}))
        }
        // ---- static deposits (SDK uses static_deposit_quote only) ----
        "StaticDepositQuote" | "static_deposit_quote" => {
            let _ = auth::require_session(&state, headers).await?;
            // Quote signing without a UTXO lookup is only acceptable where
            // coins are worthless. Refuse elsewhere rather than signing blind.
            let network = str_of(&input, "network");
            let network = if network.is_empty() {
                state.config.network.clone()
            } else {
                network
            };
            validate_network(&state, &network)?;
            if state.config.network != "REGTEST" && state.config.network != "LOCAL" {
                return Err("static deposit quotes are regtest-only".to_string());
            }
            let txid = str_of(&input, "transaction_id").to_lowercase();
            if txid.len() != 64 || !txid.bytes().all(|b| b.is_ascii_hexdigit()) {
                return Err("transaction_id must be 64 hex chars".to_string());
            }
            let vout = num_of(&input, "output_index");
            if vout > u32::MAX as u64 {
                return Err("output_index out of range".to_string());
            }
            // FAKE credit: fixed amount, must stay <= the real UTXO value or
            // the SO rejects the claim (totalAmount > utxo.Amount check).
            // TODO(live): look up the UTXO via bitcoind/esplora and apply fees.
            let credit: u64 = 100_000;
            let payload = format!("{txid}:{vout}:{credit}");
            let proposed_signature = sign_with_ssp(&state, &payload).await?;
            let sig = state
                .db
                .record_static_quote(&txid, vout as u32, credit, &proposed_signature, &now)
                .await?
                .ok_or_else(|| "static deposit output was already claimed".to_string())?;
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
            let txid = str_of(&input, "transaction_id").to_lowercase();
            let vout = num_of(&input, "output_index");
            let quote_signature = str_of(&input, "quote_signature");
            if txid.len() != 64 || !txid.bytes().all(|b| b.is_ascii_hexdigit()) {
                return Err("transaction_id must be 64 hex chars".to_string());
            }
            if vout > u32::MAX as u64 {
                return Err("output_index out of range".to_string());
            }
            if !state
                .db
                .consume_static_quote(&txid, vout as u32, &quote_signature)
                .await?
            {
                return Err("unknown, mismatched, or already claimed quote".to_string());
            }
            store_request(
                &state,
                "CLAIM_STATIC_DEPOSIT",
                &owner,
                &now,
                input.clone(),
                None,
            )
            .await?;
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
                None,
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
            let coop_exit_txid = format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple());
            let mut payload = input.clone();
            if let Some(object) = payload.as_object_mut() {
                object.insert(
                    "coop_exit_txid".to_string(),
                    Value::String(coop_exit_txid.clone()),
                );
                object.insert("exit_speed".to_string(), Value::String(exit_speed.clone()));
            }
            let rec = store_request(&state, "COOP_EXIT", &owner, &now, payload, None).await?;
            let req_id = rec["id"].as_str().unwrap_or("").to_string();
            let ext_id = str_of(&input, "user_outbound_transfer_external_id");
            if !ext_id.is_empty() {
                state
                    .db
                    .insert_transfer(&ext_id, &req_id, "COOP_EXIT", "CREATED", &owner)
                    .await?;
            }
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
                    "coop_exit_txid": coop_exit_txid,
                }
            }}))
        }
        "CompleteCoopExit" | "complete_coop_exit" => {
            let owner = auth::require_session(&state, headers).await?;
            let transfer_id = str_of(&input, "user_outbound_transfer_external_id");
            let request_id = state
                .db
                .request_id_for_transfer(&transfer_id, &owner)
                .await?
                .ok_or_else(|| "cooperative exit request not found".to_string())?;
            state
                .db
                .insert_transfer(&transfer_id, &request_id, "COOP_EXIT", "COMPLETED", &owner)
                .await?;
            Ok(json!({ "complete_coop_exit": {
                "request": {"id": request_id,
                            "status": "COMPLETED"}
            }}))
        }
        // ---- preimage mint + reveal (SSP extensions, not in stock SDK) ----
        // SSP-owned preimage model: the wallet mints a hash here FIRST, uses
        // it in createLightningHodlInvoice, and the SSP holds the preimage
        // before payment (compliant: attestor == holder). On LN arrival the
        // SSP auto-claims; cooperative wallets can also reveal explicitly.
        "MintInvoicePreimage" | "mint_invoice_preimage" => {
            let owner = auth::require_session(&state, headers).await?;
            let preimage: [u8; 32] = rand::random();
            let payment_hash = hex::encode(Sha256::digest(preimage));
            state
                .db
                .save_preimage(&payment_hash, &hex::encode(preimage), &owner, &now)
                .await?;
            Ok(json!({ "mint_invoice_preimage": {
                "__typename": "MintInvoicePreimageOutput",
                "payment_hash": payment_hash,
            }}))
        }
        "RevealPreimage" | "reveal_preimage" => {
            let owner = auth::require_session(&state, headers).await?;
            let hash = str_of(&input, "payment_hash").to_lowercase();
            let preimage = str_of(&input, "preimage").to_lowercase();
            if !state.db.has_receive_request(&hash, &owner).await? {
                return Err("no matching lightning receive request".to_string());
            }
            let claimed = crate::backend(&state)
                .await
                .reveal_and_claim(&hash, &preimage)
                .await;
            Ok(json!({ "reveal_preimage": {
                "__typename": "RevealPreimageOutput",
                "ok": claimed,
                "claimed": claimed,
            }}))
        }
        // ---- reads ----
        // SDK Transfers query only. All rows here were created by this SSP, so
        // keep the transfer-to-request join intact.
        "Transfers" | "transfers" => {
            let owner = auth::require_session(&state, headers).await?;
            let ids = ids_of(&input, v);
            let rows = state.db.transfers_for(&ids, &owner).await?;
            let map_row = |t: &Value| {
                let request_typename = match t.get("type").and_then(Value::as_str) {
                    Some("PREIMAGE_SWAP") => "LightningSendRequest",
                    Some("COOP_EXIT") => "CoopExitRequest",
                    _ => "LeavesSwapRequest",
                };
                json!({
                    "__typename": "Transfer",
                    "total_amount": {
                        "original_value": t.get("total_amount_sats").and_then(Value::as_u64).unwrap_or(0),
                        "original_unit": "SATOSHI"
                    },
                    "spark_id": t.get("spark_id").cloned().unwrap_or(Value::Null),
                    "user_request": {
                        "__typename": request_typename,
                        "id": t.get("user_request_id").cloned().unwrap_or(Value::Null),
                    },
                })
            };
            let list: Vec<Value> = rows.iter().map(map_row).collect();
            Ok(json!({ "transfers": list }))
        }
        "UserRequest" | "user_request" => {
            let owner = auth::require_session(&state, headers).await?;
            let rid = str_of(&input, "request_id");
            let found = state.db.get_request(&rid, &owner).await?;
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
pub fn apply_query_aliases(data: &mut Value, query: &str) {
    let aliases = collect_aliases(query);
    apply_aliases_to_value(data, &aliases);
}

fn collect_aliases(query: &str) -> Vec<(String, String)> {
    const MAX_ALIASES: usize = 2000;
    // Strip balanced (...) argument lists (they contain `name: value` pairs
    // that are NOT selection aliases). Track string literals and `#`
    // comments so their contents never contribute pairs.
    let mut stripped = String::with_capacity(query.len());
    let mut depth = 0usize;
    let mut in_string = false;
    let mut in_comment = false;
    let mut prev_backslash = false;
    for ch in query.chars() {
        if in_comment {
            if ch == '\n' {
                in_comment = false;
                stripped.push(ch);
            }
            continue;
        }
        if in_string {
            if ch == '"' && !prev_backslash {
                in_string = false;
            }
            prev_backslash = ch == '\\' && !prev_backslash;
            continue;
        }
        match ch {
            '#' if depth == 0 => in_comment = true,
            '"' if depth == 0 => in_string = true,
            '(' => depth += 1,
            ')' if depth > 0 => depth -= 1,
            _ if depth == 0 => stripped.push(ch),
            _ => {}
        }
        if ch != '\\' {
            prev_backslash = false;
        }
    }
    let mut out = Vec::new();
    let mut seen = std::collections::HashSet::new();
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
                    if name != "__typename"
                        && name != field
                        && !name.starts_with("...")
                        && seen.insert((name.to_string(), field.to_string()))
                    {
                        out.push((name.to_string(), field.to_string()));
                        if out.len() >= MAX_ALIASES {
                            break;
                        }
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
            let status = match crate::backend(state)
                .await
                .payment_status(pid)
                .await
                .as_str()
            {
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
            let payment_hash = p.get("payment_hash").and_then(|v| v.as_str()).unwrap_or("");
            let request_id = rec.get("id").and_then(Value::as_str).unwrap_or("");
            let owner = rec
                .get("owner_identity_pubkey")
                .and_then(Value::as_str)
                .unwrap_or("");
            let status = state
                .db
                .receive_status(payment_hash)
                .await
                .unwrap_or_else(|_| "INVOICE_CREATED".to_string());
            let transfer = state
                .db
                .transfer_for_request(request_id, owner)
                .await
                .ok()
                .flatten()
                .map(|spark_id| {
                    json!({
                        "__typename": "Transfer",
                        "total_amount": sats(amount),
                        "spark_id": spark_id,
                        "user_request": {"id": request_id},
                    })
                });
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
                "status": status,
                "transfer": transfer,
                "receiver_identity_public_key": owner,
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
            let fee = p.get("fee_sats").and_then(|v| v.as_u64()).unwrap_or(0);
            let inbound = p
                .get("inbound_transfer_spark_id")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            json!({
                "__typename": "LeavesSwapRequest",
                "id": id, "created_at": created, "updated_at": created,
                "network": net, "status": "CREATED",
                "total_amount": sats(total), "target_amount": sats(target),
                "fee": sats(fee),
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
            "fee_quote": null,
            "exit_speed": p.get("exit_speed").and_then(Value::as_str).unwrap_or("MEDIUM"),
            "status": "CREATED",
            "expires_at": null,
            "raw_connector_transaction": "", "raw_coop_exit_transaction": "",
            "coop_exit_txid": p.get("coop_exit_txid").cloned().unwrap_or(Value::Null),
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

/// Build the request_lightning_send response from a stored LIGHTNING_SEND
/// record, refreshing status from the payment tracker (M4 idempotent replay
/// shares this with the fresh-send path).
async fn send_response_from_record(
    state: &AppState,
    rec: &Value,
    now: &str,
) -> Result<Value, String> {
    // Send only inits: status stays INITIATED until SubscribeEvents
    // reports finality (see LdkBackend::apply_ln_event).
    let p = rec.get("payload").cloned().unwrap_or(Value::Null);
    let pid = p.get("payment_id").and_then(|v| v.as_str()).unwrap_or("");
    let live = crate::backend(state).await.payment_status(pid).await;
    let status = match live.as_str() {
        "SUCCEEDED" => "LIGHTNING_PAYMENT_SUCCEEDED",
        "FAILED" => "LIGHTNING_PAYMENT_FAILED",
        _ => "LIGHTNING_PAYMENT_INITIATED",
    };
    Ok(json!({ "request_lightning_send": {
        "request": {
            "__typename": "LightningSendRequest",
            "id": rec["id"],
            "created_at": rec.get("created_at").cloned().unwrap_or(Value::Null),
            "updated_at": now,
            "network": state.config.network,
            "encoded_invoice": p.get("encoded_invoice").cloned().unwrap_or(Value::Null),
            "fee": {"original_value": 0, "original_unit": "SATOSHI"},
            "idempotency_key": p.get("idempotency_key").cloned().unwrap_or(Value::Null),
            "status": status,
        }
    }}))
}

/// Insert a user-request row into sqlite and return the record shape that
/// `user_request_union` reads: {id, type, created_at, payload}.
async fn store_request(
    state: &AppState,
    kind: &str,
    owner: &str,
    now: &str,
    payload: Value,
    idempotency_key: Option<&str>,
) -> Result<Value, String> {
    let id = Uuid::new_v4().to_string();
    state
        .db
        .insert_request(&id, kind, owner, now, &payload, idempotency_key)
        .await?;
    Ok(json!({
        "id": id, "type": kind,
        "owner_identity_pubkey": owner, "created_at": now,
        "payload": payload,
    }))
}

/// Split the SSP-held preimage for `payment_hash` into FROST shares, ECIES
/// each to its operator, and store via the embedded wallet's coordinator session
/// (same `store_preimage_share_v2` call a wallet makes; owner = SSP).
async fn store_preimage_shares(
    state: &AppState,
    payment_hash_hex: &str,
    invoice: &str,
) -> Result<(), String> {
    use crate::frost;
    #[derive(serde::Deserialize)]
    struct Operator {
        id: u32,
        identifier: String,
        #[serde(rename = "identityPublicKey")]
        identity_public_key: String,
    }
    let mut operators: Vec<Operator> = if state.config.frost_operators_json.trim().is_empty() {
        Vec::new()
    } else {
        serde_json::from_str(&state.config.frost_operators_json)
            .map_err(|_| "SSP_FROST_OPERATORS is invalid".to_string())?
    };
    if operators.is_empty() {
        return Ok(());
    }
    operators.sort_by_key(|o| o.id);
    let preimage_hex = crate::backend(state)
        .await
        .preimage_for(payment_hash_hex)
        .await
        .ok_or_else(|| "preimage not held for hash".to_string())?;
    let preimage = hex::decode(preimage_hex).map_err(|e| e.to_string())?;
    let shares =
        frost::split_secret_with_proofs(&preimage, state.config.frost_threshold, operators.len())?;
    let mut wire = std::collections::HashMap::with_capacity(operators.len());
    for (op, share) in operators.iter().zip(shares.iter()) {
        let expected_identifier = format!("{:064x}", share.index);
        if share.index != op.id + 1 || op.identifier.to_lowercase() != expected_identifier {
            return Err(format!(
                "FROST operator {} does not match share index {}",
                op.id, share.index
            ));
        }
        // Self-check before sending: a bad share would fail SO-side.
        frost::validate_share(&share.share, share.index, &share.proofs)?;
        let proto = frost::encode_secret_share_proto(&share.share, &share.proofs);
        let enc = frost::encrypt_share_to_operator(&proto, &op.identity_public_key)?;
        wire.insert(op.identifier.clone(), enc);
    }
    state
        .spark
        .store_preimage_shares(
            hex::decode(payment_hash_hex).map_err(|e| e.to_string())?,
            wire,
            state.config.frost_threshold as u32,
            invoice.to_string(),
        )
        .await
}

fn str_of(v: &Value, k: &str) -> String {
    match v.get(k) {
        Some(Value::String(value)) => value.clone(),
        Some(Value::Number(value)) => value.to_string(),
        Some(Value::Bool(value)) => value.to_string(),
        _ => String::new(),
    }
}

fn validate_sats(amount: u64) -> Result<(), String> {
    if amount == 0 {
        return Err("amount_sats must be positive".to_string());
    }
    amount
        .checked_mul(1000)
        .map(|_| ())
        .ok_or_else(|| "amount_sats is too large".to_string())
}

fn validate_network(state: &AppState, requested: &str) -> Result<(), String> {
    if requested == state.config.network {
        Ok(())
    } else {
        Err(format!(
            "network mismatch: configured {}, requested {requested}",
            state.config.network
        ))
    }
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

/// SSP signature with the identity key of the embedded Spark wallet.
async fn sign_with_ssp(state: &AppState, message: &str) -> Result<String, String> {
    state.spark.sign_message(message).await
}

#[cfg(test)]
mod tests {
    use super::str_of;
    use serde_json::json;

    #[test]
    fn string_input_does_not_turn_null_into_text() {
        let input = json!({
            "missing": null,
            "object": {"unexpected": true},
            "string": "value",
            "number": 42,
        });

        assert_eq!(str_of(&input, "missing"), "");
        assert_eq!(str_of(&input, "object"), "");
        assert_eq!(str_of(&input, "string"), "value");
        assert_eq!(str_of(&input, "number"), "42");
    }
}
