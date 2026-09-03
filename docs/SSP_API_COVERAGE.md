# SSP API coverage (exactly the ops `SspClient` calls, nothing else)

Endpoint paths: `POST /graphql/spark/rc`, `POST /graphql/spark/2025-03-19`,
alias `POST /graphql`. Auth: `GetChallenge`/`VerifyChallenge` open;
all other ops need `Authorization: Bearer <session>`.
Bodies >1024 bytes arrive deflate-compressed (SDK Requester behavior) and are decoded.
Responses use raw schema names + a per-query `alias: field` rewrite, because the
SDK's generated fragments read aliased keys.

| Schema op | Status | Backend |
|---|---|---|
| get_challenge / verify_challenge | live (secp256k1 verify, DER or compact, base64 or hex) | in-memory sessions, 24h |
| lightning_receive_quote | shape-exact, real ECDSA issuer_signature | manifest is JSON (SDK proto-decodes it: LN-receive needs real TransferManifest proto, see LDK_GAPS) |
| request_lightning_receive | shape-exact | FakeLdkBackend; live: Bolt11ReceiveForHash |
| request_lightning_send | shape-exact | FakeLdkBackend; live: Bolt11Send |
| lightning_send_fee_estimate | live heuristic | ppm config |
| request_swap | live via funded sidecar (real SO inbound + change, exact SDK shape) | sidecar wallet; needs ladder liquidity (see DEPLOY.md) |
| leaves_swap_fee_estimate | live (flat config) | SSP_SWAP_FEE_SATS |
| static_deposit_quote | live, real ECDSA signature verifiable vs identity pubkey | fixed 100k credit (must stay <= UTXO; TODO UTXO lookup) |
| claim_static_deposit | shape-exact (`transfer_id` only, per fragment) | stored; SO is source of truth |
| create_instant_static_deposit_quote / create_claim_instant_static_deposit | shape stubs | TODO |
| request_coop_exit / complete_coop_exit | shape-exact | stored; TODO PSBT + OnchainSend |
| coop_exit_fee_estimates / coop_exit_fee_quote | static tiers, exact `quote{}` nesting | TODO fee estimator |
| transfers | exact Transfer shape, `user_request: null` (client null-safe) | in-memory; unknown SO ids -> [] |
| user_request | stored-or-null | TODO full union objects |
| FetchCurrentUserToUserRequestsConnection | exact empty connection | TODO postgres |
| wallet_webhooks CRUD | empty stub | TODO SubscribeEvents bridge |

SDK compat notes:
- `SspClient` default schema path `graphql/spark/2025-03-19`, LOCAL override
  `graphql/spark/rc` – both served.
- `Transfers` returns `TransferWithUserRequest`-shaped rows the SDK joins by id.
- Only `Authorization: Bearer` is checked; `x-partner-jwt` on
  `LightningReceiveQuote` is accepted but ignored (pass-through for Lightspark).
