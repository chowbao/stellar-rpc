---
name: stellar-rpc
description: |
  Interact with the Stellar blockchain via Stellar RPC — a JSON-RPC 2.0 API for
  querying network state and submitting transactions. Use when working with Stellar
  network data, reading ledger or contract state, submitting or simulating
  transactions, tracking events, or checking node health. Also triggers for:
  Soroban RPC (former name), sendTransaction, simulateTransaction, getLedgerEntries,
  getEvents, getTransaction, getTransactions, getLedgers, getLatestLedger,
  getNetwork, getHealth, getFeeStats, getVersionInfo. Use this skill whenever
  the user mentions querying the Stellar network, Soroban smart contracts, XDR
  decoding, or submitting stellar transactions — even if they don't say "RPC".
---

# Stellar RPC

Stellar RPC is a lightweight JSON-RPC 2.0 API for real-time Stellar network access.
Formerly called Soroban-RPC (renamed Nov 2024). It is **not** an indexer — it retains
at most 7 days of history (default: 24h for events and transactions).

## Request format

All methods: HTTP POST with `Content-Type: application/json`.
Parameters **must** be named (by-name object), never a positional array.

```bash
curl -X POST https://soroban-testnet.stellar.org \
  -H 'Content-Type: application/json' \
  -d '{
    "jsonrpc": "2.0",
    "id": 1,
    "method": "getHealth",
    "params": {}
  }'
```

XDR fields are base64-encoded by default. Pass `"xdrFormat": "json"` in params
to get human-readable JSON instead (unstable schema — good for exploration, not production).

## Public endpoints

| Network   | URL                                          | Provider  |
|-----------|----------------------------------------------|-----------|
| Testnet   | `https://soroban-testnet.stellar.org`        | SDF       |
| Futurenet | `https://rpc-futurenet.stellar.org`          | SDF       |
| Mainnet   | `https://soroban-rpc.mainnet.stellar.gateway.fm` | Gateway |
| Mainnet   | `https://stellar-soroban-public.nodies.app`  | Nodies    |
| Mainnet   | `https://mainnet.sorobanrpc.com`             | sorobanrpc.com |

## Methods at a glance

| Method | Purpose | Params |
|--------|---------|--------|
| `getHealth` | Node health + ledger retention window | none |
| `getNetwork` | Network passphrase, protocol version, friendbot URL | none |
| `getLatestLedger` | Current ledger sequence + hash | none |
| `getVersionInfo` | stellar-rpc + stellar-core version info | none |
| `getFeeStats` | Fee inclusion statistics for Soroban + classic txs | none |
| `getLedgers` | Full ledger metadata (LedgerCloseMeta) over a range | `startLedger`, `pagination` |
| `getTransaction` | Single transaction status + result by hash | `hash` (required) |
| `getTransactions` | Multiple transactions over a ledger range | `startLedger`, `pagination` |
| `getEvents` | Contract and system events over a ledger range | `startLedger`, `filters`, `pagination` |
| `getLedgerEntries` | Live on-chain state: accounts, contracts, trustlines | `keys[]` (required, base64 LedgerKey XDR) |
| `simulateTransaction` | Dry-run a Soroban invocation, get fee estimate + auth | `transaction` (required) |
| `sendTransaction` | Submit a signed transaction to the network | `transaction` (required) |

For full param signatures and response fields, see [references/methods.md](references/methods.md).

## Core workflows

### 1. Submit a Soroban (smart contract) transaction

Soroban transactions require a simulation step to compute fees and authorization:

```
simulateTransaction  →  assemble tx with returned fee + auth  →  sendTransaction  →  poll getTransaction
```

1. **Simulate** — call `simulateTransaction` with a transaction containing exactly one
   `invokeHostFunction` operation. Returns `minResourceFee` and `results[].auth`.
2. **Assemble** — use the SDK (`stellar-sdk`, `py-stellar-sdk`) to attach the returned
   resource fee and authorization entries to the transaction envelope.
3. **Send** — call `sendTransaction`. Returns immediately with `status: PENDING`.
4. **Poll** — call `getTransaction` with the returned `hash` every ~1s until status is
   `SUCCESS` or `FAILED` (not `NOT_FOUND`).

### 2. Read live contract state

Use `getLedgerEntries` with a base64-encoded `LedgerKey` XDR:

```json
{
  "jsonrpc": "2.0", "id": 1,
  "method": "getLedgerEntries",
  "params": {
    "keys": ["<base64-LedgerKey-XDR>"],
    "xdrFormat": "json"
  }
}
```

Use the SDK to construct LedgerKey XDR. For a contract's data entry:
`xdr.LedgerKey.contractData(contractId, key, durability)`.
Max 200 keys per request.

### 3. Track contract events

```json
{
  "jsonrpc": "2.0", "id": 1,
  "method": "getEvents",
  "params": {
    "startLedger": 54000000,
    "filters": [{
      "type": "contract",
      "contractIds": ["<contract-address>"],
      "topics": [["<topic-segment-base64>"]]
    }],
    "pagination": { "limit": 100 }
  }
}
```

Events are deduplicated by their `id` field when paginating. Max 10,000 per request.
See [references/concepts.md](references/concepts.md) for topic filter details.

### 4. Classic (non-Soroban) transaction

Classic transactions (payments, offers, etc.) do not need simulation:

```
Build + sign tx  →  sendTransaction  →  poll getTransaction
```

`sendTransaction` accepts all transaction types, not just Soroban.

## Key non-obvious details

- `sendTransaction` **does not wait** for inclusion. Always poll `getTransaction`.
- `getTransaction` returns `NOT_FOUND` while the tx is still pending — keep polling.
- XDR `xdrFormat: "json"` schema is unstable and changes with protocol upgrades; use
  base64 + SDK decoding in production.
- `getLedgers` is the most granular endpoint (full `LedgerCloseMeta`); all other
  endpoints are subsets of this data.
- `getEvents` and `getTransactions` default to 24h retention; `getLedgers` up to 7 days.
  Some providers offer archive RPC for full history via `getLedgers`.
- The OpenRPC spec is available at:
  `https://raw.githubusercontent.com/stellar/stellar-docs/main/static/stellar-rpc.openrpc.json`
- Errors follow JSON-RPC spec: codes `-32600` (invalid request), `-32601` (method not found),
  `-32602` (invalid params), `-32603` (internal error).
