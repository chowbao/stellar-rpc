# Stellar RPC — Method Reference

Full parameter signatures and response fields for all 12 methods.

## Table of contents

- [getHealth](#gethealth)
- [getNetwork](#getnetwork)
- [getLatestLedger](#getlatestledger)
- [getVersionInfo](#getversioninfo)
- [getFeeStats](#getfeestats)
- [getLedgers](#getledgers)
- [getTransaction](#gettransaction)
- [getTransactions](#gettransactions)
- [getEvents](#getevents)
- [getLedgerEntries](#getledgerentries)
- [simulateTransaction](#simulatetransaction)
- [sendTransaction](#sendtransaction)

---

## getHealth

No params.

**Response:**
- `status` string — `"healthy"`
- `latestLedger` number — most recent ledger sequence
- `oldestLedger` number — oldest ledger in retention window
- `ledgerRetentionWindow` number — `latestLedger - oldestLedger + 1`

---

## getNetwork

No params.

**Response:**
- `passphrase` string (required) — network passphrase (e.g. `"Test SDF Network ; September 2015"`)
- `protocolVersion` number (required) — current protocol version
- `friendbotUrl` string (optional) — faucet URL (testnet/futurenet only)

---

## getLatestLedger

No params.

**Response:**
- `id` string — 64-char hex hash of the latest ledger
- `sequence` number — ledger sequence number
- `closeTime` string — timestamp of ledger close
- `headerXdr` string — base64-encoded `LedgerHeader` XDR
- `metadataXdr` string — base64-encoded `LedgerCloseMeta` XDR

---

## getVersionInfo

No params.

**Response:**
- `version` string — stellar-rpc version
- `commitHash` string — git commit hash
- `buildTimestamp` string — build time
- `captiveCoreVersion` string — embedded stellar-core version
- `protocolVersion` number — supported protocol version

---

## getFeeStats

No params.

**Response** (both `sorobanInclusionFee` and `inclusionFee` have the same shape):
- `sorobanInclusionFee` object — stats for Soroban transactions
- `inclusionFee` object — stats for classic transactions
  - `max` string — max fee seen
  - `min` string — min fee seen
  - `mode` string — most common fee
  - `p10` / `p20` / `p30` / `p40` / `p50` / `p60` / `p70` / `p80` / `p90` / `p95` / `p99` — percentiles
- `latestLedger` string — reference ledger sequence for these stats

---

## getLedgers

Returns full `LedgerCloseMeta` XDR for one or more ledgers. Most granular endpoint.

**Params:**
- `startLedger` number (required if no cursor) — inclusive start sequence
- `pagination` object (optional):
  - `cursor` string — opaque paging token from previous response
  - `limit` number — max records (default 100)
- `xdrFormat` string — `"base64"` (default) or `"json"` (unstable)

**Response:**
- `ledgers` array of objects:
  - `hash` string — 64-char hex ledger hash
  - `sequence` number
  - `ledgerCloseTime` string — unix timestamp
  - `headerXdr` string — base64 `LedgerHeader`
  - `metadataXdr` string — base64 `LedgerCloseMeta` (full block data)
- `latestLedger` number
- `latestLedgerCloseTime` number
- `oldestLedger` number
- `oldestLedgerCloseTime` number
- `cursor` string — use in next request's pagination

---

## getTransaction

Poll this after `sendTransaction` to determine final tx outcome.

**Params:**
- `hash` string (required) — 64-char hex transaction hash
- `xdrFormat` string (optional) — `"base64"` or `"json"`

**Response:**
- `status` string (required) — `SUCCESS`, `FAILED`, or `NOT_FOUND`
  - `NOT_FOUND` means either not submitted or still pending — keep polling
- `latestLedger` number
- `latestLedgerCloseTime` number
- `oldestLedger` number
- `oldestLedgerCloseTime` number

When `SUCCESS` or `FAILED`:
- `ledger` number — ledger sequence where tx was applied
- `createdAt` number — unix timestamp
- `applicationOrder` number — order within the ledger
- `feeBump` boolean — whether this is a fee bump tx
- `envelopeXdr` string — the submitted transaction envelope
- `resultXdr` string — `TransactionResult` XDR
- `resultMetaXdr` string — `TransactionMeta` XDR (contains return values, state changes)

When `FAILED`:
- `diagnosticEventsXdr` array[string] — diagnostic events for debugging

---

## getTransactions

Bulk fetch transactions across a ledger range.

**Params:**
- `startLedger` number (required if no cursor)
- `endLedger` number (optional, exclusive; omit if using cursor)
- `pagination` object (optional): `cursor`, `limit`
- `xdrFormat` string (optional)

**Response:**
- `transactions` array — same fields as `getTransaction` response per entry
- `latestLedger`, `latestLedgerCloseTime`, `oldestLedger`, `oldestLedgerCloseTime` numbers
- `cursor` string

---

## getEvents

Search for contract and system events. Default retention: 24h. Max range: 7 days.

**Params:**
- `startLedger` number (required if no cursor) — inclusive
- `endLedger` number (optional, exclusive; omit if using cursor)
- `filters` array (optional, max 5 filters; events matching ANY filter are returned):
  - `type` string — `"contract"` or `"system"` (omit for all)
  - `contractIds` array[string] — contract addresses to filter on
  - `topics` array[array[string]] — topic filter segments (see [concepts.md](concepts.md#event-topic-filters))
- `pagination` object: `cursor`, `limit` (1–10000, default 100)
- `xdrFormat` string (optional)

**Response:**
- `events` array:
  - `id` string — unique event ID, use for deduplication when paginating
  - `ledger` number
  - `ledgerClosedAt` string — ISO timestamp
  - `contractId` string
  - `pagingToken` string — use as cursor for next page
  - `inSuccessfulContractCall` boolean
  - `type` string — `"contract"` or `"system"`
  - `topic` array[string] — base64-encoded `ScVal` XDR topic segments
  - `value` string — base64-encoded `ScVal` XDR event data
- `latestLedger` number
- `cursor` string

---

## getLedgerEntries

Read **live** on-chain state. Not historical. Works for: accounts, trustlines, offers,
contract data, contract code, claimable balances, liquidity pools.

**Params:**
- `keys` array[string] (required) — base64-encoded `LedgerKey` XDR strings, max 200
- `xdrFormat` string (optional) — `"base64"` or `"json"`

**Response:**
- `entries` array:
  - `key` string — base64 `LedgerKey`
  - `xdr` string — base64 `LedgerEntryData` (current value)
  - `lastModifiedLedgerSeq` number
  - `liveUntilLedgerSeq` number — 0 if entry has expired
- `latestLedger` number

**Common LedgerKey types (construct with SDK):**

```js
// Account
xdr.LedgerKey.account(xdr.LedgerKeyAccount.fromXDR(...))

// Contract data entry (persistent or temporary)
xdr.LedgerKey.contractData(new xdr.LedgerKeyContractData({
  contract: new xdr.ScAddress({ type: xdr.ScAddressType.scAddressTypeContract(), contractId: Buffer.from(contractIdHex, 'hex') }),
  key: xdr.ScVal.scvSymbol("counter"),
  durability: xdr.ContractDataDurability.persistent()
}))

// Contract WASM bytecode
xdr.LedgerKey.contractCode(new xdr.LedgerKeyContractCode({ hash: wasmHash }))
```

---

## simulateTransaction

Dry-run a Soroban invocation. Required before `sendTransaction` for smart contract calls.
Also usable for free read-only contract function calls.

**Params:**
- `transaction` string (required) — base64 `TransactionEnvelope` XDR, must contain
  exactly one `invokeHostFunction` operation
- `resourceConfig` object (optional):
  - `instructionLeeway` number — extra instructions budget padding
- `xdrFormat` string (optional)
- `authMode` string — `"enforce"` (default), `"record"`, `"record_allow_nonroot"`

**Response on success:**
- `latestLedger` number
- `minResourceFee` string — fee in stroops to add on top of network base fee
- `results` array[object]:
  - `auth` array[string] — base64 `SorobanAuthorizationEntry` XDR entries; attach to tx
  - `xdr` string — base64 `ScVal` return value
- `transactionData` string — base64 `SorobanTransactionData`; set as tx's soroban data
- `events` array[string] — base64 `DiagnosticEvent` XDR
- `restorePreamble` object (if state restore needed):
  - `minResourceFee` string
  - `transactionData` string

**Response on error:**
- `error` string — human-readable error message
- `events` array[string] — diagnostic events

**Workflow after simulate:**
```
1. Set tx.sorobanData = simulateResponse.transactionData
2. Set tx.fee += simulateResponse.minResourceFee  (add, don't replace base fee)
3. Set tx.operations[0].auth = simulateResponse.results[0].auth
4. Sign the tx
5. Call sendTransaction
```

---

## sendTransaction

Submit a signed transaction. Works for all transaction types (Soroban and classic).
Does **not** wait for ledger inclusion — returns immediately.

**Params:**
- `transaction` string (required) — base64-encoded signed `TransactionEnvelope` XDR

**Response:**
- `hash` string (required) — 64-char hex tx hash; use with `getTransaction` to poll
- `status` string (required) — `PENDING`, `DUPLICATE`, `TRY_AGAIN_LATER`, or `ERROR`
- `latestLedger` number
- `latestLedgerCloseTime` number
- `errorResultXdr` string (optional, if `ERROR`) — base64 `TransactionResult` XDR
- `diagnosticEventsXdr` array[string] (optional, if `ERROR`) — diagnostic events

**Status meanings:**
- `PENDING` — accepted, will be included in an upcoming ledger
- `DUPLICATE` — already submitted (not an error; tx will still land)
- `TRY_AGAIN_LATER` — node is overloaded; retry after a few seconds
- `ERROR` — rejected by stellar-core; check `errorResultXdr`
