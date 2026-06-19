# Stellar RPC — Concepts

## Table of contents

- [XDR and data formats](#xdr-and-data-formats)
- [Pagination](#pagination)
- [Event topic filters](#event-topic-filters)
- [Transaction lifecycle](#transaction-lifecycle)
- [Data retention windows](#data-retention-windows)
- [Simulate → send workflow](#simulate--send-workflow)

---

## XDR and data formats

Stellar uses **External Data Representation (XDR)** for encoding most data structures.
In RPC responses, XDR fields are base64-encoded strings by default, usually suffixed
with `Xdr` (e.g. `envelopeXdr`, `metadataXdr`, `resultXdr`).

### Switching to JSON format

Pass `"xdrFormat": "json"` in any method's params to receive human-readable JSON
instead. When `json` format is requested:
- `*Xdr` fields are **omitted**
- Replaced by `*Json` counterparts with the same data unpacked

**Warning:** The JSON schema is **not stable** — it changes when XDR protocol definitions
change. Use base64 + SDK decoding in production; use `json` for exploration and debugging.

```json
{
  "jsonrpc": "2.0", "id": 1,
  "method": "getTransaction",
  "params": {
    "hash": "5ee7e055...",
    "xdrFormat": "json"
  }
}
```

### Decoding XDR

Use the [Stellar Lab XDR viewer](https://lab.stellar.org/xdr/view) for one-off decoding.

In code, use an SDK:
```js
// JavaScript
import { xdr } from '@stellar/stellar-sdk'
const result = xdr.TransactionResult.fromXDR(base64String, 'base64')
```
```python
# Python
from stellar_sdk import xdr
result = xdr.TransactionResult.from_xdr(base64_string)
```

---

## Pagination

Methods supporting pagination (`getLedgers`, `getTransactions`, `getEvents`):

- `cursor` (string) — opaque paging token returned in each response; pass in next request
- `limit` (number) — max records per page (default 100; `getEvents` max is 10,000)
- When using `cursor`, omit `startLedger` and `endLedger`

```json
{
  "method": "getEvents",
  "params": {
    "startLedger": 54000000,
    "filters": [{ "contractIds": ["<address>"] }],
    "pagination": { "limit": 200 }
  }
}
```

Next page (using cursor from response):
```json
{
  "method": "getEvents",
  "params": {
    "filters": [{ "contractIds": ["<address>"] }],
    "pagination": { "cursor": "<cursor-from-previous-response>", "limit": 200 }
  }
}
```

Deduplicate events by their `id` field when paginating to handle overlapping responses.

---

## Event topic filters

Each contract event has a **topics** array (up to 4 segments) and a **data** payload,
all encoded as `ScVal` XDR (base64).

A `filters[].topics` entry is an array-of-arrays, where each inner array is a list of
acceptable values for that topic segment position. Use `"*"` as a wildcard for any position.

```json
"topics": [
  ["<base64-ScVal-1>", "<base64-ScVal-2>"],  // segment 0: match either value
  ["*"],                                       // segment 1: any value
  ["<base64-ScVal-3>"]                         // segment 2: exact match
]
```

Events must match both `contractIds` AND the topic pattern to be returned.
Up to 5 filters per request; events matching **any** filter are included.

To find the right topic values, start with `"*"` wildcards and inspect the `topic` field
in returned events, then narrow down.

---

## Transaction lifecycle

```
Build unsigned tx
  │
  ├─ [Soroban only] simulateTransaction  →  get minResourceFee + auth + transactionData
  │   └─ Assemble: attach fee, auth, sorobanData to tx
  │
  └─ Sign tx with account key(s)
       │
       └─ sendTransaction  →  returns { hash, status: PENDING|DUPLICATE|TRY_AGAIN_LATER|ERROR }
            │
            └─ poll getTransaction(hash) every ~1 second
                 ├─ NOT_FOUND  →  still pending, keep polling
                 ├─ SUCCESS    →  done, read resultMetaXdr for side effects / return values
                 └─ FAILED     →  check resultXdr / diagnosticEventsXdr
```

Ledger closes every ~5 seconds on Stellar mainnet. A typical transaction lands in 1-2 ledgers.
Stop polling after ~60 seconds and treat as uncertain if still `NOT_FOUND`.

---

## Data retention windows

| Data | Default retention | Max configurable |
|------|-------------------|------------------|
| Events | 24 hours | 7 days |
| Transactions | 24 hours | 7 days |
| Ledgers | 24 hours | 7 days |
| Ledger entries | Live state (no history) | N/A |

For data older than 7 days, use:
- [Hubble](https://developers.stellar.org/docs/data/hubble) — public BigQuery dataset
- Third-party indexers / data providers
- Archive RPC providers (some providers offer `getLedgers` with full history)

---

## Simulate → send workflow

Detailed steps when submitting a Soroban transaction:

### Step 1: Build the initial transaction

Create a transaction with one `invokeHostFunction` operation. Use placeholder resource
limits and a small fee — these will be replaced after simulation.

```js
const tx = new TransactionBuilder(account, { fee: "100", networkPassphrase })
  .addOperation(Operation.invokeContractFunction({
    contract: contractAddress,
    function: "transfer",
    args: [/* ScVal arguments */]
  }))
  .setTimeout(30)
  .build()
```

### Step 2: Simulate

```js
const sim = await server.simulateTransaction(tx)
// sim.minResourceFee — add this to the tx fee
// sim.result.auth    — authorization entries
// sim.transactionData — soroban resource data
```

If `sim.restorePreamble` is present, a ledger entry needs to be restored first
(it has expired). Submit a restore transaction using `sim.restorePreamble.transactionData`
before continuing.

### Step 3: Assemble

Use the SDK assembler to merge simulation results into the transaction:

```js
import { assembleTransaction } from '@stellar/stellar-sdk/rpc'
const assembled = assembleTransaction(tx, sim)
// assembled is a TransactionBuilder ready to sign
```

Or manually:
```js
tx.sorobanData = sim.transactionData
tx.fee = (parseInt(tx.fee) + parseInt(sim.minResourceFee)).toString()
tx.operations[0].auth = sim.result.auth
```

### Step 4: Sign and submit

```js
const signed = assembled.build()
signed.sign(keypair)
const result = await server.sendTransaction(signed)
// result.status should be PENDING or DUPLICATE
```

### Step 5: Poll

```js
let txResult
do {
  await sleep(1000)
  txResult = await server.getTransaction(result.hash)
} while (txResult.status === "NOT_FOUND")

if (txResult.status === "SUCCESS") {
  // decode txResult.resultMetaXdr to get return value and state changes
}
```
