# ADR-006: Top-Level Transaction Fields

**Status**: Accepted
**Date**: 2026-06-11
**Context**: Handlers see only log-level data plus `tx_hash`. The events-first thesis holds for state derivation, but the *identity* of a transaction — who sent it, to which contract, carrying how much ETH — is not in any log. Validated as a real gap against the production Envio Uniswap-V4 indexer (`event.transaction.from`, a swap's origin). This ADR decides how to expose `log.tx.from` / `log.tx.to` / `log.tx.value` to handlers, sourced from the block bodies a full node already stores — no traces, no archive node, no re-execution.

## Problem

Transaction fields live in block bodies, not receipts, so the flat log store never sees them. Any solution must deliver them to the handler's hot loop without breaking the fundamentals:

1. **`blocks.dat` is never mutated** — no format change that forces a re-import.
2. **The hot loop is network-free and replay-deterministic** — no RPC at dispatch.
3. **Flat files only**, advisory where possible — an old store keeps working, a new artifact is backfillable into it.
4. **Wire format = disk format** — remote indexers get the same bytes as local ones.
5. **Zero cost when unused** — an indexer that never reads `tx.*` pays nothing.

Scope: **log-producing transactions only** (a tx with no logs never reaches a handler; standalone ETH sends are trace territory, rejected in the spec). Fields: `from`, `to`, `value`. `gas` and `nonce` were considered and dropped as non-essential — no demonstrated indexer need, and 16 bytes/record across ~10⁹ records is real disk. `input` is deferred outright (calldata is unbounded). A future field rides a `txs.dat` magic/version bump, additive.

## Options

### A: Inline tx fields in the `blocks.dat` LogEntry

Extend the packed log format with the owning tx's fields.

Rejected. Duplicates the fields across every log of a multi-log tx, breaks the one format that is promised never to change, and forces a full re-import. Non-starter.

### B: Cheap fields at import, lazy `from` via SDK-side ecrecover (the spec's original sketch)

Store `to`/`value` plus the signature at import; the SDK recovers `from` on first touch, cached per tx.

Rejected on sizing. Lazy ecrecover needs the **signing hash**, and reconstructing it requires the full unsigned tx — including calldata — so the sighash must be *persisted at import*: `sighash(32) ‖ r(32) ‖ s(32) ‖ v(1)` = **97 incompressible bytes per tx** versus 20 for `from` itself. At ~950 M post-merge log-producing txs that is ~145 GB of store growth, plus a new SDK-side recover cache with its own strictness semantics. The lazy route costs 3× the disk to avoid a one-time bounded compute pass.

### C: Advisory `txs.{dat,idx}` pair, `from` recovered at import in a separate resumable pass

Store the resolved fields directly. `from` costs ecrecover once at import on the RocksDB path (~45–70 µs × ~950 M txs ÷ 7 workers ≈ 1.5–3 h, in a separate resumable pass so the 13-minute logs headline is untouched); the RPC import and follower get `from` free from `eth_getBlockByNumber(full=true)`.

Superseded by D before any implementation: the compute turns out to be unnecessary.

### D: Senders read from Nethermind's receipt rows, `to`/`value` from a crypto-free bodies pass — **disproven during implementation**

The premise was that Nethermind persists the recovered sender in every compact receipt row (the `receipt_decoder.zig` walk skips a slot the prototype labeled "sender"). Ground truth said otherwise: a raw row dump of block 15,537,394 decodes as `[status=0x01, 0x80, gas_used_total, logs]` — the sender *slot* exists but the **compact format stores it empty** on a default-config Nethermind (`eth_getTransactionReceipt` recovers on demand instead). The `Transactions` CF is only a txhash → block-number map. No on-disk sender anywhere.

### E: Hybrid — stored sender when a row carries one, splice-sighash recovery when it doesn't — **chosen**

Option C's compute returns, implemented as D's fallback already specified below: per log-producing tx with an empty sender slot, rebuild the signing payload via the **splice** (no per-type re-encoder, shape-generic through EIP-7702) and `ecrecover`. Rows that do carry a 20-byte sender (non-compact receipt configs) skip recovery for free. Correctness is pinned two ways: closed-loop tests where eth.zig signs and our splice must reproduce its `hashForSigning` and recover the signer across legacy-155/pre-155/2930/1559 plus a hand-built 7702, and a live-chain probe (block 15,537,394: 25/25 senders match RPC).

Measured single-threaded: **~420 blk/s** (recovery-bound, ~25–40 recovers/block × ~50 µs) → ~6.5 h mainnet, so the pass parallelizes per-block across the import's existing 7-worker slot pipeline → **~1–1.5 h projected**, inside option C's original envelope. `to`/`value` extraction and the bodies point-get remain as designed.

The pass stays separate from the logs import with its own resume cursor — the `timestamps.bin` pattern, shipped and proven twice — so tx fields backfill into any *existing* imported store, no re-import, and `--no-tx-fields` skips it.

## Decision — design

### On-disk format

```
txs.dat   per-block LZ4 entry, dense by block number, append-only:
  count      u32 LE     (covers the full u16 tx_index domain, no assumed cap)
  [count × TxRecord], sorted by tx_index

TxRecord (76 bytes raw):
  tx_index   u16 LE
  type       u8          tx envelope type (0x00 legacy … 0x04 EIP-7702)
  flags      u8          bit0 = to_absent (contract creation)
                         bit1 = from_unrecovered (stored zero, see 7702 note)
  from       [20]u8
  to         [20]u8      zero when to_absent
  value      [32]u8      u256 LE

txs.idx   timestamps-style header + dense (offset u64 LE, len u32 LE) per block:
  magic        [8]u8     "EMITTXSI"
  first_block  u64 LE
  count        u64 LE    published last, count×entry ≤ file size holds live
```

Records are zero-heavy (`value` is zero for most token interactions) so LZ4 recovers much of the 76 B. Expected mainnet cost: **~45–55 GB** (~+20% on the store). That is the price of the feature and it is paid only by operators who run the pass; `--no-tx-fields` skips it and absent files degrade exactly like absent `timestamps.bin`.

Blocks with no log-producing txs write a `count = 0` entry, keeping the index dense. The authoritative coverage is the idx header count; the engine `status` command reports it alongside timestamps coverage.

### Field extraction

`to`/`value`: a per-type index map over the raw signed tx RLP — both sit at fixed list positions in every envelope (legacy through EIP-7702), so the extractor is `rlp.zig` walking with no allocation and no library dependency (`tx_decode.decode`).

`from`: when the receipt row carries a 20-byte sender, read it; when the slot is empty (compact format, the default), recover via the *splice sighash* (`tx_decode.decodeSigned` + `recoverSender`) — for typed envelopes the signing payload is the signed RLP with the trailing `(y_parity, r, s)` dropped and the list header re-lengthened, so `keccak(type ‖ new_header ‖ fields[0..sig_offset])` feeds ecrecover without materializing the tx, shape-generic through EIP-7702. Legacy derives `chain_id` from `v` and appends `(chain_id, 0, 0)` per EIP-155. A recovery failure (should not occur on-chain) sets `from_unrecovered`, fail-soft per record, loud in the pass summary.

### The tx pass

One walk over Nethermind's receipts CF (the import's existing iterator pattern) paired with bodies lookups from the `blocks` DB: the receipt row yields the sender per tx and the set of log-producing `tx_index`es; the body yields `to`/`value` for those txs. Emits complete `TxRecord`s into `txs.dat`, resumes from its idx cursor, runs after or concurrent with the logs import. I/O-bound on the bodies scan (R2), no crypto.

RPC import: the existing strict timestamps pass flips to `eth_getBlockByNumber(n, full=true)` and takes `from`/`to`/`value` from the response (the node pre-recovers `from`) — same batching, same resume cursor discipline. Follower: same source per new block; the pending-ring `Entry` gains the block's tx table, and the `"EMITPEND"` magic gate makes the migration free (an old ring reads as empty and re-baselines). On finalization the table mirrors into `txs.dat` exactly as timestamps do.

### Delivery to handlers

`Manifest` gains `tx_fields: bool = false`, **comptime-gated**:

- `false` (default): `Log(E)` has no `tx` field at all. Zero bytes, zero instructions, zero format involvement — existing indexers compile byte-identical.
- `true`: phase 1 reads `txs.dat` alongside `blocks.dat` and packs the kept logs' `TxRecord`s into the filtered entry (the manifest fingerprint already hashes manifest content, so flipping the flag auto-rebuilds the filter). `Log(E).tx: TxFields { from, to, value, is_create }` is non-optional; the scanner resolves it from the entry's tx subtable by `tx_index`. `init` fails loud when `txs.dat` coverage does not span the indexed range — the `ethCall` strict contract applied to a second artifact.

Carrying records through the FilteredStore preserves **wire = disk**: remote indexers receive tx fields inside the same PUSH bytes, costing one REGISTER protocol version bump and nothing else. The <2 s re-run path never touches `txs.dat`.

## Trade-offs accepted

- **~45–55 GB store growth** for operators who enable the pass. The data itself; no encoding buys it back.
- **One-time recovery-bound tx pass** on the RocksDB path (~420 blk/s single-threaded measured, ~1–1.5 h projected on the 7-worker pipeline). Isolated behind its own cursor; the logs import headline is untouched.
- **`gas`/`nonce`/`input` not stored.** Additive later via a `txs.dat` version bump; not worth 16+ B/record on speculation.
- **Filtered-entry format change + PUSH version bump** when (and only when) a manifest opts in. Filter dirs are cheap rebuilds; the fingerprint mechanism already owns invalidation.
- **Trusting Nethermind's stored sender.** Same trust domain as the logs we already import from the same rows; it is the value the node itself serves over RPC.

## Open questions

- **R1 - RESOLVED:**
  - Keys: 40 bytes, `block_number(u64 BE) ‖ block_hash(32)` — identical to the receipts scheme. The pass therefore `get`s the body with the *same 40-byte key* as the canonical receipt row it is walking: canonical pairing is automatic, no `blockInfos` lookup, no dup-group handling.
  - Values: the **full block RLP** `[header, [txs…], [ommers…], [withdrawals…]]` (not a bare body). Enter the outer list, skip the header, walk the tx list. Verified: a probe decode of `to`/`value`/`nonce` from tx0 of block 25,296,893 matched `eth_getBlockByNumber` byte-for-byte, tx count matched (351).
  - The DB is **BlobDB** (~4,600 `.blob` files, 570 GB mainnet); values resolve transparently through the normal RocksDB read path. Two operational notes: the pass must raise `RLIMIT_NOFILE` (or set `max_open_files`) — every blob file is opened; and a freshly-written tail (~unflushed memtable) may be invisible to a read-only open, irrelevant since the pass targets finalized blocks.
  - Single `default` column family.
- **R2**: measured bodies-scan throughput on the i7-8700 fixes the pass wall-clock estimate (~10–20 min expected at Gen3 NVMe over ~570 GB; the pass reads bodies by point-`get` per canonical key rather than full iteration, so effective bytes read track the canonical chain, not the orphan superset).

## Estimated cost

| Component | LOC |
|---|---|
| `engine/tx_decode.zig` (per-type `to`/`value` index maps, 5 envelopes) | ~70 |
| `core/txs.zig` reader/writer + idx cursor | ~180 |
| RocksDB tx pass (receipts sender capture + bodies walk) | ~150 |
| RPC import pass extension + follower + `pending_format` carry | ~150 |
| SDK: filtered carry-through, `tx_fields` gate, `Log(E).tx`, coverage check | ~160 |
| Tests (per-type RLP fixtures, sender-capture fixture, fake_engine tx tables, e2e `tx.from`, RPC parity sample) | ~190 |
| **Total** | **~900 (≈710 non-test)** |
