# The Auki session input is becoming a blob, and we cannot follow yet

**Status:** DONE in this repo, on `fix/auki-session-blob-input`. Waiting on one
posemesh change before it can run.
**Affects:** `runner-reconstruction-local-auki-sdk` (`/reconstruction/local-refinement-auki/*`).

> **What is left.** The consumer is converted and its tests pass, including a
> real two-peer round trip. It needs posemesh branch
> `fix/compute-task-protocols-handle`, which populates `AukiProtocolsHandle` on
> the Compute path. Until that is pushed there is no rev to pin, so
> `server/rust/Cargo.toml` carries a clearly-marked `[patch]` block pointing at a
> sibling posemesh checkout. **Delete that block and bump the rev when the fix
> merges.**

## What changed on the other side

The robot-side publisher (`galbot-g1-compute-node-experiment`) no longer
publishes a capture recording through the **P2P dataset protocol**. Posemesh
deleted that protocol — `core/posemesh-p2p-dataset` is gone from the repo, and
`TaskCtx::p2p_dataset` with it — so the publisher moved the recording onto
**Blob v1**, the same content-addressed transport the inspection images already
used.

The Domain artifact we consume keeps its `data_type`
(`scan_path_recording_reference_json`) and its role: one small JSON document
saying where the bulk bytes are. Its **shape has changed**.

Before — a dataset reference:

```json
{
  "schema": "auki-p2p-dataset/1",
  "dataset_id": "…uuid…",
  "domain_id": "…uuid…",
  "name": "scan_path_recording_2026-09-09_10-00-00",
  "peer_id": "12D3KooW…",
  "multiaddrs": ["/ip4/10.0.0.4/tcp/4001/p2p/12D3KooW…"],
  "size_bytes": 41231,
  "sha256": "…",
  "available_until": "2026-09-09T12:00:00Z"
}
```

After — a blob manifest:

```json
{
  "schema": "auki.blob.manifest/1",
  "peer_id": "12D3KooW…",
  "routes": ["/ip4/10.0.0.4/tcp/4001/p2p/12D3KooW…"],
  "content_type": "application/zip",
  "available_until": "2026-09-09T12:00:00Z",
  "blobs": [
    {"id": "scan_path_recording_2026-09-09_10-00-00.zip",
     "sha256": "…", "size_bytes": 41231}
  ]
}
```

Same information, one indirection fewer, and one blob for the whole recording —
a capture is only meaningful whole, so the consumer still fetches exactly one
thing and unzips it.

Two differences that matter to us:

- **`multiaddrs` → `routes`**, and the peer-id/route validation in
  `input.rs::validate_multiaddrs` applies unchanged. That code is worth keeping
  verbatim; it is the most carefully-reasoned part of this input path.
- **`available_until` is now advisory.** A blob has no expiry, and nothing
  prunes the publisher's blob root. Today `validate_reference_document` *rejects*
  an expired reference; against a manifest that should become a warning, because
  the bytes are very likely still there and refusing is a worse failure than
  trying.

`domain_id` and `name` are no longer inside the document. The equivalent checks
still exist one level up, against the Domain artifact's own metadata
(`materialized.domain_id`, `materialized.name`), which is where they were always
the stronger of the two.

## Why we cannot just switch

**Not for want of a peer identity.** It is worth being precise about this,
because the obvious guess — that a compute node has no way to read from other
peers — is wrong, and it would send someone down the wrong path.

The compute node has all the pieces:

- `run_node_with_shutdown` calls `prepare_peer_identity(&cfg)`, giving it an
  `Identity`, a `PeerIdentityProof` and a `DdsP2pClient`.
- For every lease, `ComputeP2pHost::start_task` calls
  **`AukiPeer::start_external(identity, update, config)`** — the same
  constructor the robot-side publisher uses, with a DDS-issued P2P credential
  scoped to the lease's Domain.
- That peer exposes `protocol_context()`, which is exactly what
  `BlobClient::new(...)` needs.
- Its lifetime already brackets our call precisely: `start_task` (engine.rs
  ~1172) → `run_for_lease` (~1271) → `shutdown_task_peer` (~1330).

So a fully authenticated, Domain-scoped, read-capable peer **is alive for the
whole duration of the runner call.**

What is missing is one wire. The engine never hands that peer's protocol context
to the runner:

```rust
// Compute's per-task peer (ComputeP2pHost::start_task) doesn't populate an
// AukiProtocolsHandle yet -- see the doc comment on AukiProtocolsHandle.
let runners = runners.into().compose(RunnerDependencies::default())
```

The **robot** path does the equivalent wiring — composes with an empty
`AukiProtocolsHandle`, then `protocols_handle.activate(peer.protocol_context())`
once the peer is up. The compute path composes with `default()` and never
activates anything.

And we cannot reach around it: runners never receive P2P credentials.
`LeaseEnvelope::without_p2p_credentials()` strips them before the lease arrives,
deliberately — *"Runners receive protocol-specific facades explicitly in their
constructors, never through `TaskCtx` and never as the raw P2P node or
credentials."* So starting our own peer is not an option, and should not be.

`ctx.p2p_dataset` used to be that wire. Retiring the dataset protocol removed it
without replacing it. The capability is all there; the accessor is not.

## The upstream change that unblocks us

Mirror what the robot path already does, per task rather than per process:

1. Compose with an empty handle instead of `default()`:

   ```rust
   let protocols_handle = prepared_peer.as_ref().map(|_| AukiProtocolsHandle::default());
   let runners = runners.into().compose(RunnerDependencies {
       protocols: protocols_handle.clone(),
   })?;
   ```

2. Activate it from the task peer once `start_task` returns, before
   `run_for_lease`, and clear it in `shutdown_task_peer`.

`AukiProtocolsHandle` is already built for this: it is an
`Arc<RwLock<Option<AukiPeerProtocolContext>>>` whose `activate` is a plain
assignment, so re-activating it per task is naturally supported. The one
addition it needs is a `clear()` on teardown, so a runner cannot hold a context
belonging to a peer that has been shut down.

Per-task activation is the right shape here — the peer's lifetime already
matches the runner's exactly, so there is no need for the compute node to grow a
persistent peer. Worth confirming with the posemesh owners, but as a review
question rather than an open design decision.

## What this repo does when it lands

1. Bump `posemesh` from `4bed3f76` to whatever revision carries the handle.
   Two things bite immediately, both already scouted:
   - `auki-p2p` **moved out of posemesh into the `auki-sdk` repo**, so its
     dependency line must be repointed, not just re-revved.
   - That pulls a newer `wasm-bindgen-futures` than this workspace's lockfile
     currently resolves, so expect a lockfile negotiation rather than a clean
     `cargo update -p`.
2. Take an `AukiProtocolsHandle` in `RunnerReconstructionLocalAukiSdk`'s
   constructor and register it through `RunnerComposition::with_protocols` in
   `bin/src/main.rs` (today it builds a plain `RunnerRegistry`).
3. In `input.rs`, replace `DatasetReferenceDocument` with the manifest above and
   `p2p_dataset.fetch(...)` with
   `BlobClient::fetch_exact(peer_id, route, sha256)`, trying each validated
   route in turn. Keep `validate_multiaddrs` and the extraction path unchanged.
4. `input_p2p_tests.rs` is built on `P2pDatasetAdapter`/`P2pDatasetServer`, which
   no longer exist. The publisher's own test suite has a working two-peer blob
   round trip (`runner-proxy/tests/artifacts.rs::the_manifest_route_is_actually_fetchable_by_an_unrelated_peer`)
   that is the natural template for the replacement.

## What actually changed here

Done, on `fix/auki-session-blob-input`:

- `input.rs` parses and validates the manifest and fetches through
  `BlobClient::fetch_exact`, trying each validated route in turn.
- `RunnerReconstructionLocalAukiSdk` takes an `AukiProtocolsHandle`; `bin`
  composes with `RunnerComposition::with_protocols`.
- Pins move posemesh `4bed3f76` → `c0f475c`, with `auki-p2p` repointed at the
  `auki-sdk` repo it moved to. The wasm-bindgen family moves forward as a set,
  since those crates are `=`-pinned to one another.
- `input_p2p_tests.rs` → `input_blob_tests.rs`.

Deliberately unchanged: `validate_multiaddrs` and all its route
canonicalisation, the Domain-binding check against the artifact's own metadata,
the safe extraction path, and the Python-facing directory layout.

### Two behaviour changes to review

**An expired manifest is a warning, not a rejection.** A blob has no expiry, so
a passed `available_until` means "this manifest is old", not "the bytes are
gone". Refusing would turn a probably-fine fetch into a certain failure.

**`AUKI_P2P_ENABLED` is effectively mandatory for this binary**, because
`with_protocols` fails composition without a peer identity. `with_dataset` had
the same property, so this is not new behaviour — but it is newly relevant,
because this binary did not previously compose with either.

### Three old tests intentionally not carried over

`hash and size mismatches leave no partial zip`, `interrupted transfer retries
from zero`, and `wrong Robot peer id fails before bytes are accepted` now test
the SDK rather than us: `fetch_exact` verifies every byte's SHA-256 and mutually
authenticates the remote before returning, and we write to disk only after a
verified fetch — so there is no partial-file window left to assert on. The
replacement adds a real two-peer round trip instead.
