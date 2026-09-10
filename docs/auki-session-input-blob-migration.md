# The Auki session input moved from the P2P dataset protocol to Blob v1

**Status:** DONE, on `feature/dds-p2p-demo`. Pinned to posemesh `5daee4e`.
**Affects:** `runner-reconstruction-local-auki-sdk` (`/reconstruction/local-refinement-auki/*`).

## What changed on the other side

The robot-side publisher (`galbot-g1-compute-node-experiment`) no longer
publishes a capture recording through the **P2P dataset protocol**. Posemesh
deleted that protocol in `c0f475c` — `core/posemesh-p2p-dataset` is gone, and
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
  prunes the publisher's blob root. The old code *rejected* an expired
  reference; against a manifest that is now a warning, because the bytes are
  very likely still there and refusing is a worse failure than trying.

`domain_id` and `name` are no longer inside the document. The equivalent checks
still exist one level up, against the Domain artifact's own metadata
(`materialized.domain_id`, `materialized.name`), which is where they were always
the stronger of the two.

## How we reach the bytes: the task peer's protocol surface

Retiring the dataset protocol removed `ctx.p2p_dataset`, which had been this
runner's only wire to robot-published bulk data. Posemesh replaced it in
`e088ee9` ("expose task peer protocols to capability runners").

**A runner cannot open that wire for itself, and that is deliberate.** Worth
stating plainly, because the obvious workaround is a dead end someone will
otherwise rediscover:

- A compute node *does* have a peer identity. `run_node_with_shutdown` calls
  `prepare_peer_identity(&cfg)`, and for every lease `ComputeP2pHost::start_task`
  calls `AukiPeer::start_external(identity, update, config)` — the same
  constructor the robot-side publisher uses, with a DDS-issued P2P credential
  scoped to the lease's Domain. So a fully authenticated, Domain-scoped,
  read-capable peer is alive for the whole runner call.
- But the credential that peer is built from never reaches a runner.
  `LeaseEnvelope::without_p2p_credentials()` nulls `p2p_access_token` before the
  lease is handed over, and the field is `#[doc(hidden)]` in the SDK.
  `DdsP2pClient::external_authority_update` is the only way to mint an
  `ExternalAuthorityUpdate` and it requires that token. `ctx.access_token` is
  the domain-HTTP bearer — a different credential for a different service.
- Reusing the node's keypair to start a second peer would collide on Peer ID
  with the task peer already running on it; a fresh keypair is not DDS-authorized
  for the Domain.

So the engine publishes the peer it already has, rather than runners building
their own. `ComputeP2pHost` holds an `AukiProtocolsHandle`; `start_task`
activates it with `peer.protocol_context()` immediately after the peer starts,
and `ComputeTaskPeer` holds the resulting `TaskProtocolActivation` — an RAII
guard, ordered before the peer field, that clears the slot on drop including on
cancellation and error. A second activation is refused outright.

**The contract this puts on us:** call `get()` inside the runner, per task. A
context retained from an earlier task belongs to a peer that has been stopped.
We comply by construction — `RunnerReconstructionLocalAukiSdk` stores the
`AukiProtocolsHandle`, never the context, and calls `get()` inside
`materialize_session_zip`.

Two consequences worth knowing when reading a failure:

- `get()` fails between tasks, and for a lease that carries no P2P authority.
- Robot and Compute now populate the same handle type with different lifetimes:
  Robot activates once per process, Compute once per task.

## What actually changed here

- `input.rs` parses and validates the manifest and fetches through
  `BlobClient::fetch_exact`, trying each validated route in turn.
- `RunnerReconstructionLocalAukiSdk` takes an `AukiProtocolsHandle`; `bin`
  composes with `RunnerComposition::with_protocols` (it previously built a plain
  `RunnerRegistry`).
- Pins move posemesh `4bed3f76` → `5daee4e`, with `auki-p2p` repointed at the
  `auki-sdk` repo it moved to — a relocation, not just a re-rev. The
  wasm-bindgen family moves forward as a set, since those crates are `=`-pinned
  to one another.
- `server/rust/Cargo.toml`'s `[patch.crates-io]` redirects
  `posemesh-compute-node-runner-api` to that same git rev. **Keep it in lockstep
  with the direct `posemesh-compute-node` pins** — if they disagree the runner
  API resolves twice and the trait impls come from distinct copies, which fails
  in a thoroughly confusing way. Note also that posemesh kept the runner API at
  version `0.1.2` across the removal of the dataset types, so cargo gives no
  version signal on a rev bump.
- `input_p2p_tests.rs` → `input_blob_tests.rs`, including a real two-peer round
  trip.

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

## Not yet verified

Nothing here has run against a real robot, a real DMS, or a live Domain Server.
The two-peer round trip in `input_blob_tests.rs` activates the handle directly;
it does not prove the engine populates it end-to-end on a real lease.
