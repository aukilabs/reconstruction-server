# The Auki session input is becoming a blob, and we cannot follow yet

**Status:** blocked upstream. Nothing in this repo has changed yet.
**Affects:** `runner-reconstruction-local-auki-sdk` (`/reconstruction/local-refinement-auki/*`).

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

`ctx.p2p_dataset` was how a **compute-side** runner fetched robot-hosted bulk
data. Fetching a blob instead needs a `BlobClient`, which needs an
`AukiPeerProtocols` — and a runner cannot get one:

- Runners never receive P2P credentials. `LeaseEnvelope::without_p2p_credentials()`
  strips them before the lease reaches us, deliberately: *"Runners receive
  protocol-specific facades explicitly in their constructors, never through
  `TaskCtx` and never as the raw P2P node or credentials."* So we cannot start
  our own peer.
- The engine's **robot** path builds an `AukiProtocolsHandle` and hands it to
  runners through `RunnerComposition::with_protocols`. That is how the publisher
  mounts its Blob v1 endpoint.
- The engine's **compute** path does not. From `engine.rs`, in both the pin we
  use and posemesh HEAD:

  ```rust
  // Compute's per-task peer (ComputeP2pHost::start_task) doesn't populate an
  // AukiProtocolsHandle yet -- see the doc comment on AukiProtocolsHandle.
  let runners = runners.into().compose(RunnerDependencies::default())
  ```

So retiring the dataset protocol removed the only mechanism a compute runner had
for fetching robot-published bulk data, and nothing replaced it. This is an
upstream gap, not something this repo can work around.

## The upstream change that unblocks us

Populate an `AukiProtocolsHandle` on the compute path the same way
`run_robot_node_with_shutdowns` already does for the robot path, and pass it in
`RunnerDependencies`. Roughly:

```rust
let protocols_handle = prepared_peer.as_ref().map(|_| AukiProtocolsHandle::default());
let runners = runners.into().compose(RunnerDependencies {
    protocols: protocols_handle.clone(),
})?;
// …and activate it once the compute peer's protocol context exists.
```

The wrinkle is lifetime: compute starts a peer **per task**, while the robot
starts one for the process, so "the" protocol context is not a single long-lived
thing on this side. Whoever picks this up should agree with the posemesh owners
whether the handle is activated per task or the compute node grows a persistent
peer. That decision is theirs, not ours.

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

## Until then

This runner still consumes dataset references and still works against a
publisher that produces them. It will **fail to parse** a manifest from an
updated publisher — `DatasetReferenceDocument` is `deny_unknown_fields`, so the
failure is loud and early rather than silent, which is the behaviour we want
while the two sides are out of step.

Do not deploy an updated robot publisher against this consumer expecting Auki
session refinement to work.
