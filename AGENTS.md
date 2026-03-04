# Posemesh — AI Agent Reference

This document is the primary context file for AI agents working on Auki Labs posemesh repositories.
Read this first before exploring any service's code.

---

## What is the Posemesh?

The **posemesh** is a decentralised spatial computing network. It lets apps share persistent 3D
coordinate systems — called **domains** — so multiple devices and AI agents can agree on where
things are in physical space. Domains are created by scanning a physical space with the Domain
Management Tool (DMT) iOS app, processing those scans with computer vision, and storing the
resulting 3D map on a **domain server**.

---

## Services in This Repo

The `reference-code/domain-service` submodule contains **two** services in separate subdirectories:

| Folder | Language | Role |
|--------|----------|------|
| `reference-code/api` | Go | Identity layer: users, orgs, apps, domain metadata, JWTs |
| `reference-code/domain-service/dds` | Go | Trust authority: JWT issuance, domain server registry, routing |
| `reference-code/domain-service/domain-server` | Go | Spatial data layer: stores 3D domain data & lighthouse poses |
| `reference-code/domain-manager-service` | Rust | Compute orchestrator: job queue for reconstruction tasks |
| `reference-code/reconstruction-server` | Python + Rust | Compute node: runs SfM pipeline on scan data |
| `reference-code/network-credit-service` | Go | Financial layer: credit ledger, DAU pricing, token economics |
| `reference-code/console` | TypeScript/React | Web dashboard: org/app/domain/node management UI |

The two services in `domain-service` share internal Go packages under `domain-service/pkg/`
(auth, HTTP utilities, models, errors, logging).

### DDS (Domain Discovery Service) — `reference-code/domain-service/dds`

DDS is the central trust authority of the posemesh. It runs in production at `dds.auki.network`.

- Issues signed JWTs (domain access tokens, service tokens, node identity tokens via SIWE wallet auth).
- Maintains a registry of domain server instances; health-checks them via background jobs.
- Routes clients to the correct domain server for a given domain (`POST /api/v1/domains/{id}/auth`).
- Manages node and server registration (both domain servers and compute nodes).
- Owns domain and lighthouse records at the discovery layer (CRUD, transfers, access control).
- Background jobs (Redis queue): healthchecks, session cleanup, lighthouse size updates, NFT minting.

Every other service validates DDS-signed JWTs. The public signing key is served at
`GET /service/public-key` and `GET /service/public-key.pem`, fetched and cached by each consumer.

---

## Architecture

```
 ┌────────────────────────────────────────────────────────────────┐
 │          Console (React SPA)  reference-code/console           │
 │  • org / app / domain / lighthouse / server management UI      │
 │  • credit balance, DAU, staking, NFTs via blockchain (Wagmi)   │
 └──┬──────────────┬───────────────────┬──────────────────────────┘
    │ auth/crud     │ credits/DAU        │ domains/servers
    ▼               ▼                   ▼
 ┌─────────┐  ┌──────────────┐  ┌──────────────────────────────────┐
 │   API   │  │     NCS      │  │  DDS (Domain Discovery Service)  │
 │  (Go)   │  │    (Go)      │  │  reference-code/domain-service/  │
 │         │◄─┤  credits &   │  │  dds                             │
 │ users   │  │  token econ  │  │  • issues JWTs                   │
 │ orgs    │  │  DAU pricing │  │  • domain server registry        │
 │ apps    │  │  locks/debit │  │  • routes clients to correct DS  │
 │ domains │  │  grants      │  │  • node/server registration      │
 │ l.houses│  │  receipts    │  │  • domain + lighthouse records   │
 └────┬────┘  └──────┬───────┘  │  • background healthcheck jobs  │
      │ wallet       │ DAU/      └─────────────────┬────────────────┘
      │ verify       │ receipt                      │ routes + healthchecks
      └──────────────▼                              ▼
                    DDS ◄───────────────  Domain Server (DS)
                                          reference-code/domain-service/
                                          domain-server
                                          • stores 3D spatial data
                                          • stores lighthouse poses
                                          • triggers reconstruction jobs ─►
                                          • registered with DDS on startup
                                                    │ POST /jobs  (app JWT)
                                                    ▼
                                          Domain Manager Service (DMS) — Rust
                                          • any authorized app submits jobs
                                          • compute nodes claim tasks
                                          • issues per-task domain tokens
                                          • sweeps expired leases
                                                    │ claim → complete
                                                    ▼
                                          Reconstruction Server — Py+Rust
                                          • polls DMS for tasks
                                          • runs SfM pipeline
                                          • downloads/uploads data on DS
```

---

## Service Details

### 1. API (`reference-code/api`)

**Language:** Go 1.23 · **Framework:** chi · **DB:** PostgreSQL (pgx) · **Auth:** JWT (local) + Zitadel OIDC

**Purpose:** User and resource ownership management.

**Key responsibilities:**
- User registration, login, sessions, password reset, email verification.
- Organisations — groups of users that own resources.
- Apps — client applications; each app has an `app_key` + `app_secret` and obtains a JWT via `POST /auth`.
- Domains — metadata records (name, redirect URI). Actual spatial data is on DS.
- Lighthouses — named anchor points inside a domain (metadata only; poses are on DS).
- Wallets — EVM wallet binding per organisation (calls DDS to verify signatures).
- Service tokens — issues access tokens for DS, NCS, HDS (`POST /service/*-access-token`).

**Important endpoints:**
```
POST /auth                           # app_key + app_secret → JWT
POST /user/login                     # user credentials → JWT
POST /service/domains-access-token  # issue DS access token (used by DS itself)
GET  /service/public-key             # public key for verifying API-issued tokens
POST /domains                        # create domain record
POST /domains/{id}/lighthouses       # register a lighthouse in a domain
```

**Auth model:** App requests use `Authorization: Basic base64(appKey:appSecret)` or a bearer JWT
obtained from `POST /auth`. User requests use a bearer JWT from login.

**Local dev:**
```bash
make install   # install deps
make run       # build + run (needs Postgres + env vars)
make test      # unit tests
```

---

### 2. DDS (`reference-code/domain-service/dds`)

**Language:** Go 1.23 · **Framework:** chi · **DB:** PostgreSQL (pgx) + Redis · **Auth:** ECDSA JWT (self-signed)

**Purpose:** Central trust authority and discovery hub. Runs in production at `dds.auki.network`.
All other services fetch its public key and validate the JWTs it issues.

**Key responsibilities:**
- Issue JWTs: domain access tokens (scoped per domain+resource), service tokens, node identity tokens.
- Wallet-based node auth via SIWE (Sign-In with Ethereum): `POST /internal/v1/auth/siwe/request` + verify.
- Domain server registry: accepts registrations, runs healthcheck jobs, removes stale servers.
- Route clients to the right DS for a domain (`POST /api/v1/domains/{domainID}/auth`).
- Node/server management CRUD for users: `/api/v1/nodes/*`, `/api/v1/servers/*`.
- Domain + lighthouse records at the discovery layer (separate from domain data on DS).
- Background jobs via Redis queue: healthchecks, session cleanup, lighthouse size sync, NCS aggregation.

**Important endpoints:**
```
GET  /service/public-key                       # DDS public signing key (PEM JSON)
GET  /service/public-key.pem                   # DDS public signing key (raw PEM)
GET  /health                                   # liveness
POST /internal/v1/register                     # domain server → DDS registration
POST /internal/v1/nodes/register               # compute node → DDS registration
POST /internal/v1/auth/siwe/request            # SIWE nonce for wallet-based node auth
POST /internal/v1/auth/siwe/verify             # SIWE verify → node identity token
POST /api/v1/domains/{domainID}/auth           # get domain access token + DS URL
GET  /api/v1/domains/{domainID}                # domain info (requires domain access token)
GET  /api/v1/lighthouses/{lighthouseID}        # lighthouse lookup
GET  /api/v1/domains?mac_address=AA:BB:CC:DD:EE:FF  # lookup domains by MAC address
GET  /api/v1/domains?lighthouse=ABCDE123456    # lookup domains by lighthouse shortID
POST /api/v1/domains/{domainID}/mac-addresses  # add MAC address to domain
GET  /api/v1/domains/{domainID}/mac-addresses  # list MAC addresses for domain
PUT  /api/v1/domains/{domainID}/mac-addresses/{id}  # update MAC address
DELETE /api/v1/domains/{domainID}/mac-addresses/{id} # remove MAC address
GET  /api/v1/nodes                             # list compute nodes (user/app JWT)
GET  /api/v1/servers                           # list domain servers (user/app JWT)
```

**Auth model:** Public key endpoints and health are unauthenticated. Domain-scoped endpoints
require a DDS-signed domain access token. User/app management endpoints require a user JWT
(from API) or app JWT. Node registration uses SIWE wallet signatures.

**Shared packages:** `reference-code/domain-service/pkg/` contains shared Go code used by both
DDS and DS: `authpkg`, `httppkg`, `logpkg`, `models`, `errpkg`, `utils`, `tokens`, `ncs`.

**Local dev:**
```bash
cd reference-code/domain-service/dds
# Supply Postgres, Redis, and token config env vars
go run ./cmd/...
curl localhost:<PORT>/health
```

---

### 3. Domain Server (`reference-code/domain-service/domain-server`)

**Language:** Go 1.23 · **Framework:** chi · **DB:** PostgreSQL (pgx) · **Storage:** local FS or S3

**Purpose:** Stateful spatial data server. Each DS instance serves one or more domains.
Registers itself with DDS so DDS can route clients to the right server.

**Key responsibilities:**
- Store and serve domain data blobs (point clouds, gaussian splats, bounding boxes).
- Store lighthouse poses (6-DOF position + orientation) contributed by devices.
- Trigger the reconstruction pipeline when a domain is scanned (`POST /api/v1/domains/{id}/process`).
- Backup and restore entire domains.
- Sync data between DS nodes (internal DDS-initiated sync endpoint).

**Auth model:** All public API calls require a **domain access token** — a JWT issued by DDS that
specifies the domain ID and allowed scopes (`data:read`, `data:write`, `pose:read`, etc.).
Internal endpoints (`/internal/v1/...`) require a DDS-signed service token.

**Domain data resources:**
```
GET/POST/PUT/DELETE /api/v1/domains/{domainID}/data        # blobs
GET/PUT/DELETE      /api/v1/domains/{domainID}/lighthouses # poses
GET                 /api/v1/domains/{domainID}/backup      # full domain archive
PUT                 /api/v1/domains/{domainID}/restore     # restore from archive
POST                /api/v1/domains/{domainID}/process     # trigger reconstruction
```

**Processing flow:** When `POST /process` is called (requires `data:write` scope), DS — acting
as an authorized app — calls DMS `POST /jobs` with a DDS-signed app JWT. Any app that holds a
valid DDS app JWT can submit jobs to DMS the same way. The job payload is a `CreateJobRequest`
(tasks + optional DAG edges). Each task's `inputs_cids` field contains full DS data URLs of the
form `https://<ds-host>/api/v1/domains/{domainID}/data/{dataID}`. DMS stores the job and
reconstruction nodes claim tasks from it; the node receives the `inputs_cids` URLs plus a
domain access token in the task lease response and downloads the data straight from DS.

> **Note:** `reference-code/domain-service/domain-server/pkg/processing/reconstruction.go`
> contains an older `TriggerJobRequest` client that POSTed a compact payload to the
> reconstruction server with X-API-Key auth. That pattern is superseded; the current
> production path calls DMS directly with a DDS JWT and the full `CreateJobRequest` format.

**Storage:** Configured via `STORAGE_TYPE` env var:
- `local` (default): stores blobs on the filesystem at `STORAGE_LOCAL_PATH`.
- `s3`: stores blobs in an S3 bucket.

**DDS registration:** On startup, DS calls `ddsClient.Register(...)` to announce its public URL
and capability. DDS then sends health-check pings to `/health` and routes domain access token
requests to this DS instance.

**Local dev:**
```bash
cd reference-code/domain-service/domain-server
go run ./...
curl -v localhost:4000/health
```

---

### 4. Domain Manager Service (`reference-code/domain-manager-service`)

**Language:** Rust 2021 · **Framework:** Axum · **DB:** PostgreSQL (sqlx) · **Metrics:** Prometheus

> This service has its own agent instructions at
> `reference-code/domain-manager-service/AGENTS.md`. Read it before making changes.
> Key directives: write the smallest change that solves the problem; understand the module first;
> run `cargo fmt`, `cargo clippy -- -D warnings`, and the narrowest `cargo test` before finishing.

**Purpose:** Job queue and task orchestrator for compute workloads (primarily reconstruction).
Models work as a **DAG of tasks** (Job → Tasks with optional dependency edges).

**Internal architecture (conceptual layers):**
- `server-core` — domain logic: simple data structures and free functions; no trait machinery unless behavior reuse is undeniable.
- `server-adapters` — HTTP glue: map request → use case → response, nothing more.
- `server-bin` — entrypoint: owns configuration, environment handling, and integration seams.

**Key responsibilities:**
- Accept job submissions from any authorized app (DS, client apps, or other services) via DDS-signed app JWTs.
- Let compute nodes claim runnable tasks, heartbeat to extend leases, and mark complete/failed.
- Issue per-task domain access tokens to nodes (via DDS admin API) so they can fetch data from DS.
- Sweep expired leases (background task every 30 s).
- Optionally lock credits via NCS (Network Credit Service) admin API.

**Ports:**
- `PORT` (default 8080): public API for apps (job submission) and nodes (task lifecycle).
- `ADMIN_PORT` (default 18190): Prometheus metrics.

**Key endpoints:**
```
POST   /jobs                         # create job with DAG (app JWT); tasks carry inputs_cids
POST   /jobs/estimate                # estimate credit cost for a job (app JWT)
GET    /jobs                         # list jobs (app JWT)
GET    /jobs/{id}                    # job status + task details
DELETE /jobs/{id}                    # delete job
POST   /jobs/{id}/cancel             # cancel job
GET    /nodes/busy                   # list nodes with active tasks (app JWT)
GET    /tasks                        # claim next runnable task (node identity required)
POST   /tasks/{id}/heartbeat         # extend lease (promotes Leased→Running on first call)
POST   /tasks/{id}/complete          # mark done, persist receipt
POST   /tasks/{id}/fail              # mark failed / requeue
```

**Job payload shape:**
```json
{
  "label": "...",
  "domain_id": "<uuid>",
  "priority": 0,
  "tasks": [
    {
      "label": "local",
      "stage": "local",
      "capability": "/reconstruction/local/v1",
      "inputs_cids": ["https://<ds-host>/api/v1/domains/<id>/data/<scan-id>"],
      "max_attempts": 3
    },
    {
      "label": "global",
      "stage": "global",
      "capability": "/reconstruction/global/v1",
      "inputs_cids": ["https://<ds-host>/api/v1/domains/<id>/data/<manifest-id>"],
      "max_attempts": 3
    }
  ],
  "edges": [{"from": "local", "to": "global"}]
}
```

**Task lifecycle:**
```
Pending → Leased (claim) → Running (first heartbeat) → Completed
                                                     ↘ Failed (or requeued if attempts remain)
```

**Auth:**
- Jobs API: `RequireAppJwtLayer` — DDS-signed JWT, scope `domain:rw` or `domain-data:rw`.
- Tasks API: `RequireNodeIdentityLayer` — DDS access token with `organization_id` + `node_mode`.

**Required env vars:**
```
DATABASE_URL                     # Postgres DSN
DDS_AUDIENCE                     # expected aud claim in DDS tokens
DDS_SERVICE_PUBLIC_KEY_URL       # URL to fetch DDS PEM public key
DDS_ADMIN_URL                    # DDS admin API base URL (for node domain tokens)
```

**Local dev:**
```bash
cp env.template .env.local       # fill in required values
make dev-up                      # start Postgres via docker compose
make run                         # run service with .env.local
cargo test --workspace           # run tests (requires Postgres)
```

**Swagger UI:** available at `http://localhost:8080/docs` when running.

---

### 5. Reconstruction Server (`reference-code/reconstruction-server`)

**Languages:** Python 3 (SfM pipeline) + Rust (compute node harness)
**GPU:** NVIDIA CUDA required (≥ 8 GiB VRAM)

**Purpose:** Compute node that processes raw scans into 3D representations of physical spaces.

**Architecture:**
```
compute-node (Rust binary)
 ├── posemesh-compute-node crate   # DDS registration, DMS task polling, lifecycle
 ├── runner-reconstruction-local   # Rust runner: invokes Python for local refinement
 └── runner-reconstruction-global  # Rust runner: invokes Python for global refinement
        │
        └── Python pipeline (main.py / local_main.py / global_main.py)
             ├── hloc              # image matching / feature extraction
             ├── COLMAP            # structure-from-motion
             ├── Open3D            # point cloud processing
             ├── PyTorch           # deep learning models (EigenPlaces, etc.)
             └── Ceres / Eigen     # bundle adjustment (C++ extension)
```

**Processing types:**
- `local_refinement` — refines a single scan (per-session SfM).
- `global_refinement` — merges multiple scans into a unified map.
- `local_and_global_refinement` — both in sequence.

**Task flow:**
1. Node registers with DDS using `REG_SECRET` + `SECP256K1_PRIVHEX`.
2. Rust engine polls DMS (`GET /tasks?capability=reconstruction_*`).
3. Claims a task, receives a domain access token from DMS (issued by DDS admin).
4. Downloads scan data from DS using the domain access token.
5. Runs Python SfM pipeline.
6. Uploads results (PLY point cloud, gaussian splat, bounding box) back to DS.
7. Calls `POST /tasks/{id}/complete` on DMS.

**Required credentials (`.env`):**
```
REG_SECRET=<from Posemesh Console, Manage Nodes>
SECP256K1_PRIVHEX=<hex EVM private key of staked wallet>
```

**Optional overrides:**
```
DMS_BASE_URL=https://dms.auki.network/v1
DDS_BASE_URL=https://dds.auki.network
LOCAL_RUNNER_CPU_WORKERS=2
GLOBAL_RUNNER_CPU_WORKERS=2
```

**Running (production):**
```bash
docker pull aukilabs/reconstruction-node:stable
docker run --gpus all --shm-size 512m --env-file .env -d aukilabs/reconstruction-node:stable
```

**Dev container:** Open in VS Code/Cursor → "Dev Containers: Reopen in Container".
The post-create script builds C++ and Rust components automatically.

### 6. Network Credit Service (`reference-code/network-credit-service`)

**Language:** Go 1.23 · **Framework:** chi · **DB:** PostgreSQL (pgx) · **Blockchain:** Ethereum (ethclient)

**Purpose:** Financial backbone of the posemesh. Manages the $AUKI credit ledger — locks credits for compute jobs, debits on completion, issues participation rewards, tracks DAU, and exposes token supply / price data. DMS calls its admin API to lock and debit credits per job.

**Key responsibilities:**
- Credit ledger: organisation balances, locks (held during a job), debits (on completion), releases.
- Grants: sign-up bonuses and other credit grants (pending → in-progress → used / revoked).
- Participation rewards: domain servers, compute nodes, and smoke-test receipts → reward payouts.
- DAU (Daily Active User) receipts: apps submit DAU events → NCS debits credits at configured price.
- Token economics: $AUKI price from Uniswap V2 oracle, total/circulating supply from on-chain.
- Token burning: executes burns via on-chain contract using a configured wallet key.
- Payment processing: reward payouts via safe wallet integration.

**Ports:**
- Public API (default `:4008`): credit balance queries, DAU, receipts, supply/price.
- Admin API (default `:18190`): credit locks/debits/releases (called by DMS), Prometheus metrics.

**Key endpoints:**
```
# Public
GET  /token/price/usd                          # $AUKI spot price
GET  /supply/total                             # total token supply
GET  /supply/circulating                       # circulating supply
POST /domains/dau/average                      # batch weekly avg DAU for domains
POST /receipt                                  # participation receipt (node reward)
POST /daily-active-user-receipt                # app DAU event → credit debit
GET  /app/{appID}/balance                      # app credit balance
GET  /organizations/{orgID}/network-credits    # org credit balance
GET  /organizations/{orgID}/grants/pending     # pending credit grants
GET  /service/public-key                       # NCS public signing key

# Admin (called by DMS, not exposed publicly)
POST /admin/locks                              # create credit lock for a job
POST /admin/locks/{lockID}/debit               # debit on task completion
POST /admin/locks/{lockID}/release             # release unused lock
```

**Auth:** Public routes use API-issued app JWTs. Admin routes use a shared service token. NCS
validates tokens against the API's public key (fetched from `NCS_API_ENDPOINT`).

**Required env vars:**
```
NCS_ADDR                        # public listen address (default :4008)
NCS_ADMIN_ADDR                  # admin listen address (default :18190)
NCS_POSTGRES_URL                # PostgreSQL DSN
NCS_API_ENDPOINT                # API base URL (for public key fetch)
NCS_DDS_INTERNAL_API_ENDPOINT   # DDS internal URL (for domain/org lookups)
NCS_SERVICE_TOKEN_PRIVATE_KEY   # ES256 private key for NCS-issued JWTs
NCS_RPC_URL                     # Ethereum RPC endpoint
NCS_AUKI_TOKEN_CONTRACT_ADDRESS # $AUKI ERC-20 contract
NCS_UNISWAP_V2_ETH_AUKI_PAIR    # Uniswap V2 pair for price oracle
NCS_DAU_PRICE                   # credits debited per DAU event (default 0.01)
```

**Local dev:**
```bash
cd reference-code/network-credit-service
make services   # start Postgres via docker compose
make run        # build + run
make test       # run tests
```

---

### 7. Console (`reference-code/console`)

**Language:** TypeScript · **Framework:** React 19 + Vite · **State:** MobX / MobX State Tree · **UI:** MUI v7 + Tailwind CSS

**Purpose:** Web dashboard for managing posemesh resources. Operators and developers use it to register organisations, create apps and domains, manage compute nodes, monitor credit balances, and interact with on-chain staking and NFT contracts.

**Key pages:**

| Route | Description |
|-------|-------------|
| `/login`, `/register` | Auth (email/password) |
| `/dashboard` | Overview |
| `/organizations/{orgId}/home` | Org summary |
| `/organizations/{orgId}/apps/list` | App management (create, view keys) |
| `/organizations/{orgId}/domains/list` | Domains v2 (via DDS) |
| `/organizations/{orgId}/domains/{id}/data` | Domain data viewer |
| `/organizations/{orgId}/portals/list` | Portal management |
| `/organizations/{orgId}/domain-servers/list` | Domain server management |
| `/organizations/{orgId}/domain-servers/staking` | Server staking UI |
| `/organizations/{orgId}/lighthouses/list` | Lighthouse management |
| `/organizations/{orgId}/domains/{id}/mac-addresses` | MAC address management for a domain |
| `/organizations/{orgId}/wallets/list` | EVM wallet binding |

**Service integrations:**

| API client | Calls | Purpose |
|-----------|-------|---------|
| `authApi` | API | Login, register, password reset |
| `appApi` | API | App CRUD, key management |
| `domainApi` | API | Legacy domain/lighthouse metadata |
| `dsDomainApi` | DDS / DS | Domain data, portals, server list (v2) |
| `ddsMacAddressApi` | DDS | MAC address CRUD for domains |
| `ncsApi` | NCS | Credit balance, grants, DAU averages |
| Wagmi/viem | Ethereum | Staking contracts, NFT minting, token burns |

**Required env vars (`.env.template`):**
```
VITE_API_URL                  # API base URL
VITE_NCS_URL                  # NCS base URL
VITE_DDS_URL                  # DDS base URL
VITE_CONTRACTS_CHAIN_ID       # EVM chain ID (default: 0x2105 Base Mainnet)
VITE_AUKI_TOKEN_ADDRESS        # $AUKI ERC-20 contract
VITE_PUBLIC_MODE_STAKING_CONTRACT_ADDRESS
VITE_DEDICATED_MODE_STAKING_CONTRACT_ADDRESS
VITE_WALLETCONNECT_PROJECT_ID
```

**Feature flags** (set in browser `localStorage`):
```js
localStorage.setItem("domainservice", true)  // enable Domains v2 (DDS/DS)
localStorage.setItem("blockchain", true)     // enable on-chain features
localStorage.setItem("experiments", true)    // enable experimental UI
```

**Local dev:**
```bash
cd reference-code/console
cp .env.template .env        # fill in service URLs
npm install
npm start                    # Vite dev server with hot reload
npm test                     # Vitest
npm run build                # production build
```

---

## Key Concepts

| Term | Definition |
|------|-----------|
| **Domain** | A persistent 3D coordinate frame anchored to a physical space. Has an ID, name, and lives on a DS instance. |
| **Lighthouse** | A named 6-DOF pose within a domain, used for localization. Metadata in API, actual pose in DS. |
| **Domain data** | Arbitrary blobs attached to a domain: point clouds, splats, raw scan images, etc. Stored in DS. |
| **DDS token** | A JWT issued by DDS. App access tokens allow managing orgs/domains. Domain access tokens are scoped to a specific domain and resource (data/pose, read/write). Node identity tokens identify compute nodes. |
| **App** | A registered client with `app_key` + `app_secret`. Obtains a bearer JWT from `POST /auth` on the API. |
| **Job / Task** | DMS concepts. A Job is a unit of compute work, broken into Tasks (nodes in the DAG). Compute nodes claim and execute Tasks. |
| **NCS** | Network Credit Service (`reference-code/network-credit-service`) — $AUKI credit ledger, DAU pricing, participation rewards. DMS calls its admin API to lock/debit credits per job. |
| **DAU** | Daily Active User — an app submits a DAU receipt to NCS when a user interacts with a domain; NCS debits credits at the configured `NCS_DAU_PRICE` rate. |
| **Credit lock** | A temporary credit hold created by DMS on NCS when a job starts. Released if the job is cancelled; debited on completion. |
| **MAC Address** | A device MAC address associated with a domain for discovery. Stored on DDS. Not globally unique — same MAC can appear in multiple domains. Each has a human-editable name. |

---

## Token Flow

```
1. App auth:
   App → POST /auth (API) → API-signed JWT

2. Domain access:
   App JWT → POST /service/domains-access-token (API) → DS access token (API-signed)
   OR
   User/App → DDS token endpoint → DDS-signed domain access token

3. Node identity:
   Compute node registers with DDS → DDS issues node identity token
   DMS validates this token on task endpoints

4. Per-task domain token:
   Compute node claims task → DMS calls DDS admin → returns domain access token
   Node uses this token to read/write domain data on DS
```

---

## Inter-Service Calls

| Caller | Callee | Purpose |
|--------|--------|---------|
| Console | API | Auth, org/app/domain/lighthouse management |
| Console | NCS | Credit balance, grants, DAU averages |
| Console | DDS | Domain v2 / portal / server queries |
| Console | Ethereum | Staking, NFT, token contracts (via Wagmi) |
| API | DDS | Wallet binding verification |
| DS | DDS | Node registration + re-registration on startup |
| Any authorized app (DS, client apps, etc.) | DMS | `POST /jobs` — `CreateJobRequest` with task `inputs_cids` as DS data URLs (DDS app JWT) |
| DDS | DS | Healthcheck pings, lighthouse size sync, domain deletion (internal endpoints on DS) |
| DDS | NCS | Domain server / compute participation aggregation |
| DMS | DDS admin | Issue per-task domain access tokens for compute nodes |
| DMS | NCS admin | Create credit lock on job start; debit on completion; release on cancel |
| App (via Console or SDK) | NCS | Submit DAU receipt → triggers credit debit |
| NCS | API | Fetch API public key for JWT validation |
| NCS | DDS | Internal domain / org lookups |
| NCS | Ethereum | $AUKI price oracle (Uniswap V2), token burns |
| NCS | Safe wallet | Reward payout processing |
| Reconstruction Node | DDS | SIWE wallet auth → node identity token |
| Reconstruction Node | DMS | Claim tasks, heartbeat, complete/fail |
| Reconstruction Node | DS | Download scan data, upload 3D results |

---

## Adding a New Service

To add another repo as reference code:
```bash
./add-reference-repo.sh https://github.com/aukilabs/<repo-name>
```

Then document it in this file under "Services in This Repo" and in `docs/architecture.md`.
