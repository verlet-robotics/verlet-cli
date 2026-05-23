# `verlet` CLI — Use Cases, Coverage & Gap Analysis

_Last reviewed: 2026-05-22 (CLI `0.9.0`, post-showcase-reconciliation + deprecation cleanup)._

This document inventories what the `verlet` CLI does today for its **two
audiences** — **showcase prospects** and **platform clients** — maps each
command to its backend endpoint, and flags the gaps where the backend
already exposes capability the CLI does not surface, plus net-new feature
ideas.

---

## 1. The two audiences

The CLI is credential-kind-aware. Every command resolves the active
profile's `kind` and branches on it. There are effectively two products
sharing one binary:

| Audience | Credential kind | How they authenticate | What they are |
|----------|-----------------|------------------------|---------------|
| **Showcase prospect** | `showcase_access_code` | `verlet auth login --kind showcase` (access code → short-lived JWT) | A lead/evaluator handed an access code by a Verlet rep. Sees only the datasets their code is *granted*, scoped by `ego_showcase_access_grant` rows. |
| **Platform client** | `device_flow` / `pat` | `verlet auth login` (OAuth device flow) or a Personal Access Token | A paying or trial account holder with a real platform identity, purchases, and a billing relationship. |
| _(sub-case)_ **Bundle grantee** | `bundle_grant` | `verlet bundles redeem <code>` | A researcher given a research-bundle access code. A narrow slice of the platform audience. |

The split matters because **showcase is a sales-funnel surface** (evaluate
before you buy) and **platform is the post-sale surface** (you own data,
you have a bill). The CLI serves the first well and the second only
partially.

---

## 2. Command surface — what exists today

```
verlet
├── auth
│   ├── login            --kind device|showcase, --no-browser, --api-url
│   ├── logout
│   ├── status           --json, --refresh
│   └── tokens
│       ├── create       --name --scope --expires-in --save-to --no-save
│       ├── list
│       ├── revoke <id|name>
│       ├── show <id|name>
│       └── set hf <token>
├── datasets
│   ├── list             --task --robot --category --since --limit --kind --json
│   ├── info <slug>      --json
│   ├── download <slug>  --variant --scope --episode-ids --segment-ids
│   │                    --format --detach --dry-run --resume --force ...
│   ├── push <slug>      --to huggingface://org/repo --format
│   └── jobs [<job_id>]  reattach to a conversion job
├── bundles
│   ├── browse           anonymous public research-bundle catalog
│   ├── redeem <code>    research-access code → bundle_grant profile
│   ├── list             --all
│   ├── info <id>
│   ├── download <id>    --format --variant
│   └── export-manifest <id>   portable offline manifest
├── config
│   └── telemetry        status | enable | disable
├── docs
│   └── export           (maintainer — regenerate MDX reference)
└── update               self-update
```

> **0.9.0 cleanup.** The deprecated shims `verlet pull`, top-level `verlet login`,
> and the hidden `verlet ego` migration stub were all removed in 0.9.0 (breaking).
> `datasets download` and `auth login --kind showcase` are the only paths now —
> the surface above is the complete, non-deprecated command list.

---

## 3. Showcase audience — use cases

### 3.1 What works today

| # | Use case | Command | Backend endpoint |
|---|----------|---------|------------------|
| S1 | Redeem an access code into a working session | `verlet auth login --kind showcase` | `POST /api/v1/showcase/auth` |
| S2 | See which datasets my code unlocked | `verlet datasets list` | `GET /api/v1/showcase/datasets` |
| S3 | Inspect one granted dataset + see my grants | `verlet datasets info <slug>` | `GET /api/v1/showcase/datasets/{id_or_slug}` |
| S4 | Download a free-sample subset to evaluate | `verlet datasets download <slug> --scope samples` | `GET /api/v1/showcase/datasets/{slug}/download?scope=samples` |
| S5 | Download the full granted dataset | `verlet datasets download <slug> --scope full` | same, `scope=full` |
| S6 | Check what credential I'm on / when it expires | `verlet auth status` | (local creds file) |
| S7 | Scripted / JSON output for evaluation pipelines | `--json` on `list` / `info` | — |

This path is **clean and well-bounded**. The post-reconciliation design is
correct: showcase codes never see internal segment IDs, downloads are
whole-dataset, the grant decides the variant, and `404` deliberately
conflates "no such dataset" with "no grant" to block enumeration.

### 3.2 Showcase gaps

| Gap | Detail | Severity |
|-----|--------|----------|
| **G-S1 — no quota visibility** | The gated download endpoint enforces a per-grant quota and returns `429` on exhaustion (`EgoShowcaseDownloadLog`). The CLI surfaces the `429` only *after* a failed attempt. There is no `verlet datasets info` field or `verlet auth status` line showing "downloads used: 3 / 10". A prospect cannot plan their evaluation. | High |
| **G-S2 — no grant expiry surfaced** | Access-code JWTs are short-lived and grants can have an end date. `auth status` shows token expiry but not *grant* expiry, and `datasets info` shows grants without a "valid until" column. | Medium |
| **G-S3 — `operation-stats` unused** | `GET /api/v1/showcase/operation-stats` returns fleet-aggregate counts (episodes collected, fleet size) — exactly the kind of credibility number a prospect wants. No CLI command exposes it. A `verlet showcase stats` would be a cheap, compelling addition. | Medium |
| **G-S4 — no "request access" affordance** | When `info`/`download` 404s, the message says "contact your Verlet rep" as free text. No structured next step (rep email, a `verlet showcase request-access` that pings sales). | Low |
| **G-S5 — `--task`/`--robot` filters are client-side only** | The showcase list endpoint takes no server filters, so `verlet datasets list --task cooking` filters the already-returned page in memory. Fine at current grant sizes; will mislead if a code is ever granted >100 datasets. | Low |
| **G-S6 — no preview without download** | A prospect must download sample files to see anything. No `verlet datasets preview <slug>` that streams a thumbnail / hero-reel URL. The platform side has `hero-reel` and `episode-playback-meta` endpoints; showcase has none equivalent wired to the CLI. | Medium |

---

## 4. Platform-client audience — use cases

### 4.1 What works today

| # | Use case | Command | Backend endpoint |
|---|----------|---------|------------------|
| P1 | Sign in with a real account | `verlet auth login` (device flow) | platform device-flow endpoints |
| P2 | Mint a CI/automation token | `verlet auth tokens create --scope ...` | `auth/tokens` |
| P3 | Manage PATs | `verlet auth tokens list/show/revoke` | `auth/tokens` |
| P4 | Browse the public catalog | `verlet datasets list` | `GET /api/platform/v1/catalog/datasets` |
| P5 | Inspect a dataset | `verlet datasets info <slug>` | `GET /api/platform/v1/catalog/datasets/{id_or_slug}` |
| P6 | Download a dataset (whole / by episode / by segment) | `verlet datasets download <slug> [--episode-ids ...]` | `GET /api/platform/v1/downloads/{slug}/manifest` |
| P7 | Convert format on the way out (HDF5, etc.) | `... download --format hdf5` | `downloads/{slug}/manifest` → 202 job |
| P8 | Queue a conversion and walk away | `... download --format hdf5 --detach` then `verlet datasets jobs <id>` | `downloads/jobs/{id}` |
| P9 | Push a dataset to HuggingFace | `verlet datasets push <slug> --to huggingface://...` | `POST /api/platform/v1/downloads/{slug}/push` |
| P10 | Redeem a research bundle | `verlet bundles redeem <code>` | platform_bundles |
| P11 | List / inspect / download bundles | `verlet bundles list/info/download` | platform_bundles + per-dataset manifest |
| P12 | Emit an offline/air-gapped manifest | `verlet bundles export-manifest <id>` | per-dataset manifest fan-out |

### 4.2 Platform gaps — backend capability the CLI does not surface

The platform backend is **substantially larger than the CLI exposes.**
Endpoints that exist and are unwired:

| Gap | Backend endpoint(s) | What's missing in the CLI | Severity |
|-----|--------------------|-----------------------------|----------|
| **G-P1 — no "my library"** | `GET /api/platform/v1/downloads/library` | A client cannot list datasets *they have purchased*. `verlet bundles list` lists bundles only. There is no `verlet datasets library` / no way to see owned single-dataset purchases from the CLI. This is the single biggest hole — a paying client can't enumerate what they paid for. | **Critical** |
| **G-P2 — no purchase / checkout flow** | `services/purchases` — `POST /checkout`, `/confirm`, `GET /` (purchase list), `/{id}/snapshot` | The CLI can *download* what you own but cannot *buy*. No `verlet purchase <slug>` / `verlet datasets buy`. Entirely web-only today. | High (by design? — see §6) |
| **G-P3 — no billing visibility** | `purchases/billing/summary`, `/billing/history` | No `verlet billing` command. CI/finance users must use the web app. | Medium |
| **G-P4 — cloud destinations ignored** | `downloads/destinations` CRUD, `/destinations/connect/init`+`/callback`, `/test-connection`, `aws-cfn-template.yaml` | `verlet datasets push` is **HuggingFace-only**. The backend supports first-class S3 / GCS cloud destinations with an OAuth-style connect flow and a CloudFormation template. None of it is in the CLI — yet "push my purchased data to my own S3 bucket" is a core enterprise workflow. | **High** |
| **G-P5 — per-dataset conversion listing** | `GET /api/platform/v1/downloads/{slug}/conversions` | `verlet datasets jobs` (no arg) prints "listing not supported by server" — but a *per-dataset* conversion list endpoint **does exist**. `verlet datasets jobs --slug <slug>` could list jobs for that dataset today, no backend work needed. | Medium |
| **G-P6 — QC / analytics blind** | `catalog/.../ego-quality`, `.../episode-qc-detail`, `.../dataset-qc-distributions`, `.../dataset-analytics`, `.../episode-trajectory` | A buyer evaluating data quality from the CLI has nothing. `verlet datasets quality <slug>` / `verlet datasets analytics <slug>` would let researchers vet a dataset before downloading gigabytes. | Medium |
| **G-P7 — no episode/segment browsing** | `catalog/.../episodes`, `.../segments`, `.../segments/{id}`, `downloads/{slug}/episodes` | `download --episode-ids 1,2,3` requires the user to *already know* the IDs. There is no `verlet datasets episodes <slug>` to list them. The selection flag is unusable without out-of-band knowledge. | Medium |
| **G-P8 — subscriptions unrepresented** | `services/platform_subscriptions` — plans, current, usage, seats | No `verlet subscription status` / `verlet subscription usage`. Seat-based plan holders have no CLI insight into seat/usage limits. | Low |
| **G-P9 — no catalog stats** | `GET /api/platform/v1/catalog/stats` | No headline-numbers command. Minor, but trivially cheap. | Low |
| **G-P10 — push is one-shot, no status command** | `GET /downloads/pushes/recent` | `push` polls inline until done; if the terminal is closed there is no `verlet datasets pushes` to re-inspect recent pushes (the endpoint exists and is already consumed by `push`). | Low |

---

## 5. Cross-cutting observations

- **The legacy-shim maintenance tax is paid off.** `verlet pull` (which
  duplicated `datasets download` against a *different* platform path,
  `/catalog/datasets/{slug}/samples/download`), top-level `verlet login`, and
  the hidden `ego` stub were all removed in 0.9.0. There is now exactly one
  code path per job. The only loose end: the `/catalog/.../samples/download`
  endpoint `pull` used has no remaining CLI consumer (see §7).
- **`datasets jobs` with no argument is a dead end** that tells the user a
  capability is unavailable when a partial form of it (G-P5) is reachable
  today. Worth fixing the message at minimum.
- **Auth surface is the most complete part of the CLI** — device flow,
  showcase codes, PATs with scopes/expiry, aux HF token. The download and
  *post*-download story is where the gaps cluster.
- **The CLI is read-and-fetch oriented.** It can browse and pull. It cannot
  *transact* (buy, manage billing, manage seats) or *manage infrastructure*
  (cloud destinations). Whether that's intentional is a product call (§6).

---

## 6. Recommended additions, ranked

**Tier 1 — close the "I paid, now what" hole**

1. **`verlet datasets library`** (G-P1) — list owned/purchased datasets via
   `GET /downloads/library`. Mirror `bundles list`. Highest value, low cost.
2. **`verlet datasets push --to s3://...` / `gs://...`** (G-P4) — wire the
   existing `destinations` API. Add `verlet destinations add/list/test` and
   `verlet destinations connect` for the OAuth-style flow. Enterprise table
   stakes.
3. **Showcase quota in `info` / `status`** (G-S1) — surface
   `downloads used / quota` and grant expiry so a prospect isn't surprised
   by a `429`.

**Tier 2 — make datasets inspectable before the download**

4. **`verlet datasets episodes <slug>`** (G-P7) — list episodes/segments so
   `--episode-ids` is actually usable.
5. **`verlet datasets quality <slug>` / `analytics <slug>`** (G-P6) — QC
   distributions + analytics for pre-purchase / pre-download vetting.
6. **`verlet datasets jobs --slug <slug>`** (G-P5) — per-dataset conversion
   listing; the endpoint already exists. Fix the no-arg dead-end message.
7. **`verlet showcase stats`** (G-S3) — expose `operation-stats`; cheap
   credibility numbers for the sales funnel.

**Tier 3 — transactional surface (product decision required)**

8. **`verlet purchase` / `verlet billing` / `verlet subscription`**
   (G-P2/3/8) — only if the product intends the CLI to be a transactional
   surface. If checkout stays web-only by design, document that explicitly
   in `--help` and `README` so the absence reads as intentional, not as a
   missing feature.

**Tier 4 — cleanup**

9. ~~Remove `verlet pull` and `verlet login` shims.~~ **Done in 0.9.0** —
   alongside the hidden `verlet ego` stub.
10. Add a `verlet showcase request-access` (or a structured `404` footer)
    so the "contact your rep" path is actionable (G-S4).

---

## 7. Endpoint coverage matrix

Legend: ✅ used by CLI · ⚠️ partially · ❌ exists, unused.

| Backend area | Endpoint family | CLI status |
|--------------|-----------------|-----------|
| Showcase | `POST /showcase/auth` | ✅ |
| Showcase | `GET /showcase/datasets` (+`/{id}`, `/{id}/download`) | ✅ |
| Showcase | `GET /showcase/operation-stats` | ❌ G-S3 |
| Platform catalog | `datasets` list / detail | ✅ |
| Platform catalog | `stats` | ❌ G-P9 |
| Platform catalog | `ego-quality`, `*-qc-*`, `analytics`, `trajectory` | ❌ G-P6 |
| Platform catalog | `episodes`, `segments`, `segment/{id}`, `playback-meta` | ❌ G-P7 |
| Platform catalog | `datasets/{slug}/samples/download` | ❌ no consumer since 0.9.0 removed `pull` |
| Platform catalog | `research-bundles` list/detail | ✅ (bundles) |
| Platform catalog | `hero-reel`, `public-feature-flags` | ❌ |
| Downloads | `{slug}/manifest`, `ego/.../manifest` | ✅ |
| Downloads | `{slug}/convert`, `jobs/{id}` | ✅ |
| Downloads | `{slug}/conversions` (per-dataset list) | ❌ G-P5 |
| Downloads | `{slug}/push`, `pushes/recent` | ✅ (HF only) |
| Downloads | `library`, `library/bundles/.../expand`, `library/.../manifest` | ❌ G-P1 |
| Downloads | `destinations` CRUD + connect + CFN template | ❌ G-P4 |
| Downloads | `{slug}/episodes` | ❌ G-P7 |
| Purchases | `checkout`, `confirm`, list, `billing/*` | ❌ G-P2/3 |
| Cart | all | ❌ |
| Subscriptions | `plans`, `current`, `usage`, `seats` | ❌ G-P8 |
| Auth | device flow, showcase, PATs, `tokens set hf` | ✅ |
