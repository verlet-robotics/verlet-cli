# Changelog

All notable changes to the `verlet` CLI are documented in this file.

The format is loosely based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project follows [Semantic Versioning](https://semver.org/).

## [Unreleased]

## [0.13.0] — 2026-06-03

Showcase downloads can now pull a subset instead of the whole dataset.

### Added

- **`--limit N` on `verlet datasets download` (showcase).** Download just the
  first N units — episodes for teleop, segments for ego — in stable order
  instead of the entire dataset. Folded together with the grant's remaining
  quota (whichever is smaller binds), applies to the `samples` preview too,
  and only the served units are charged against quota. Platform accounts are
  pointed at `--episode-ids` / `--segment-ids` instead. Pairs with a backend
  change that accepts a `limit` query param on the gated download endpoint.

## [0.12.0] — 2026-06-02

A proactive "a newer verlet is available" notice, plus a synchronous
`verlet update --check`. Pairs with a backend change that requires the
showcase JWT to carry an email before the dataset download endpoint will
issue a manifest (the email is the one collected at `verlet auth login
--kind showcase`).

### Added

- **Out-of-date notice.** Any command now prints a one-line upgrade notice to
  **stderr** when a newer release is on PyPI, so it never corrupts stdout
  (JSON manifests, pipes). It reads a cached "latest" version and refreshes
  that cache in a detached background process at most once every 24h, so the
  check never blocks the foreground command. First run on a fresh machine
  prints nothing and silently warms the cache. Opt out with `verlet config
  update-check disable` or `VERLET_NO_UPDATE_CHECK=1`.
- **`verlet update --check`.** Report whether a newer release exists on PyPI
  without upgrading (also warms the notice cache).
- **`verlet config update-check status|enable|disable`.** Manage the
  automatic notice.

## [0.11.0] — 2026-05-31

Showcase login now collects an email so dataset pulls are attributable to a
person, plus download quota-truncation warnings. **Pairs with a backend
change: the showcase `/auth` endpoint now requires an email**, so upgrading
is required to keep `verlet login --kind showcase` working — older clients
get an `HTTP 422`.

### Added

- **Email prompt at showcase login.** `verlet login` / `verlet auth login
  --kind showcase` now prompt for an email (after the access code) and send
  it to `/auth`. The server requires it, so the email lets a (possibly
  shared) customer access code be attributed to the individual pulling data.
  A missing or malformed email surfaces the server's validation message
  instead of a bare `HTTP 422`.
- **Quota-truncation warning on downloads.** When a `--scope full` pull is
  trimmed to a grant's remaining episode quota, the CLI warns before
  downloading so a short result is explained, and the truncated-grant CTA
  points to founders@verlet.co.

### Fixed

- **`active_operators` reflects the 7-day window** in showcase operation
  stats output (was an unscoped count).
- **Docs site no longer breaks on `<placeholder>` tokens or colons in Click
  help strings.** `verlet docs export` now YAML-quotes the frontmatter
  `description` field and backslash-escapes `<word>` / `<digit>` in body
  prose and option-help cells, so Fumadocs / MDX 3 doesn't try to parse
  them as JSX tag openers. Mirrors the same escape pass into a new
  `verlet docs mirror-changelog` subcommand (used by the release workflow)
  so the changelog page survives raw CHANGELOG content like `<24h old`.

## [0.10.2] — 2026-05-23

A tiny render fix paired with a backend change shipping the same day.
No breaking changes.

### Fixed

- **Samples-scope grants render as `free preview` instead of `unlimited`.**
  The grants table in `datasets info <slug>` was showing the same paid
  quota on every expanded row, including the samples-scope rows — making
  one 10-unit grant look like 40 units across four buckets. The backend
  now returns `quota_remaining=null` for samples (samples are an
  unmetered preview that neither check nor decrement the budget), so the
  CLI shows `free preview` on those rows and reserves `unlimited` for
  full-scope grants that really are uncapped.

## [0.10.1] — 2026-05-23

Usability + reliability follow-ups to 0.10.0, all surfaced by hands-on
testing of the showcase user journey. No breaking changes.

### Fixed

- **Showcase ego datasets render and download correctly.** The live
  showcase backend omits the explicit `modality` field on the wire
  (despite the Pydantic schema declaring it), which caused every ego
  dataset to be labelled `(teleop)` in `datasets list` / `info`, the
  segments count to be mis-labelled "episodes", `variants_available`
  to render as `—` even when grants clearly listed `raw` + `processed`,
  and `datasets download <ego-slug>` (with no `--variant`) to leak the
  raw FastAPI 422 list-of-dicts to stderr. A new `resolve_modality`
  helper now cascades through `modality` → `task_type` →
  `robot_embodiment` → `ego_task_dataset_id`, the renderer derives
  `variants_available` from `effective_grants` when missing, and the
  CLI pre-flight-fetches the detail body before download so it can
  raise `Error: '<slug>' is an ego dataset — --variant is required.
  Your grants cover: <list>.` instead of relaying the 422.
- **Pydantic 422 validation errors no longer leak.** `_format_detail`
  flattens FastAPI's `[{"loc": [...], "msg": "..."}]` list into
  `field: msg; field: msg` form across every call site that goes
  through `friendly_http`, with the source prefix (query/body/path)
  stripped.
- **`datasets list` and `datasets info <ego-slug>` are legible at
  80 cols.** Tables now use Rich's `expand=True` + per-column
  `no_wrap` / `overflow="ellipsis"` / `ratio` / `max_width`, and
  empty Size/Variants/Tiers columns get dropped when no row has data
  — instead of every cell collapsing to `t…` / `C…` / `1…`.
- **`auth status` reports expiry correctly on fresh tokens.** A
  24-hour showcase token issued 30 seconds ago no longer shows up as
  "Expiring soon" — near-expiry thresholds are now per-credential-kind
  (`device_flow=1h`, `showcase_access_code=2h`, others=24h) and the
  hint command matches the profile's kind (e.g. `verlet auth login
  --kind showcase` instead of the device-flow default).
- **Expired tokens surface a kind-aware hint everywhere.** Every
  authed surface (`pull`, `push`, `datasets info / download`, etc.)
  now fails pre-HTTP with `Token expired. Re-authenticate with:
  verlet auth login --kind <kind>` instead of a bare backend error
  string. Catalog browse is anonymous-OK and falls back rather than
  failing on an expired profile.
- **`destinations add` works for AWS deeplink + manual providers
  today.** When the backend returns `manual_fields: null` (a known
  gap), the CLI falls back to a built-in `FALLBACK_FIELDS` table for
  R2 / S3 / HuggingFace (GCS short-circuits to a `--credentials-json`
  hint), so the command prompts for what the provider actually needs
  instead of writing an empty credential blob.

## [0.10.0] — 2026-05-22

Closes the "I paid, now what" gap and makes datasets inspectable before a
download. Seven new capabilities, all additive — no breaking changes.

### Added

- **`verlet datasets library`** — list the datasets and bundles your account
  has purchased (`GET /downloads/library`). Showcase access codes, which own
  no purchases, are rejected with a pointer to `verlet datasets list`.
- **`verlet destinations`** command group — manage cloud push destinations:
  - `destinations list` / `providers` — saved destinations and connectable
    providers.
  - `destinations add <provider>` — dispatches on the provider's auth kind:
    manual providers (R2, etc.) take credentials directly (`--credential
    KEY=VALUE` or `--credentials-json`); AWS S3 runs the CloudFormation
    deeplink flow (opens a browser, paste back the RoleArn). OAuth providers
    are deferred — connect them in the web app for now.
  - `destinations rm <name-or-id>` — delete a saved destination.
- **`verlet datasets push --destination <name-or-id>`** — push a purchased
  dataset to a saved cloud destination. `--to huggingface://…` (ad-hoc HF
  push) is unchanged; exactly one of the two is required.
- **`verlet datasets episodes <slug>` / `segments <slug>`** — browse a
  dataset's episodes/segments, surfacing the indices that
  `download --episode-ids` / `--segment-ids` consume.
- **`verlet datasets quality <slug>` / `analytics <slug>`** — QC-metric
  distributions and aggregate analytics, for vetting a dataset before
  downloading gigabytes.
- **`verlet datasets jobs`** now lists conversion jobs: `--slug <ds>` for one
  dataset, no argument for every job your account has triggered (replacing
  the former "listing not supported" stub).
- **`verlet showcase stats`** — Verlet fleet operation stats (fleet size,
  recent throughput, QC pass rate). Requires a showcase access code.

## [0.9.0] — 2026-05-22

Showcase CLI reconciled with the dataset grant system; deprecated command
surface retired.

### Removed

- **`verlet ego` command group.** Ego data is now served through
  `verlet datasets list / info / download`. The old `verlet ego`
  commands called an ungated catalog endpoint that returned every ego
  segment to any valid access code and exposed internal segment IDs.
  The hidden migration-hint stub has also been removed — `verlet ego …`
  now resolves to the standard "No such command" error.
- **`verlet pull`.** Use `verlet datasets download <slug>` — it routes by
  credential kind to the same gated endpoints. `pull` was a hidden,
  deprecation-warned shim for several releases.
- **`verlet login`** (legacy top-level showcase access-code login). Use
  `verlet auth login --kind showcase`. Hidden and deprecation-warned for
  several releases prior to removal.

### Changed

- **`verlet datasets` routes by credential kind.** Showcase access codes
  (`verlet auth login --kind showcase`) now drive `verlet datasets
  list / info / download` against the gated `/api/v1/showcase/datasets/*`
  endpoints — so the CLI shows only the datasets the access code is
  granted, never internal segment IDs. Platform accounts are unaffected.
- `verlet datasets download` gained `--scope` (showcase-only:
  `samples` | `full`). Showcase downloads are whole-dataset; the
  platform-only flags (`--episode-ids`, `--segment-ids`, `--format`,
  `--detach`) are rejected for showcase credentials.

## [0.8.7] — 2026-05-21

Showcase CLI fix.

### Fixed

- **Showcase commands now reach the API.** `verlet auth login --kind
  showcase`, `verlet ego list` / `info` / `download`, and `verlet pull`
  all addressed `/api/v1/ego/showcase/*`, but the backend moved the
  entire showcase surface to its own service at `/api/v1/showcase/*` —
  so every showcase request returned HTTP 404. Updated the three stale
  path constants (`auth/showcase.py`, `ego/catalog.py`,
  `pull/commands.py`) to the new prefix. No flag or behavior changes.

## [0.8.6] — 2026-05-11

CI fix. No CLI behavior change.

### Fixed

- **brew-bump auto-PR finally works.** Five consecutive releases
  (v0.8.1 → v0.8.5) failed the brew-bump step with `Unable to
  determine dependencies for "verlet==X.Y.Z"`, leaving every release
  with a manual tap commit as the workaround. Root cause:
  `dawidd6/action-homebrew-bump-formula@v3` wraps
  `brew bump-formula-pr`, which passes
  `--uploaded-prior-to=<now - 24h>` to pip's dry-run resolver to make
  resource bumps reproducible. Every just-tagged version is <24h old
  by definition, so the resolver always filtered it out. Tried
  `livecheck: true` in v0.8.5 — same failure (brew still walks
  resources via the cutoff'd dry-run regardless of the
  version-detection strategy).

  Replaced with a hand-rolled bump job that:

  1. Polls PyPI's JSON API for the just-published sdist (with a
     12×10s wait loop for index propagation).
  2. Reads `urls[].url` and `urls[].digests.sha256` directly — no
     `--uploaded-prior-to` cutoff anywhere on this path.
  3. Patches only the formula's top-level `url` + `sha256` (anchored
     to the 2-space-indented pair via `^  url …\n  sha256 …` regex
     so resource blocks are untouched).
  4. Pushes a `bump/verlet-<version>` branch on the tap and opens a
     PR via `gh pr create`. D-DIST3 invariant preserved: never
     direct-push to the tap's main branch.

  Resources (anyio, certifi, click, h11, httpcore, httpx, idna,
  markdown-it-py, mdurl, pygments, rich, typing-extensions) stay
  pinned by the tap on a separate cadence — maintainer-driven
  `brew bump-formula-pr` calls when a security/compat bump is
  actually needed. The auto-bump job only changes verlet's own
  version.

### Tests

- `tests/test_release_workflow.py` rewritten to assert
  behavior-invariants instead of the now-removed dawidd6 action:
  job runs only on tag push, uses `HOMEBREW_TAP_GITHUB_TOKEN`,
  checks out `verlet-robotics/homebrew-verlet`, calls `gh pr create`,
  does NOT call `git push origin main`, reads from
  `pypi.org/pypi/verlet`, and explicitly verifies the dawidd6 action
  has NOT come back.

## [0.8.5] — 2026-05-11

Internal cleanup. No user-visible behavior change.

### Changed

- **`verlet.ego.catalog` now uses the shared
  `verlet._http_errors.friendly_http` context manager** introduced in
  0.8.4 (and removes its in-module `_raise_http` helper). The three
  ego API helpers — `fetch_ego_catalog`, `presign_ego_asset`,
  `fetch_training_bundle` — go from a try / `except
  httpx.HTTPStatusError` / `except httpx.RequestError` triad each to
  a single `with friendly_http(...)` block, deleting ~12 lines of
  duplicated error handling. End-user error output for `verlet ego
  list` / `verlet ego info` / `verlet ego download` is functionally
  identical (same render path), just sourced from the shared helper.
  Tone aligned to the rest of the CLI: ``Error: fetching ego
  catalog: <detail>`` (was ``Error: Failed to fetch ego catalog:
  <detail>``).

### Verified

- Cuts a real release tag specifically to exercise the
  `livecheck: true` change in `release.yml` (verlet-cli@c430d45). If
  brew-bump opens a PR against `verlet-robotics/homebrew-verlet` this
  time, the auto-update flow is fixed for good. If it still fails on
  the same `--uploaded-prior-to` issue, the fallback plan is a
  hand-rolled bump workflow (Option B per the working notes).

## [0.8.4] — 2026-05-11

UX fix. No new functionality. Continuation of the 0.8.3 traceback
cleanup.

### Fixed

- **API helpers no longer surface ``httpx.HTTPStatusError`` tracebacks
  on 4xx/5xx responses.** ``verlet datasets info <nonexistent-slug>``,
  ``verlet datasets list`` against a misconfigured API, ``verlet
  bundles browse`` during a server outage, etc. previously emitted a
  30-line Python stack ending in
  ``httpx.HTTPStatusError: Client error '404 Not Found' …``. Now they
  print ``Error: fetching dataset 'foo': Dataset 'foo' not found.``
  (or whatever the server's ``{"detail": …}`` envelope contains) on
  stderr and exit 1.

  Implementation: new ``verlet._http_errors.friendly_http(context)``
  context manager — promotes the in-module ``_raise_http`` pattern
  that has lived in ``verlet.ego.catalog`` since the start to a shared
  utility, and applies it across every call site in
  ``verlet.datasets._api`` (5 sites: catalog list/detail, arm manifest,
  conversion-job poll, ego manifest) and ``verlet.bundles._api``
  (3 sites: browse, list, detail). The wrapper also catches
  ``httpx.RequestError`` (DNS / TLS / connection refused / timeouts)
  and renders them as ``Error: Network error <context>: <reason>``.

### Still not covered

- ``verlet.auth.login`` (device-flow polling), ``verlet.auth.tokens``
  (PAT create/revoke), ``verlet.datasets.convert`` and
  ``verlet.datasets.push`` (long-running conversion / push polling),
  and ``verlet.download`` (asset download) each have a few bare
  ``raise_for_status()`` calls left. They sit in flows where the user
  is mid-workflow and would typically benefit from richer per-stage
  error messages — applying the generic wrapper there would be a
  regression in UX detail. Tackled in a future release if real
  reports come in.

## [0.8.3] — 2026-05-11

UX fix. No new functionality.

### Fixed

- **Auth-requiring commands no longer dump a Python traceback when
  no profile exists.** Running any command that builds an
  `AuthenticatedClient` (e.g. `verlet bundles list`,
  `verlet auth tokens list`, `verlet datasets push`) without a
  configured profile previously exited with a 30-line stack trace
  ending in `ProfileNotFoundError: No profile named 'default'…`. Now
  it prints `Error: No profile named 'default' (run `verlet
  --profile default auth login` to create it).` on stderr and exits
  1, uniformly across every subcommand. Implementation:
  `ProfileNotFoundError` now inherits from `click.ClickException`, so
  Click's top-level handler catches it for every command — the two
  ad-hoc `except` blocks in `auth/commands.py` that masked this bug
  for `auth status` and `auth logout` only have been removed.

### Known limitations (not fixed in 0.8.3)

- **Anonymous-OK commands still surface raw `httpx.HTTPStatusError`
  tracebacks on 4xx/5xx responses.** E.g. `verlet datasets info
  <nonexistent-slug>` returns a 404 from the server and the CLI
  prints a traceback ending in
  `httpx.HTTPStatusError: Client error '404 Not Found' …`. This is
  unrelated to the `ProfileNotFoundError` path and is tracked as a
  separate cleanup — wrap each `resp.raise_for_status()` call in a
  helper that converts `HTTPStatusError` to a `click.ClickException`
  with a status-code-aware friendly message.

## [0.8.2] — 2026-05-11

Dependency hygiene release. No CLI behavior changes.

### Changed

- **Drop unused `huggingface_hub` runtime dependency.** The CLI declared
  `huggingface_hub>=0.32` in `pyproject.toml` but did not import it
  anywhere in `src/` (confirmed via
  `grep -rn 'huggingface_hub\|from huggingface' src/` → no matches).
  HuggingFace pushes are server-side via
  `POST /api/platform/v1/downloads/{slug}/push`; the CLI only parses
  `huggingface://org/repo` URLs (regex in `verlet/datasets/_validation.py`)
  and forwards `HF_TOKEN`. The dep pulled in `hf_xet` (a Rust extension
  whose source build fails inside Homebrew's `superenv` because
  superenv strips the `-I` flags `cc-rs` hands clang for `aws-lc-sys`),
  which forced the homebrew-verlet tap into a hand-rolled wheel-pinning
  install. Removing the dep lets the tap revert to the stock
  `virtualenv_install_with_resources` install path.

## [0.8.1] — 2026-05-09

CI/distribution polish release — exercises the full release path
(PyPI → smoke matrices → brew-bump → open-docs-pr) end-to-end after
the v0.8.0 bootstrap. No CLI behavior changes.

### Changed

- **Homebrew install is live.** `brew install verlet-robotics/verlet/verlet`
  installs the CLI from the [`homebrew-verlet`](https://github.com/verlet-robotics/homebrew-verlet)
  tap. Future releases auto-PR formula bumps via
  [`dawidd6/action-homebrew-bump-formula`](https://github.com/dawidd6/action-homebrew-bump-formula),
  gated by `brew test-bot` CI on the tap.

### Fixed

- **CI: bump deprecated GitHub Actions to Node 24 versions** (#1).
  `astral-sh/setup-uv` v3 → v8, `actions/checkout` v4 → v6,
  `actions/upload-artifact` v4 → v7, `actions/download-artifact` v4 → v8,
  `actions/setup-python` v5 → v6. Resolves the `Unexpected input(s)
  'python-version'` warning surfaced by v0.8.0 — v3 silently ignored
  the input, so `uv build` and `uvx verlet` were running against
  whatever Python the runner shipped with. v8 honors the pin.
- **CI: provision `VERLET_SERVER_DOCS_PR_TOKEN` for cross-repo docs PR job.**
  The `open-docs-pr` job in `release.yml` opens PRs against
  `verlet-robotics/verlet-server` to refresh the CLI ref + CHANGELOG
  mirror; the fine-grained PAT for that is now in GH Actions secrets.

## [0.8.0] — 2026-05-08

CLI 1.0 polish release — wires the merged Phase 30 super-phase (formats +
bundles + distribution) end-to-end. Adds server-side format conversion,
HuggingFace push, the full `verlet bundles` command group, install-method-aware
self-update, opt-in version-ping telemetry, and a maintainer-driven
`verlet docs export` MDX generator. Ships the PyPI Trusted Publishing release
workflow with pipx + uvx smoke matrix; **PyPI publish gated on first-run
admin approval — see Distribution notes below**.

### Added (Phase 30 — Format Conversion + HuggingFace Push)

- **`verlet datasets download <slug> --format <fmt>`** — server-side conversion
  to any of 8 formats (`lerobot-v2`, `lerobot-v3`, `hdf5`, `zarr`, `rlds`,
  `rosbag`, `robodm`, `egomimic`). Foreground polling by default;
  `--detach` returns the `job_id` immediately so CI / automation can poll on
  their own cadence. Native (`lerobot-v2`) returns presigned URLs in 1
  round-trip; non-native formats return 202 + job-id and the CLI polls
  `/api/platform/v1/datasets/jobs/{id}` until the manifest is ready.
  (CLIDATA-07)
- **`verlet datasets push <slug> --to huggingface://org/repo`** — push a
  purchased / converted dataset to HuggingFace via the existing server-side
  `/downloads/push` pipeline. HuggingFace token resolved from
  `verlet auth tokens set hf <token>` (preferred) → `HF_TOKEN` env →
  verbatim `NO_HF_TOKEN_MSG` UsageError. (CLIDATA-07)
- **`verlet datasets jobs [<id>]`** — reattach to a specific conversion job's
  polling loop; bare `verlet datasets jobs` short-circuits with a "listing
  endpoint not available" message and 0 HTTP calls (server listing endpoint
  deferred). (CLIDATA-07)

### Added (Phase 30 — `verlet bundles`)

- **`verlet bundles browse`** — anonymous public research-bundle catalog via
  `/api/platform/v1/catalog/ego/research`. No auth required. (CLIBUNDLE-01)
- **`verlet bundles redeem <code>`** — redeem a research-access code; mints
  a `bundle_grant` profile in `~/.verlet/credentials.json` with the
  server-issued time-bounded token. Idempotent: re-redeeming replaces the
  profile entry rather than merging fields, since the prior token is dead the
  moment the server reissues. (CLIBUNDLE-02)
- **`verlet bundles list [--all]`** — list active entitlements (default) or
  every entitlement including expired/revoked (`--all`). Unified
  `GET /api/platform/v1/bundles` merges research grants + purchased bundles
  into one view. (CLIBUNDLE-03)
- **`verlet bundles info <id>`** — bundle detail with included dataset slugs,
  segment categories, format availability, license terms, and citation
  string for research bundles. (CLIBUNDLE-04)
- **`verlet bundles download <id> [--format <fmt>]`** — fan out per-dataset
  downloads of every dataset in a bundle. Processed-only enforced at
  parse-time; `--variant raw` rejected with the verbatim
  `BUNDLES_ARE_PROCESSED_ONLY` message before any HTTP work starts.
  (CLIBUNDLE-05)
- **`verlet bundles export-manifest <id> --out manifest.json`** — emit a
  portable, time-bounded JSON manifest for offline / air-gapped pipelines.
  Fails fast on 202 (conversion enqueued) — air-gapped pipelines need URLs
  not job-ids. (CLIBUNDLE-06)

### Added (Phase 30 — Distribution + Telemetry + Docs)

- **`verlet update`** rewritten install-method-aware. Detects pipx / brew /
  uvx / unknown via `sys.executable` path patterns; runs the correct upgrade
  command per install method. Replaces the broken 0.7.x `pip install
  --upgrade` stub that corrupted pipx envs in some configurations.
  Locale-safe (`LANG=LC_ALL=C.UTF-8` subprocess). (CLIDIST-04)
- **`verlet config telemetry status|enable|disable`** — opt-in version-ping
  telemetry stored in `~/.verlet/config.json`. **Default OFF.** When
  enabled, payload contains only `cli_version`, `python_version`, and
  `platform/arch` shipped via the `User-Agent` header on every CLI → backend
  request. No command names, paths, dataset slugs, or identities ever leave
  the machine. Strict `is True` check guards against truthy-but-not-True
  config values. (CLIDIST-05)
- **`verlet docs export --out <dir>`** — maintainer-driven Click → Fumadocs
  MDX generator. Walks `cli.commands` and emits one MDX per leaf command
  (28 reference pages for the 0.8.0 surface). 6 production commands ship
  `bash recipe` runnable epilogs (`auth login`, `auth tokens create`,
  `datasets download`, `datasets push`, `bundles redeem`, `bundles
  download`); Plan 30-13 recipe-CI lifts those blocks against staging.
  (CLIDIST-06)
- **`verlet auth tokens set hf <token>`** subcommand — store a HuggingFace
  token alongside the active Verlet profile so `verlet datasets push --to
  huggingface://...` resolves the token without `HF_TOKEN` in the env.

### Changed

- `pyproject.toml` version bumped `0.7.0` → `0.8.0`; dependencies
  alphabetized; `huggingface_hub>=0.32` added (kept `>=0.32` floor to
  preserve `hf_xet` automatic-install behavior). `verlet =
  "verlet.cli:cli"` entry point unchanged.
- `verlet auth status` recognizes `kind="bundle_grant"` profiles and
  renders an expiry hint alongside the existing
  `device_flow` / `pat` / `showcase_access_code` kinds.

### Distribution

- **PyPI** (gated): `pipx install verlet` + `uvx verlet --version` — the
  `.github/workflows/release.yml` workflow ships in this commit and uses
  PyPI Trusted Publishing (`pypa/gh-action-pypi-publish@release/v1`,
  `id-token: write`) with a `pipx-smoke` matrix
  (`{macos-latest, ubuntu-latest} × {3.11, 3.12}`) and a `uvx-smoke`
  matrix (`{macos-latest, ubuntu-latest}`). **First publish requires
  human approval at https://pypi.org/manage/project/verlet/settings/publishing/
  and a tag push (`git tag v0.8.0 && git push origin v0.8.0`); deferred
  intentionally — see CLIDIST-01 / CLIDIST-02 in REQUIREMENTS.md.**
  (CLIDIST-01, CLIDIST-02)
- **Homebrew tap**: `brew install verlet-robotics/verlet/verlet` — the
  Homebrew formula lands in Plan 30-13. (CLIDIST-03)

### Privacy

- Telemetry default OFF. When opted in (`verlet config telemetry enable`),
  no command names, paths, dataset slugs, or identities leave the machine.
  Aggregate use is reconstructed from existing access logs only — the CLI
  never sends a separate analytics payload.

### Known limitations / Deferred

- v0.8.0 is **not yet on PyPI**. The git tag `v0.8.0` has not been pushed;
  the first PyPI publish requires admin approval at the PyPI Trusted
  Publishing settings page and is held until the operator is ready to cut
  the public release. To publish: bump nothing further, then run
  `git tag v0.8.0 && git push origin v0.8.0` and approve the workflow's
  first run if PyPI surfaces a prompt.
- `verlet datasets jobs` listing endpoint is deferred server-side; bare
  `verlet datasets jobs` (no id) short-circuits cleanly until the backend
  ships the listing endpoint in a future plan.

## [0.7.0] — 2026-05-07

### Added (Phase 29)

- **`verlet datasets list|info|download`** — unified top-level command group
  hitting the platform-catalog API. Works for both anonymous showcase visitors
  (public catalog) and authenticated platform users (account-restricted rows
  + paid downloads). Auto-detects modality (arm vs ego) from the catalog row
  and dispatches to the correct Phase 27 manifest endpoint. (CLIDATA-04,
  CLIDATA-05, CLIDATA-06)
- `--task`, `--robot`, `--category`, `--since`, `--limit`, `--kind` filters on
  `verlet datasets list`. `--task` and `--robot` are repeatable (Click
  `multiple=True`), e.g. `--task pick-and-place --task push`.
- `--variant raw|processed` (REQUIRED for ego rows; rejected for arm rows),
  `--episode-ids` (raw + arm only), `--segment-ids` (processed only),
  `--format lerobot-v2` (native, arm only in 0.7.0), `--parallel`, `--resume`,
  `--dry-run`, `--force`, `--output` on `verlet datasets download`.
- `--json` machine-readable output on `verlet datasets list` and
  `verlet datasets info`. Direct `CatalogDatasetListItem` /
  `CatalogDatasetDetail` Pydantic dump from the server response.
- Pre-flight flag-validation matrix (`verlet/datasets/_validation.py`) —
  surfaces clear error messages before any HTTP call (e.g. "--variant is
  ego-only; this is a teleop dataset").
- Anonymous browse: `verlet datasets list` and `verlet datasets info` work
  without an active profile against public catalog rows. Authenticated calls
  additionally see Phase 19 account-restricted rows. `verlet datasets
  download` always requires auth (`verlet auth login` hint on no-profile).

### Removed (BREAKING)

- **`verlet teleop` command group** — replaced by `verlet datasets`. No
  deprecation shim, no stderr warning, no removal-version planning.
  Pre-users-no-deprecation principle: the project is pre-users; the cost of
  carrying compatibility code outweighed the (non-existent) compatibility
  audience. Run `verlet datasets list --kind teleop` for the arm-only view.
  Files deleted: `verlet/teleop/__init__.py`, `verlet/teleop/commands.py`,
  `verlet/teleop/catalog.py`. Reference removed from `verlet/cli.py`.

### Changed

- Backend: `GET /api/platform/v1/catalog/datasets` now accepts
  `?since=<iso-8601>` filtering on `published_at` (Phase 29 D-FL5 backend
  extension; ships in the same release window). Required because the
  `--since 2026-04-01` recipe needs server-side filtering to keep
  pagination semantics correct.

### Notes for Upgrading

- `pip install -U verlet` to refresh the entry-points cache. After upgrade,
  `verlet teleop --help` exits with Click's `Error: No such command 'teleop'.`
  (status 2) — this is intentional, not a regression.
- Non-native formats (`hdf5`, `zarr`, `rlds`, `rosbag`, `robodm`,
  `egomimic`, `lerobot-v3`) print "format X requires the Phase 30
  conversion engine — coming soon" and exit cleanly. Phase 30 wires the
  rosetta engine end-to-end.
- Slug-primary identity: pass dataset slugs (e.g. `pick-and-place-yam-v3`)
  to `verlet datasets info|download`. Full UUIDs still work as a fallback
  for scripts; 8-character ID-prefix matching from the old
  `verlet teleop info` is dropped (slug-first model from Phase 22+).

## 0.6.0 — 2026-05-07

### Added

- `verlet auth` Click group with `login | logout | tokens | status` subcommands.
- OAuth 2.0 Device Authorization Grant (RFC 8628) login via `verlet auth login`
  (8h JWT + 7d refresh token, `--no-browser` for headless / CI environments).
- Personal Access Token (PAT) management via `verlet auth tokens create | list | revoke | show`.
  Plaintext is displayed exactly once at mint with a yellow `SAVE THIS NOW` warning;
  `list` and `show` never echo plaintext (defensive assertion in `list_pats`).
- Multi-profile credentials at `~/.verlet/credentials.json` with a `kind`
  discriminator (`device_flow`, `pat`, `showcase_access_code`). Three coequal
  kinds in one file — `verlet auth status` enumerates whichever is active.
- Global `--profile` option and `VERLET_PROFILE` environment variable.
  Precedence: `--profile` flag → `VERLET_PROFILE` env → `default_profile`
  field in `credentials.json` → literal `"default"`.
- `verlet auth status` with kind-aware text output (device_flow / pat /
  showcase_access_code), `--json` mode for CI scripting, `--refresh` to
  re-probe `/auth/me` and update cached identity. Within 24h of expiry:
  yellow near-expiry warning. Past expiry: red EXPIRED line + exit code 1.
- Central `verlet/api_client.py` injects `Authorization: Bearer …` for all
  CLI HTTP calls and runs opportunistic refresh on `kind=device_flow`
  profiles when the access token is within 5 minutes of expiry.
- POSIX mode-0600 enforcement on `~/.verlet/credentials.json` with a
  bad-permission warning on every read (no-op on Windows; relies on
  inherited `%USERPROFILE%` NTFS ACL — matches `gh` / `aws` CLI behavior).

### Changed

- `verlet ego catalog` and `verlet teleop catalog` now use
  `api_client.auth_headers_for_profile()` instead of inline
  `Authorization: Bearer …` construction. Any of the three profile kinds
  routes through the same `Bearer` header dispatch the backend's middleware
  in `core/domains/client_user/middleware.py` already understands.
- Legacy `~/.verlet/token.json` is migrated losslessly into the `default`
  profile under `kind=showcase_access_code` on first 0.6.0 invocation. The
  legacy file is left in place (so a downgrade still works); it can be
  deleted manually after the new file has settled.

### Deprecated

- Top-level `verlet login` (showcase access-code flow) is now a thin shim
  into `verlet auth login --kind showcase` and prints a one-line stderr
  deprecation hint:
  `DEPRECATED: \`verlet login\` will be removed in 0.7.0. Use \`verlet auth
  login --kind showcase\` instead.`
  The shim will be removed in 0.7.0.

### Locked vocabulary

- PAT scopes: `read:catalog`, `read:datasets`, `read:ego_segments`,
  `read:account`, `read:purchases`, `write:push`, `write:tokens`. The CLI
  validates client-side against this 7-element set and rejects unknown
  scopes before any HTTP call. Mirrors
  `backend/core/domains/personal_access_token/schema.py:17-37` exactly; a
  future scope addition requires touching both.

### Known limitations

- No server-side device-flow session revoke endpoint exists yet. `verlet
  auth logout` on a `kind=device_flow` profile clears credentials locally
  but the issued JWT continues to work until natural expiry. Tracked as a
  follow-up backend issue.
- Windows credentials.json relies on the inherited `%USERPROFILE%` NTFS
  ACL; explicit ACL hardening via `icacls` is deferred to a future
  release. POSIX users get explicit `chmod 0600` after every write.
- `verlet auth status` defaults to offline (file-only, no network). Use
  `--refresh` to re-probe `/auth/me` for `device_flow` and `pat` profiles.
  Showcase JWTs intentionally never call `/auth/me` (the server rejects
  `type=showcase` JWTs at that endpoint per Research §1.4).

## 0.4.0

Last release before the Phase 28 auth refactor. Single-profile showcase
access-code flow at `~/.verlet/token.json`; `verlet ego` and `verlet
teleop` subcommands operate against the showcase JWT only. See git
history for details.
