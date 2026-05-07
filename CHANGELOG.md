# Changelog

All notable changes to the `verlet` CLI are documented in this file.

The format is loosely based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project follows [Semantic Versioning](https://semver.org/).

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
