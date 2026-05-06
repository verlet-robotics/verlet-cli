"""Phase 28 — verlet auth package.

Modules:
    credentials  — kind-discriminated multi-profile JSON store at ~/.verlet/credentials.json
    profiles     — --profile / VERLET_PROFILE / default_profile resolution
    migration    — lossless one-shot move of legacy ~/.verlet/token.json into the
                   default profile under kind=showcase_access_code
"""
