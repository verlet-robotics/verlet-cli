"""Per-provider manual-credential field hints — backend fallback.

``ProviderInfoResponse.manual_fields`` is intended to drive prompt-based
credential entry. As of 2026-05-23 the backend returns ``null`` for every
provider (the adapters know their credential shape but it isn't surfaced
on the public ``GET /destinations/providers`` response). To keep
``verlet destinations add`` usable today, the CLI carries a static
fallback derived from each adapter's actual credential reads — verified
against ``backend/core/cloud_destinations/providers/``:

* ``aws_s3``      — ``access_key_id`` + ``secret_access_key`` (manual mode;
                    deeplink mode is separately driven by the connect flow)
* ``r2``          — ``account_id`` + ``access_key_id`` + ``secret_access_key``
* ``huggingface`` — ``token``
* ``gcs``         — the entire service-account JSON document is the
                    credential blob (no per-field decomposition), so the
                    CLI points users at ``--credentials-json`` instead of
                    prompting field-by-field.

The moment the backend starts populating ``manual_fields``, those win —
this table is a fallback only.
"""
from __future__ import annotations

# Sentinel: this provider's credentials are a single JSON document
# (currently only GCS — paste a service-account JSON).
JSON_ONLY = "json_only"

# A field spec is a dict {"key": str, "label": str, "secret"?: bool}.
# ``secret`` defaults to False; when True the prompt hides input.
FALLBACK_FIELDS: dict[str, "list[dict] | str"] = {
    "aws_s3": [
        {"key": "access_key_id", "label": "AWS Access Key ID"},
        {"key": "secret_access_key", "label": "AWS Secret Access Key", "secret": True},
    ],
    "r2": [
        {"key": "account_id", "label": "Cloudflare Account ID"},
        {"key": "access_key_id", "label": "R2 Access Key ID"},
        {
            "key": "secret_access_key",
            "label": "R2 Secret Access Key",
            "secret": True,
        },
    ],
    "huggingface": [
        {
            "key": "token",
            "label": "HuggingFace Access Token (hf_…)",
            "secret": True,
        },
    ],
    "gcs": JSON_ONLY,
}


def field_keys(provider_name: str) -> list[str]:
    """Return the credential keys expected for a per-field-style provider.

    Empty list for JSON_ONLY providers or unknown providers — the caller
    decides how to surface either case.
    """
    spec = FALLBACK_FIELDS.get(provider_name)
    if isinstance(spec, list):
        return [f["key"] for f in spec]
    return []


def fallback_summary(provider_name: str) -> str:
    """One-line summary of credentials needed; rendered in `providers` table.

    Returns an empty string when the provider is unknown — callers print
    an em-dash, matching the rest of the renderer conventions.
    """
    spec = FALLBACK_FIELDS.get(provider_name)
    if spec == JSON_ONLY:
        return "--credentials-json <sa.json>"
    if isinstance(spec, list):
        return ", ".join(f["key"] for f in spec)
    return ""
