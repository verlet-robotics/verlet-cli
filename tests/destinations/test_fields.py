"""Unit tests for the static per-provider field-hint fallback.

Backstop for the day backend stops returning ``manual_fields=null``: a
provider listed here must keep its keys in lockstep with the backend
adapter's actual credential reads. If a provider's keys drift, the
``add`` command silently breaks at the test-connection step.
"""
from __future__ import annotations

from verlet.destinations._fields import (
    FALLBACK_FIELDS,
    JSON_ONLY,
    fallback_summary,
    field_keys,
)


def test_aws_s3_fields_match_adapter_reads():
    assert field_keys("aws_s3") == ["access_key_id", "secret_access_key"]


def test_r2_fields_match_adapter_reads():
    assert field_keys("r2") == [
        "account_id",
        "access_key_id",
        "secret_access_key",
    ]


def test_huggingface_fields_match_adapter_reads():
    assert field_keys("huggingface") == ["token"]


def test_gcs_is_json_only():
    """GCS credentials are a single service-account JSON blob; per-field
    decomposition is wrong for that adapter shape."""
    assert FALLBACK_FIELDS["gcs"] == JSON_ONLY
    assert field_keys("gcs") == []


def test_unknown_provider_returns_empty():
    assert field_keys("turing_complete_storage") == []
    assert fallback_summary("turing_complete_storage") == ""


def test_fallback_summary_gcs_points_at_json_flag():
    """The providers table renders this string verbatim — GCS users must
    see the JSON flag hint, not a list of bogus keys."""
    assert fallback_summary("gcs") == "--credentials-json <sa.json>"


def test_fallback_summary_per_field_providers_lists_keys():
    assert fallback_summary("r2") == "account_id, access_key_id, secret_access_key"
    assert fallback_summary("huggingface") == "token"


def test_secret_flag_set_on_secret_fields():
    """Prompts must hide input for any field carrying a credential value
    (everything except the AWS Access Key ID and R2's account_id which are
    not themselves the secret)."""
    secrets = {
        "aws_s3": {"secret_access_key"},
        "r2": {"secret_access_key"},
        "huggingface": {"token"},
    }
    for provider, expected_secret_keys in secrets.items():
        fields = FALLBACK_FIELDS[provider]
        assert isinstance(fields, list)
        actually_secret = {
            f["key"] for f in fields if f.get("secret")
        }
        assert actually_secret == expected_secret_keys, provider
