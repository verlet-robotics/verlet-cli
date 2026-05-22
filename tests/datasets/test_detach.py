"""Plan 30-06 Task 1 — `verlet datasets download <slug> --format <fmt> --detach`.

D-FORMAT1 detach behavior contract:

  * `--detach` requires `--format` (foreground native is meaningless to detach).
  * `--detach` against a 200 (native) manifest is an error (no job to detach
    from — the file is already ready).
  * `--detach` against a 202 (job_id) manifest prints `job_id=<id>` and exits 0
    immediately, with NO further polling and NO download.
  * `--detach --quiet` outputs ONLY the job_id (one line on stdout, nothing else).

The four behavior tests below exercise the wiring end-to-end through Click's
CliRunner with respx-mocked manifest endpoints. ``--dry-run`` is explicitly
NOT used here — the contract is "exit 0 before any download driver fires,"
not "use the dry-run short-circuit."
"""
from __future__ import annotations

from verlet.auth.credentials import upsert_profile

from tests.conftest import combined_output


def _seed_default_profile() -> None:
    upsert_profile(
        "default",
        kind="device_flow",
        api_url="https://api.verlet.co",
        access_token="jwt.access.value",
        refresh_token="rt",
        expires_at="2099-01-01T00:00:00+00:00",
        identity={"display_name": "Jane", "email": "jane@x.com"},
    )


def _arm_detail(slug: str = "pick-and-place-yam-v3") -> dict:
    return {
        "id": "00000000-0000-0000-0000-000000000001",
        "slug": slug,
        "title": "Pick and Place YAM v3",
        "data_tiers": ["processed"],
        "available_variants": ["processed"],
    }


def test_detach_with_format_hdf5_prints_job_id_and_exits_zero(
    cli_runner, respx_mock, tmp_home,
):
    """`--format hdf5 --detach` — manifest endpoint 202+job_id; CLI prints
    job_id=<id> and exits 0 immediately. NO poll endpoint hit. NO download
    driver invoked."""
    from verlet.cli import cli

    _seed_default_profile()

    slug = "pick-and-place-yam-v3"
    respx_mock.get(
        f"https://api.verlet.co/api/platform/v1/catalog/datasets/{slug}",
    ).respond(200, json=_arm_detail(slug))
    # Manifest returns 202 + job_id.
    respx_mock.get(
        f"https://api.verlet.co/api/platform/v1/downloads/{slug}/manifest",
    ).respond(
        202,
        json={
            "job_id": "job-detach-1",
            "status": "processing",
            "poll_url": "/api/platform/v1/downloads/jobs/job-detach-1",
        },
    )
    # NB: NO /downloads/jobs/ stub registered — if the CLI polls the job, the
    # respx unmatched-route assertion will fail the test.

    result = cli_runner.invoke(
        cli,
        ["datasets", "download", slug, "--format", "hdf5", "--detach"],
    )

    assert result.exit_code == 0, result.output
    # job_id should appear in output.
    assert "job-detach-1" in result.output, result.output

    # No /downloads/jobs/ calls — the detach short-circuit fires before polling.
    poll_calls = [
        c for c in respx_mock.calls
        if "/downloads/jobs/" in c.request.url.path
    ]
    assert poll_calls == [], (
        f"--detach must NOT poll the jobs endpoint; "
        f"got {[c.request.url.path for c in poll_calls]}"
    )


def test_detach_without_format_raises_usage_error(
    cli_runner, respx_mock, tmp_home,
):
    """`--detach` with no `--format` → Click usage error (exit 2).

    Foreground native (no --format, no conversion) is meaningless to detach
    from — there is no server-side job to background.
    """
    from verlet.cli import cli

    _seed_default_profile()

    slug = "pick-and-place-yam-v3"

    result = cli_runner.invoke(
        cli, ["datasets", "download", slug, "--detach"],
    )
    # Click UsageError → exit 2.
    assert result.exit_code == 2, result.output
    combined = combined_output(result)
    assert "--detach requires --format" in combined, combined

    # Zero HTTP — guard fires before any network call.
    assert len(respx_mock.calls) == 0, (
        f"--detach without --format must short-circuit before HTTP, "
        f"got {len(respx_mock.calls)} calls"
    )


def test_detach_with_native_200_response_errors(
    cli_runner, respx_mock, tmp_home,
):
    """Manifest endpoint returns 200 (native, no conversion job needed) +
    `--detach` — error: "no conversion job to detach from; native format ready".

    The user passed a format that the server treated as native (e.g. lerobot-v2
    on an arm row that already lives in lerobot-v2). There is no async job to
    background, so detaching is meaningless.
    """
    from verlet.cli import cli

    _seed_default_profile()

    slug = "pick-and-place-yam-v3"
    respx_mock.get(
        f"https://api.verlet.co/api/platform/v1/catalog/datasets/{slug}",
    ).respond(200, json=_arm_detail(slug))
    # 200 native manifest (no job_id).
    respx_mock.get(
        f"https://api.verlet.co/api/platform/v1/downloads/{slug}/manifest",
    ).respond(
        200,
        json={
            "dataset_id": "00000000-0000-0000-0000-000000000001",
            "dataset_slug": slug,
            "format": "lerobot-v2",
            "files": [],
        },
    )

    result = cli_runner.invoke(
        cli,
        [
            "datasets",
            "download",
            slug,
            "--format",
            "lerobot-v2",
            "--detach",
        ],
    )
    # Should fail (not exit 0).
    assert result.exit_code != 0, result.output
    combined = combined_output(result)
    assert "no conversion job to detach from" in combined, combined


def test_detach_quiet_prints_only_job_id(
    cli_runner, respx_mock, tmp_home,
):
    """`--detach --quiet` outputs ONLY the job_id (one line on stdout, nothing
    else — no Rich formatting, no reattach hint)."""
    from verlet.cli import cli

    _seed_default_profile()

    slug = "pick-and-place-yam-v3"
    respx_mock.get(
        f"https://api.verlet.co/api/platform/v1/catalog/datasets/{slug}",
    ).respond(200, json=_arm_detail(slug))
    respx_mock.get(
        f"https://api.verlet.co/api/platform/v1/downloads/{slug}/manifest",
    ).respond(
        202,
        json={
            "job_id": "job-detach-quiet",
            "status": "processing",
            "poll_url": "/api/platform/v1/downloads/jobs/job-detach-quiet",
        },
    )

    result = cli_runner.invoke(
        cli,
        [
            "datasets",
            "download",
            slug,
            "--format",
            "hdf5",
            "--detach",
            "--quiet",
        ],
    )

    assert result.exit_code == 0, result.output
    # In --quiet mode, stdout should be JUST the job_id, possibly with a trailing
    # newline. Strip + check the only non-empty line equals the job_id.
    stdout_lines = [
        line for line in (result.output or "").splitlines() if line.strip()
    ]
    assert stdout_lines == ["job-detach-quiet"], (
        f"--detach --quiet must emit only the job_id; got {stdout_lines!r}"
    )
