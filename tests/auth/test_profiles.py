"""CLIAUTH-08 — multi-profile resolution + isolation."""

import click
import pytest

from verlet.auth import credentials as creds
from verlet.auth.profiles import (
    ProfileNotFoundError,
    require_profile,
    resolve_profile_name,
)


def test_env_var_precedence(tmp_home, monkeypatch):
    """When ctx.obj["profile"] is None (neither flag nor env), default applies.

    Click's ``envvar=`` already lifts ``VERLET_PROFILE`` into the option value
    at the cli.py boundary, so by the time the resolver sees ``ctx_flag_value``
    it is either the user-typed string (flag), the env var string, or ``None``
    (neither set). The resolver only needs to handle the post-Click value.
    """
    monkeypatch.setenv("VERLET_PROFILE", "ci")
    # Post-Click value of "ci" → resolver returns "ci".
    assert resolve_profile_name("ci") == "ci"

    # No env, no flag, no file → literal "default".
    monkeypatch.delenv("VERLET_PROFILE", raising=False)
    assert resolve_profile_name(None) == "default"


def test_flag_beats_env(tmp_home, monkeypatch, cli_runner):
    """End-to-end through Click: --profile ci with VERLET_PROFILE=prod -> 'ci'."""
    monkeypatch.setenv("VERLET_PROFILE", "prod")

    @click.group()
    @click.option("--profile", default=None, envvar="VERLET_PROFILE")
    @click.pass_context
    def root(ctx, profile):
        ctx.ensure_object(dict)
        ctx.obj["profile"] = profile

    @root.command()
    @click.pass_context
    def echo(ctx):
        click.echo(resolve_profile_name(ctx.obj.get("profile")))

    # Flag wins over env.
    result = cli_runner.invoke(root, ["--profile", "ci", "echo"])
    assert result.exit_code == 0, result.output
    assert result.output.strip() == "ci"

    # No flag → env wins.
    result2 = cli_runner.invoke(root, ["echo"])
    assert result2.exit_code == 0, result2.output
    assert result2.output.strip() == "prod"


def test_profile_isolation(tmp_home, cli_runner):
    """Two named profiles in the same file must not contaminate each other."""
    creds.upsert_profile(
        "default",
        kind="device_flow",
        access_token="jwt_default",
        api_url="https://api.verlet.co",
    )
    creds.upsert_profile(
        "ci",
        kind="pat",
        access_token="pat_a_b",
        api_url="https://api.verlet.co",
    )
    assert require_profile("default")["access_token"] == "jwt_default"
    assert require_profile("ci")["access_token"] == "pat_a_b"
    with pytest.raises(ProfileNotFoundError):
        require_profile("does-not-exist")
