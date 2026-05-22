"""Verlet CLI entry point."""
from importlib.metadata import PackageNotFoundError, version as _pkg_version

import click

try:
    __version__ = _pkg_version("verlet")
except PackageNotFoundError:
    __version__ = "0.0.0+unknown"


@click.group()
@click.version_option(version=__version__, prog_name="verlet")
@click.option(
    "--profile",
    default=None,
    envvar="VERLET_PROFILE",
    help="Named credential profile (default: 'default').",
)
@click.pass_context
def cli(ctx: click.Context, profile: str | None) -> None:
    """Verlet Data CLI — download ego and teleop datasets."""
    ctx.ensure_object(dict)
    ctx.obj["profile"] = profile

    # Best-effort one-shot legacy migration. Never block the CLI on failure —
    # users with a healthy ~/.verlet/credentials.json should not pay for a
    # corrupt or unreadable legacy ~/.verlet/token.json.
    try:
        from verlet.auth.migration import migrate_legacy_token_json

        migrate_legacy_token_json()
    except Exception as exc:  # pragma: no cover — defensive guard
        import sys

        sys.stderr.write(f"warning: legacy migration skipped: {exc}\n")


@cli.command(
    "login",
    help="DEPRECATED: use `verlet auth login --kind showcase`.",
)
@click.option("--api-url", default=None, help="Override API URL")
@click.pass_context
def legacy_login(ctx: click.Context, api_url: str | None) -> None:
    """Legacy showcase access-code login — deprecation shim into showcase_login.

    Removed in 0.7.0 per Research §13.4. The shim prints a one-line stderr
    deprecation hint and routes the call into the same showcase_login()
    helper that ``verlet auth login --kind showcase`` uses, so 0.5.x users
    keep working without code changes.
    """
    import sys

    from verlet.auth.credentials import load_credentials
    from verlet.auth.profiles import resolve_profile_name
    from verlet.auth.showcase import showcase_login

    sys.stderr.write(
        "DEPRECATED: `verlet login` will be removed in 0.7.0. "
        "Use `verlet auth login --kind showcase` instead.\n"
    )

    profile_name = resolve_profile_name(ctx.obj.get("profile"))
    # Resolve api_url: flag wins, else active profile's api_url, else default.
    doc = load_credentials()
    existing = doc["profiles"].get(profile_name, {})
    resolved_api_url = (
        api_url or existing.get("api_url") or "https://api.verlet.co"
    )
    showcase_login(api_url=resolved_api_url, profile_name=profile_name)


# ---------------------------------------------------------------------------
# verlet config telemetry status|enable|disable — CLIDIST-05 / D-DIST1.
#
# Local CLI config (separate from credentials.json). The flag persists in
# ``~/.verlet/config.json`` with mode 0o600. Default = OFF. The User-Agent
# header on every CLI -> backend request is bare ``verlet-cli/<version>``
# until enabled, after which it includes ``python/<py> <os>/<arch>``.
# ---------------------------------------------------------------------------


@click.group("config")
def config_group() -> None:
    """Local CLI configuration (separate from credentials)."""


@config_group.group("telemetry")
def telemetry_cmd() -> None:
    """Manage opt-in version telemetry."""


@telemetry_cmd.command("status")
def telemetry_status() -> None:
    """Print 'enabled' or 'disabled' (default: disabled)."""
    from verlet.telemetry import telemetry_enabled

    click.echo("enabled" if telemetry_enabled() else "disabled")


@telemetry_cmd.command("enable")
def telemetry_enable_cmd() -> None:
    """Opt in: every CLI request will send python/os/arch in the User-Agent."""
    from verlet.config import load_config, save_config

    cfg = load_config()
    cfg["telemetry_enabled"] = True
    save_config(cfg)
    click.echo("Telemetry enabled. User-Agent will include python/os/arch.")


@telemetry_cmd.command("disable")
def telemetry_disable_cmd() -> None:
    """Opt out: User-Agent reverts to bare 'verlet-cli/<version>'."""
    from verlet.config import load_config, save_config

    cfg = load_config()
    cfg["telemetry_enabled"] = False
    save_config(cfg)
    click.echo("Telemetry disabled.")


# ---------------------------------------------------------------------------
# verlet docs export -- Plan 30-11 / CLIDIST-06.
#
# Maintainer-facing walker that regenerates the Phase 35 MDX reference tree
# from the live Click command tree. Lives under a sibling ``docs`` group so
# future doc-related utilities (e.g. linkcheck, recipe-extract) can join the
# same surface without polluting the top-level command list.
# ---------------------------------------------------------------------------


@click.group("docs")
def docs_group() -> None:
    """Documentation utilities (maintainer)."""


from verlet.docs_export import docs_export  # noqa: E402

docs_group.add_command(docs_export)


# ---------------------------------------------------------------------------
# `verlet ego` — REMOVED. The ego command group was retired when the showcase
# CLI was reconciled with the grant system: ego data is now served through
# `verlet datasets`, which routes showcase access codes to the gated
# `/api/v1/showcase/datasets/*` endpoints. A hidden stub stays registered so
# scripted `verlet ego …` calls fail loudly with a migration hint instead of
# a bare "No such command". Drop in a future release.
# ---------------------------------------------------------------------------


@cli.command(
    "ego",
    hidden=True,
    context_settings={"ignore_unknown_options": True},
)
@click.argument("args", nargs=-1, type=click.UNPROCESSED)
def ego_removed(args: tuple[str, ...]) -> None:
    """REMOVED — use `verlet datasets` instead."""
    raise click.ClickException(
        "`verlet ego` was removed. Ego data is now served through "
        "`verlet datasets`:\n"
        "  verlet ego list       ->  verlet datasets list\n"
        "  verlet ego info ID    ->  verlet datasets info <slug>\n"
        "  verlet ego download   ->  verlet datasets download <slug>\n"
        "Downloads are now per-dataset (by slug), not per-segment."
    )


# Register subcommand groups
from verlet.auth.commands import auth_group  # noqa: E402
from verlet.bundles import bundles_group  # noqa: E402
from verlet.datasets import datasets_group  # noqa: E402
from verlet.pull import pull_command  # noqa: E402
from verlet.update import update as update_command  # noqa: E402

cli.add_command(auth_group)
cli.add_command(bundles_group)
cli.add_command(config_group)
cli.add_command(datasets_group)
cli.add_command(docs_group)
cli.add_command(pull_command)
cli.add_command(update_command)


if __name__ == "__main__":
    cli()
