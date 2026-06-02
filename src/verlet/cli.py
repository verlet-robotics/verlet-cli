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

    # Best-effort "a newer verlet is available" notice. Reads a cached latest
    # version and prints to stderr only; refreshes the cache in the background
    # when stale. Never blocks, never touches stdout. Skipped for `verlet
    # update` itself (the user is already upgrading) and fully self-guarded.
    if ctx.invoked_subcommand != "update":
        try:
            from verlet.version_check import notify_if_outdated

            notify_if_outdated()
        except Exception:  # pragma: no cover — defensive guard
            pass


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
# verlet config update-check status|enable|disable — CLIDIST-04 (alert half).
#
# Opt-out switch for the proactive "a newer verlet is available" notice. The
# notice is ON by default; this persists ``update_check_enabled`` in
# ``~/.verlet/config.json``. The env var ``VERLET_NO_UPDATE_CHECK`` overrides
# this for one-off / CI suppression without touching config.
# ---------------------------------------------------------------------------


@config_group.group("update-check")
def update_check_cmd() -> None:
    """Manage the automatic 'newer version available' notice."""


@update_check_cmd.command("status")
def update_check_status() -> None:
    """Print 'enabled' or 'disabled' (default: enabled)."""
    from verlet.version_check import check_disabled

    click.echo("disabled" if check_disabled() else "enabled")


@update_check_cmd.command("enable")
def update_check_enable_cmd() -> None:
    """Opt in (default): show a notice when a newer release is on PyPI."""
    from verlet.config import load_config, save_config

    cfg = load_config()
    cfg["update_check_enabled"] = True
    save_config(cfg)
    click.echo("Update check enabled.")


@update_check_cmd.command("disable")
def update_check_disable_cmd() -> None:
    """Opt out: never show the 'newer version available' notice."""
    from verlet.config import load_config, save_config

    cfg = load_config()
    cfg["update_check_enabled"] = False
    save_config(cfg)
    click.echo("Update check disabled.")


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


from verlet.docs_export import docs_export, mirror_changelog  # noqa: E402

docs_group.add_command(docs_export)
docs_group.add_command(mirror_changelog)


# Register subcommand groups
from verlet.auth.commands import auth_group  # noqa: E402
from verlet.bundles import bundles_group  # noqa: E402
from verlet.datasets import datasets_group  # noqa: E402
from verlet.destinations import destinations_group  # noqa: E402
from verlet.showcase import showcase_group  # noqa: E402
from verlet.update import update as update_command  # noqa: E402

cli.add_command(auth_group)
cli.add_command(bundles_group)
cli.add_command(config_group)
cli.add_command(datasets_group)
cli.add_command(destinations_group)
cli.add_command(docs_group)
cli.add_command(showcase_group)
cli.add_command(update_command)


if __name__ == "__main__":
    cli()
