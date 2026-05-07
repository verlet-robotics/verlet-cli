"""Verlet CLI entry point."""
from importlib.metadata import PackageNotFoundError, version as _pkg_version

import click
import httpx

from verlet.display import console

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


@cli.command()
def update():
    """Update verlet to the latest version."""
    import subprocess
    import sys

    current = __version__
    console.print(f"[dim]Current version: {current}[/dim]")
    console.print("Checking for updates...")

    try:
        resp = httpx.get("https://pypi.org/pypi/verlet/json", timeout=10.0)
        resp.raise_for_status()
        latest = resp.json()["info"]["version"]
    except Exception:
        console.print("[yellow]Could not check PyPI for latest version. Upgrading anyway...[/yellow]")
        latest = None

    if latest and latest == current:
        console.print(f"[green]Already up to date (v{current})[/green]")
        return

    if latest:
        console.print(f"[bold]Updating v{current} -> v{latest}[/bold]")

    result = subprocess.run(
        [sys.executable, "-m", "pip", "install", "--upgrade", "verlet"],
        capture_output=True,
        text=True,
    )

    if result.returncode == 0:
        version_label = f"v{latest}" if latest else "latest"
        console.print(f"[green]Updated to {version_label}[/green]")
    else:
        console.print(f"[red]Update failed:[/red]\n{result.stderr.strip()}")
        raise SystemExit(1)


# Register subcommand groups
from verlet.auth.commands import auth_group  # noqa: E402
from verlet.bundles import bundles_group  # noqa: E402
from verlet.datasets import datasets_group  # noqa: E402
from verlet.ego.commands import ego_group  # noqa: E402

cli.add_command(auth_group)
cli.add_command(bundles_group)
cli.add_command(datasets_group)
cli.add_command(ego_group)


if __name__ == "__main__":
    cli()
