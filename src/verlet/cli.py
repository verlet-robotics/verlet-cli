"""Verlet CLI entry point."""
import asyncio

import click
import httpx

from verlet.config import get_api_url, save_credentials
from verlet.display import console


@click.group()
@click.version_option(version="0.3.0", prog_name="verlet")
def cli():
    """Verlet Data CLI — download ego and teleop datasets."""
    pass


@cli.command()
@click.option("--api-url", default=None, help="Override API URL")
def login(api_url: str | None):
    """Authenticate with your access code."""
    code = click.prompt("Access code", hide_input=True)

    base_url = api_url or get_api_url()
    url = f"{base_url}/api/v1/ego/showcase/auth"

    try:
        resp = httpx.post(url, json={"code": code}, timeout=30.0)
        resp.raise_for_status()
        data = resp.json()
        save_credentials(data["token"], data["customer_name"], api_url)
        console.print(f"[green]Authenticated as {data['customer_name']}[/green]")
    except httpx.HTTPStatusError as e:
        detail = "Invalid access code"
        try:
            detail = e.response.json().get("detail", detail)
        except Exception:
            pass
        console.print(f"[red]Authentication failed: {detail}[/red]")
        raise SystemExit(1)
    except Exception as e:
        console.print(f"[red]Connection error: {e}[/red]")
        raise SystemExit(1)


@cli.command()
def update():
    """Update verlet to the latest version."""
    import subprocess
    import sys

    current = "0.2.0"
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
from verlet.ego.commands import ego_group  # noqa: E402
from verlet.teleop.commands import teleop_group  # noqa: E402

cli.add_command(ego_group)
cli.add_command(teleop_group)


if __name__ == "__main__":
    cli()
