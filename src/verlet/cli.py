"""Top-level CLI group — registers auth commands and modality subgroups."""

from __future__ import annotations

import click

from verlet import __version__
from verlet.auth import login, logout
from verlet.config import DEFAULT_API_BASE
from verlet.ego.commands import ego_group


@click.group()
@click.version_option(__version__, prog_name="verlet")
def cli():
    """Verlet CLI — download and explore Verlet datasets."""


@cli.command()
@click.option("--api-base", default=None, help=f"API base URL (default: {DEFAULT_API_BASE}).")
def login_cmd(api_base: str | None):
    """Authenticate with your access code."""
    login(api_base)


# Register as 'login' not 'login-cmd'
login_cmd.name = "login"


@cli.command()
def logout_cmd():
    """Remove stored credentials."""
    logout()


logout_cmd.name = "logout"

# Register modality subgroups
cli.add_command(ego_group)
