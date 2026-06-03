"""Verlet Data CLI."""
from importlib.metadata import PackageNotFoundError, version as _pkg_version

# Single source of truth for the CLI version. Plan 28-04 / Research §13.5
# bumps this to 0.6.0 alongside pyproject.toml [project] version. We still
# prefer the installed-package metadata when present (covers editable/dev
# installs that pick up an unstaged version bump from pyproject.toml), and
# fall back to the literal so source checkouts without an install still
# report a real version instead of "0.0.0+unknown".
__version__: str = "0.13.0"

try:
    __version__ = _pkg_version("verlet")
except PackageNotFoundError:
    pass
