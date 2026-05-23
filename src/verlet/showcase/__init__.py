"""verlet showcase — showcase-prospect commands (G-S3).

Commands here require a showcase access-code profile
(`verlet auth login --kind showcase`). Layout mirrors the other groups:
- commands.py — Click group + the `stats` subcommand
- _api.py     — httpx wrappers for the gated showcase surface
- _render.py  — Rich renderers
"""
from verlet.showcase.commands import showcase_group

__all__ = ["showcase_group"]
