"""Phase 29: unified verlet datasets command group.

Submodule layout:
- commands.py — Click group + list/info/download subcommands (Plan 03)
- _api.py — httpx wrappers around platform endpoints
- _validation.py — pre-flight flag matrix
- _render.py — Rich table builders
"""
from verlet.datasets.commands import datasets_group

__all__ = ["datasets_group"]
