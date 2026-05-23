"""verlet destinations — manage cloud push destinations (G-P4).

Submodule layout mirrors ``verlet.datasets`` / ``verlet.bundles``:
- commands.py    — Click group + list/providers/add/rm subcommands
- _api.py        — httpx wrappers for the cloud-destinations API
- _connect.py    — the manual / deeplink connect sub-flows
- _render.py     — Rich table builders
- _validation.py — credential parsing + name→id resolution
"""
from verlet.destinations.commands import destinations_group

__all__ = ["destinations_group"]
