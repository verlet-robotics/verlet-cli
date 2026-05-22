"""verlet pull — top-level command for grant-gated dataset downloads.

Resolves the active profile's ``kind`` to choose the right backend:
  * ``showcase_access_code`` → ``/api/v1/showcase/datasets/{slug}/download``
  * ``device_flow`` / ``pat`` → the platform's public free-samples endpoint
    (CLI side of ``/api/platform/v1/catalog/datasets/{slug}/samples/download``)
"""

from .commands import pull_command

__all__ = ["pull_command"]
