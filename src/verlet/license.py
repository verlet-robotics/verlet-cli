"""CC BY-NC-SA 4.0 license enforcement for downloaded data."""
from pathlib import Path

from verlet.config import CONFIG_DIR

LICENSE_TEXT = """\
Verlet Data — License

This dataset is provided by Verlet Robotics (verlet.ai) and is licensed under
the Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International
License (CC BY-NC-SA 4.0).

Full license text: https://creativecommons.org/licenses/by-nc-sa/4.0/legalcode

You are free to:
  - Share — copy and redistribute the material in any medium or format
  - Adapt — remix, transform, and build upon the material

Under the following terms:
  - Attribution — You must give appropriate credit, provide a link to the
    license, and indicate if changes were made.
  - NonCommercial — You may not use the material for commercial purposes.
  - ShareAlike — If you remix, transform, or build upon the material, you
    must distribute your contributions under the same license.

Academic Use:
  If you use this data in academic work (papers, theses, presentations, etc.),
  please cite:

    Verlet Robotics
    https://verlet.ai

  BibTeX:

    @misc{verlet_data,
      author = {Verlet Robotics},
      title  = {Verlet Teleoperation and Egocentric Demonstration Data},
      year   = {2025},
      url    = {https://verlet.ai},
    }

© Verlet Robotics. All rights reserved.
"""

_ACCEPTED_FLAG = CONFIG_DIR / "license_accepted"


def write_license_file(dest_dir: Path) -> None:
    """Write a LICENSE file into the downloaded dataset directory."""
    dest_dir.mkdir(parents=True, exist_ok=True)
    (dest_dir / "LICENSE").write_text(LICENSE_TEXT)


def check_license_accepted() -> bool:
    """Check if the user has previously accepted the license terms."""
    return _ACCEPTED_FLAG.exists()


def prompt_license_acceptance() -> bool:
    """Show license terms and prompt the user to accept. Returns True if accepted."""
    from rich.console import Console
    from rich.panel import Panel

    console = Console()
    console.print()
    console.print(Panel(
        "[bold]Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International[/bold]\n\n"
        "• Commercial use is [bold red]not permitted[/bold red].\n"
        "• Derivatives must use the same license.\n"
        "• Academic work must cite: [bold]Verlet Robotics, verlet.ai[/bold]\n\n"
        "Full terms: [link]https://creativecommons.org/licenses/by-nc-sa/4.0/[/link]",
        title="Data License — CC BY-NC-SA 4.0",
        border_style="yellow",
    ))
    console.print()

    accepted = click_confirm("Do you accept these license terms?")
    if accepted:
        CONFIG_DIR.mkdir(parents=True, exist_ok=True)
        _ACCEPTED_FLAG.write_text("accepted\n")
    return accepted


def click_confirm(message: str) -> bool:
    """Prompt user for yes/no confirmation via click."""
    import click
    return click.confirm(message, default=False)
