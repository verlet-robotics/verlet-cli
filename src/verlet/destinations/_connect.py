"""Connect sub-flows for ``verlet destinations add`` (G-P4).

Two auth kinds are handled natively; ``commands.add`` dispatches on the
provider's ``auth_kind``:

  * **manual**   — credentials entered directly (``--credential`` /
    ``--credentials-json``); ``POST /destinations`` in one shot. For a manual
    provider, ``connect/init`` + ``connect/callback`` is a pure passthrough, so
    the direct create endpoint is used instead.
  * **deeplink** — AWS CloudFormation: ``connect/init`` returns a quick-create
    URL, the user deploys the trust-role stack and pastes the ``RoleArn`` back,
    ``connect/callback`` finishes. The backend's ``exchange`` does an
    ``sts:AssumeRole`` — so a successful callback already proves the role works.

The **oauth** kind is deferred (the OAuth ``code`` lands on a verlet.co web
page the CLI cannot observe); ``commands.add`` rejects it with ``OAUTH_DEFERRED_MSG``.
"""
from __future__ import annotations

import webbrowser

import click

from verlet.destinations._api import connect_callback, connect_init, create_destination
from verlet.destinations._validation import validate_role_arn
from verlet.display import console

OAUTH_DEFERRED_MSG = (
    "'{provider}' connects via OAuth, which the CLI cannot complete yet — "
    "the browser redirect lands on a web page the terminal cannot read. "
    "Connect it from the Destinations page in the Verlet web app, then it "
    "will appear in `verlet destinations list`."
)


async def run_manual(
    profile_name: str | None,
    *,
    provider: str,
    name: str,
    bucket: str,
    prefix: str | None,
    region: str | None,
    credentials: dict,
) -> dict:
    """Manual provider: create the destination directly (one round trip)."""
    body = {
        "name": name,
        "provider": provider,
        "bucket": bucket,
        "prefix": prefix,
        "region": region,
        "credentials": credentials,
    }
    return await create_destination(profile_name, body)


async def run_deeplink(
    profile_name: str | None,
    *,
    provider: str,
    name: str,
    bucket: str,
    prefix: str | None,
    region: str | None,
    no_browser: bool,
) -> dict:
    """AWS deeplink: connect/init → CloudFormation → paste RoleArn → callback."""
    init = await connect_init(
        profile_name,
        {
            "provider": provider,
            "name": name,
            "bucket": bucket,
            "prefix": prefix,
            "region": region,
        },
    )
    url = init.get("authorize_url")
    if not url:
        raise click.ClickException(
            "The server did not return a CloudFormation URL for this provider "
            "— it may not be configured for the deeplink connect flow."
        )

    console.print(
        "[bold]Connect AWS S3[/bold] — deploy a CloudFormation trust-role stack."
    )
    if no_browser:
        console.print(f"Open this URL to create the stack:\n  {url}")
    else:
        console.print(f"Opening CloudFormation: [dim]{url}[/dim]")
        webbrowser.open(url)
    console.print(
        "\nAfter the stack finishes, open its [bold]Outputs[/bold] tab and "
        "copy the [bold]RoleArn[/bold] value."
    )
    role_arn = validate_role_arn(click.prompt("Paste the RoleArn"))
    return await connect_callback(
        profile_name,
        {"state": init["state"], "payload": {"role_arn": role_arn}},
    )
