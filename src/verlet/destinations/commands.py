"""verlet destinations — manage cloud push destinations (G-P4).

Subcommands:
  * list      — saved destinations
  * providers — connectable providers (public; no auth)
  * add       — create a destination; dispatches on the provider's auth_kind
  * rm        — delete a saved destination

A standalone ``test`` command is intentionally not shipped: the test-connection
endpoint needs raw credentials, which a saved destination never returns. The
``add`` flow tests the connection at creation time (manual path), and a
successful deeplink callback already proves the role via ``sts:AssumeRole``.
"""
from __future__ import annotations

import asyncio
import json

import click

from verlet.auth.profiles import (
    ProfileNotFoundError,
    require_profile,
    resolve_profile_name,
)
from verlet.display import console


@click.group("destinations")
def destinations_group() -> None:
    """Manage cloud destinations for `verlet datasets push`."""


def _require_auth(ctx: click.Context) -> str:
    """Resolve the active profile, failing fast (pre-HTTP, pre-prompt) if
    none is configured or the configured token has already expired.

    The expiry check is hoisted here (instead of relying solely on the
    AuthenticatedClient ctor check) so ``destinations add`` doesn't make
    a user type three secrets at the credential prompts before learning
    their session is dead.
    """
    from verlet.auth.expiry import is_profile_expired, refresh_command
    from verlet.auth.credentials import get_profile

    profile_name = resolve_profile_name(
        ctx.obj.get("profile") if ctx.obj else None
    )
    try:
        require_profile(profile_name)
    except ProfileNotFoundError:
        raise click.ClickException(
            "Not authenticated. Run `verlet auth login` to manage destinations."
        )
    profile = get_profile(profile_name) or {}
    if profile.get("kind") != "device_flow" and is_profile_expired(profile):
        raise click.ClickException(
            f"Your {profile.get('kind', 'token')} has expired. "
            f"Refresh with `{refresh_command(profile)}`."
        )
    return profile_name


@destinations_group.command("list")
@click.option("--json", "as_json", is_flag=True, help="Emit raw JSON.")
@click.pass_context
def list_destinations(ctx: click.Context, as_json: bool) -> None:
    """List your saved cloud destinations."""
    from verlet.destinations._api import fetch_destinations
    from verlet.destinations._render import destinations_table

    profile_name = _require_auth(ctx)
    items = asyncio.run(fetch_destinations(profile_name))

    if as_json:
        click.echo(json.dumps(items, indent=2, default=str))
        return
    if not items:
        console.print(
            "[dim]No cloud destinations. Add one with "
            "`verlet destinations add <provider>`.[/dim]"
        )
        return
    console.print(destinations_table(items))


@destinations_group.command("providers")
@click.option("--json", "as_json", is_flag=True, help="Emit raw JSON.")
def list_providers(as_json: bool) -> None:
    """List connectable destination providers (public; no auth required)."""
    from verlet.destinations._api import fetch_providers
    from verlet.destinations._render import providers_table

    items = asyncio.run(fetch_providers())
    if as_json:
        click.echo(json.dumps(items, indent=2, default=str))
        return
    console.print(providers_table(items))


def _gather_manual_credentials(
    provider_info: dict,
    credential_pairs: tuple[str, ...],
    credentials_json,
) -> dict:
    """Collect a manual provider's credentials dict.

    Priority: ``--credential KEY=VALUE`` pairs > ``--credentials-json`` file >
    interactive prompts driven by ``provider_info.manual_fields`` (server-
    advertised) or the CLI's per-provider static fallback in
    ``_fields.FALLBACK_FIELDS`` (used while the backend still returns
    ``manual_fields=null``). GCS is JSON-only and short-circuits to a
    pointer at ``--credentials-json``.
    """
    from verlet.destinations._fields import (
        FALLBACK_FIELDS,
        JSON_ONLY,
        fallback_summary,
    )
    from verlet.destinations._validation import parse_credential_pairs

    if credential_pairs:
        return parse_credential_pairs(credential_pairs)
    if credentials_json is not None:
        try:
            data = json.load(credentials_json)
        except json.JSONDecodeError as exc:
            raise click.BadParameter(
                f"--credentials-json is not valid JSON: {exc}"
            )
        if not isinstance(data, dict):
            raise click.BadParameter(
                "--credentials-json must contain a JSON object."
            )
        return data

    provider_name = provider_info.get("name", "")
    # Backend-advertised manual_fields win; otherwise fall back to the CLI's
    # static knowledge of each adapter's credential shape.
    manual_fields = provider_info.get("manual_fields")
    if not manual_fields:
        fallback = FALLBACK_FIELDS.get(provider_name)
        if fallback == JSON_ONLY:
            raise click.UsageError(
                f"'{provider_name}' takes a service-account JSON document, "
                "not key=value pairs. Pass --credentials-json /path/to/sa.json "
                "(use '-' to read from stdin)."
            )
        if isinstance(fallback, list):
            manual_fields = fallback

    if manual_fields:
        creds: dict[str, str] = {}
        for field in manual_fields:
            key = field.get("key") or field.get("name")
            if not key:
                continue
            label = field.get("label") or key
            creds[key] = click.prompt(label, hide_input=bool(field.get("secret")))
        return creds

    hint = fallback_summary(provider_name)
    extra = f" (e.g. {hint})" if hint else ""
    raise click.UsageError(
        "This provider needs credentials. Pass --credential KEY=VALUE "
        f"(repeatable) or --credentials-json <file>{extra}."
    )


@destinations_group.command("add")
@click.argument("provider")
@click.option("--name", default=None, help="Destination name (prompted if omitted).")
@click.option("--bucket", default=None, help="Bucket / repo (prompted if omitted).")
@click.option("--prefix", default=None, help="Optional key prefix within the bucket.")
@click.option("--region", default=None, help="Optional region (AWS / GCS).")
@click.option(
    "--credential",
    "credential_pairs",
    multiple=True,
    help="Manual credential as KEY=VALUE (repeatable).",
)
@click.option(
    "--credentials-json",
    "credentials_json",
    default=None,
    type=click.File("r"),
    help="Manual credentials as a JSON file ('-' reads stdin).",
)
@click.option(
    "--no-browser",
    is_flag=True,
    help="Deeplink providers: print the URL instead of opening a browser.",
)
@click.option(
    "--no-test",
    is_flag=True,
    help="Skip the post-create connection test (manual providers).",
)
@click.pass_context
def add(
    ctx: click.Context,
    provider: str,
    name: str | None,
    bucket: str | None,
    prefix: str | None,
    region: str | None,
    credential_pairs: tuple[str, ...],
    credentials_json,
    no_browser: bool,
    no_test: bool,
) -> None:
    """Add a cloud destination.

    The connect flow is selected per-provider by the server. For every
    provider configured as ``manual`` (today: all four), credentials are
    entered directly — interactive prompts when no ``--credential`` /
    ``--credentials-json`` is given, or non-interactive for CI.

    \b
    Examples (manual mode):
      verlet destinations add r2 --name my-r2 --bucket data \\
        --credential account_id=abc --credential access_key_id=... \\
        --credential secret_access_key=...

    \b
      # AWS S3 — access keys (manual mode)
      verlet destinations add aws_s3 --name my-s3 --bucket data \\
        --credential access_key_id=... --credential secret_access_key=...

    \b
      # GCS — paste a service-account JSON document
      verlet destinations add gcs --name my-gcs --bucket data \\
        --credentials-json /path/to/service-account.json

    \b
      # HuggingFace — single token
      verlet destinations add huggingface --name my-hf --bucket org/dataset \\
        --credential token=hf_...

    \b
    Server-managed flows (auto-selected when the backend advertises them):

      * AWS S3 deeplink — opens a CloudFormation quick-create URL and
        prompts for the resulting RoleArn. Triggered when the server's
        AWS trust-account is configured.
      * OAuth (GCS / HuggingFace) — currently deferred from the CLI; use
        the Verlet web app's Destinations page instead.
    """
    from verlet.destinations._api import fetch_providers, test_connection
    from verlet.destinations._connect import (
        OAUTH_DEFERRED_MSG,
        run_deeplink,
        run_manual,
    )

    profile_name = _require_auth(ctx)

    if credential_pairs and credentials_json is not None:
        raise click.UsageError(
            "Pass either --credential or --credentials-json, not both."
        )

    # Resolve the provider's auth_kind from the public providers list.
    providers = asyncio.run(fetch_providers())
    match = next((p for p in providers if p.get("name") == provider), None)
    if match is None:
        names = ", ".join(sorted(p.get("name", "") for p in providers))
        raise click.ClickException(
            f"Unknown provider '{provider}'. Available: {names}."
        )
    auth_kind = match.get("auth_kind")

    creds_given = bool(credential_pairs) or credentials_json is not None
    if auth_kind != "manual" and creds_given:
        raise click.BadParameter(
            "--credential / --credentials-json apply only to manual-credential "
            f"providers; '{provider}' connects via {auth_kind}."
        )

    if auth_kind == "oauth":
        raise click.ClickException(OAUTH_DEFERRED_MSG.format(provider=provider))

    # name + bucket are required for every connect kind.
    name = name or click.prompt("Destination name")
    bucket = bucket or click.prompt("Bucket / repo")

    if auth_kind == "deeplink":
        dest = asyncio.run(
            run_deeplink(
                profile_name,
                provider=provider,
                name=name,
                bucket=bucket,
                prefix=prefix,
                region=region,
                no_browser=no_browser,
            )
        )
        console.print(
            f"[green]Connected[/green] destination "
            f"'[cyan]{dest['name']}[/cyan]' ({dest['provider']})."
        )
        return

    # auth_kind == "manual"
    creds = _gather_manual_credentials(match, credential_pairs, credentials_json)
    dest = asyncio.run(
        run_manual(
            profile_name,
            provider=provider,
            name=name,
            bucket=bucket,
            prefix=prefix,
            region=region,
            credentials=creds,
        )
    )
    console.print(
        f"[green]Saved[/green] destination "
        f"'[cyan]{dest['name']}[/cyan]' ({dest['provider']})."
    )

    if not no_test:
        result = asyncio.run(
            test_connection(
                profile_name,
                {
                    "provider": provider,
                    "bucket": bucket,
                    "region": region,
                    "credentials": creds,
                },
            )
        )
        if result.get("success"):
            console.print(
                f"[green]Connection test passed[/green] — "
                f"{result.get('message', '')}"
            )
        else:
            console.print(
                f"[red]Connection test failed[/red] — "
                f"{result.get('message', '')}"
            )


@destinations_group.command("rm")
@click.argument("name_or_id")
@click.option("--yes", is_flag=True, help="Skip the confirmation prompt.")
@click.pass_context
def rm(ctx: click.Context, name_or_id: str, yes: bool) -> None:
    """Delete a saved destination by name or id."""
    from verlet.destinations._api import delete_destination
    from verlet.destinations._validation import resolve_destination_ref

    profile_name = _require_auth(ctx)
    dest_id = asyncio.run(resolve_destination_ref(name_or_id, profile_name))

    if not yes and not click.confirm(f"Delete destination '{name_or_id}'?"):
        console.print("[dim]Aborted.[/dim]")
        return

    asyncio.run(delete_destination(profile_name, dest_id))
    console.print(f"[green]Deleted[/green] destination '{name_or_id}'.")
