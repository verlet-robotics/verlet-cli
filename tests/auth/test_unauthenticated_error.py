"""Regression test for the 0.8.3 fix: commands that build an
``AuthenticatedClient`` before a profile exists should report
``Error: No profile named '<name>' …`` cleanly, not a 30-line Python
traceback.

The bug (present in 0.8.0–0.8.2): ``ProfileNotFoundError`` inherited
from plain ``Exception``, so only the two subcommands with explicit
``except ProfileNotFoundError`` handlers — ``auth status`` and
``auth logout`` — printed a friendly message. Every *other*
authenticated command (``bundles *``, ``datasets push``,
``auth tokens *``) surfaced the exception as an uncaught traceback.

Fix: make ``ProfileNotFoundError`` inherit from
``click.ClickException`` so Click's top-level handler in ``cli.main()``
catches it for every command, uniformly rendering
``Error: <message>`` on stderr with exit code 1. The redundant
``except`` blocks in ``auth/commands.py`` have been removed so that
``auth status`` and ``auth logout`` now flow through the same handler
as everything else.

Out of scope: ``datasets info``/``ego info``/``ego list`` are
anonymous-OK paths (no profile required) — their tracebacks when the
API returns 4xx come from uncaught ``httpx.HTTPStatusError``, not
``ProfileNotFoundError``. That's a separate fix, tracked elsewhere.
"""

from verlet.cli import cli


# Each entry hits AuthenticatedClient(profile_name) somewhere in its
# code path, so each should now route through Click's ClickException
# handler. One representative per affected module — exhaustive
# coverage would just multiply this list.
_PROFILE_REQUIRED_COMMANDS = [
    ["bundles", "list"],
    ["auth", "tokens", "list"],
    # auth status + auth logout used to be the *only* commands that
    # caught ProfileNotFoundError explicitly. Verify the
    # explicit-handler removal didn't regress them.
    ["auth", "status"],
    ["auth", "logout"],
]


def test_unauthenticated_commands_print_friendly_error(tmp_home, cli_runner):
    """No profile + auth-requiring command → exit 1 + friendly stderr."""
    for argv in _PROFILE_REQUIRED_COMMANDS:
        result = cli_runner.invoke(cli, argv)
        # ClickException renders to stderr (mixed into ``output`` by
        # CliRunner's default). exit_code 1 is ClickException's default.
        assert result.exit_code == 1, (
            f"{' '.join(argv)} exit_code={result.exit_code} "
            f"(expected 1); output={result.output!r}; "
            f"exception={result.exception!r}"
        )
        assert "No profile named 'default'" in result.output, (
            f"{' '.join(argv)} output did not contain the friendly "
            f"message: {result.output!r}"
        )
        # The bug we're guarding against: raw Python traceback.
        # CliRunner exposes the exception object via result.exception
        # when one was raised but caught by the framework — for
        # ClickException, that's expected; we just want to confirm the
        # *visible* output is friendly.
        assert "Traceback" not in result.output, (
            f"{' '.join(argv)} regressed to a raw traceback:\n"
            f"{result.output}"
        )


def test_explicit_profile_flag_uses_friendly_handler(tmp_home, cli_runner):
    """``--profile <missing>`` shows the named profile in the error,
    not the default one — proves the message threads through correctly
    via the global flag, not just the default."""
    result = cli_runner.invoke(
        cli, ["--profile", "nonexistent-profile-xyz", "bundles", "list"]
    )
    assert result.exit_code == 1, (result.output, result.exception)
    assert "No profile named 'nonexistent-profile-xyz'" in result.output
    assert "Traceback" not in result.output


def test_profile_not_found_error_is_click_exception():
    """Compile-time guard: ``ProfileNotFoundError`` must be a
    ``ClickException`` subclass. The fix relies on this for uniform
    handling across every command — if a future refactor accidentally
    breaks the inheritance, this test catches it before the rest of
    the suite does."""
    import click

    from verlet.auth.profiles import ProfileNotFoundError

    assert issubclass(ProfileNotFoundError, click.ClickException), (
        "ProfileNotFoundError must inherit from click.ClickException "
        "so Click's top-level handler renders it as a friendly error. "
        "See CHANGELOG 0.8.3."
    )
