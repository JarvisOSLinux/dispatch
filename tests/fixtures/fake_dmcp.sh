#!/bin/sh
# Fake dmcp for dispatch integration tests, installed on PATH as `dmcp`.
# Covers only the surface dispatch touches: `paths` (the availability probe),
# `info <server> --json` (the manifest dispatch reads for per-tool metadata)
# and `call <server> <tool> ...`, with behavior keyed on the tool name.
# stderr is the live-progress channel under test; stdout carries the result
# exactly as real dmcp does, and status rides the exit code.

case "$1" in
paths)
    exit 0
    ;;
info)
    # Manifests, printed exactly as `dmcp info --json` does. An unknown server
    # fails like the real thing, so the "no metadata available" path is covered.
    case "$2" in
    fake)
        printf '{"id":"fake","tools":[{"name":"quick"},{"name":"slow_progress","description":"declares nothing"}]}'
        ;;
    blocking)
        printf '{"id":"blocking","tools":[{"name":"run_job","blocking":true,"suggestedRemindAfter":2}]}'
        ;;
    blocking_no_suggestion)
        printf '{"id":"blocking_no_suggestion","tools":[{"name":"run_job","blocking":true}]}'
        ;;
    *)
        printf 'Server not found: %s\n' "$2" >&2
        exit 1
        ;;
    esac
    exit 0
    ;;
call)
    tool="$3"
    case "$tool" in
    quick)
        printf 'quick-result'
        exit 0
        ;;
    run_job)
        # The blocking case in miniature: it prints a question nobody can see
        # from the signal window and then produces nothing until it gives up.
        printf 'continue? [y/N] ' >&2
        sleep 6
        printf 'job-done'
        exit 0
        ;;
    slow_progress)
        printf 'phase-one started\n' >&2
        sleep 3
        printf 'phase-two done\n' >&2
        printf 'slow-result'
        exit 0
        ;;
    daemonize)
        # Grandchild inherits the stderr write end and outlives this process,
        # so the pipe never reaches EOF; stdout is redirected away so the
        # call's stdout EOF (and thus its result) is immediate.
        sleep 20 >/dev/null &
        printf 'daemon-started'
        exit 0
        ;;
    fail_with_stderr)
        printf 'failure detail on stderr\n' >&2
        exit 1
        ;;
    fail_with_stdout)
        printf 'tool-reported error detail'
        printf 'noise on stderr\n' >&2
        exit 2
        ;;
    *)
        printf 'unknown fake tool: %s\n' "$tool" >&2
        exit 1
        ;;
    esac
    ;;
*)
    exit 0
    ;;
esac
