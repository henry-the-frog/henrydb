# TODO.md — Task Intake

Persistent buffer for things that need doing. Feeds into the daily queue.
- **Urgent:** Do next if a task finishes early
- **Normal:** Fold into next morning's standup queue
- **Low:** Backlog — do when interesting or when time permits

Items get removed (not checked off) once they enter the queue or are completed.

## Urgent
(empty)

## Normal
- [ ] GitHub 2FA setup by May 8 (needs Jordan's help with authenticator)
- [ ] Review Daniel's PRs when they land: JIT instrumentation + AST serializer fixes (enables mimule fuzzer)

## Low
- [ ] HenryDB parser: CAST() || operator chaining doesn't work (deeper architecture issue)
- [ ] HenryDB parser: Nested SUBSTRING inside function fails (FROM keyword conflict)
- [ ] HenryDB parser: GROUP BY alias doesn't resolve correctly
- [ ] Named Cloudflare tunnel for dashboard webhook (URL changes on restart)
