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
- [ ] HenryDB: Fix TRUNCATE TABLE persistence (WAL recovery restores truncated rows after close/reopen)

## Low
- [ ] Named Cloudflare tunnel for dashboard webhook (URL changes on restart)
