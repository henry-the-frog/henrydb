# TODO.md

## Urgent
(none)

## Urgent
(none)

## Normal
- HenryDB: Strings >4068 bytes silently truncated (data corruption) — need TOAST or error on oversized values
- HenryDB: EXPLAIN classic/Volcano plan disagree on scan type (classic uses selectivity heuristic, Volcano uses cost model)
- HenryDB: WAL syncMode validation — unrecognized values silently disable sync
- HenryDB: Add DATE/TIME modifier support (+30 days, -1 month, 'now')
- HenryDB: Refactor parseSelectColumn to use parseExpr for general expressions
- HenryDB: Fix TRUE/FALSE as boolean keywords (currently parsed as strings)
- HenryDB: Add REGEXP support
- HenryDB: Fix LIMIT with subquery (returns all rows instead of subquery result)
- Neural-net: Write training tutorial/walkthrough
- Update HenryDB blog post with 167 features and trigger fix
- monkey-lang: Add CI + README update

## Low
- HenryDB: Add GENERATED columns
- HenryDB: Add ATTACH DATABASE
- HenryDB: Add CROSS APPLY / OUTER APPLY (SQL Server syntax)
- HenryDB: Expression indexes
