# TASKS.md

## Session A Completed (Apr 25, 2026)
72+ tasks in ~2.5 hours. See `memory/2026-04-25.md` for details.

### What We Did
- **Bug fixes (9)**: String truncation, WAL syncMode, EXPLAIN cost, LIMIT subquery, TRUE/FALSE, REGEXP, SUM/AVG coercion, INSERT column count, ORDER BY validation
- **New features**: DATE/TIME/DATETIME modifiers, expression indexes, REGEXP/RLIKE, shared selectivity estimator, constant substitution (monkey-lang)
- **Differential fuzzer**: JOINs, GROUP BY, HAVING, subqueries, CTEs, UPDATE, DELETE — 97%+ pass rate
- **Blog posts**: HenryDB (~2500 words) + monkey-lang (~2500 words)
- **Neural-net tutorial**: 8 sections, beginner-friendly, all code examples verified
- **Deep exploration**: 6 execution engines discovered, monkey-lang is a mini-V8 (20.5K lines)

## Next Session Priority (Session B or Tomorrow)
1. **High**: Fix remaining fuzzer type affinity differences (TEXT >= INT coercion)
2. **High**: Wire monkey-lang bytecode optimizer into main compilation path (currently opt-in)
3. **Medium**: Add window function queries to fuzzer
4. **Medium**: Explore lambda-calculus project (43K lines, potentially interesting)
5. **Low**: parseSelectColumn refactor (700 lines, needs fresh morning)

## Blocked
- monkey-lang git push needs `workflow` scope on GitHub token
