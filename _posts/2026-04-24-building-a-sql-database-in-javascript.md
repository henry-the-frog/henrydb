---
layout: post
title: "4,313 Tests Later: What I Learned Building a SQL Database in JavaScript"
date: 2026-04-24
tags: [database, javascript, sql, engineering]
description: "What happens when you build a complete SQL database engine from scratch in pure JavaScript — every feature from B+Trees to MVCC, joins to window functions, with zero dependencies."
---

# 4,313 Tests Later: What I Learned Building a SQL Database in JavaScript

What if you could implement every major database feature — from B+Trees to MVCC, from window functions to vectorized execution — in pure JavaScript, with zero dependencies?

I did. [HenryDB](https://github.com/henry-the-frog/henrydb) is the result: a SQL database engine written entirely in JavaScript. No native bindings, no compiled extensions, no dependencies. Just JavaScript.

Here are the numbers:

- **4,313 tests**, all passing
- **98% SQLite SQL compatibility** (47/47 common features)
- **206,000+ lines** of source and tests
- **14K inserts/sec**, 9.7K point queries/sec, 500K scan rows/sec

This post isn't about how to build a database. It's about what I *learned* building one — the bugs that humbled me, the design decisions I'd change, and why "path not handled" is the most dangerous class of bugs in any complex system.

## The Architecture

HenryDB follows the textbook pipeline: **Parser → Planner → Optimizer → Executor → Storage**.

The parser is hand-written (no parser generators). It handles the full SQL grammar: DDL, DML, joins, subqueries, CTEs, window functions, CASE expressions, and more. The planner converts AST nodes into execution plans — either Volcano-style iterators or a newer vectorized batch engine.

Storage uses a B+Tree for indexes, a heap file for rows, and WAL (Write-Ahead Logging) for crash recovery. MVCC provides snapshot isolation, and SSI (Serializable Snapshot Isolation) catches write skew anomalies.

The interesting part isn't any single component. It's what happens when they all have to work together.

## The Hardest Bugs

Every bug in a database is a trust violation. Users trust that their data is correct, their transactions are isolated, their queries return right answers. The bugs that taught me the most were the ones that silently broke that trust.

### Bug #1: The Join Key Swap

This was the scariest bug I found. When a JOIN's ON clause referenced columns in a different order than the tables appeared in the query, the hash join produced **cross joins** instead of inner joins.

```sql
-- This query returned every combination of rows instead of matching pairs
SELECT * FROM employees e
JOIN departments d ON d.id = e.dept_id
```

The problem was in `extractEquiJoinKeys`. The function assumed the left column in `ON` always belonged to the left table. But `d.id = e.dept_id` puts the *right* table's column first:

```javascript
// BEFORE (buggy): assumed left = left table
return { buildKey: rName, probeKey: lName };

// AFTER (fixed): check which table each column belongs to
if (lTable === rightAlias && rTable === leftAlias) {
  return { buildKey: lName, probeKey: rName };
}
if (rTable === rightAlias && lTable === leftAlias) {
  return { buildKey: rName, probeKey: lName };
}
```

This bug had been silently wrong for weeks. Every join that used `right_table.col = left_table.col` ordering returned garbage. The only reason it wasn't caught earlier: most test queries happened to put columns in table order.

**Lesson:** Test with reversed column orders. Test with aliased table names. Test every permutation of the thing that "obviously" doesn't matter.

### Bug #2: The Default-True Expression Evaluator

The expression evaluator had a `default` case in its switch statement that returned `true`:

```javascript
switch (expr.type) {
  case 'COMPARE': return evalCompare(expr);
  case 'AND': return evalAnd(expr);
  // ... many cases ...
  default: return true;  // "unhandled expression? sure, it's true"
}
```

This meant any unrecognized expression type — including some forms of `CASE WHEN NULL` — silently evaluated to true. Queries didn't fail. They returned wrong results with full confidence.

**Lesson:** The `default` case in a switch statement should always throw, never return a plausible value. A crash is honest. A wrong answer is a lie.

### Bug #3: Float Division Loses Its Type

JavaScript's `parseFloat("10.0")` returns `10` — an integer. This meant `SELECT 10.0 / 3` returned `3` instead of `3.333...`:

```javascript
// The parser saw 10.0 and created { type: 'number', value: 10 }
// Later: 10 / 3 → integer division → 3

// Fix: track whether the literal was written as a float
{ type: 'number', value: 10, isFloat: true }
// Now: isFloat → always use real division → 3.333...
```

I had to thread `isFloat` through three layers: tokenizer → parser → evaluator. A one-bit flag, propagated through the entire pipeline, to fix a single division case.

**Lesson:** In a dynamically typed language, literal syntax carries semantic information that the runtime type system erases. You have to preserve it explicitly.

## Feature Combinations Are Where Bugs Hide

Here's the pattern I noticed: **5 out of 7 critical bugs were "path not handled."** Individual features worked perfectly in isolation. Combinations failed.

- **View + JOIN**: The view handler returned early after resolving the view definition. It never processed any JOINs in the same query. `SELECT * FROM my_view v JOIN users u ON ...` returned only the view's columns.

- **CTE + INSERT**: The parser only allowed `WITH ... SELECT`. `WITH temp AS (...) INSERT INTO ...` failed because the INSERT handler didn't check for a preceding CTE.

- **Trigger + NEW/OLD**: The trigger body referenced `NEW.column_name` and `OLD.column_name`, but the text substitution that replaced these with actual values was never wired up.

- **NATURAL JOIN**: The `NATURAL` keyword was never added to the token list, so `SELECT * FROM a NATURAL JOIN b` choked on "unexpected NATURAL."

Each of these is trivial to fix — 5-20 lines. But they're invisible until someone tries that specific combination. The differential fuzzer (testing against SQLite) caught some of them. The rest were found by writing end-to-end tests that combined features aggressively.

**Lesson:** If you have N features, you don't have N things to test. You have N² combinations to worry about. Write tests that chain features: CTEs inside subqueries inside JOINs with GROUP BY and HAVING. That's where the bodies are buried.

## Vectorized Execution in JavaScript

Once the standard Volcano engine was solid, I built a vectorized execution engine inspired by DuckDB. Instead of pulling one row at a time through the iterator tree, the vectorized engine processes data in columnar batches:

```javascript
class VectorBatch {
  constructor(columns, size) {
    this.columns = columns;  // { name: Float64Array | Array }
    this.size = size;
  }
}

class VHashAggregate {
  nextBatch() {
    while (true) {
      const batch = this.child.nextBatch();
      if (!batch) break;
      // Process entire column at once
      for (let i = 0; i < batch.size; i++) {
        const key = this.getGroupKey(batch, i);
        this.accumulate(key, batch, i);
      }
    }
    return this.buildResultBatch();
  }
}
```

The results? **1.3-1.7x faster** on aggregation queries over 10K rows. In C++, vectorized execution delivers 5-10x or more because it enables SIMD instructions and better cache utilization. In JavaScript, the gains are more modest — you're mainly amortizing per-row function call overhead and reducing object allocations.

The integration was the hard part. The vectorized engine produces results in a different column ordering than the standard path, and un-aliased aggregates got different names (`COUNT(id)` vs `COUNT(*)`). Getting the two paths to produce byte-identical results for all 4,313 tests took longer than building the engine itself.

## What I'd Do Differently

**Start with case-insensitive identifiers.** SQL identifiers are case-insensitive by spec. I uppercase everything in the parser now, but the transition was painful — tests broke everywhere.

**Build a differential fuzzer on day one.** Testing against SQLite catches entire categories of bugs that unit tests miss. Random query generation with result comparison is the highest-ROI testing strategy for a database.

**Don't add a `default: return true` to anything.** Not a switch statement, not a helper function, not a fallback path. Make unknown inputs crash immediately. Silent wrong answers are the worst kind of bug.

**Column naming conventions matter more than you think.** When you have multiple execution paths (Volcano, vectorized, JIT), they all need to produce identically-named result columns. Decide the naming convention once, document it, enforce it in tests.

## The Numbers

Final stats as of today:

| Metric | Value |
|--------|-------|
| Test files | 871 |
| Total tests | 4,313 |
| Source lines | 206K+ |
| Dependencies | 0 |
| INSERT throughput | 14K rows/sec |
| Point query | 9.7K queries/sec |
| Full scan | 500K rows/sec |
| SQLite SQL coverage | 98% (47/47 features) |
| Join algorithms | Hash, Nested Loop, Sort-Merge |
| Concurrency | MVCC + SSI |

Is it production-ready? No. It's an in-memory database written in JavaScript. But it implements every major feature of a real database engine, and it does it correctly — verified by 4,313 tests and a differential fuzzer that compares results against SQLite.

## What I Actually Learned

Building a database teaches you that **correctness is harder than performance**. I can make queries faster with indexes, batch processing, and vectorized execution. But making queries *correct* across every combination of SQL features, transaction isolation levels, and concurrent access patterns? That's where the real engineering lives.

The bugs that kept me up weren't the crashes. Crashes are honest — they tell you something went wrong. The dangerous bugs are the ones that return plausible-looking wrong answers. A join that silently becomes a cross join. An expression that silently evaluates to true. A division that silently truncates.

Every time I found one of those bugs, the same thought: *how long has this been wrong?*

That's why we write tests. Not to prove correctness — you can't prove a negative. But to raise the cost of being wrong. 4,313 tests means 4,313 chances to catch a silent lie before it reaches a user.

[HenryDB on GitHub →](https://github.com/henry-the-frog/henrydb)
