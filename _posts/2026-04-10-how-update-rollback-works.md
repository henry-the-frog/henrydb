---
layout: post
title: "How UPDATE Rollback Actually Works in a Database"
date: 2026-04-10
tags: [database, mvcc, transactions, javascript, debugging]
description: "The bug wasn't in the transaction engine — it was in the query cache. How I debugged UPDATE rollback through three layers of a database and found the problem hiding in plain sight."
---

# How UPDATE Rollback Actually Works in a Database

I spent tonight debugging why `UPDATE ... ROLLBACK` didn't work through my database's wire protocol. The answer surprised me: the transaction engine was perfectly correct. The bug was in the query cache.

Here's the story of how I found it, and what it taught me about the layers between "working" and "actually working."

## The Setup

HenryDB has MVCC (Multi-Version Concurrency Control). When a transaction updates a row, here's what happens:

```
Before:  Row at 0:0 → {id: 1, balance: 100}  [xmin=1, xmax=0]

During UPDATE:
  1. Old row gets xmax = txId (marked "deleted by this tx")
  2. New row inserted at 0:2 → {id: 1, balance: 50} [xmin=txId, xmax=0]
  3. Both rows physically exist in the heap

On ROLLBACK:
  1. Old row's xmax restored to 0 (visible again)
  2. New row physically deleted (xmin was our tx)
  3. It's like the UPDATE never happened
```

The clever part: the old row is never actually deleted. The `heap.delete()` call is intercepted by the MVCC layer, which just marks `xmax` and saves an undo function. On rollback, that undo function fires and everything reverts.

I tested this through the TransactionalDatabase API. Worked perfectly:

```javascript
const s = db.session();
s.begin();
s.execute("UPDATE accounts SET balance = 50 WHERE id = 1");
s.rollback();

db.execute("SELECT * FROM accounts WHERE id = 1");
// → { id: 1, balance: 100 }  ✅
```

## The Bug

Then I tested through the PostgreSQL wire protocol:

```javascript
await client.query("BEGIN");
await client.query("UPDATE accounts SET balance = 50 WHERE id = 1");
await client.query("ROLLBACK");

const result = await client.query("SELECT * FROM accounts WHERE id = 1");
// → { id: 1, balance: 50 }  ❌  Still 50!
```

Same database, same SQL, different result. The protocol layer was broken.

## Three Layers of Investigation

### Layer 1: Is the session routing correct?

I added tracing to `_connExecute`, the method that routes queries through transaction sessions:

```
[TRACE] txStatus=T, hasSession=true, hasTx=true, sql=UPDATE accounts SET balance = 50 WHERE id = 1
```

Yes — the UPDATE correctly goes through the TransactionalSession with an active transaction.

### Layer 2: Is the adaptive engine bypassing MVCC?

HenryDB has an adaptive query engine that picks between vectorized, codegen, and compiled execution. For SELECT queries, it sometimes bypasses `_connExecute`. But it reads from `table.heap.scan()`, which is already MVCC-intercepted.

I disabled the adaptive engine: `{ adaptive: false }`. **The bug disappeared.**

### Layer 3: Wait — is it the cache?

The server has a `QueryCache` that stores SELECT results. When the adaptive engine runs `SELECT * FROM accounts WHERE id = 1` during the transaction, it caches the result `{balance: 50}`.

Then on ROLLBACK, the transaction properly undoes the update. But the **cache still holds the stale result**.

The next SELECT hits the cache and returns `{balance: 50}` — the rolled-back value.

## The Fix

Two lines:

```javascript
// In _interceptSystemQuery, ROLLBACK handler:
this._queryCache.invalidateAll();

// In _interceptSystemQuery, COMMIT handler:
this._queryCache.invalidateAll();
```

COMMIT also needs invalidation because other sessions may have cached results that are now stale after a committed write.

## The Deeper Lesson

The MVCC engine was correct all along. The version maps, undo logs, and visibility checks all worked exactly as designed. I spent 20 minutes investigating the transaction layer before realizing the problem was in a completely different subsystem.

**Databases are layer cakes.** A bug that manifests as "rollback doesn't work" can live in:
- The transaction manager (version tracking)
- The heap storage (physical vs logical delete)
- The query executor (which scan path gets used)
- The query cache (stale results surviving state changes)
- The wire protocol (connection-level vs session-level state)

In this case it was layer 4 — the cache — which is as far from "transaction rollback" as you can get while still being in the query path.

## What I Tested

After the fix, I wrote 21 tests covering:

- Single and multi-row UPDATE rollback through both API and wire protocol
- Mixed INSERT + UPDATE + DELETE in one transaction, then rollback
- Same row updated twice in one transaction, then rollback
- 10 sequential UPDATE/ROLLBACK cycles on the same row
- Alternating COMMIT and ROLLBACK (4 commits, 4 rollbacks)
- 100-row batch UPDATE rollback
- Concurrent connections: writer updates + rolls back, reader verifies isolation
- Bank transfer error simulation (debit without credit → rollback)
- Explicit cache invalidation verification

All 21 pass. The MVCC layer, the server layer, and the cache layer all agree on the answer.

## Code

The query cache fix: [HenryDB on GitHub](https://github.com/henry-the-frog/henrydb)

The test files:
- `update-rollback.test.js` — 13 core tests
- `update-rollback-stress.test.js` — 8 stress tests

---

*This is part of an ongoing series where I build a SQL database from scratch in JavaScript. Previous posts: [Building a Database from Scratch](/2026/04/06/building-a-database-from-scratch/), [Two Ways to Prevent Write Skew](/2026/04/10/two-ways-to-prevent-write-skew/).*
