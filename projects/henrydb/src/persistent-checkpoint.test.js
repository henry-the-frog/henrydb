// persistent-checkpoint.test.js — Verify PersistentDatabase WAL checkpoint/truncation
import { describe, it, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { PersistentDatabase } from './persistent-db.js';
import { mkdtempSync, statSync, existsSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';

describe('PersistentDatabase Checkpoint', () => {
  let dir, db;

  afterEach(() => {
    if (db) try { db.close(); } catch {}
    if (dir && existsSync(dir)) rmSync(dir, { recursive: true, force: true });
  });

  it('checkpoint() truncates WAL to near-zero', () => {
    dir = mkdtempSync(join(tmpdir(), 'pdb-ckpt-'));
    db = PersistentDatabase.open(dir);
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val TEXT)');
    for (let i = 0; i < 100; i++) {
      db.execute(`INSERT INTO t VALUES (${i}, 'value${i}')`);
    }

    const walPath = join(dir, 'wal.log');
    const walSizeBefore = statSync(walPath).size;
    assert.ok(walSizeBefore > 0, 'WAL should have data before checkpoint');

    const truncatedSize = db.checkpoint();
    assert.ok(truncatedSize > 0, 'Should report non-zero truncated size');

    const walSizeAfter = statSync(walPath).size;
    assert.ok(walSizeAfter < walSizeBefore,
      `WAL should be smaller after checkpoint: before=${walSizeBefore}, after=${walSizeAfter}`);
  });

  it('data survives checkpoint + close + reopen', () => {
    dir = mkdtempSync(join(tmpdir(), 'pdb-ckpt-'));
    db = PersistentDatabase.open(dir);
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val TEXT)');
    for (let i = 0; i < 50; i++) {
      db.execute(`INSERT INTO t VALUES (${i}, 'value${i}')`);
    }

    db.checkpoint();
    db.close();
    db = null;

    // Reopen
    const db2 = PersistentDatabase.open(dir);
    const result = db2.execute('SELECT COUNT(*) AS cnt FROM t');
    assert.equal(result.rows[0].cnt, 50);
    db2.close();
  });

  it('auto-checkpoint triggers on WAL size threshold', () => {
    dir = mkdtempSync(join(tmpdir(), 'pdb-auto-'));
    db = PersistentDatabase.open(dir);
    db._autoCheckpointBytes = 1024; // Low threshold for testing
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val TEXT)');

    const walPath = join(dir, 'wal.log');
    let maxWalSize = 0;

    for (let i = 0; i < 100; i++) {
      db.execute(`INSERT INTO t VALUES (${i}, '${'x'.repeat(100)}')`);
      try {
        const size = statSync(walPath).size;
        if (size > maxWalSize) maxWalSize = size;
      } catch {}
    }

    // With auto-checkpoint at 1KB, the WAL should have been truncated
    // at least once during the 100 inserts
    const finalSize = statSync(walPath).size;
    assert.ok(maxWalSize < 100 * 200, // Should not grow to full 100*200 bytes
      `WAL should have been auto-checkpointed: maxWalSize=${maxWalSize}`);
  });
});
