// checkpoint-explore.test.js — WAL checkpoint and truncation tests
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync, statSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { TransactionalDatabase } from './transactional-db.js';

describe('WAL Checkpoint', () => {

  it('checkpoint truncates WAL', () => {
    const dir = mkdtempSync(join(tmpdir(), 'henrydb-ckpt-'));
    const db = TransactionalDatabase.open(dir);
    
    db.execute("CREATE TABLE data (id INT, val TEXT)");
    for (let i = 0; i < 20; i++) {
      db.execute(`INSERT INTO data VALUES (${i}, 'row-${i}')`);
    }
    
    const walPath = join(dir, 'wal.log');
    const walBefore = statSync(walPath).size;
    console.log('WAL size before checkpoint:', walBefore, 'bytes');
    assert.ok(walBefore > 0, 'WAL should have data');
    
    const result = db.checkpoint();
    console.log('Checkpoint result:', result);
    
    const walAfter = statSync(walPath).size;
    console.log('WAL size after checkpoint:', walAfter, 'bytes');
    assert.equal(walAfter, 0, 'WAL should be truncated');
    
    // Data should still be accessible
    const rows = db.execute("SELECT COUNT(*) as n FROM data");
    assert.equal(rows.rows[0].n, 20, 'Data should still be accessible');
    
    db.close();
    rmSync(dir, { recursive: true });
  });

  it('data survives checkpoint + restart', () => {
    const dir = mkdtempSync(join(tmpdir(), 'henrydb-ckpt-'));
    
    const db1 = TransactionalDatabase.open(dir);
    db1.execute("CREATE TABLE scores (id INT, score INT)");
    for (let i = 0; i < 10; i++) {
      db1.execute(`INSERT INTO scores VALUES (${i}, ${i * 100})`);
    }
    
    db1.checkpoint();
    db1.close();
    
    const db2 = TransactionalDatabase.open(dir);
    const result = db2.execute("SELECT COUNT(*) as n FROM scores");
    assert.equal(result.rows[0].n, 10, 'All rows should survive checkpoint + restart');
    
    const sum = db2.execute("SELECT SUM(score) as total FROM scores");
    assert.equal(sum.rows[0].total, 4500); // 0+100+200+...+900
    
    db2.close();
    rmSync(dir, { recursive: true });
  });

  it('checkpoint rejects while transaction in progress', () => {
    const dir = mkdtempSync(join(tmpdir(), 'henrydb-ckpt-'));
    const db = TransactionalDatabase.open(dir);
    
    db.execute("CREATE TABLE t (id INT)");
    
    const s = db.session();
    s.begin();
    s.execute("INSERT INTO t VALUES (1)");
    
    assert.throws(() => db.checkpoint(), /transactions are in progress/);
    
    s.rollback();
    s.close();
    
    // Now it should work
    db.checkpoint(); // no throw
    db.close();
    rmSync(dir, { recursive: true });
  });

  it('multiple checkpoint cycles', () => {
    const dir = mkdtempSync(join(tmpdir(), 'henrydb-ckpt-'));
    const db = TransactionalDatabase.open(dir);
    db.execute("CREATE TABLE log (id INT)");
    
    for (let cycle = 0; cycle < 5; cycle++) {
      // Write some data
      for (let i = 0; i < 10; i++) {
        db.execute(`INSERT INTO log VALUES (${cycle * 10 + i})`);
      }
      
      // Checkpoint
      db.checkpoint();
      
      const walSize = statSync(join(dir, 'wal.log')).size;
      assert.equal(walSize, 0, `WAL should be empty after checkpoint ${cycle}`);
    }
    
    const count = db.execute("SELECT COUNT(*) as n FROM log");
    assert.equal(count.rows[0].n, 50, 'All 50 rows should be accessible');
    
    db.close();
    
    // Verify after restart
    const db2 = TransactionalDatabase.open(dir);
    const count2 = db2.execute("SELECT COUNT(*) as n FROM log");
    assert.equal(count2.rows[0].n, 50, 'All 50 rows survive 5 checkpoints + restart');
    db2.close();
    rmSync(dir, { recursive: true });
  });

  it('WAL growth tracking', () => {
    const dir = mkdtempSync(join(tmpdir(), 'henrydb-ckpt-'));
    const db = TransactionalDatabase.open(dir);
    db.execute("CREATE TABLE growing (id INT, data TEXT)");
    
    const sizes = [];
    for (let i = 0; i < 100; i++) {
      db.execute(`INSERT INTO growing VALUES (${i}, '${'x'.repeat(50)}')`);
      if (i % 20 === 19) {
        sizes.push(statSync(join(dir, 'wal.log')).size);
      }
    }
    
    console.log('WAL growth (every 20 inserts):', sizes.map(s => `${(s/1024).toFixed(1)}KB`));
    
    // WAL should be growing
    for (let i = 1; i < sizes.length; i++) {
      assert.ok(sizes[i] > sizes[i-1], 'WAL should grow over time');
    }
    
    // Checkpoint should shrink it
    db.checkpoint();
    const afterCheckpoint = statSync(join(dir, 'wal.log')).size;
    assert.equal(afterCheckpoint, 0, 'Checkpoint should truncate WAL');
    
    db.close();
    rmSync(dir, { recursive: true });
  });
});
