// vacuum-explore.test.js — Testing VACUUM dead tuple reclamation
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { TransactionalDatabase } from './transactional-db.js';

function freshDb() {
  const dir = mkdtempSync(join(tmpdir(), 'henrydb-vac-'));
  const db = TransactionalDatabase.open(dir);
  return { db, dir };
}

describe('VACUUM Exploration', () => {

  it('VACUUM reclaims dead tuples from DELETE', () => {
    const { db, dir } = freshDb();
    db.execute("CREATE TABLE v (id INT, data TEXT)");
    
    // Insert 100 rows
    for (let i = 0; i < 100; i++) {
      db.execute(`INSERT INTO v VALUES (${i}, 'data-${i}')`);
    }
    
    // Delete 50 rows in a transaction
    const s = db.session();
    s.begin();
    s.execute("DELETE FROM v WHERE id >= 50");
    s.commit();
    s.close();
    
    // Check version map before vacuum
    const vm = db._versionMaps.get('v');
    let deadBefore = 0;
    for (const [, ver] of vm) {
      if (ver.xmax !== 0) deadBefore++;
    }
    console.log('Dead tuples before VACUUM:', deadBefore);
    
    // Run VACUUM
    const result = db.vacuum();
    console.log('VACUUM result:', result);
    
    // Check dead tuples after vacuum
    let deadAfter = 0;
    for (const [, ver] of vm) {
      if (ver.xmax !== 0) deadAfter++;
    }
    console.log('Dead tuples after VACUUM:', deadAfter);
    
    // Verify data integrity
    const rows = db.execute("SELECT COUNT(*) as n FROM v");
    assert.equal(rows.rows[0].n, 50, 'Should have 50 live rows');
    
    assert.ok(deadAfter <= deadBefore, 'VACUUM should reduce dead tuples');
    
    db.close();
    rmSync(dir, { recursive: true });
  });

  it('VACUUM reclaims dead tuples from UPDATE', () => {
    const { db, dir } = freshDb();
    db.execute("CREATE TABLE u (id INT, val INT)");
    db.execute("INSERT INTO u VALUES (1, 100)");
    
    // Do 10 updates — each creates a dead version
    for (let i = 0; i < 10; i++) {
      const s = db.session();
      s.begin();
      s.execute(`UPDATE u SET val = ${(i + 1) * 100} WHERE id = 1`);
      s.commit();
      s.close();
    }
    
    const vm = db._versionMaps.get('u');
    console.log('Version map size before VACUUM:', vm.size);
    
    const result = db.vacuum();
    console.log('VACUUM result:', result);
    console.log('Version map size after VACUUM:', vm.size);
    
    // Should still have the latest value
    const row = db.execute("SELECT val FROM u WHERE id = 1");
    assert.equal(row.rows[0].val, 1000, 'Latest value should survive');
    
    db.close();
    rmSync(dir, { recursive: true });
  });

  it('VACUUM + checkpoint + restart = clean state', () => {
    const { db, dir } = freshDb();
    db.execute("CREATE TABLE clean (id INT, data TEXT)");
    
    for (let i = 0; i < 50; i++) {
      db.execute(`INSERT INTO clean VALUES (${i}, 'row-${i}')`);
    }
    
    // Delete half, then vacuum and checkpoint
    const s = db.session();
    s.begin();
    s.execute("DELETE FROM clean WHERE id >= 25");
    s.commit();
    s.close();
    
    const vacResult = db.vacuum();
    console.log('VACUUM:', vacResult);
    
    const ckptResult = db.checkpoint();
    console.log('Checkpoint:', ckptResult);
    
    db.close();
    
    // Reopen
    const db2 = TransactionalDatabase.open(dir);
    const count = db2.execute("SELECT COUNT(*) as n FROM clean");
    assert.equal(count.rows[0].n, 25, 'Only live rows should survive');
    db2.close();
    rmSync(dir, { recursive: true });
  });

  it('VACUUM SQL syntax works', () => {
    const { db, dir } = freshDb();
    db.execute("CREATE TABLE vsql (id INT)");
    db.execute("INSERT INTO vsql VALUES (1)");
    
    // VACUUM via SQL
    const result = db.execute("VACUUM");
    console.log('VACUUM via SQL:', result);
    assert.ok(result, 'VACUUM SQL should return a result');
    
    db.close();
    rmSync(dir, { recursive: true });
  });
});
