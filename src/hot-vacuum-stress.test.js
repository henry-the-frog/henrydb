import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

function query(db, sql) {
  const r = db.execute(sql);
  return r && r.rows ? r.rows : r;
}

describe('HOT Chain + VACUUM Stress Tests', () => {
  
  it('should handle rapid sequential HOT updates on same row', () => {
    const db = new Database();
    db.execute('CREATE TABLE hot_stress (id INTEGER PRIMARY KEY, counter INTEGER, data TEXT)');
    db.execute('CREATE INDEX idx_hot_id ON hot_stress (id)');
    db.execute("INSERT INTO hot_stress VALUES (1, 0, 'initial')");
    
    // Update non-indexed column 50 times — all should be HOT
    for (let i = 1; i <= 50; i++) {
      db.execute(`UPDATE hot_stress SET counter = ${i} WHERE id = 1`);
    }
    
    // Verify latest value visible
    const rows = query(db, 'SELECT * FROM hot_stress WHERE id = 1');
    assert.equal(rows.length, 1);
    assert.equal(rows[0].counter, 50);
  });

  it('should maintain index correctness after many HOT updates + VACUUM', () => {
    const db = new Database();
    db.execute('CREATE TABLE idx_stress (id INTEGER PRIMARY KEY, name TEXT, score INTEGER)');
    db.execute('CREATE INDEX idx_score ON idx_stress (score)');
    
    // Insert rows
    for (let i = 0; i < 20; i++) {
      db.execute(`INSERT INTO idx_stress VALUES (${i}, 'row_${i}', ${i * 10})`);
    }
    
    // HOT update non-indexed column (name) repeatedly
    for (let round = 0; round < 5; round++) {
      for (let i = 0; i < 20; i++) {
        db.execute(`UPDATE idx_stress SET name = 'round_${round}_row_${i}' WHERE id = ${i}`);
      }
    }
    
    // VACUUM to clean up
    db.execute('VACUUM');
    
    // Verify index scan still works correctly
    const byScore = query(db, 'SELECT * FROM idx_stress WHERE score = 100');
    assert.equal(byScore.length, 1);
    assert.equal(byScore[0].id, 10);
    assert.equal(byScore[0].name, 'round_4_row_10');
    
    // Verify all rows present
    const all = query(db, 'SELECT COUNT(*) AS c FROM idx_stress');
    assert.equal(all[0].c, 20);
  });

  it('should correctly handle mixed HOT and non-HOT updates', () => {
    const db = new Database();
    db.execute('CREATE TABLE mixed (id INTEGER PRIMARY KEY, indexed_col INTEGER, data TEXT)');
    db.execute('CREATE INDEX idx_mixed ON mixed (indexed_col)');
    db.execute("INSERT INTO mixed VALUES (1, 100, 'start')");
    
    // HOT update (data only)
    db.execute("UPDATE mixed SET data = 'hot1' WHERE id = 1");
    db.execute("UPDATE mixed SET data = 'hot2' WHERE id = 1");
    
    // Non-HOT update (indexed column changes)
    db.execute("UPDATE mixed SET indexed_col = 200 WHERE id = 1");
    
    // HOT update again
    db.execute("UPDATE mixed SET data = 'hot3' WHERE id = 1");
    
    // Verify through both PK and index scan
    const byPk = query(db, 'SELECT * FROM mixed WHERE id = 1');
    assert.equal(byPk[0].indexed_col, 200);
    assert.equal(byPk[0].data, 'hot3');
    
    // Old index value shouldn't find anything
    const oldIdx = query(db, 'SELECT * FROM mixed WHERE indexed_col = 100');
    assert.equal(oldIdx.length, 0);
    
    // New index value should find the row
    const newIdx = query(db, 'SELECT * FROM mixed WHERE indexed_col = 200');
    assert.equal(newIdx.length, 1);
    assert.equal(newIdx[0].data, 'hot3');
  });

  it('should handle VACUUM pruning HOT chains across many rows', () => {
    const db = new Database();
    db.execute('CREATE TABLE prune_test (id INTEGER PRIMARY KEY, val TEXT)');
    
    // Insert 50 rows
    for (let i = 0; i < 50; i++) {
      db.execute(`INSERT INTO prune_test VALUES (${i}, 'v0')`);
    }
    
    // Update each row 3 times (creates HOT chains)
    for (let round = 1; round <= 3; round++) {
      for (let i = 0; i < 50; i++) {
        db.execute(`UPDATE prune_test SET val = 'v${round}' WHERE id = ${i}`);
      }
    }
    
    // VACUUM should prune chains
    const result = db.execute('VACUUM');
    
    // All rows should have latest value
    const rows = query(db, 'SELECT * FROM prune_test ORDER BY id');
    assert.equal(rows.length, 50);
    for (const row of rows) {
      assert.equal(row.val, 'v3', `Row ${row.id} should have val=v3`);
    }
  });

  it('should handle DELETE after HOT updates + VACUUM', () => {
    const db = new Database();
    db.execute('CREATE TABLE del_hot (id INTEGER PRIMARY KEY, name TEXT, extra TEXT)');
    db.execute('CREATE INDEX idx_del_name ON del_hot (name)');
    
    // Insert and HOT-update
    for (let i = 0; i < 10; i++) {
      db.execute(`INSERT INTO del_hot VALUES (${i}, 'name_${i}', 'v0')`);
    }
    for (let i = 0; i < 10; i++) {
      db.execute(`UPDATE del_hot SET extra = 'v1' WHERE id = ${i}`);
    }
    
    // Delete half
    for (let i = 0; i < 5; i++) {
      db.execute(`DELETE FROM del_hot WHERE id = ${i}`);
    }
    
    // VACUUM
    db.execute('VACUUM');
    
    // Should have 5 rows remaining
    const remaining = query(db, 'SELECT COUNT(*) AS c FROM del_hot');
    assert.equal(remaining[0].c, 5);
    
    // Index scan should still work
    const byName = query(db, "SELECT * FROM del_hot WHERE name = 'name_7'");
    assert.equal(byName.length, 1);
    assert.equal(byName[0].extra, 'v1');
  });

  it('should handle concurrent-like interleaved updates on different rows', () => {
    const db = new Database();
    db.execute('CREATE TABLE interleave (id INTEGER PRIMARY KEY, a INTEGER, b TEXT)');
    db.execute('CREATE INDEX idx_a ON interleave (a)');
    
    // Insert 3 rows
    db.execute("INSERT INTO interleave VALUES (1, 10, 'x')");
    db.execute("INSERT INTO interleave VALUES (2, 20, 'x')");
    db.execute("INSERT INTO interleave VALUES (3, 30, 'x')");
    
    // Interleave HOT updates across rows
    for (let round = 0; round < 10; round++) {
      db.execute(`UPDATE interleave SET b = 'r${round}' WHERE id = 1`);
      db.execute(`UPDATE interleave SET b = 'r${round}' WHERE id = 2`);
      db.execute(`UPDATE interleave SET b = 'r${round}' WHERE id = 3`);
    }
    
    // All rows should reflect latest update
    const rows = query(db, 'SELECT * FROM interleave ORDER BY id');
    assert.equal(rows.length, 3);
    for (const row of rows) {
      assert.equal(row.b, 'r9');
    }
    
    // Index should work for all original values
    assert.equal(query(db, 'SELECT * FROM interleave WHERE a = 10').length, 1);
    assert.equal(query(db, 'SELECT * FROM interleave WHERE a = 20').length, 1);
    assert.equal(query(db, 'SELECT * FROM interleave WHERE a = 30').length, 1);
  });

  it('should handle INSERT after VACUUM on HOT-updated table', () => {
    const db = new Database();
    db.execute('CREATE TABLE reuse (id INTEGER PRIMARY KEY, val TEXT)');
    
    // Create + HOT update + VACUUM cycle
    for (let i = 0; i < 10; i++) {
      db.execute(`INSERT INTO reuse VALUES (${i}, 'initial')`);
    }
    for (let i = 0; i < 10; i++) {
      db.execute(`UPDATE reuse SET val = 'updated' WHERE id = ${i}`);
    }
    db.execute('VACUUM');
    
    // Insert more rows — should work fine
    for (let i = 10; i < 20; i++) {
      db.execute(`INSERT INTO reuse VALUES (${i}, 'new')`);
    }
    
    const total = query(db, 'SELECT COUNT(*) AS c FROM reuse');
    assert.equal(total[0].c, 20);
    
    const newRows = query(db, "SELECT * FROM reuse WHERE val = 'new'");
    assert.equal(newRows.length, 10);
  });

  it('should handle VACUUM on table with no HOT chains', () => {
    const db = new Database();
    db.execute('CREATE TABLE no_hot (id INTEGER PRIMARY KEY, name TEXT)');
    db.execute("INSERT INTO no_hot VALUES (1, 'Alice')");
    db.execute("INSERT INTO no_hot VALUES (2, 'Bob')");
    
    // VACUUM without any updates
    db.execute('VACUUM');
    
    const rows = query(db, 'SELECT * FROM no_hot ORDER BY id');
    assert.equal(rows.length, 2);
    assert.equal(rows[0].name, 'Alice');
  });

  it('should survive rapid update-delete-insert cycle', () => {
    const db = new Database();
    db.execute('CREATE TABLE cycle_test (id INTEGER PRIMARY KEY, counter INTEGER)');
    
    // Rapid cycle: insert → update × 5 → delete → repeat
    for (let cycle = 0; cycle < 5; cycle++) {
      const baseId = cycle * 3;
      db.execute(`INSERT INTO cycle_test VALUES (${baseId}, 0)`);
      db.execute(`INSERT INTO cycle_test VALUES (${baseId + 1}, 0)`);
      db.execute(`INSERT INTO cycle_test VALUES (${baseId + 2}, 0)`);
      
      for (let u = 1; u <= 5; u++) {
        db.execute(`UPDATE cycle_test SET counter = ${u} WHERE id = ${baseId}`);
      }
      
      db.execute(`DELETE FROM cycle_test WHERE id = ${baseId + 1}`);
    }
    
    // VACUUM at end
    db.execute('VACUUM');
    
    // Should have 10 rows (3 per cycle minus 1 deleted = 2, × 5 cycles = 10)
    const count = query(db, 'SELECT COUNT(*) AS c FROM cycle_test');
    assert.equal(count[0].c, 10);
    
    // First row of each cycle should have counter=5
    for (let cycle = 0; cycle < 5; cycle++) {
      const row = query(db, `SELECT * FROM cycle_test WHERE id = ${cycle * 3}`);
      assert.equal(row.length, 1);
      assert.equal(row[0].counter, 5);
    }
  });
});
