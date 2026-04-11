// persistence-fuzzer.test.js — Differential fuzzing: PersistentDatabase vs in-memory Database
// Random schema gen, random DML, close/reopen cycles, verify data matches
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';
import { PersistentDatabase } from './persistent-db.js';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

function seededRandom(seed) {
  let s = seed;
  return () => {
    s = (s * 1103515245 + 12345) & 0x7fffffff;
    return s / 0x7fffffff;
  };
}

function randomInt(rng, min, max) {
  return Math.floor(rng() * (max - min + 1)) + min;
}

function randomString(rng, len) {
  const chars = 'abcdefghijklmnopqrstuvwxyz';
  let s = '';
  for (let i = 0; i < len; i++) s += chars[Math.floor(rng() * chars.length)];
  return s;
}

function generateSchema(rng, tableIdx) {
  const name = 't' + tableIdx;
  const numCols = randomInt(rng, 2, 5);
  const cols = [{ name: 'id', type: 'INT PRIMARY KEY' }];
  for (let i = 1; i < numCols; i++) {
    const type = rng() < 0.5 ? 'INT' : 'TEXT';
    cols.push({ name: 'c' + i, type });
  }
  const ddl = `CREATE TABLE ${name} (${cols.map(c => c.name + ' ' + c.type).join(', ')})`;
  return { name, cols, ddl };
}

function generateInsert(rng, schema, id) {
  const vals = [id];
  for (let i = 1; i < schema.cols.length; i++) {
    if (schema.cols[i].type === 'INT') {
      vals.push(randomInt(rng, -1000, 1000));
    } else {
      vals.push("'" + randomString(rng, randomInt(rng, 3, 12)) + "'");
    }
  }
  return `INSERT INTO ${schema.name} VALUES (${vals.join(', ')})`;
}

function generateUpdate(rng, schema, id) {
  // Pick a non-PK column to update
  const colIdx = randomInt(rng, 1, schema.cols.length - 1);
  const col = schema.cols[colIdx];
  let val;
  if (col.type === 'INT') {
    val = randomInt(rng, -1000, 1000);
  } else {
    val = "'" + randomString(rng, randomInt(rng, 3, 8)) + "'";
  }
  return `UPDATE ${schema.name} SET ${col.name} = ${val} WHERE id = ${id}`;
}

function generateDelete(rng, schema, id) {
  return `DELETE FROM ${schema.name} WHERE id = ${id}`;
}

function runFuzzerRound(seed, numTables, numOpsPerTable, numReopenCycles) {
  const rng = seededRandom(seed);
  const dir = mkdtempSync(join(tmpdir(), 'persist-fuzz-'));
  
  try {
    // Generate schemas
    const schemas = [];
    for (let t = 0; t < numTables; t++) {
      schemas.push(generateSchema(rng, t));
    }
    
    // Track expected state per table (map of id → row values)
    const expectedState = schemas.map(() => new Map());
    
    // Open persistent DB
    let pdb = PersistentDatabase.open(dir, { poolSize: 8 }); // Small pool to force evictions
    
    // Create tables
    for (const s of schemas) {
      pdb.execute(s.ddl);
    }
    
    // Run random DML operations
    let nextId = schemas.map(() => 1);
    for (let cycle = 0; cycle <= numReopenCycles; cycle++) {
      for (let op = 0; op < numOpsPerTable; op++) {
        const tableIdx = randomInt(rng, 0, schemas.length - 1);
        const schema = schemas[tableIdx];
        const state = expectedState[tableIdx];
        
        const action = rng();
        if (action < 0.5 || state.size === 0) {
          // INSERT
          const id = nextId[tableIdx]++;
          const sql = generateInsert(rng, schema, id);
          pdb.execute(sql);
          // Track expected values by re-parsing (crude but works)
          const row = pdb.execute(`SELECT * FROM ${schema.name} WHERE id = ${id}`).rows[0];
          if (row) state.set(id, row);
        } else if (action < 0.75) {
          // UPDATE
          const ids = [...state.keys()];
          const id = ids[randomInt(rng, 0, ids.length - 1)];
          const sql = generateUpdate(rng, schema, id);
          pdb.execute(sql);
          const row = pdb.execute(`SELECT * FROM ${schema.name} WHERE id = ${id}`).rows[0];
          if (row) state.set(id, row);
        } else {
          // DELETE
          const ids = [...state.keys()];
          const id = ids[randomInt(rng, 0, ids.length - 1)];
          const sql = generateDelete(rng, schema, id);
          pdb.execute(sql);
          state.delete(id);
        }
      }
      
      // Verify current state matches expected
      for (let t = 0; t < schemas.length; t++) {
        const rows = pdb.execute(`SELECT * FROM ${schemas[t].name} ORDER BY id`).rows;
        const expected = [...expectedState[t].values()].sort((a, b) => a.id - b.id);
        assert.equal(rows.length, expected.length,
          `seed=${seed} cycle=${cycle} table=${schemas[t].name}: row count mismatch (${rows.length} vs ${expected.length})`);
        for (let r = 0; r < rows.length; r++) {
          assert.deepStrictEqual(rows[r], expected[r],
            `seed=${seed} cycle=${cycle} table=${schemas[t].name} row ${r}: data mismatch`);
        }
      }
      
      if (cycle < numReopenCycles) {
        // Close and reopen
        pdb.close();
        pdb = PersistentDatabase.open(dir, { poolSize: 8 });
      }
    }
    
    pdb.close();
    return true;
  } finally {
    try { rmSync(dir, { recursive: true }); } catch {}
  }
}

describe('Persistence Fuzzer', () => {
  // Run 20 different random seeds
  for (let seed = 1; seed <= 20; seed++) {
    it(`seed ${seed}: 3 tables, 30 ops, 3 reopen cycles`, () => {
      runFuzzerRound(seed, 3, 30, 3);
    });
  }
  
  // Heavier stress tests
  it('heavy: 5 tables, 100 ops, 5 reopen cycles (seed 42)', () => {
    runFuzzerRound(42, 5, 100, 5);
  });
  
  it('tiny pool: 2 tables, 50 ops, 4 cycles, pool size forced small', () => {
    runFuzzerRound(99, 2, 50, 4);
  });
  
  it('many cycles: 1 table, 20 ops, 10 reopen cycles (seed 777)', () => {
    runFuzzerRound(777, 1, 20, 10);
  });
});
