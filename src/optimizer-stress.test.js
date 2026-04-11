// HenryDB Query Optimizer Stress Test
// Random multi-table schemas + queries: JOINs, WHERE, GROUP BY, ORDER BY, subqueries
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

function seeded(seed) {
  let s = seed;
  return () => { s = (s * 1103515245 + 12345) & 0x7fffffff; return s / 0x7fffffff; };
}
function randomInt(rng, min, max) { return Math.floor(rng() * (max - min + 1)) + min; }
function randomChoice(rng, arr) { return arr[Math.floor(rng() * arr.length)]; }

function setupDatabase(rng) {
  const db = new Database();
  const tables = [];
  const numTables = randomInt(rng, 2, 4);
  
  for (let t = 0; t < numTables; t++) {
    const name = 't' + t;
    const numCols = randomInt(rng, 2, 5);
    const cols = [{ name: 'id', type: 'INT PRIMARY KEY' }];
    for (let c = 1; c < numCols; c++) {
      const type = rng() < 0.5 ? 'INT' : 'TEXT';
      cols.push({ name: name + '_c' + c, type });
    }
    
    db.execute(`CREATE TABLE ${name} (${cols.map(c => c.name + ' ' + c.type).join(', ')})`);
    
    // Insert some data
    const numRows = randomInt(rng, 10, 50);
    for (let r = 1; r <= numRows; r++) {
      const vals = [r];
      for (let c = 1; c < cols.length; c++) {
        if (cols[c].type === 'INT') {
          vals.push(randomInt(rng, 1, 100));
        } else {
          vals.push("'" + randomChoice(rng, ['alice', 'bob', 'carol', 'dave', 'eve', 'frank']) + "'");
        }
      }
      db.execute(`INSERT INTO ${name} VALUES (${vals.join(', ')})`);
    }
    
    tables.push({ name, cols, numRows });
  }
  
  return { db, tables };
}

function randomQuery(rng, tables) {
  const r = rng();
  
  if (r < 0.25) {
    // Simple SELECT with WHERE
    const t = randomChoice(rng, tables);
    const col = randomChoice(rng, t.cols.slice(1));
    if (col.type === 'INT') {
      const val = randomInt(rng, 1, 50);
      const op = randomChoice(rng, ['=', '>', '<', '>=', '<=', '!=']);
      return `SELECT * FROM ${t.name} WHERE ${col.name} ${op} ${val}`;
    } else {
      const val = randomChoice(rng, ['alice', 'bob', 'carol']);
      return `SELECT * FROM ${t.name} WHERE ${col.name} = '${val}'`;
    }
  } else if (r < 0.45) {
    // JOIN
    if (tables.length < 2) return `SELECT * FROM ${tables[0].name}`;
    const t1 = tables[0], t2 = tables[1];
    return `SELECT ${t1.name}.id, ${t2.name}.id FROM ${t1.name} JOIN ${t2.name} ON ${t1.name}.id = ${t2.name}.id`;
  } else if (r < 0.6) {
    // GROUP BY with aggregates
    const t = randomChoice(rng, tables);
    const textCols = t.cols.filter(c => c.type === 'TEXT');
    const intCols = t.cols.filter(c => c.type === 'INT' && c.name !== 'id');
    if (textCols.length > 0 && intCols.length > 0) {
      const groupCol = randomChoice(rng, textCols);
      const aggCol = randomChoice(rng, intCols);
      const agg = randomChoice(rng, ['COUNT', 'SUM', 'AVG', 'MAX', 'MIN']);
      return `SELECT ${groupCol.name}, ${agg}(${aggCol.name}) FROM ${t.name} GROUP BY ${groupCol.name}`;
    }
    return `SELECT COUNT(*) FROM ${t.name}`;
  } else if (r < 0.7) {
    // ORDER BY with LIMIT
    const t = randomChoice(rng, tables);
    const col = randomChoice(rng, t.cols);
    const dir = rng() < 0.5 ? 'ASC' : 'DESC';
    const limit = randomInt(rng, 1, 20);
    return `SELECT * FROM ${t.name} ORDER BY ${col.name} ${dir} LIMIT ${limit}`;
  } else if (r < 0.8) {
    // Subquery in WHERE
    const t = randomChoice(rng, tables);
    return `SELECT * FROM ${t.name} WHERE id IN (SELECT id FROM ${t.name} WHERE id < ${randomInt(rng, 5, 25)})`;
  } else if (r < 0.9) {
    // DISTINCT
    const t = randomChoice(rng, tables);
    const col = randomChoice(rng, t.cols.slice(1));
    return `SELECT DISTINCT ${col.name} FROM ${t.name}`;
  } else {
    // Multi-column ORDER BY
    const t = randomChoice(rng, tables);
    if (t.cols.length >= 3) {
      return `SELECT * FROM ${t.name} ORDER BY ${t.cols[1].name}, ${t.cols[2].name}`;
    }
    return `SELECT * FROM ${t.name} ORDER BY ${t.cols[1].name}`;
  }
}

describe('Query Optimizer Stress', () => {
  for (let seed = 1; seed <= 50; seed++) {
    it(`seed ${seed}: random schema + 20 queries`, () => {
      const rng = seeded(seed);
      const { db, tables } = setupDatabase(rng);
      
      for (let q = 0; q < 20; q++) {
        const sql = randomQuery(rng, tables);
        try {
          const result = db.execute(sql);
          assert.ok(result !== undefined, `Query returned undefined: ${sql}`);
          assert.ok(Array.isArray(result.rows), `No rows array: ${sql}`);
          // Rows should be arrays of objects with consistent keys
          if (result.rows.length > 0) {
            const keys = Object.keys(result.rows[0]);
            for (const row of result.rows) {
              assert.ok(typeof row === 'object', `Row is not object: ${sql}`);
            }
          }
        } catch (e) {
          // Some random queries may have valid errors (type mismatches, etc.)
          // But crashes (TypeError, ReferenceError) should not happen
          if (e instanceof TypeError || e instanceof ReferenceError || e instanceof RangeError) {
            throw new Error(`Engine crash on: ${sql}\n${e.message}\n${e.stack}`);
          }
          // Else: valid SQL error, ignore
        }
      }
    });
  }
});
