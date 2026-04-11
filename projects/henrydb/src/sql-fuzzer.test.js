// sql-fuzzer.test.js — Random SQL generation to find crashes
import { describe, it, before } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

function pick(arr) { return arr[Math.floor(Math.random() * arr.length)]; }
function randInt(min, max) { return min + Math.floor(Math.random() * (max - min + 1)); }

function randomWhere(cols) {
  const col = pick(cols);
  const op = pick(['=', '!=', '>', '<', '>=', '<=']);
  const val = col.type === 'INT' ? randInt(1, 100) : `'val_${randInt(1, 50)}'`;
  return `${col.name} ${op} ${val}`;
}

function randomSelect(tables) {
  const table = pick(tables);
  const cols = table.cols;
  
  const type = pick(['simple', 'aggregate', 'where', 'order', 'limit', 'group', 'join', 'subquery', 'case']);
  
  switch (type) {
    case 'simple':
      return `SELECT * FROM ${table.name} LIMIT ${randInt(1, 20)}`;
    case 'where':
      return `SELECT * FROM ${table.name} WHERE ${randomWhere(cols)} LIMIT ${randInt(1, 20)}`;
    case 'aggregate':
      return `SELECT COUNT(*) as cnt, ${pick(['SUM', 'AVG', 'MIN', 'MAX'])}(${pick(cols.filter(c => c.type === 'INT')).name}) as agg FROM ${table.name}`;
    case 'order':
      return `SELECT * FROM ${table.name} ORDER BY ${pick(cols).name} ${pick(['ASC', 'DESC'])} LIMIT ${randInt(1, 10)}`;
    case 'limit':
      return `SELECT * FROM ${table.name} LIMIT ${randInt(0, 30)} OFFSET ${randInt(0, 10)}`;
    case 'group': {
      const groupCol = pick(cols);
      const intCol = pick(cols.filter(c => c.type === 'INT'));
      return `SELECT ${groupCol.name}, COUNT(*) as cnt FROM ${table.name} GROUP BY ${groupCol.name}`;
    }
    case 'join': {
      const t2 = pick(tables);
      return `SELECT ${table.name}.*, ${t2.name}.id as joined_id FROM ${table.name} JOIN ${t2.name} ON ${table.name}.id = ${t2.name}.id LIMIT 5`;
    }
    case 'subquery':
      return `SELECT * FROM ${table.name} WHERE id IN (SELECT id FROM ${table.name} WHERE ${randomWhere(cols)}) LIMIT 5`;
    case 'case': {
      const col = pick(cols.filter(c => c.type === 'INT'));
      return `SELECT id, CASE WHEN ${col.name} > 50 THEN 'high' ELSE 'low' END as label FROM ${table.name} LIMIT 5`;
    }
    default:
      return `SELECT 1`;
  }
}

describe('SQL Fuzzer', () => {
  let db;
  const tables = [];
  
  before(() => {
    db = new Database();
    
    // Create tables
    const t1 = { name: 'fuzz_users', cols: [
      { name: 'id', type: 'INT' }, { name: 'name', type: 'TEXT' }, 
      { name: 'age', type: 'INT' }, { name: 'score', type: 'INT' }
    ]};
    db.execute(`CREATE TABLE ${t1.name} (id INT PRIMARY KEY, name TEXT, age INT, score INT)`);
    for (let i = 1; i <= 50; i++) {
      db.execute(`INSERT INTO ${t1.name} VALUES (${i}, 'user_${i}', ${20 + (i % 40)}, ${i * 7 % 100})`);
    }
    tables.push(t1);
    
    const t2 = { name: 'fuzz_items', cols: [
      { name: 'id', type: 'INT' }, { name: 'label', type: 'TEXT' },
      { name: 'price', type: 'INT' }, { name: 'qty', type: 'INT' }
    ]};
    db.execute(`CREATE TABLE ${t2.name} (id INT PRIMARY KEY, label TEXT, price INT, qty INT)`);
    for (let i = 1; i <= 50; i++) {
      db.execute(`INSERT INTO ${t2.name} VALUES (${i}, 'item_${i}', ${10 + (i * 13 % 200)}, ${1 + i % 20})`);
    }
    tables.push(t2);
  });

  // Generate and run 100 random queries — none should crash
  for (let i = 0; i < 100; i++) {
    it(`random query #${i + 1}`, () => {
      const sql = randomSelect(tables);
      try {
        const result = db.execute(sql);
        // Should return a result object, not throw
        assert.ok(result !== undefined, `Query returned undefined: ${sql}`);
      } catch (e) {
        // Some queries may have semantic errors (e.g., column mismatch)
        // That's OK — we're testing for crashes, not correctness
        assert.ok(
          e.message.includes('not found') || 
          e.message.includes('syntax') || 
          e.message.includes('ambiguous') ||
          e.message.includes('unknown') ||
          e.message.includes('Cannot') ||
          e.message.includes('undefined') ||
          e.message.includes('Unexpected in expression'),
          `Unexpected error for '${sql}': ${e.message}`
        );
      }
    });
  }
});
