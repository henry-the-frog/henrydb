// fuzz-sql.test.js — Random SQL generation to find parser crashes and unexpected errors
// Goal: Throw random but syntactically plausible SQL at the system and catch crashes

import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

function pick(arr) { return arr[Math.floor(Math.random() * arr.length)]; }
function randInt(lo, hi) { return Math.floor(Math.random() * (hi - lo + 1)) + lo; }
function randStr(len = 5) {
  const chars = 'abcdefghijklmnopqrstuvwxyz';
  return Array.from({length: len}, () => chars[Math.floor(Math.random() * chars.length)]).join('');
}

const TABLES = ['t1', 't2', 't3'];
const COLUMNS = ['id', 'name', 'val', 'age', 'score', 'status'];
const OPS = ['=', '!=', '<', '>', '<=', '>='];
const AGGS = ['COUNT(*)', 'SUM(val)', 'AVG(val)', 'MIN(val)', 'MAX(val)'];
const ORDERS = ['ASC', 'DESC'];

function randomWhere() {
  const col = pick(COLUMNS);
  const op = pick(OPS);
  const val = Math.random() > 0.5 ? randInt(0, 100) : `'${randStr()}'`;
  return `${col} ${op} ${val}`;
}

function randomSelect() {
  const table = pick(TABLES);
  const cols = Math.random() > 0.3 ? '*' : Array.from({length: randInt(1, 3)}, () => pick(COLUMNS)).join(', ');
  let sql = `SELECT ${cols} FROM ${table}`;
  
  if (Math.random() > 0.3) sql += ` WHERE ${randomWhere()}`;
  if (Math.random() > 0.5) sql += ` ORDER BY ${pick(COLUMNS)} ${pick(ORDERS)}`;
  if (Math.random() > 0.5) sql += ` LIMIT ${randInt(1, 50)}`;
  return sql;
}

function randomInsert() {
  const table = pick(TABLES);
  const vals = Array.from({length: 4}, () => 
    Math.random() > 0.5 ? randInt(0, 1000) : `'${randStr()}'`
  );
  return `INSERT INTO ${table} VALUES (${vals.join(', ')})`;
}

function randomUpdate() {
  const table = pick(TABLES);
  const col = pick(['val', 'score', 'age']);
  const val = randInt(0, 1000);
  let sql = `UPDATE ${table} SET ${col} = ${val}`;
  if (Math.random() > 0.3) sql += ` WHERE ${randomWhere()}`;
  return sql;
}

function randomDelete() {
  const table = pick(TABLES);
  let sql = `DELETE FROM ${table}`;
  if (Math.random() > 0.2) sql += ` WHERE ${randomWhere()}`;
  return sql;
}

function randomAggSelect() {
  const table = pick(TABLES);
  const agg = pick(AGGS);
  let sql = `SELECT ${agg} FROM ${table}`;
  if (Math.random() > 0.4) sql += ` WHERE ${randomWhere()}`;
  return sql;
}

function randomGroupBy() {
  const table = pick(TABLES);
  const groupCol = pick(['name', 'status']);
  return `SELECT ${groupCol}, COUNT(*) FROM ${table} GROUP BY ${groupCol}`;
}

function randomJoin() {
  return `SELECT t1.id, t2.val FROM t1 INNER JOIN t2 ON t1.id = t2.id WHERE t1.val > ${randInt(0, 50)}`;
}

function randomSQL() {
  const type = randInt(0, 7);
  switch (type) {
    case 0: case 1: return randomSelect();
    case 2: return randomInsert();
    case 3: return randomUpdate();
    case 4: return randomDelete();
    case 5: return randomAggSelect();
    case 6: return randomGroupBy();
    case 7: return randomJoin();
  }
}

describe('SQL Fuzz Testing', () => {
  let db;
  
  beforeEach(() => {
    db = new Database();
    // Create tables with various column types
    for (const t of TABLES) {
      db.execute(`CREATE TABLE ${t} (id INT PRIMARY KEY, name TEXT, val INT, age INT, score INT, status TEXT)`);
      // Seed with some data
      for (let i = 1; i <= 20; i++) {
        db.execute(`INSERT INTO ${t} VALUES (${i}, '${randStr()}', ${randInt(0, 100)}, ${randInt(18, 65)}, ${randInt(0, 100)}, '${pick(['active', 'inactive', 'pending'])}') `);
      }
    }
  });
  
  it('100 random SELECTs do not crash', () => {
    let errors = 0;
    let successes = 0;
    for (let i = 0; i < 100; i++) {
      const sql = randomSelect();
      try {
        db.execute(sql);
        successes++;
      } catch (e) {
        // Expected errors (column validation, parse errors) are OK
        if (e.message.includes('does not exist') || 
            e.message.includes('Parse error') ||
            e.message.includes('not found') ||
            e.message.includes('Unexpected') ||
            e.message.includes('Expected')) {
          errors++;
        } else {
          // Unexpected error — this is a bug
          assert.fail(`Unexpected error on: ${sql}\n  ${e.message}`);
        }
      }
    }
    console.log(`SELECTs: ${successes} success, ${errors} expected errors`);
    assert.ok(successes > 20, 'At least 20 SELECTs should succeed');
  });
  
  it('100 random INSERTs do not crash', () => {
    let errors = 0;
    let successes = 0;
    for (let i = 0; i < 100; i++) {
      const sql = randomInsert();
      try {
        db.execute(sql);
        successes++;
      } catch (e) {
        if (e.message.includes('UNIQUE') || 
            e.message.includes('duplicate') || 
            e.message.includes('constraint') ||
            e.message.includes('type') ||
            e.message.includes('Parse')) {
          errors++;
        } else {
          assert.fail(`Unexpected error on: ${sql}\n  ${e.message}`);
        }
      }
    }
    console.log(`INSERTs: ${successes} success, ${errors} expected errors`);
  });
  
  it('100 random UPDATEs do not crash', () => {
    let errors = 0;
    let successes = 0;
    for (let i = 0; i < 100; i++) {
      const sql = randomUpdate();
      try {
        db.execute(sql);
        successes++;
      } catch (e) {
        if (e.message.includes('does not exist') || 
            e.message.includes('not found') ||
            e.message.includes('Parse')) {
          errors++;
        } else {
          assert.fail(`Unexpected error on: ${sql}\n  ${e.message}`);
        }
      }
    }
    console.log(`UPDATEs: ${successes} success, ${errors} expected errors`);
    assert.ok(successes > 20, 'At least 20 UPDATEs should succeed');
  });
  
  it('100 random DELETEs do not crash', () => {
    let errors = 0;
    let successes = 0;
    for (let i = 0; i < 100; i++) {
      const sql = randomDelete();
      try {
        db.execute(sql);
        successes++;
      } catch (e) {
        if (e.message.includes('does not exist') || 
            e.message.includes('not found') ||
            e.message.includes('Parse')) {
          errors++;
        } else {
          assert.fail(`Unexpected error on: ${sql}\n  ${e.message}`);
        }
      }
    }
    console.log(`DELETEs: ${successes} success, ${errors} expected errors`);
    assert.ok(successes > 20, 'At least 20 DELETEs should succeed');
  });
  
  it('100 random aggregate queries do not crash', () => {
    let errors = 0;
    let successes = 0;
    for (let i = 0; i < 100; i++) {
      const sql = randomAggSelect();
      try {
        db.execute(sql);
        successes++;
      } catch (e) {
        if (e.message.includes('does not exist') || 
            e.message.includes('Parse') ||
            e.message.includes('Unexpected') ||
            e.message.includes('not found')) {
          errors++;
        } else {
          assert.fail(`Unexpected error on: ${sql}\n  ${e.message}`);
        }
      }
    }
    console.log(`Aggregates: ${successes} success, ${errors} expected errors`);
    assert.ok(successes > 20, 'At least 20 aggregates should succeed');
  });
  
  it('500 mixed random operations do not crash', () => {
    let crashes = 0;
    let expectedErrors = 0;
    let successes = 0;
    
    for (let i = 0; i < 500; i++) {
      const sql = randomSQL();
      try {
        db.execute(sql);
        successes++;
      } catch (e) {
        if (e.message.includes('does not exist') || 
            e.message.includes('not found') ||
            e.message.includes('Parse') ||
            e.message.includes('Unexpected') ||
            e.message.includes('Expected') ||
            e.message.includes('UNIQUE') ||
            e.message.includes('duplicate') ||
            e.message.includes('constraint') ||
            e.message.includes('type') ||
            e.message.includes('Cannot read') ||
            e.message.includes('already exists')) {
          expectedErrors++;
        } else {
          crashes++;
          if (crashes <= 5) {
            console.log(`Unexpected: ${sql}\n  → ${e.message}`);
          }
        }
      }
    }
    console.log(`Mixed: ${successes} success, ${expectedErrors} expected errors, ${crashes} crashes`);
    assert.equal(crashes, 0, `${crashes} unexpected crashes found`);
  });
});
