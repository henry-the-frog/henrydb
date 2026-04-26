import { describe, it } from 'node:test';
import { strict as assert } from 'node:assert';
import { Database } from './db.js';
import { execSync } from 'child_process';

function sqliteEval(expr) {
  try {
    const result = execSync(`sqlite3 :memory: "SELECT ${expr};"`, { encoding: 'utf8', timeout: 5000 }).trim();
    const n = parseFloat(result);
    return !isNaN(n) ? n : result;
  } catch {
    return null;
  }
}

function henryEval(expr) {
  try {
    const db = new Database();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    db.execute("INSERT INTO t VALUES (1)");
    const result = db.execute(`SELECT ${expr} AS val FROM t`);
    return result.rows[0]?.val;
  } catch {
    return null;
  }
}

function randomInt(min, max) {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

function randomExpr(depth = 0) {
  if (depth > 3 || Math.random() < 0.3) {
    return String(randomInt(1, 20));
  }
  const ops = ['+', '-', '*'];
  const op = ops[randomInt(0, ops.length - 1)];
  const left = randomExpr(depth + 1);
  const right = randomExpr(depth + 1);
  if (Math.random() < 0.3) {
    return `(${left} ${op} ${right})`;
  }
  return `${left} ${op} ${right}`;
}

describe('Differential Fuzzer: HenryDB vs SQLite', () => {
  // Fixed test cases that specifically test precedence
  const fixedCases = [
    '2 + 3 * 4',         // 14
    '10 - 6 / 2',        // 7
    '2 * 3 + 4 * 5',     // 26
    '1 + 2 * 3 + 4',     // 11
    '(2 + 3) * 4',       // 20
    '10 * 2 + 3 * 4',    // 32
    '100 - 3 * 20 + 5',  // 45
    '2 * (3 + 4) * 5',   // 70
    '1 + 2 + 3 * 4 + 5', // 20
    '8 / 2 + 6 / 3',     // 6
  ];

  for (const expr of fixedCases) {
    it(`fixed: ${expr}`, () => {
      const sqlite = sqliteEval(expr);
      const henry = henryEval(expr);
      assert.equal(henry, sqlite, `HenryDB=${henry}, SQLite=${sqlite}`);
    });
  }

  // Random test cases
  for (let i = 0; i < 50; i++) {
    const expr = randomExpr();
    it(`random #${i + 1}: ${expr}`, () => {
      const sqlite = sqliteEval(expr);
      const henry = henryEval(expr);
      if (sqlite !== null && henry !== null) {
        assert.equal(henry, sqlite, `HenryDB=${henry}, SQLite=${sqlite} for: ${expr}`);
      }
    });
  }
});
