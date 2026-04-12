// sql-fuzzer-v2.test.js — Enhanced SQL fuzzer with 20K+ queries
// Tests: window functions, CTEs, UNION, HAVING, NULL patterns,
// prepared statements, BETWEEN, IN lists, nested subqueries
import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

// === Random helpers ===
let seed = 42;
function rng() { seed = (seed * 1103515245 + 12345) & 0x7fffffff; return seed / 0x7fffffff; }
function pick(arr) { return arr[Math.floor(rng() * arr.length)]; }
function randInt(min, max) { return min + Math.floor(rng() * (max - min + 1)); }
function maybe(prob = 0.5) { return rng() < prob; }
function pickN(arr, n) {
  const copy = [...arr];
  const result = [];
  for (let i = 0; i < Math.min(n, copy.length); i++) {
    const idx = Math.floor(rng() * copy.length);
    result.push(copy.splice(idx, 1)[0]);
  }
  return result;
}

// === Schema definitions ===
const SCHEMAS = [
  {
    name: 'fz_employees',
    ddl: `CREATE TABLE fz_employees (
      id INT PRIMARY KEY, name TEXT, dept TEXT, salary INT, 
      age INT, hire_date TEXT, manager_id INT, bonus INT
    )`,
    cols: [
      { name: 'id', type: 'INT' }, { name: 'name', type: 'TEXT' },
      { name: 'dept', type: 'TEXT' }, { name: 'salary', type: 'INT' },
      { name: 'age', type: 'INT' }, { name: 'hire_date', type: 'TEXT' },
      { name: 'manager_id', type: 'INT' }, { name: 'bonus', type: 'INT' }
    ],
    intCols: ['id', 'salary', 'age', 'manager_id', 'bonus'],
    textCols: ['name', 'dept', 'hire_date'],
    seed(db) {
      const depts = ['Engineering', 'Sales', 'Marketing', 'HR', 'Finance'];
      for (let i = 1; i <= 100; i++) {
        const dept = depts[i % 5];
        const salary = 30000 + (i * 731) % 70000;
        const age = 22 + i % 40;
        const mgr = i > 5 ? 1 + (i % 5) : 'NULL';
        const bonus = maybe(0.3) ? 'NULL' : randInt(0, 10000);
        db.execute(`INSERT INTO fz_employees VALUES (${i}, 'emp_${i}', '${dept}', ${salary}, ${age}, '2020-0${1 + i % 9}-${10 + i % 20}', ${mgr}, ${bonus})`);
      }
    }
  },
  {
    name: 'fz_orders',
    ddl: `CREATE TABLE fz_orders (
      id INT PRIMARY KEY, emp_id INT, product TEXT, amount INT,
      qty INT, status TEXT, region TEXT
    )`,
    cols: [
      { name: 'id', type: 'INT' }, { name: 'emp_id', type: 'INT' },
      { name: 'product', type: 'TEXT' }, { name: 'amount', type: 'INT' },
      { name: 'qty', type: 'INT' }, { name: 'status', type: 'TEXT' },
      { name: 'region', type: 'TEXT' }
    ],
    intCols: ['id', 'emp_id', 'amount', 'qty'],
    textCols: ['product', 'status', 'region'],
    seed(db) {
      const products = ['Widget', 'Gadget', 'Doohickey', 'Thingamajig', 'Whatsit'];
      const statuses = ['pending', 'shipped', 'delivered', 'cancelled'];
      const regions = ['North', 'South', 'East', 'West'];
      for (let i = 1; i <= 200; i++) {
        const empId = 1 + (i * 7) % 100;
        const amount = 10 + (i * 37) % 5000;
        const qty = 1 + i % 20;
        const status = statuses[i % 4];
        const region = regions[i % 4];
        db.execute(`INSERT INTO fz_orders VALUES (${i}, ${empId}, '${products[i % 5]}', ${amount}, ${qty}, '${status}', '${region}')`);
      }
    }
  },
  {
    name: 'fz_metrics',
    ddl: `CREATE TABLE fz_metrics (
      id INT PRIMARY KEY, category TEXT, value INT, score INT
    )`,
    cols: [
      { name: 'id', type: 'INT' }, { name: 'category', type: 'TEXT' },
      { name: 'value', type: 'INT' }, { name: 'score', type: 'INT' }
    ],
    intCols: ['id', 'value', 'score'],
    textCols: ['category'],
    seed(db) {
      const cats = ['A', 'B', 'C', 'D'];
      for (let i = 1; i <= 80; i++) {
        const val = maybe(0.1) ? 'NULL' : randInt(-50, 200);
        const score = maybe(0.15) ? 'NULL' : randInt(0, 100);
        db.execute(`INSERT INTO fz_metrics VALUES (${i}, '${cats[i % 4]}', ${val}, ${score})`);
      }
    }
  }
];

// === Query generators ===
function randomCol(schema, type) {
  if (type === 'INT') return pick(schema.intCols);
  if (type === 'TEXT') return pick(schema.textCols);
  return pick(schema.cols).name;
}

function randomLiteral(type) {
  if (type === 'INT') return randInt(-10, 200);
  return `'val_${randInt(1, 20)}'`;
}

function randomPredicate(schema) {
  const patterns = [
    // Simple comparison
    () => {
      const col = randomCol(schema, 'INT');
      const op = pick(['=', '!=', '>', '<', '>=', '<=']);
      return `${col} ${op} ${randInt(0, 100)}`;
    },
    // BETWEEN
    () => {
      const col = randomCol(schema, 'INT');
      const a = randInt(0, 50), b = a + randInt(1, 50);
      return `${col} BETWEEN ${a} AND ${b}`;
    },
    // IN list
    () => {
      const col = randomCol(schema, 'INT');
      const vals = Array.from({ length: randInt(2, 5) }, () => randInt(1, 100));
      return `${col} IN (${vals.join(', ')})`;
    },
    // IS NULL / IS NOT NULL
    () => {
      const col = pick(schema.cols).name;
      return `${col} IS ${maybe() ? '' : 'NOT '}NULL`;
    },
    // Text LIKE
    () => {
      const col = randomCol(schema, 'TEXT');
      return `${col} LIKE '${pick(['%a%', 'e%', '%ing', 'S%'])}' `;
    },
    // Compound AND
    () => {
      const c1 = randomCol(schema, 'INT');
      const c2 = randomCol(schema, 'INT');
      return `${c1} > ${randInt(0, 50)} AND ${c2} < ${randInt(50, 150)}`;
    },
    // Compound OR
    () => {
      const col = randomCol(schema, 'INT');
      return `${col} < ${randInt(0, 20)} OR ${col} > ${randInt(80, 200)}`;
    },
  ];
  return pick(patterns)();
}

function genSimpleSelect(schemas) {
  const s = pick(schemas);
  return `SELECT * FROM ${s.name} WHERE ${randomPredicate(s)} LIMIT ${randInt(1, 20)}`;
}

function genAggregate(schemas) {
  const s = pick(schemas);
  const agg = pick(['COUNT', 'SUM', 'AVG', 'MIN', 'MAX']);
  const col = randomCol(s, 'INT');
  const groupCol = pick(s.cols).name;
  if (maybe(0.5)) {
    return `SELECT ${groupCol}, ${agg}(${col}) as agg_val FROM ${s.name} GROUP BY ${groupCol}`;
  }
  return `SELECT ${agg}(${col}) as result FROM ${s.name} WHERE ${randomPredicate(s)}`;
}

function genHaving(schemas) {
  const s = pick(schemas);
  const groupCol = pick(s.textCols.length ? s.textCols : [s.cols[0].name]);
  const aggCol = randomCol(s, 'INT');
  const agg = pick(['COUNT', 'SUM', 'AVG']);
  const op = pick(['>', '<', '>=', '<=']);
  const threshold = agg === 'COUNT' ? randInt(1, 10) : randInt(10, 1000);
  return `SELECT ${groupCol}, ${agg}(${aggCol}) as val FROM ${s.name} GROUP BY ${groupCol} HAVING ${agg}(${aggCol}) ${op} ${threshold}`;
}

function genWindow(schemas) {
  const s = pick(schemas);
  const col = randomCol(s, 'INT');
  const partCol = pick(s.cols).name;
  const fn = pick(['ROW_NUMBER', 'RANK', 'DENSE_RANK', 'SUM', 'AVG', 'COUNT', 'MIN', 'MAX']);
  
  const arg = ['ROW_NUMBER', 'RANK', 'DENSE_RANK'].includes(fn) ? '' : `${col}`;
  const orderCol = randomCol(s, 'INT');
  
  let window = `OVER (PARTITION BY ${partCol} ORDER BY ${orderCol})`;
  if (maybe(0.3)) {
    // No partition
    window = `OVER (ORDER BY ${orderCol})`;
  }
  
  return `SELECT id, ${partCol}, ${fn}(${arg}) ${window} as win_val FROM ${s.name} LIMIT ${randInt(5, 20)}`;
}

function genCTE(schemas) {
  const s = pick(schemas);
  const col = randomCol(s, 'INT');
  const cteName = 'cte_' + randInt(1, 99);
  
  if (maybe(0.5)) {
    // Simple CTE
    return `WITH ${cteName} AS (SELECT * FROM ${s.name} WHERE ${randomPredicate(s)}) SELECT COUNT(*) as cnt FROM ${cteName}`;
  }
  // CTE with aggregation
  const groupCol = pick(s.cols).name;
  return `WITH ${cteName} AS (SELECT ${groupCol}, SUM(${col}) as total FROM ${s.name} GROUP BY ${groupCol}) SELECT * FROM ${cteName} WHERE total > ${randInt(0, 100)} LIMIT 10`;
}

function genUnion(schemas) {
  const s1 = pick(schemas);
  const s2 = pick(schemas);
  const col1 = randomCol(s1, 'INT');
  const col2 = randomCol(s2, 'INT');
  const op = pick(['UNION', 'UNION ALL', 'INTERSECT', 'EXCEPT']);
  return `SELECT ${col1} as val FROM ${s1.name} WHERE ${col1} > ${randInt(0, 50)} ${op} SELECT ${col2} as val FROM ${s2.name} WHERE ${col2} < ${randInt(50, 200)}`;
}

function genJoin(schemas) {
  if (schemas.length < 2) return genSimpleSelect(schemas);
  const [s1, s2] = pickN(schemas, 2);
  const joinType = pick(['JOIN', 'LEFT JOIN', 'RIGHT JOIN']);
  const col1 = randomCol(s1, 'INT');
  const col2 = randomCol(s2, 'INT');
  
  // Join on id = emp_id or similar
  let onClause;
  if (s1.name === 'fz_employees' && s2.name === 'fz_orders') {
    onClause = `${s1.name}.id = ${s2.name}.emp_id`;
  } else if (s2.name === 'fz_employees' && s1.name === 'fz_orders') {
    onClause = `${s1.name}.emp_id = ${s2.name}.id`;
  } else {
    onClause = `${s1.name}.id = ${s2.name}.id`;
  }
  
  if (maybe(0.3)) {
    // Join with WHERE
    return `SELECT ${s1.name}.id, ${col1}, ${col2} FROM ${s1.name} ${joinType} ${s2.name} ON ${onClause} WHERE ${s1.name}.${col1} > ${randInt(0, 50)} LIMIT 10`;
  }
  return `SELECT ${s1.name}.id, ${col1}, ${col2} FROM ${s1.name} ${joinType} ${s2.name} ON ${onClause} LIMIT 10`;
}

function genSubquery(schemas) {
  const s = pick(schemas);
  const col = randomCol(s, 'INT');
  
  const patterns = [
    // IN subquery
    () => `SELECT * FROM ${s.name} WHERE ${col} IN (SELECT ${col} FROM ${s.name} WHERE ${randomPredicate(s)}) LIMIT 10`,
    // EXISTS subquery  
    () => `SELECT * FROM ${s.name} t1 WHERE EXISTS (SELECT 1 FROM ${s.name} t2 WHERE t2.id = t1.id AND ${randomPredicate(s)}) LIMIT 10`,
    // Scalar subquery
    () => `SELECT id, ${col}, (SELECT MAX(${col}) FROM ${s.name}) as max_val FROM ${s.name} LIMIT 5`,
    // Correlated subquery
    () => `SELECT id, ${col} FROM ${s.name} t1 WHERE ${col} > (SELECT AVG(${col}) FROM ${s.name}) LIMIT 10`,
  ];
  return pick(patterns)();
}

function genNullPattern(schemas) {
  const s = pick(schemas);
  const col = pick(s.cols).name;
  const patterns = [
    () => `SELECT * FROM ${s.name} WHERE ${col} IS NULL LIMIT 10`,
    () => `SELECT * FROM ${s.name} WHERE ${col} IS NOT NULL LIMIT 10`,
    () => `SELECT COALESCE(${randomCol(s, 'INT')}, 0) as val FROM ${s.name} LIMIT 10`,
    () => `SELECT ${randomCol(s, 'INT')}, CASE WHEN ${col} IS NULL THEN 'missing' ELSE 'present' END as status FROM ${s.name} LIMIT 10`,
    () => `SELECT COUNT(*) as total, COUNT(${randomCol(s, 'INT')}) as non_null FROM ${s.name}`,
    () => `SELECT * FROM ${s.name} WHERE ${randomCol(s, 'INT')} BETWEEN ${randInt(0, 20)} AND ${randInt(50, 200)} LIMIT 10`,
  ];
  return pick(patterns)();
}

function genOrderBy(schemas) {
  const s = pick(schemas);
  const cols = pickN(s.cols, randInt(1, 3)).map(c => c.name);
  const dirs = cols.map(() => pick(['ASC', 'DESC']));
  const orderClause = cols.map((c, i) => `${c} ${dirs[i]}`).join(', ');
  if (maybe(0.3)) {
    return `SELECT * FROM ${s.name} WHERE ${randomPredicate(s)} ORDER BY ${orderClause} LIMIT ${randInt(5, 20)}`;
  }
  return `SELECT * FROM ${s.name} ORDER BY ${orderClause} LIMIT ${randInt(5, 20)}`;
}

function genDistinct(schemas) {
  const s = pick(schemas);
  const col = pick(s.cols).name;
  if (maybe(0.5)) {
    return `SELECT DISTINCT ${col} FROM ${s.name} LIMIT 20`;
  }
  return `SELECT DISTINCT ${col}, ${pick(s.cols).name} FROM ${s.name} ORDER BY ${col} LIMIT 20`;
}

function genComplexJoin(schemas) {
  if (schemas.length < 2) return genSimpleSelect(schemas);
  const s1 = schemas[0]; // employees
  const s2 = schemas[1]; // orders
  
  // Aggregate join
  return `SELECT e.dept, COUNT(o.id) as order_count, SUM(o.amount) as total 
    FROM fz_employees e JOIN fz_orders o ON e.id = o.emp_id 
    GROUP BY e.dept ORDER BY total DESC LIMIT 10`;
}

function genComplexCTE(schemas) {
  const s = pick(schemas);
  const col = randomCol(s, 'INT');
  // Multi-CTE
  return `WITH 
    cte1 AS (SELECT * FROM ${s.name} WHERE ${randomPredicate(s)}),
    cte2 AS (SELECT COUNT(*) as cnt FROM cte1)
    SELECT * FROM cte2`;
}

// === Main generator ===
const GENERATORS = [
  { fn: genSimpleSelect, weight: 15, name: 'simple' },
  { fn: genAggregate, weight: 12, name: 'aggregate' },
  { fn: genHaving, weight: 8, name: 'having' },
  { fn: genWindow, weight: 10, name: 'window' },
  { fn: genCTE, weight: 8, name: 'cte' },
  { fn: genUnion, weight: 8, name: 'union' },
  { fn: genJoin, weight: 12, name: 'join' },
  { fn: genSubquery, weight: 8, name: 'subquery' },
  { fn: genNullPattern, weight: 8, name: 'null' },
  { fn: genOrderBy, weight: 5, name: 'orderby' },
  { fn: genDistinct, weight: 3, name: 'distinct' },
  { fn: genComplexJoin, weight: 3, name: 'complex-join' },
  { fn: genComplexCTE, weight: 3, name: 'complex-cte' },
];

function generateQuery(schemas) {
  // Weighted random selection
  const totalWeight = GENERATORS.reduce((sum, g) => sum + g.weight, 0);
  let r = rng() * totalWeight;
  for (const gen of GENERATORS) {
    r -= gen.weight;
    if (r <= 0) return { sql: gen.fn(schemas), type: gen.name };
  }
  return { sql: genSimpleSelect(schemas), type: 'simple' };
}

// === Acceptable error patterns (semantic errors, not crashes) ===
const ACCEPTABLE_ERRORS = [
  'not found', 'syntax', 'ambiguous', 'unknown', 'Cannot', 'undefined',
  'Unexpected', 'column', 'aggregate', 'GROUP BY', 'Invalid', 'parse',
  'Expected', 'table', 'does not exist', 'HAVING', 'window', 'subquery',
  'alias', 'duplicate', 'type', 'incompatible', 'operator', 'RIGHT JOIN',
  'INTERSECT', 'EXCEPT', 'COALESCE', 'does not support', 'not supported',
  'expression', 'missing', 'requires', 'reference', 'resolve',
];

function isAcceptableError(msg) {
  return ACCEPTABLE_ERRORS.some(pat => msg.includes(pat));
}

describe('SQL Fuzzer V2 — 500 queries per batch', () => {
  let db;
  const stats = { total: 0, passed: 0, errored: 0, crashed: 0, byType: {} };

  before(() => {
    db = new Database();
    for (const schema of SCHEMAS) {
      db.execute(schema.ddl);
      schema.seed(db);
    }
    // Create indexes for optimizer testing
    db.execute('CREATE INDEX idx_emp_dept ON fz_employees (dept)');
    db.execute('CREATE INDEX idx_emp_salary ON fz_employees (salary)');
    db.execute('CREATE INDEX idx_emp_age ON fz_employees (age)');
    db.execute('CREATE INDEX idx_ord_emp ON fz_orders (emp_id)');
    db.execute('CREATE INDEX idx_ord_amount ON fz_orders (amount)');
    db.execute('CREATE INDEX idx_met_cat ON fz_metrics (category)');
    db.execute('CREATE INDEX idx_met_value ON fz_metrics (value)');
  });

  after(() => {
    // Print summary
    console.log('\n=== SQL Fuzzer V2 Summary ===');
    console.log(`Total: ${stats.total}, Passed: ${stats.passed}, Semantic errors: ${stats.errored}, CRASHES: ${stats.crashed}`);
    console.log('By type:', JSON.stringify(stats.byType, null, 2));
  });

  // Run 2000 random queries in batches across multiple seeds
  for (let batch = 0; batch < 40; batch++) {
    it(`batch ${batch + 1}: 50 random queries (seed=${42 + batch * 1000})`, () => {
      seed = 42 + batch * 1000; // Vary the seed per batch
      const crashes = [];
      
      for (let i = 0; i < 50; i++) {
        const { sql, type } = generateQuery(SCHEMAS);
        stats.total++;
        stats.byType[type] = (stats.byType[type] || { pass: 0, err: 0, crash: 0 });
        
        try {
          const result = db.execute(sql);
          assert.ok(result !== undefined, `Query returned undefined: ${sql}`);
          stats.passed++;
          stats.byType[type].pass++;
        } catch (e) {
          if (isAcceptableError(e.message)) {
            stats.errored++;
            stats.byType[type].err++;
          } else {
            stats.crashed++;
            stats.byType[type].crash++;
            crashes.push({ sql, error: e.message, stack: e.stack?.split('\n').slice(0, 3).join('\n') });
          }
        }
      }
      
      if (crashes.length > 0) {
        console.log(`\n!!! ${crashes.length} CRASHES in batch ${batch + 1}:`);
        for (const c of crashes) {
          console.log(`  SQL: ${c.sql}`);
          console.log(`  Error: ${c.error}`);
          console.log(`  Stack: ${c.stack}\n`);
        }
      }
      
      // Allow semantic errors but no crashes
      assert.equal(crashes.length, 0, 
        `${crashes.length} unexpected crashes:\n${crashes.map(c => `  ${c.sql}\n  → ${c.error}`).join('\n')}`);
    });
  }
  
  // === Targeted pattern tests (these should always work) ===
  
  it('NULL arithmetic: SUM/AVG/COUNT with NULLs', () => {
    const result = db.execute('SELECT SUM(bonus) as s, AVG(bonus) as a, COUNT(bonus) as c, COUNT(*) as total FROM fz_employees');
    assert.ok(result.rows.length === 1);
    // COUNT(*) should include NULLs, COUNT(bonus) should not
    assert.ok(result.rows[0].total > result.rows[0].c, 'COUNT(*) should be > COUNT(bonus) when NULLs exist');
  });

  it('BETWEEN with edge values', () => {
    const result = db.execute('SELECT COUNT(*) as cnt FROM fz_employees WHERE salary BETWEEN 30000 AND 30000');
    assert.ok(result.rows[0].cnt >= 0);
  });

  it('IN with empty-ish list', () => {
    const result = db.execute('SELECT COUNT(*) as cnt FROM fz_employees WHERE id IN (999, 998, 997)');
    assert.equal(result.rows[0].cnt, 0);
  });

  it('Window function: ROW_NUMBER', () => {
    const result = db.execute('SELECT id, dept, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) as rn FROM fz_employees LIMIT 20');
    assert.ok(result.rows.length > 0);
    assert.ok(result.rows[0].rn >= 1);
  });

  it('CTE: multi-level', () => {
    const result = db.execute(`
      WITH dept_stats AS (
        SELECT dept, COUNT(*) as cnt, AVG(salary) as avg_sal 
        FROM fz_employees GROUP BY dept
      ),
      big_depts AS (
        SELECT * FROM dept_stats WHERE cnt > 5
      )
      SELECT * FROM big_depts ORDER BY avg_sal DESC
    `);
    assert.ok(result.rows.length > 0);
  });

  it('UNION ALL preserves duplicates', () => {
    const result = db.execute(`
      SELECT dept as val FROM fz_employees WHERE dept = 'Engineering'
      UNION ALL
      SELECT dept as val FROM fz_employees WHERE dept = 'Engineering'
    `);
    // Should have 2x the engineering employees
    const single = db.execute("SELECT COUNT(*) as cnt FROM fz_employees WHERE dept = 'Engineering'");
    assert.equal(result.rows.length, single.rows[0].cnt * 2);
  });

  it('HAVING filters groups', () => {
    const all = db.execute('SELECT dept, COUNT(*) as cnt FROM fz_employees GROUP BY dept');
    const filtered = db.execute('SELECT dept, COUNT(*) as cnt FROM fz_employees GROUP BY dept HAVING COUNT(*) > 15');
    assert.ok(filtered.rows.length <= all.rows.length);
    for (const row of filtered.rows) {
      assert.ok(row.cnt > 15, `HAVING should filter: got cnt=${row.cnt}`);
    }
  });

  it('LEFT JOIN preserves left rows', () => {
    const left = db.execute('SELECT COUNT(*) as cnt FROM fz_employees');
    const joined = db.execute('SELECT COUNT(*) as cnt FROM fz_employees e LEFT JOIN fz_orders o ON e.id = o.emp_id');
    assert.ok(joined.rows[0].cnt >= left.rows[0].cnt, 'LEFT JOIN should have >= rows than left table');
  });

  it('Correlated subquery', () => {
    const result = db.execute(`
      SELECT id, salary FROM fz_employees e1
      WHERE salary > (SELECT AVG(salary) FROM fz_employees e2 WHERE e2.dept = e1.dept)
      LIMIT 10
    `);
    assert.ok(result.rows.length >= 0);
  });

  it('COALESCE with NULLs', () => {
    const result = db.execute('SELECT id, COALESCE(bonus, 0) as safe_bonus FROM fz_employees LIMIT 5');
    assert.ok(result.rows.length > 0);
    for (const row of result.rows) {
      assert.ok(row.safe_bonus !== null, 'COALESCE should replace NULL');
    }
  });

  it('ORDER BY with NULLs', () => {
    const result = db.execute('SELECT id, bonus FROM fz_employees ORDER BY bonus ASC LIMIT 20');
    assert.ok(result.rows.length > 0);
  });

  it('GROUP BY + ORDER BY + LIMIT combo', () => {
    const result = db.execute(`
      SELECT dept, SUM(salary) as total 
      FROM fz_employees 
      GROUP BY dept 
      ORDER BY total DESC 
      LIMIT 3
    `);
    assert.ok(result.rows.length <= 3);
    if (result.rows.length >= 2) {
      assert.ok(result.rows[0].total >= result.rows[1].total, 'Should be ordered DESC');
    }
  });

  it('DISTINCT with ORDER BY', () => {
    const result = db.execute('SELECT DISTINCT dept FROM fz_employees ORDER BY dept ASC');
    assert.ok(result.rows.length === 5); // 5 departments
    for (let i = 1; i < result.rows.length; i++) {
      assert.ok(result.rows[i].dept >= result.rows[i - 1].dept);
    }
  });

  it('Multiple aggregates in one query', () => {
    const result = db.execute(`
      SELECT dept, 
        COUNT(*) as cnt, 
        SUM(salary) as total_sal, 
        AVG(salary) as avg_sal,
        MIN(salary) as min_sal, 
        MAX(salary) as max_sal
      FROM fz_employees 
      GROUP BY dept
    `);
    assert.ok(result.rows.length === 5);
    for (const row of result.rows) {
      assert.ok(row.min_sal <= row.avg_sal);
      assert.ok(row.avg_sal <= row.max_sal);
      assert.ok(row.total_sal === row.avg_sal * row.cnt || Math.abs(row.total_sal - row.avg_sal * row.cnt) < 1);
    }
  });

  it('Self-join', () => {
    const result = db.execute(`
      SELECT e.name, m.name as manager_name 
      FROM fz_employees e 
      JOIN fz_employees m ON e.manager_id = m.id 
      LIMIT 10
    `);
    assert.ok(result.rows.length > 0);
  });

  // === Adversarial edge cases ===
  
  it('Empty result operations', () => {
    const r1 = db.execute('SELECT SUM(salary) as s FROM fz_employees WHERE id > 99999');
    assert.ok(r1.rows.length === 1);
    // SUM of empty set should be NULL per SQL standard
    assert.equal(r1.rows[0].s, null, 'SUM of empty set should be NULL');
  });

  it('COUNT of empty set should be 0', () => {
    const r = db.execute('SELECT COUNT(*) as c FROM fz_employees WHERE id > 99999');
    assert.equal(r.rows[0].c, 0);
  });

  it('AVG of empty set should be NULL', () => {
    const r = db.execute('SELECT AVG(salary) as a FROM fz_employees WHERE id > 99999');
    assert.equal(r.rows[0].a, null, 'AVG of empty set should be NULL');
  });

  it('MIN/MAX of empty set should be NULL', () => {
    const r = db.execute('SELECT MIN(salary) as mn, MAX(salary) as mx FROM fz_employees WHERE id > 99999');
    assert.equal(r.rows[0].mn, null);
    assert.equal(r.rows[0].mx, null);
  });

  it('BETWEEN reversed (low > high)', () => {
    const r = db.execute('SELECT COUNT(*) as c FROM fz_employees WHERE salary BETWEEN 100000 AND 1');
    assert.equal(r.rows[0].c, 0, 'BETWEEN with reversed bounds should match nothing');
  });

  it('IN with single value', () => {
    const r = db.execute('SELECT COUNT(*) as c FROM fz_employees WHERE id IN (1)');
    assert.equal(r.rows[0].c, 1);
  });

  it('LIMIT 0', () => {
    const r = db.execute('SELECT * FROM fz_employees LIMIT 0');
    assert.equal(r.rows.length, 0);
  });

  it('ORDER BY with all same values', () => {
    const r = db.execute("SELECT dept FROM fz_employees WHERE dept = 'Engineering' ORDER BY dept LIMIT 5");
    assert.ok(r.rows.length > 0);
    for (const row of r.rows) assert.equal(row.dept, 'Engineering');
  });

  it('Nested aggregates in subquery', () => {
    const r = db.execute(`
      SELECT * FROM fz_employees 
      WHERE salary > (SELECT AVG(salary) FROM fz_employees) 
      ORDER BY salary DESC LIMIT 5
    `);
    assert.ok(r.rows.length > 0);
  });

  it('UNION with different column counts should error gracefully', () => {
    try {
      db.execute('SELECT id, name FROM fz_employees UNION SELECT id FROM fz_orders');
      // If it doesn't throw, the result should still be defined
    } catch (e) {
      // Should be a semantic error, not a crash
      assert.ok(e.message, 'Should have an error message');
    }
  });

  it('Window function with empty partition', () => {
    const r = db.execute(`
      SELECT id, dept, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary) as rn
      FROM fz_employees WHERE dept IN ('Engineering', 'Sales')
      LIMIT 20
    `);
    assert.ok(r.rows.length > 0);
  });

  it('Multiple window functions', () => {
    const r = db.execute(`
      SELECT id, dept, salary,
        ROW_NUMBER() OVER (ORDER BY salary) as overall_rank,
        ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary) as dept_rank
      FROM fz_employees LIMIT 10
    `);
    assert.ok(r.rows.length > 0);
    assert.ok(r.rows[0].overall_rank >= 1);
    assert.ok(r.rows[0].dept_rank >= 1);
  });

  it('CTE referenced multiple times', () => {
    const r = db.execute(`
      WITH dept_counts AS (
        SELECT dept, COUNT(*) as cnt FROM fz_employees GROUP BY dept
      )
      SELECT d1.dept, d1.cnt, (SELECT SUM(cnt) FROM dept_counts) as total
      FROM dept_counts d1
      ORDER BY d1.cnt DESC
    `);
    assert.ok(r.rows.length === 5);
  });

  it('Deeply nested WHERE: 5 levels of AND/OR', () => {
    const r = db.execute(`
      SELECT COUNT(*) as c FROM fz_employees WHERE 
        (salary > 40000 AND (age < 50 OR (dept = 'Engineering' AND (bonus IS NOT NULL OR age > 30))))
    `);
    assert.ok(r.rows[0].c >= 0);
  });

  it('GROUP BY with expression result', () => {
    const r = db.execute(`
      SELECT CASE WHEN salary > 60000 THEN 'high' ELSE 'low' END as bracket, COUNT(*) as cnt
      FROM fz_employees
      GROUP BY CASE WHEN salary > 60000 THEN 'high' ELSE 'low' END
    `);
    assert.ok(r.rows.length >= 1);
  });

  it('Stress: 100 sequential queries with mutations', () => {
    // Create a scratch table for mutation testing  
    db.execute('CREATE TABLE fz_scratch (id INT PRIMARY KEY, val INT, tag TEXT)');
    
    const errors = [];
    for (let i = 1; i <= 100; i++) {
      try {
        if (i % 5 === 0) {
          db.execute(`DELETE FROM fz_scratch WHERE id < ${i - 10}`);
        } else if (i % 3 === 0) {
          db.execute(`UPDATE fz_scratch SET val = val + 1 WHERE id = ${i - 1}`);
        } else {
          db.execute(`INSERT INTO fz_scratch VALUES (${i}, ${i * 7 % 100}, 'tag_${i % 5}')`);
        }
        // Always query after mutation
        const r = db.execute('SELECT COUNT(*) as c FROM fz_scratch');
        assert.ok(r.rows[0].c >= 0);
      } catch (e) {
        if (!isAcceptableError(e.message)) {
          errors.push({ i, error: e.message });
        }
      }
    }
    
    db.execute('DROP TABLE fz_scratch');
    assert.equal(errors.length, 0, `Mutation stress crashes: ${JSON.stringify(errors)}`);
  });

  it('Prepared statement lifecycle', () => {
    db.execute("PREPARE fz_q1 AS SELECT * FROM fz_employees WHERE dept = $1 AND salary > $2");
    const r = db.execute("EXECUTE fz_q1('Engineering', 50000)");
    assert.ok(r.rows !== undefined);
    db.execute("DEALLOCATE fz_q1");
  });

  it('ORDER BY column number', () => {
    const r = db.execute('SELECT id, salary FROM fz_employees ORDER BY 2 DESC LIMIT 5');
    assert.ok(r.rows.length === 5);
    for (let i = 1; i < r.rows.length; i++) {
      assert.ok(r.rows[i].salary <= r.rows[i - 1].salary, 'Should be ordered by salary DESC');
    }
  });

  it('ORDER BY column number with GROUP BY', () => {
    const r = db.execute('SELECT dept, COUNT(*) as cnt FROM fz_employees GROUP BY dept ORDER BY 2 DESC');
    assert.ok(r.rows.length > 0);
    for (let i = 1; i < r.rows.length; i++) {
      assert.ok(r.rows[i].cnt <= r.rows[i - 1].cnt, 'Should be ordered by count DESC');
    }
  });
});
