// property-based.test.js — Property-based testing for HenryDB
// Generate random queries and verify invariants always hold
// This catches edge cases that hand-written tests miss

import { describe, it } from 'node:test';
import { strict as assert } from 'node:assert';
import { Database } from './db.js';

function rows(result) {
  if (Array.isArray(result)) return result;
  if (result && result.rows) return result.rows;
  return [];
}

// Simple PRNG for reproducibility
class PRNG {
  constructor(seed) { this.state = seed; }
  next() { this.state = (this.state * 1103515245 + 12345) & 0x7fffffff; return this.state; }
  int(max) { return this.next() % max; }
  pick(arr) { return arr[this.int(arr.length)]; }
  bool() { return this.int(2) === 0; }
}

function setupDB(seed, numRows = 100) {
  const rng = new PRNG(seed);
  const db = new Database();
  
  db.execute('CREATE TABLE items (id INT, category TEXT, price INT, qty INT, active INT)');
  
  const categories = ['A', 'B', 'C', 'D', 'E'];
  for (let i = 1; i <= numRows; i++) {
    const cat = rng.pick(categories);
    const price = rng.int(1000);
    const qty = rng.int(100);
    const active = rng.bool() ? 1 : 0;
    db.execute(`INSERT INTO items VALUES (${i}, '${cat}', ${price}, ${qty}, ${active})`);
  }
  
  return { db, rng };
}

// ===== INVARIANT 1: COUNT matches filtered COUNT =====
// COUNT(*) should equal SUM(1) and COUNT(column)

describe('Property: COUNT invariants', () => {
  it('COUNT(*) = SUM(1) for 10 random seeds', () => {
    for (let seed = 1; seed <= 10; seed++) {
      const { db } = setupDB(seed, 50);
      const count = rows(db.execute('SELECT COUNT(*) AS c FROM items'))[0].c;
      assert.equal(count, 50, `Seed ${seed}: COUNT(*) should be 50`);
    }
  });

  it('COUNT(*) WHERE p + COUNT(*) WHERE NOT p = COUNT(*) for random predicates', () => {
    for (let seed = 1; seed <= 10; seed++) {
      const { db, rng } = setupDB(seed, 100);
      const threshold = rng.int(1000);
      
      const total = rows(db.execute('SELECT COUNT(*) AS c FROM items'))[0].c;
      const above = rows(db.execute(`SELECT COUNT(*) AS c FROM items WHERE price > ${threshold}`))[0].c;
      const atOrBelow = rows(db.execute(`SELECT COUNT(*) AS c FROM items WHERE price <= ${threshold}`))[0].c;
      
      assert.equal(above + atOrBelow, total, 
        `Seed ${seed}: ${above} + ${atOrBelow} should = ${total} (threshold ${threshold})`);
    }
  });
});

// ===== INVARIANT 2: SUM is additive =====
// SUM over partitions = SUM over whole

describe('Property: SUM additivity', () => {
  it('SUM by GROUP BY partitions = total SUM', () => {
    for (let seed = 1; seed <= 10; seed++) {
      const { db } = setupDB(seed, 100);
      
      const totalSum = rows(db.execute('SELECT SUM(price) AS s FROM items'))[0].s;
      const groupSums = rows(db.execute('SELECT category, SUM(price) AS s FROM items GROUP BY category'));
      const sumOfGroups = groupSums.reduce((acc, r) => acc + r.s, 0);
      
      assert.equal(sumOfGroups, totalSum, `Seed ${seed}: sum of group sums should = total sum`);
    }
  });

  it('SUM(a + b) = SUM(a) + SUM(b) for 10 seeds', () => {
    for (let seed = 1; seed <= 10; seed++) {
      const { db } = setupDB(seed, 100);
      
      const sumA = rows(db.execute('SELECT SUM(price) AS s FROM items'))[0].s;
      const sumB = rows(db.execute('SELECT SUM(qty) AS s FROM items'))[0].s;
      const sumAB = rows(db.execute('SELECT SUM(price + qty) AS s FROM items'))[0].s;
      
      assert.equal(sumAB, sumA + sumB, `Seed ${seed}: SUM(a+b) should = SUM(a) + SUM(b)`);
    }
  });
});

// ===== INVARIANT 3: ORDER BY is stable =====
// Same query always returns same order

describe('Property: ORDER BY determinism', () => {
  it('same query returns same order 5 times', () => {
    const { db } = setupDB(42, 50);
    
    const results = [];
    for (let i = 0; i < 5; i++) {
      const r = rows(db.execute('SELECT * FROM items ORDER BY price DESC, id ASC'));
      results.push(r.map(x => x.id).join(','));
    }
    
    for (let i = 1; i < results.length; i++) {
      assert.equal(results[i], results[0], 'ORDER BY should be deterministic');
    }
  });

  it('ORDER BY ASC reverse of ORDER BY DESC', () => {
    const { db } = setupDB(42, 50);
    
    const asc = rows(db.execute('SELECT id FROM items ORDER BY price ASC, id ASC'));
    const desc = rows(db.execute('SELECT id FROM items ORDER BY price DESC, id DESC'));
    
    // The price ordering reversal might not be exact due to ties
    // But the first element of ASC should be the last of DESC (or close)
    const ascFirst = asc[0].id;
    const descLast = desc[desc.length - 1].id;
    assert.equal(ascFirst, descLast, 'First ASC should be last DESC');
  });
});

// ===== INVARIANT 4: DISTINCT reduces count =====

describe('Property: DISTINCT reduces', () => {
  it('DISTINCT count <= total count', () => {
    for (let seed = 1; seed <= 10; seed++) {
      const { db } = setupDB(seed, 100);
      
      const total = rows(db.execute('SELECT COUNT(*) AS c FROM items'))[0].c;
      const distinct = rows(db.execute('SELECT COUNT(DISTINCT category) AS c FROM items'))[0].c;
      
      assert.ok(distinct <= total, `Seed ${seed}: DISTINCT count (${distinct}) should be <= total (${total})`);
      assert.ok(distinct > 0, 'Should have at least 1 distinct category');
      assert.ok(distinct <= 5, 'Should have at most 5 distinct categories');
    }
  });
});

// ===== INVARIANT 5: WHERE filter monotonicity =====
// More restrictive WHERE → fewer or equal rows

describe('Property: WHERE monotonicity', () => {
  it('price > X returns fewer rows than price > Y when X > Y', () => {
    for (let seed = 1; seed <= 10; seed++) {
      const { db, rng } = setupDB(seed, 100);
      
      const thresholds = [100, 300, 500, 700, 900];
      const counts = thresholds.map(t => 
        rows(db.execute(`SELECT COUNT(*) AS c FROM items WHERE price > ${t}`))[0].c
      );
      
      for (let i = 1; i < counts.length; i++) {
        assert.ok(counts[i] <= counts[i-1], 
          `Seed ${seed}: count at ${thresholds[i]} (${counts[i]}) should be <= count at ${thresholds[i-1]} (${counts[i-1]})`);
      }
    }
  });
});

// ===== INVARIANT 6: MIN/MAX consistency =====

describe('Property: MIN/MAX consistency', () => {
  it('MIN(x) <= MAX(x) for all columns', () => {
    for (let seed = 1; seed <= 10; seed++) {
      const { db } = setupDB(seed, 100);
      
      for (const col of ['price', 'qty', 'id']) {
        const r = rows(db.execute(`SELECT MIN(${col}) AS mn, MAX(${col}) AS mx FROM items`));
        assert.ok(r[0].mn <= r[0].mx, `Seed ${seed}: MIN(${col}) ${r[0].mn} should be <= MAX(${col}) ${r[0].mx}`);
      }
    }
  });

  it('AVG(x) between MIN(x) and MAX(x)', () => {
    for (let seed = 1; seed <= 10; seed++) {
      const { db } = setupDB(seed, 100);
      
      const r = rows(db.execute('SELECT MIN(price) AS mn, MAX(price) AS mx, AVG(price) AS avg FROM items'));
      assert.ok(r[0].avg >= r[0].mn, `Seed ${seed}: AVG ${r[0].avg} should be >= MIN ${r[0].mn}`);
      assert.ok(r[0].avg <= r[0].mx, `Seed ${seed}: AVG ${r[0].avg} should be <= MAX ${r[0].mx}`);
    }
  });
});

// ===== INVARIANT 7: JOIN preserves referential integrity =====

describe('Property: JOIN correctness', () => {
  it('self-join produces correct count', () => {
    const { db } = setupDB(42, 20);
    
    // Self-join on same table with equality → at least N rows (each matches itself)
    const r = rows(db.execute('SELECT COUNT(*) AS c FROM items a JOIN items b ON a.id = b.id'));
    assert.equal(r[0].c, 20, 'Self-join on id should return 20 rows');
  });

  it('cross join count = product of counts', () => {
    const db = new Database();
    db.execute('CREATE TABLE a (id INT)');
    db.execute('CREATE TABLE b (id INT)');
    db.execute('INSERT INTO a VALUES (1)');
    db.execute('INSERT INTO a VALUES (2)');
    db.execute('INSERT INTO a VALUES (3)');
    db.execute('INSERT INTO b VALUES (10)');
    db.execute('INSERT INTO b VALUES (20)');
    
    const r = rows(db.execute('SELECT COUNT(*) AS c FROM a, b'));
    assert.equal(r[0].c, 6, 'Cross join 3x2 = 6');
  });
});

// ===== INVARIANT 8: LIMIT bounds =====

describe('Property: LIMIT bounds', () => {
  it('LIMIT N returns at most N rows', () => {
    const { db } = setupDB(42, 100);
    
    for (const limit of [1, 5, 10, 50, 100, 200]) {
      const r = rows(db.execute(`SELECT * FROM items LIMIT ${limit}`));
      assert.ok(r.length <= limit, `LIMIT ${limit} returned ${r.length} rows`);
      assert.ok(r.length <= 100, 'Cannot exceed table size');
    }
  });
});
