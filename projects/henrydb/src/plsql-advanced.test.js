// plsql-advanced.test.js — Tests for PL/HenryDB advanced features
// Covers: CASE, DML, cursors, FOR query, stored functions, LOOP, EXIT WHEN

import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { PLParser, PLInterpreter, PLRaise } from './plsql.js';
import { Database } from './db.js';

let db;
function run(src, params = {}) {
  const parser = new PLParser(src);
  const ast = parser.parse();
  const interp = new PLInterpreter(db);
  const result = interp.execute(ast, params);
  return { result, notices: interp.notices };
}

describe('PL/HenryDB Advanced Features', () => {
  beforeEach(() => {
    db = new Database();
    db.execute('CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, price INTEGER)');
    db.execute("INSERT INTO items VALUES (1, 'apple', 10)");
    db.execute("INSERT INTO items VALUES (2, 'banana', 5)");
    db.execute("INSERT INTO items VALUES (3, 'cherry', 15)");
  });

  describe('CASE Statement', () => {
    it('searched CASE with WHEN conditions', () => {
      const { result } = run(`
        DECLARE grade INT := 85; BEGIN
          CASE WHEN grade >= 90 THEN RETURN 'A';
               WHEN grade >= 80 THEN RETURN 'B';
               WHEN grade >= 70 THEN RETURN 'C';
               ELSE RETURN 'F';
          END CASE;
        END
      `);
      assert.equal(result, 'B');
    });

    it('simple CASE with subject', () => {
      const { result } = run(`
        DECLARE x INT := 2; BEGIN
          CASE x
            WHEN 1 THEN RETURN 'one';
            WHEN 2 THEN RETURN 'two';
            WHEN 3 THEN RETURN 'three';
            ELSE RETURN 'other';
          END CASE;
        END
      `);
      assert.equal(result, 'two');
    });

    it('CASE with ELSE', () => {
      const { result } = run("DECLARE x INT := 99; BEGIN CASE x WHEN 1 THEN RETURN 'one'; ELSE RETURN 'unknown'; END CASE; END");
      assert.equal(result, 'unknown');
    });

    it('CASE_NOT_FOUND when no branch matches', () => {
      assert.throws(() => {
        run("DECLARE x INT := 99; BEGIN CASE x WHEN 1 THEN RETURN 1; END CASE; END");
      });
    });

    it('nested CASE statements', () => {
      const { result } = run(`
        DECLARE x INT := 5; y INT := 10; BEGIN
          CASE WHEN x > 3 THEN
            CASE WHEN y > 5 THEN RETURN 'both';
                 ELSE RETURN 'x only';
            END CASE;
          ELSE RETURN 'neither';
          END CASE;
        END
      `);
      assert.equal(result, 'both');
    });
  });

  describe('DML in PL Blocks', () => {
    it('INSERT inside BEGIN block', () => {
      run("BEGIN INSERT INTO items VALUES (4, 'date', 20); END");
      const r = db.execute('SELECT COUNT(*) as cnt FROM items');
      assert.equal(r.rows[0].cnt, 4);
    });

    it('UPDATE inside BEGIN block', () => {
      run("BEGIN UPDATE items SET price = 99 WHERE id = 1; END");
      const r = db.execute('SELECT price FROM items WHERE id = 1');
      assert.equal(r.rows[0].price, 99);
    });

    it('DELETE inside BEGIN block', () => {
      run("BEGIN DELETE FROM items WHERE id = 2; END");
      const r = db.execute('SELECT COUNT(*) as cnt FROM items');
      assert.equal(r.rows[0].cnt, 2);
    });

    it('DML in loop', () => {
      run("DECLARE i INT := 10; BEGIN FOR i IN 10..12 LOOP INSERT INTO items VALUES (i, 'item', i * 10); END LOOP; END");
      const r = db.execute('SELECT COUNT(*) as cnt FROM items');
      assert.equal(r.rows[0].cnt, 6); // 3 original + 3 new
    });
  });

  describe('Cursors', () => {
    it('basic cursor loop', () => {
      const { result } = run(`
        DECLARE cur CURSOR FOR SELECT name FROM items ORDER BY price;
          rec RECORD; names TEXT := '';
        BEGIN
          OPEN cur;
          LOOP FETCH cur INTO rec; EXIT WHEN NOT FOUND;
            names := names || rec.name || ',';
          END LOOP;
          CLOSE cur;
          RETURN names;
        END
      `);
      assert.equal(result, 'banana,apple,cherry,');
    });

    it('cursor with accumulation', () => {
      const { result } = run(`
        DECLARE cur CURSOR FOR SELECT price FROM items;
          rec RECORD; total INT := 0;
        BEGIN
          OPEN cur;
          LOOP FETCH cur INTO rec; EXIT WHEN NOT FOUND;
            total := total + rec.price;
          END LOOP;
          CLOSE cur;
          RETURN total;
        END
      `);
      assert.equal(result, 30);
    });

    it('cursor with conditional', () => {
      const { result } = run(`
        DECLARE cur CURSOR FOR SELECT name, price FROM items;
          rec RECORD; result TEXT := '';
        BEGIN
          OPEN cur;
          LOOP FETCH cur INTO rec; EXIT WHEN NOT FOUND;
            IF rec.price > 8 THEN
              result := result || rec.name || ' ';
            END IF;
          END LOOP;
          CLOSE cur;
          RETURN result;
        END
      `);
      assert.ok(result.includes('apple'));
      assert.ok(result.includes('cherry'));
      assert.ok(!result.includes('banana'));
    });
  });

  describe('FOR Query Loop', () => {
    it('iterates over query results', () => {
      const { result } = run(`
        DECLARE rec RECORD; names TEXT := '';
        BEGIN
          FOR rec IN SELECT name FROM items ORDER BY price LOOP
            names := names || rec.name || ',';
          END LOOP;
          RETURN names;
        END
      `);
      assert.equal(result, 'banana,apple,cherry,');
    });

    it('accumulates values from query', () => {
      const { result } = run(`
        DECLARE rec RECORD; total INT := 0;
        BEGIN
          FOR rec IN SELECT price FROM items LOOP
            total := total + rec.price;
          END LOOP;
          RETURN total;
        END
      `);
      assert.equal(result, 30);
    });

    it('supports EXIT WHEN in query loop', () => {
      const { result } = run(`
        DECLARE rec RECORD; result TEXT := '';
        BEGIN
          FOR rec IN SELECT name FROM items ORDER BY name LOOP
            result := result || rec.name;
            EXIT WHEN rec.name = 'banana';
          END LOOP;
          RETURN result;
        END
      `);
      assert.equal(result, 'applebanana');
    });
  });

  describe('RAISE NOTICE', () => {
    it('captures notice with format args', () => {
      const { result, notices } = run("BEGIN RAISE NOTICE 'Hello %', 'world'; RETURN 42; END");
      assert.equal(result, 42);
      assert.equal(notices.length, 1);
      assert.equal(notices[0].message, 'Hello world');
    });

    it('captures multiple format args', () => {
      const { notices } = run("DECLARE x INT := 10; BEGIN RAISE NOTICE 'x=%, x*2=%', x, x * 2; RETURN 0; END");
      assert.equal(notices[0].message, 'x=10, x*2=20');
    });
  });

  describe('Stored Functions', () => {
    it('CREATE FUNCTION and call from SQL', () => {
      db.execute("CREATE FUNCTION double_price(p INTEGER) RETURNS INTEGER AS $$BEGIN RETURN p * 2; END$$ LANGUAGE plhenrydb");
      const r = db.execute('SELECT double_price(25) as result');
      assert.equal(r.rows[0].result, 50);
    });

    it('stored function with DB access', () => {
      db.execute("CREATE FUNCTION total_items() RETURNS INTEGER AS $$DECLARE cnt INT; BEGIN SELECT COUNT(*) INTO cnt FROM items; RETURN cnt; END$$ LANGUAGE plhenrydb");
      const r = db.execute('SELECT total_items() as result');
      assert.equal(r.rows[0].result, 3);
    });

    it('function in WHERE clause', () => {
      db.execute("CREATE FUNCTION factorial(n INTEGER) RETURNS INTEGER AS $$DECLARE r INT := 1; i INT := 1; BEGIN WHILE i <= n LOOP r := r * i; i := i + 1; END LOOP; RETURN r; END$$ LANGUAGE plhenrydb");
      const r = db.execute('SELECT * FROM items WHERE price > factorial(2)');
      // factorial(2) = 2, so items with price > 2: all three
      assert.equal(r.rows.length, 3);
    });
  });

  describe('LOOP with EXIT', () => {
    it('infinite LOOP with EXIT WHEN', () => {
      const { result } = run(`
        DECLARE i INT := 0;
        BEGIN
          LOOP
            i := i + 1;
            EXIT WHEN i >= 5;
          END LOOP;
          RETURN i;
        END
      `);
      assert.equal(result, 5);
    });

    it('unconditional EXIT', () => {
      const { result } = run(`
        DECLARE i INT := 0;
        BEGIN
          LOOP
            i := i + 1;
            IF i = 3 THEN EXIT; END IF;
          END LOOP;
          RETURN i;
        END
      `);
      assert.equal(result, 3);
    });
  });
});
