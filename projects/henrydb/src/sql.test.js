import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';
import { tokenize, parse } from './sql.js';

// ===== Tokenizer Tests =====
describe('SQL Tokenizer', () => {
  it('tokenizes SELECT', () => {
    const tokens = tokenize('SELECT * FROM users');
    assert.equal(tokens[0].value, 'SELECT');
    assert.equal(tokens[1].type, '*');
    assert.equal(tokens[2].value, 'FROM');
    assert.equal(tokens[3].value, 'users');
  });

  it('tokenizes string literals', () => {
    const tokens = tokenize("INSERT INTO t VALUES ('hello')");
    assert.ok(tokens.some(t => t.type === 'STRING' && t.value === 'hello'));
  });

  it('tokenizes comparison operators', () => {
    const tokens = tokenize('WHERE x >= 5 AND y != 3');
    assert.ok(tokens.some(t => t.type === 'GE'));
    assert.ok(tokens.some(t => t.type === 'NE'));
  });

  it('tokenizes numbers', () => {
    const tokens = tokenize('WHERE price = 19.99');
    assert.ok(tokens.some(t => t.type === 'NUMBER' && t.value === 19.99));
  });
});

// ===== Parser Tests =====
describe('SQL Parser', () => {
  it('parses SELECT *', () => {
    const ast = parse('SELECT * FROM users');
    assert.equal(ast.type, 'SELECT');
    assert.equal(ast.columns[0].type, 'star');
    assert.equal(ast.from.table, 'users');
  });

  it('parses SELECT with columns', () => {
    const ast = parse('SELECT name, age FROM users');
    assert.equal(ast.columns.length, 2);
    assert.equal(ast.columns[0].name, 'name');
    assert.equal(ast.columns[1].name, 'age');
  });

  it('parses WHERE clause', () => {
    const ast = parse('SELECT * FROM users WHERE age > 21');
    assert.ok(ast.where);
    assert.equal(ast.where.type, 'COMPARE');
    assert.equal(ast.where.op, 'GT');
  });

  it('parses AND/OR', () => {
    const ast = parse('SELECT * FROM users WHERE age > 21 AND name = \'Alice\'');
    assert.equal(ast.where.type, 'AND');
  });

  it('parses ORDER BY', () => {
    const ast = parse('SELECT * FROM users ORDER BY age DESC');
    assert.equal(ast.orderBy[0].column, 'age');
    assert.equal(ast.orderBy[0].direction, 'DESC');
  });

  it('parses LIMIT', () => {
    const ast = parse('SELECT * FROM users LIMIT 10');
    assert.equal(ast.limit, 10);
  });

  it('parses INSERT', () => {
    const ast = parse("INSERT INTO users VALUES (1, 'Alice', 30)");
    assert.equal(ast.type, 'INSERT');
    assert.equal(ast.table, 'users');
    assert.equal(ast.rows[0].length, 3);
  });

  it('parses INSERT with columns', () => {
    const ast = parse("INSERT INTO users (name, age) VALUES ('Bob', 25)");
    assert.deepStrictEqual(ast.columns, ['name', 'age']);
  });

  it('parses UPDATE', () => {
    const ast = parse("UPDATE users SET name = 'Charlie' WHERE id = 1");
    assert.equal(ast.type, 'UPDATE');
    assert.equal(ast.assignments[0].column, 'name');
  });

  it('parses DELETE', () => {
    const ast = parse('DELETE FROM users WHERE age < 18');
    assert.equal(ast.type, 'DELETE');
    assert.ok(ast.where);
  });

  it('parses CREATE TABLE', () => {
    const ast = parse('CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT)');
    assert.equal(ast.type, 'CREATE_TABLE');
    assert.equal(ast.columns.length, 3);
    assert.ok(ast.columns[0].primaryKey);
  });

  it('parses DROP TABLE', () => {
    const ast = parse('DROP TABLE users');
    assert.equal(ast.type, 'DROP_TABLE');
    assert.equal(ast.table, 'users');
  });

  it('parses aggregates', () => {
    const ast = parse('SELECT COUNT(*), AVG(age) FROM users');
    assert.equal(ast.columns[0].type, 'aggregate');
    assert.equal(ast.columns[0].func, 'COUNT');
    assert.equal(ast.columns[1].func, 'AVG');
  });

  it('parses JOIN', () => {
    const ast = parse('SELECT * FROM orders JOIN users ON orders.user_id = users.id');
    assert.equal(ast.joins.length, 1);
    assert.equal(ast.joins[0].table, 'users');
  });
});

// ===== Database Executor Tests =====
describe('Database', () => {
  let db;

  beforeEach(() => {
    db = new Database();
    db.execute('CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT)');
    db.execute("INSERT INTO users VALUES (1, 'Alice', 30)");
    db.execute("INSERT INTO users VALUES (2, 'Bob', 25)");
    db.execute("INSERT INTO users VALUES (3, 'Charlie', 35)");
  });

  describe('CREATE TABLE', () => {
    it('creates a table', () => {
      const result = db.execute('CREATE TABLE products (id INT, name TEXT)');
      assert.equal(result.type, 'OK');
    });

    it('rejects duplicate table', () => {
      assert.throws(() => db.execute('CREATE TABLE users (id INT)'));
    });
  });

  describe('INSERT', () => {
    it('inserts a row', () => {
      const result = db.execute("INSERT INTO users VALUES (4, 'Diana', 28)");
      assert.equal(result.count, 1);
    });

    it('inserts multiple rows', () => {
      const result = db.execute("INSERT INTO users VALUES (4, 'Diana', 28), (5, 'Eve', 22)");
      assert.equal(result.count, 2);
    });
  });

  describe('SELECT', () => {
    it('SELECT *', () => {
      const result = db.execute('SELECT * FROM users');
      assert.equal(result.rows.length, 3);
    });

    it('SELECT specific columns', () => {
      const result = db.execute('SELECT name, age FROM users');
      assert.equal(result.rows.length, 3);
      assert.equal(result.rows[0].name, 'Alice');
      assert.equal(result.rows[0].age, 30);
      assert.equal(result.rows[0].id, undefined);
    });

    it('WHERE equality', () => {
      const result = db.execute("SELECT * FROM users WHERE name = 'Bob'");
      assert.equal(result.rows.length, 1);
      assert.equal(result.rows[0].name, 'Bob');
    });

    it('WHERE comparison', () => {
      const result = db.execute('SELECT * FROM users WHERE age > 28');
      assert.equal(result.rows.length, 2);
    });

    it('WHERE AND', () => {
      const result = db.execute('SELECT * FROM users WHERE age > 20 AND age < 31');
      assert.equal(result.rows.length, 2);
    });

    it('WHERE OR', () => {
      const result = db.execute("SELECT * FROM users WHERE name = 'Alice' OR name = 'Charlie'");
      assert.equal(result.rows.length, 2);
    });

    it('ORDER BY ASC', () => {
      const result = db.execute('SELECT * FROM users ORDER BY age ASC');
      assert.equal(result.rows[0].name, 'Bob');
      assert.equal(result.rows[2].name, 'Charlie');
    });

    it('ORDER BY DESC', () => {
      const result = db.execute('SELECT * FROM users ORDER BY age DESC');
      assert.equal(result.rows[0].name, 'Charlie');
    });

    it('LIMIT', () => {
      const result = db.execute('SELECT * FROM users ORDER BY id LIMIT 2');
      assert.equal(result.rows.length, 2);
    });

    it('OFFSET', () => {
      const result = db.execute('SELECT * FROM users ORDER BY id LIMIT 1 OFFSET 1');
      assert.equal(result.rows.length, 1);
      assert.equal(result.rows[0].name, 'Bob');
    });
  });

  describe('Aggregates', () => {
    it('COUNT(*)', () => {
      const result = db.execute('SELECT COUNT(*) AS total FROM users');
      assert.equal(result.rows[0].total, 3);
    });

    it('AVG', () => {
      const result = db.execute('SELECT AVG(age) AS avg_age FROM users');
      assert.equal(result.rows[0].avg_age, 30); // (30+25+35)/3 = 30
    });

    it('MIN and MAX', () => {
      const result = db.execute('SELECT MIN(age) AS youngest, MAX(age) AS oldest FROM users');
      assert.equal(result.rows[0].youngest, 25);
      assert.equal(result.rows[0].oldest, 35);
    });

    it('SUM', () => {
      const result = db.execute('SELECT SUM(age) AS total FROM users');
      assert.equal(result.rows[0].total, 90);
    });
  });

  describe('UPDATE', () => {
    it('updates matching rows', () => {
      const result = db.execute("UPDATE users SET age = 31 WHERE name = 'Alice'");
      assert.equal(result.count, 1);
      const select = db.execute("SELECT age FROM users WHERE name = 'Alice'");
      assert.equal(select.rows[0].age, 31);
    });

    it('updates all rows without WHERE', () => {
      const result = db.execute('UPDATE users SET age = 99');
      assert.equal(result.count, 3);
    });
  });

  describe('DELETE', () => {
    it('deletes matching rows', () => {
      const result = db.execute("DELETE FROM users WHERE name = 'Bob'");
      assert.equal(result.count, 1);
      const select = db.execute('SELECT * FROM users');
      assert.equal(select.rows.length, 2);
    });

    it('deletes all rows without WHERE', () => {
      db.execute('DELETE FROM users');
      const select = db.execute('SELECT * FROM users');
      assert.equal(select.rows.length, 0);
    });
  });

  describe('DROP TABLE', () => {
    it('drops a table', () => {
      db.execute('DROP TABLE users');
      assert.throws(() => db.execute('SELECT * FROM users'));
    });
  });

  describe('JOINs', () => {
    beforeEach(() => {
      db.execute('CREATE TABLE orders (id INT, user_id INT, amount INT)');
      db.execute('INSERT INTO orders VALUES (1, 1, 100)');
      db.execute('INSERT INTO orders VALUES (2, 2, 200)');
      db.execute('INSERT INTO orders VALUES (3, 1, 150)');
    });

    it('INNER JOIN', () => {
      const result = db.execute('SELECT * FROM orders JOIN users ON orders.user_id = users.id');
      assert.equal(result.rows.length, 3);
    });

    it('JOIN with WHERE', () => {
      const result = db.execute("SELECT * FROM orders JOIN users ON orders.user_id = users.id WHERE users.name = 'Alice'");
      assert.equal(result.rows.length, 2);
    });
  });

  describe('Edge cases', () => {
    it('empty result', () => {
      const result = db.execute("SELECT * FROM users WHERE name = 'Nobody'");
      assert.equal(result.rows.length, 0);
    });

    it('null values', () => {
      db.execute('CREATE TABLE nullable (id INT, val TEXT)');
      db.execute('INSERT INTO nullable VALUES (1, NULL)');
      const result = db.execute('SELECT * FROM nullable');
      assert.equal(result.rows[0].val, null);
    });

    it('many rows', () => {
      db.execute('CREATE TABLE big (id INT, data TEXT)');
      for (let i = 0; i < 100; i++) {
        db.execute(`INSERT INTO big VALUES (${i}, 'row${i}')`);
      }
      const result = db.execute('SELECT * FROM big WHERE id >= 50');
      assert.equal(result.rows.length, 50);
    });

    it('LIMIT with scalar subquery', () => {
      db.execute('CREATE TABLE items (id INT)');
      for (let i = 1; i <= 20; i++) db.execute(`INSERT INTO items VALUES (${i})`);
      
      // LIMIT (SELECT 5)
      const r1 = db.execute('SELECT * FROM items ORDER BY id LIMIT (SELECT 5)');
      assert.equal(r1.rows.length, 5);
      assert.equal(r1.rows[0].id, 1);
      assert.equal(r1.rows[4].id, 5);
    });

    it('LIMIT with subquery from another table', () => {
      db.execute('CREATE TABLE data (id INT)');
      for (let i = 1; i <= 10; i++) db.execute(`INSERT INTO data VALUES (${i})`);
      db.execute("CREATE TABLE config (key TEXT, val INT)");
      db.execute("INSERT INTO config VALUES ('limit', 3)");
      
      const r = db.execute("SELECT * FROM data ORDER BY id LIMIT (SELECT val FROM config WHERE key = 'limit')");
      assert.equal(r.rows.length, 3);
    });
  });
});
