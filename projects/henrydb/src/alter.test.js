// alter.test.js — ALTER TABLE tests for HenryDB
import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('ALTER TABLE', () => {
  let db;

  beforeEach(() => {
    db = new Database();
    db.execute('CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT)');
    db.execute("INSERT INTO users VALUES (1, 'Alice', 30)");
    db.execute("INSERT INTO users VALUES (2, 'Bob', 25)");
    db.execute("INSERT INTO users VALUES (3, 'Charlie', 35)");
  });

  describe('ADD COLUMN', () => {
    it('adds column with NULL default', () => {
      db.execute('ALTER TABLE users ADD COLUMN email TEXT');
      const result = db.execute('SELECT * FROM users WHERE id = 1');
      assert.equal(result.rows[0].email, null);
    });

    it('adds column with explicit default', () => {
      db.execute("ALTER TABLE users ADD COLUMN status TEXT DEFAULT 'active'");
      const result = db.execute('SELECT * FROM users WHERE id = 1');
      assert.equal(result.rows[0].status, 'active');
    });

    it('adds column with numeric default', () => {
      db.execute('ALTER TABLE users ADD COLUMN score INT DEFAULT 0');
      const result = db.execute('SELECT * FROM users');
      assert.ok(result.rows.every(r => r.score === 0));
    });

    it('new inserts use new column', () => {
      db.execute('ALTER TABLE users ADD COLUMN email TEXT');
      db.execute("INSERT INTO users VALUES (4, 'Diana', 28, 'diana@test.com')");
      const result = db.execute('SELECT * FROM users WHERE id = 4');
      assert.equal(result.rows[0].email, 'diana@test.com');
    });

    it('errors on duplicate column', () => {
      assert.throws(() => {
        db.execute('ALTER TABLE users ADD COLUMN name TEXT');
      }, /already exists/);
    });

    it('errors on non-existent table', () => {
      assert.throws(() => {
        db.execute('ALTER TABLE ghost ADD COLUMN col TEXT');
      }, /not found/);
    });

    it('ADD COLUMN keyword optional', () => {
      db.execute('ALTER TABLE users ADD email TEXT');
      const result = db.execute('SELECT * FROM users WHERE id = 1');
      assert.equal(result.rows[0].email, null);
    });
  });

  describe('DROP COLUMN', () => {
    it('drops non-PK column', () => {
      db.execute('ALTER TABLE users DROP COLUMN age');
      const result = db.execute('SELECT * FROM users WHERE id = 1');
      assert.equal(result.rows[0].name, 'Alice');
      assert.equal(result.rows[0].age, undefined);
    });

    it('errors on dropping PK', () => {
      assert.throws(() => {
        db.execute('ALTER TABLE users DROP COLUMN id');
      }, /primary key/);
    });

    it('errors on non-existent column', () => {
      assert.throws(() => {
        db.execute('ALTER TABLE users DROP COLUMN ghost');
      }, /not found/);
    });

    it('DROP COLUMN keyword optional', () => {
      db.execute('ALTER TABLE users DROP age');
      const result = db.execute('SELECT * FROM users WHERE id = 1');
      assert.equal(result.rows[0].age, undefined);
    });

    it('drops column with index', () => {
      db.execute('CREATE INDEX idx_age ON users (age)');
      db.execute('ALTER TABLE users DROP COLUMN age');
      // Index should be gone
      assert.equal(db.indexCatalog.has('idx_age'), false);
    });

    it('remaining data intact after drop', () => {
      db.execute('ALTER TABLE users DROP COLUMN age');
      const result = db.execute('SELECT * FROM users ORDER BY id');
      assert.equal(result.rows.length, 3);
      assert.equal(result.rows[0].name, 'Alice');
      assert.equal(result.rows[1].name, 'Bob');
    });
  });

  describe('RENAME COLUMN', () => {
    it('renames column', () => {
      db.execute('ALTER TABLE users RENAME COLUMN name TO full_name');
      const result = db.execute('SELECT * FROM users WHERE id = 1');
      assert.equal(result.rows[0].full_name, 'Alice');
    });

    it('renamed column works in queries', () => {
      db.execute('ALTER TABLE users RENAME COLUMN name TO full_name');
      const result = db.execute("SELECT * FROM users WHERE full_name = 'Bob'");
      assert.equal(result.rows.length, 1);
    });

    it('errors on non-existent column', () => {
      assert.throws(() => {
        db.execute('ALTER TABLE users RENAME COLUMN ghost TO new_ghost');
      }, /not found/);
    });

    it('errors on duplicate target name', () => {
      assert.throws(() => {
        db.execute('ALTER TABLE users RENAME COLUMN name TO age');
      }, /already exists/);
    });

    it('updates index on rename', () => {
      db.execute('CREATE INDEX idx_age ON users (age)');
      db.execute('ALTER TABLE users RENAME COLUMN age TO years');
      // Index should work with new column name
      const table = db.tables.get('users');
      assert.ok(table.indexes.has('years'));
      assert.ok(!table.indexes.has('age'));
    });
  });

  describe('RENAME TABLE', () => {
    it('renames table', () => {
      db.execute('ALTER TABLE users RENAME TO people');
      const result = db.execute('SELECT * FROM people');
      assert.equal(result.rows.length, 3);
    });

    it('old name no longer valid', () => {
      db.execute('ALTER TABLE users RENAME TO people');
      assert.throws(() => {
        db.execute('SELECT * FROM users');
      }, /not found/);
    });

    it('inserts work with new name', () => {
      db.execute('ALTER TABLE users RENAME TO people');
      db.execute("INSERT INTO people VALUES (4, 'Diana', 28)");
      const result = db.execute('SELECT * FROM people');
      assert.equal(result.rows.length, 4);
    });
  });
});
