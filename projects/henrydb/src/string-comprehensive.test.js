// string-functions-comprehensive.test.js

import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('LIKE Pattern Matching', () => {
  it('% matches any sequence', () => {
    const db = new Database();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, s TEXT)");
    db.execute("INSERT INTO t VALUES (1, 'hello'), (2, 'world'), (3, 'hello world')");
    assert.equal(db.execute("SELECT COUNT(*) as c FROM t WHERE s LIKE '%hello%'").rows[0].c, 2);
  });
  
  it('_ matches single char', () => {
    const db = new Database();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, s TEXT)");
    db.execute("INSERT INTO t VALUES (1, 'cat'), (2, 'cut'), (3, 'cart')");
    assert.equal(db.execute("SELECT COUNT(*) as c FROM t WHERE s LIKE 'c_t'").rows[0].c, 2);
  });

  it('NOT LIKE', () => {
    const db = new Database();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, s TEXT)");
    db.execute("INSERT INTO t VALUES (1, 'abc'), (2, 'def'), (3, 'abc def')");
    assert.equal(db.execute("SELECT COUNT(*) as c FROM t WHERE s NOT LIKE '%abc%'").rows[0].c, 1);
  });

  it('LIKE with NULL returns no rows', () => {
    const db = new Database();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, s TEXT)");
    db.execute("INSERT INTO t VALUES (1, NULL)");
    assert.equal(db.execute("SELECT COUNT(*) as c FROM t WHERE s LIKE '%'").rows[0].c, 0);
  });
});

describe('ILIKE (Case-Insensitive)', () => {
  it('matches regardless of case', () => {
    const db = new Database();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, s TEXT)");
    db.execute("INSERT INTO t VALUES (1, 'Hello'), (2, 'WORLD'), (3, 'hello')");
    assert.equal(db.execute("SELECT COUNT(*) as c FROM t WHERE s ILIKE 'hello'").rows[0].c, 2);
  });

  it('ILIKE with wildcards', () => {
    const db = new Database();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, s TEXT)");
    db.execute("INSERT INTO t VALUES (1, 'FooBar'), (2, 'foobar'), (3, 'FOOBAR')");
    assert.equal(db.execute("SELECT COUNT(*) as c FROM t WHERE s ILIKE 'foo%'").rows[0].c, 3);
  });
});

describe('String Functions', () => {
  it('UPPER and LOWER', () => {
    const db = new Database();
    assert.equal(db.execute("SELECT UPPER('hello') as r").rows[0].r, 'HELLO');
    assert.equal(db.execute("SELECT LOWER('HELLO') as r").rows[0].r, 'hello');
  });

  it('LENGTH', () => {
    const db = new Database();
    assert.equal(db.execute("SELECT LENGTH('hello') as r").rows[0].r, 5);
    assert.equal(db.execute("SELECT LENGTH('') as r").rows[0].r, 0);
  });

  it('CONCAT', () => {
    const db = new Database();
    assert.equal(db.execute("SELECT CONCAT('a', 'b', 'c') as r").rows[0].r, 'abc');
  });

  it('SUBSTRING', () => {
    const db = new Database();
    assert.equal(db.execute("SELECT SUBSTRING('hello world', 7, 5) as r").rows[0].r, 'world');
    assert.equal(db.execute("SELECT SUBSTRING('hello world' FROM 7 FOR 5) as r").rows[0].r, 'world');
  });

  it('REPLACE', () => {
    const db = new Database();
    assert.equal(db.execute("SELECT REPLACE('aabbcc', 'bb', 'XX') as r").rows[0].r, 'aaXXcc');
  });

  it('TRIM', () => {
    const db = new Database();
    assert.equal(db.execute("SELECT TRIM('  hello  ') as r").rows[0].r, 'hello');
  });

  it('LEFT and RIGHT', () => {
    const db = new Database();
    assert.equal(db.execute("SELECT LEFT('hello', 3) as r").rows[0].r, 'hel');
    assert.equal(db.execute("SELECT RIGHT('hello', 3) as r").rows[0].r, 'llo');
  });

  it('LPAD and RPAD', () => {
    const db = new Database();
    assert.equal(db.execute("SELECT LPAD('42', 5, '0') as r").rows[0].r, '00042');
    assert.equal(db.execute("SELECT RPAD('hi', 5, '.') as r").rows[0].r, 'hi...');
  });

  it('REVERSE', () => {
    const db = new Database();
    assert.equal(db.execute("SELECT REVERSE('hello') as r").rows[0].r, 'olleh');
  });

  it('INITCAP', () => {
    const db = new Database();
    assert.equal(db.execute("SELECT INITCAP('hello world') as r").rows[0].r, 'Hello World');
  });

  it('POSITION', () => {
    const db = new Database();
    assert.equal(db.execute("SELECT POSITION('world' IN 'hello world') as r").rows[0].r, 7);
  });

  it('REPEAT', () => {
    const db = new Database();
    assert.equal(db.execute("SELECT REPEAT('ab', 3) as r").rows[0].r, 'ababab');
  });
});

describe('New String Functions', () => {
  it('SPLIT_PART', () => {
    const db = new Database();
    assert.equal(db.execute("SELECT SPLIT_PART('a,b,c', ',', 1) as r").rows[0].r, 'a');
    assert.equal(db.execute("SELECT SPLIT_PART('a,b,c', ',', 2) as r").rows[0].r, 'b');
    assert.equal(db.execute("SELECT SPLIT_PART('a,b,c', ',', 3) as r").rows[0].r, 'c');
    assert.equal(db.execute("SELECT SPLIT_PART('a,b,c', ',', 4) as r").rows[0].r, '');
  });

  it('OVERLAY', () => {
    const db = new Database();
    assert.equal(db.execute("SELECT OVERLAY('hello world' PLACING 'there' FROM 7 FOR 5) as r").rows[0].r, 'hello there');
  });

  it('TRANSLATE', () => {
    const db = new Database();
    assert.equal(db.execute("SELECT TRANSLATE('hello', 'helo', 'HELO') as r").rows[0].r, 'HELLO');
  });

  it('CHR and ASCII', () => {
    const db = new Database();
    assert.equal(db.execute("SELECT CHR(65) as r").rows[0].r, 'A');
    assert.equal(db.execute("SELECT ASCII('A') as r").rows[0].r, 65);
  });

  it('MD5', () => {
    const db = new Database();
    const r = db.execute("SELECT MD5('hello') as r").rows[0].r;
    assert.ok(typeof r === 'string' && r.length > 0);
  });

  it('REGEXP_REPLACE', () => {
    const db = new Database();
    assert.equal(db.execute("SELECT REGEXP_REPLACE('hello 123', '[0-9]+', 'NUM') as r").rows[0].r, 'hello NUM');
  });
});

describe('String Functions with NULL', () => {
  it('UPPER(NULL) = NULL', () => {
    const db = new Database();
    assert.equal(db.execute("SELECT UPPER(NULL) as r").rows[0].r, null);
  });

  it('LENGTH(NULL) = NULL', () => {
    const db = new Database();
    assert.equal(db.execute("SELECT LENGTH(NULL) as r").rows[0].r, null);
  });

  it('SPLIT_PART with NULL = NULL', () => {
    const db = new Database();
    assert.equal(db.execute("SELECT SPLIT_PART(NULL, ',', 1) as r").rows[0].r, null);
  });
});

describe('SIMILAR TO', () => {
  it('basic pattern matching', () => {
    const db = new Database();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, s TEXT)");
    db.execute("INSERT INTO t VALUES (1, 'hello'), (2, 'world'), (3, 'help')");
    assert.equal(db.execute("SELECT COUNT(*) as c FROM t WHERE s SIMILAR TO 'hel%'").rows[0].c, 2);
  });
});
