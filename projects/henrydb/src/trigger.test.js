import { test, describe } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Triggers', () => {
  test('AFTER INSERT fires with NEW values', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, name TEXT)');
    db.execute('CREATE TABLE log (action TEXT, val TEXT)');
    db.execute("CREATE TRIGGER ti AFTER INSERT ON t BEGIN INSERT INTO log VALUES ('INSERT', NEW.name); END");
    
    db.execute("INSERT INTO t VALUES (1, 'Alice')");
    const log = db.execute('SELECT * FROM log').rows;
    assert.equal(log.length, 1);
    assert.equal(log[0].action, 'INSERT');
    assert.equal(log[0].val, 'Alice');
  });

  test('AFTER UPDATE fires with NEW values', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, name TEXT)');
    db.execute('CREATE TABLE log (action TEXT, val TEXT)');
    db.execute("CREATE TRIGGER tu AFTER UPDATE ON t BEGIN INSERT INTO log VALUES ('UPDATE', NEW.name); END");
    
    db.execute("INSERT INTO t VALUES (1, 'Alice')");
    db.execute("UPDATE t SET name = 'Alicia' WHERE id = 1");
    const log = db.execute('SELECT * FROM log').rows;
    assert.equal(log.length, 1);
    assert.equal(log[0].val, 'Alicia');
  });

  test('BEFORE DELETE fires with OLD values', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, name TEXT)');
    db.execute('CREATE TABLE log (action TEXT, val TEXT)');
    db.execute("CREATE TRIGGER td BEFORE DELETE ON t BEGIN INSERT INTO log VALUES ('DELETE', OLD.name); END");
    
    db.execute("INSERT INTO t VALUES (1, 'Alice')");
    db.execute('DELETE FROM t WHERE id = 1');
    const log = db.execute('SELECT * FROM log').rows;
    assert.equal(log.length, 1);
    assert.equal(log[0].val, 'Alice');
  });

  test('multiple triggers on same event fire in order', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT)');
    db.execute('CREATE TABLE log (seq INT)');
    db.execute("CREATE TRIGGER t1 AFTER INSERT ON t BEGIN INSERT INTO log VALUES (1); END");
    db.execute("CREATE TRIGGER t2 AFTER INSERT ON t BEGIN INSERT INTO log VALUES (2); END");
    
    db.execute('INSERT INTO t VALUES (1)');
    const log = db.execute('SELECT * FROM log ORDER BY seq').rows;
    assert.equal(log.length, 2);
    assert.equal(log[0].seq, 1);
    assert.equal(log[1].seq, 2);
  });

  test('trigger fires for each row in multi-row INSERT', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT)');
    db.execute('CREATE TABLE log (val INT)');
    db.execute("CREATE TRIGGER ti AFTER INSERT ON t BEGIN INSERT INTO log VALUES (NEW.id); END");
    
    db.execute('INSERT INTO t VALUES (1), (2), (3)');
    const log = db.execute('SELECT * FROM log ORDER BY val').rows;
    assert.equal(log.length, 3);
    assert.deepEqual(log.map(r => r.val), [1, 2, 3]);
  });

  test('UPDATE trigger has both NEW and OLD', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, val INT)');
    db.execute('CREATE TABLE log (old_val INT, new_val INT)');
    db.execute("CREATE TRIGGER tu AFTER UPDATE ON t BEGIN INSERT INTO log VALUES (OLD.val, NEW.val); END");
    
    db.execute('INSERT INTO t VALUES (1, 100)');
    db.execute('UPDATE t SET val = 200 WHERE id = 1');
    const log = db.execute('SELECT * FROM log').rows;
    assert.equal(log.length, 1);
    assert.equal(log[0].old_val, 100);
    assert.equal(log[0].new_val, 200);
  });

  test('trigger on different table does not fire', () => {
    const db = new Database();
    db.execute('CREATE TABLE t1 (id INT)');
    db.execute('CREATE TABLE t2 (id INT)');
    db.execute('CREATE TABLE log (msg TEXT)');
    db.execute("CREATE TRIGGER tr AFTER INSERT ON t1 BEGIN INSERT INTO log VALUES ('t1 triggered'); END");
    
    db.execute('INSERT INTO t2 VALUES (1)');
    const log = db.execute('SELECT * FROM log').rows;
    assert.equal(log.length, 0);
  });

  test('NULL values in trigger substitution', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, name TEXT)');
    db.execute('CREATE TABLE log (val TEXT)');
    db.execute("CREATE TRIGGER ti AFTER INSERT ON t BEGIN INSERT INTO log VALUES (NEW.name); END");
    
    db.execute('INSERT INTO t VALUES (1, NULL)');
    const log = db.execute('SELECT * FROM log').rows;
    assert.equal(log.length, 1);
    assert.equal(log[0].val, null);
  });
});

describe('UPDATE OF column triggers', () => {
  it('fires only when specified column changes', () => {
    const db = new Database();
    db.execute('CREATE TABLE emp (id INTEGER PRIMARY KEY, name TEXT, salary INTEGER, dept TEXT)');
    db.execute('CREATE TABLE audit (msg TEXT)');
    db.execute("CREATE TRIGGER salary_change AFTER UPDATE OF salary ON emp BEGIN INSERT INTO audit VALUES ('salary changed') END");
    db.execute("INSERT INTO emp VALUES (1, 'Alice', 50000, 'Eng')");

    // Update salary → trigger fires
    db.execute('UPDATE emp SET salary = 60000 WHERE id = 1');
    assert.equal(db.execute('SELECT count(*) as cnt FROM audit').rows[0].cnt, 1);

    // Update dept → trigger does NOT fire
    db.execute("UPDATE emp SET dept = 'HR' WHERE id = 1");
    assert.equal(db.execute('SELECT count(*) as cnt FROM audit').rows[0].cnt, 1);

    // Update salary again → trigger fires
    db.execute('UPDATE emp SET salary = 70000 WHERE id = 1');
    assert.equal(db.execute('SELECT count(*) as cnt FROM audit').rows[0].cnt, 2);
  });

  it('fires when any of multiple specified columns change', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (a INTEGER, b INTEGER, c INTEGER)');
    db.execute('CREATE TABLE log (msg TEXT)');
    db.execute("CREATE TRIGGER t_ab AFTER UPDATE OF a, b ON t BEGIN INSERT INTO log VALUES ('changed') END");
    db.execute('INSERT INTO t VALUES (1, 2, 3)');

    // Only c changed → no fire
    db.execute('UPDATE t SET c = 99');
    assert.equal(db.execute('SELECT count(*) as cnt FROM log').rows[0].cnt, 0);

    // a changed → fire
    db.execute('UPDATE t SET a = 10');
    assert.equal(db.execute('SELECT count(*) as cnt FROM log').rows[0].cnt, 1);

    // b changed → fire
    db.execute('UPDATE t SET b = 20');
    assert.equal(db.execute('SELECT count(*) as cnt FROM log').rows[0].cnt, 2);
  });

  it('fires when salary+dept both change and trigger watches salary', () => {
    const db = new Database();
    db.execute('CREATE TABLE emp (id INTEGER PRIMARY KEY, salary INTEGER, dept TEXT)');
    db.execute('CREATE TABLE audit (msg TEXT)');
    db.execute("CREATE TRIGGER s_change AFTER UPDATE OF salary ON emp BEGIN INSERT INTO audit VALUES ('yes') END");
    db.execute('INSERT INTO emp VALUES (1, 50000, \'Eng\')');

    db.execute("UPDATE emp SET salary = 80000, dept = 'Sales' WHERE id = 1");
    assert.equal(db.execute('SELECT count(*) as cnt FROM audit').rows[0].cnt, 1);
  });

  it('regular UPDATE trigger (no OF) fires on any column change', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (a INTEGER, b INTEGER)');
    db.execute('CREATE TABLE log (msg TEXT)');
    db.execute("CREATE TRIGGER t_any AFTER UPDATE ON t BEGIN INSERT INTO log VALUES ('any') END");
    db.execute('INSERT INTO t VALUES (1, 2)');

    db.execute('UPDATE t SET a = 10');
    assert.equal(db.execute('SELECT count(*) as cnt FROM log').rows[0].cnt, 1);

    db.execute('UPDATE t SET b = 20');
    assert.equal(db.execute('SELECT count(*) as cnt FROM log').rows[0].cnt, 2);
  });
});
