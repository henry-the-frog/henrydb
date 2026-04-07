import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { WAL, TransactionManager, TransactionalDB } from './transaction.js';
import { Database } from './db.js';

// ===== WAL Tests =====
describe('WAL', () => {
  it('appends entries with incrementing LSN', () => {
    const wal = new WAL();
    const lsn1 = wal.append(1, 'INSERT', 'users', { values: [1, 'alice'] });
    const lsn2 = wal.append(1, 'INSERT', 'users', { values: [2, 'bob'] });
    assert.equal(lsn1, 0);
    assert.equal(lsn2, 1);
    assert.equal(wal.size, 2);
  });

  it('retrieves entries since LSN', () => {
    const wal = new WAL();
    wal.append(1, 'BEGIN', null, null);
    wal.append(1, 'INSERT', 'users', {});
    wal.append(1, 'COMMIT', null, null);
    const entries = wal.getEntriesSince(1);
    assert.equal(entries.length, 2);
    assert.equal(entries[0].type, 'INSERT');
  });

  it('retrieves entries for transaction', () => {
    const wal = new WAL();
    wal.append(1, 'BEGIN', null, null);
    wal.append(2, 'BEGIN', null, null);
    wal.append(1, 'INSERT', 'users', {});
    wal.append(2, 'INSERT', 'orders', {});
    wal.append(1, 'COMMIT', null, null);
    const tx1Entries = wal.getEntriesForTx(1);
    assert.equal(tx1Entries.length, 3);
  });

  it('finds uncommitted transactions', () => {
    const wal = new WAL();
    wal.append(1, 'BEGIN', null, null);
    wal.append(2, 'BEGIN', null, null);
    wal.append(1, 'INSERT', 'users', {});
    wal.append(1, 'COMMIT', null, null);
    // tx 2 is still uncommitted
    const uncommitted = wal.getUncommitted();
    assert.deepStrictEqual(uncommitted, [2]);
  });

  it('checkpoint updates LSN', () => {
    const wal = new WAL();
    wal.append(1, 'BEGIN', null, null);
    wal.append(1, 'COMMIT', null, null);
    const cpLSN = wal.checkpoint();
    assert.equal(cpLSN, 2);
  });
});

// ===== Transaction Manager Tests =====
describe('TransactionManager', () => {
  let tm;

  beforeEach(() => {
    tm = new TransactionManager();
  });

  it('begins a transaction', () => {
    const tx = tm.begin();
    assert.equal(tx.txId, 1);
    assert.ok(!tx.committed);
    assert.ok(!tx.rolledBack);
  });

  it('commits a transaction', () => {
    const tx = tm.begin();
    tx.commit();
    assert.ok(tx.committed);
  });

  it('rolls back a transaction', () => {
    const tx = tm.begin();
    let undoRan = false;
    tx.undoLog.push(() => { undoRan = true; });
    tx.rollback();
    assert.ok(tx.rolledBack);
    assert.ok(undoRan);
  });

  it('snapshot isolation: tx2 does not see tx1 uncommitted writes', () => {
    const tx1 = tm.begin();
    const tx2 = tm.begin();
    // tx1 writes but doesn't commit
    tx1.recordWrite('key1', 'value1', () => {});
    // tx2 should not see tx1's writes
    assert.ok(!tx2.isVisible(tx1.txId));
  });

  it('snapshot isolation: tx2 sees previously committed tx', () => {
    const tx1 = tm.begin();
    tx1.commit();
    const tx2 = tm.begin();
    assert.ok(tx2.isVisible(tx1.txId));
  });

  it('snapshot isolation: tx sees own writes', () => {
    const tx1 = tm.begin();
    assert.ok(tx1.isVisible(tx1.txId));
  });

  it('assigns unique transaction IDs', () => {
    const tx1 = tm.begin();
    const tx2 = tm.begin();
    const tx3 = tm.begin();
    assert.notEqual(tx1.txId, tx2.txId);
    assert.notEqual(tx2.txId, tx3.txId);
  });

  it('rollback undoes writes in reverse', () => {
    const tx = tm.begin();
    const order = [];
    tx.undoLog.push(() => order.push('first'));
    tx.undoLog.push(() => order.push('second'));
    tx.undoLog.push(() => order.push('third'));
    tx.rollback();
    assert.deepStrictEqual(order, ['third', 'second', 'first']);
  });
});

// ===== Transactional DB Tests =====
describe('TransactionalDB', () => {
  let db, txdb;

  beforeEach(() => {
    db = new Database();
    db.execute('CREATE TABLE accounts (id INT PRIMARY KEY, name TEXT, balance INT)');
    db.execute("INSERT INTO accounts VALUES (1, 'Alice', 1000)");
    db.execute("INSERT INTO accounts VALUES (2, 'Bob', 500)");
    txdb = new TransactionalDB(db);
  });

  it('insert within transaction', () => {
    const tx = txdb.begin();
    txdb.insertInTx(tx, 'accounts', [3, 'Charlie', 750]);
    tx.commit();
    const result = db.execute('SELECT * FROM accounts');
    assert.equal(result.rows.length, 3);
  });

  it('rollback undoes insert', () => {
    const tx = txdb.begin();
    txdb.insertInTx(tx, 'accounts', [3, 'Charlie', 750]);
    // Before rollback, row is visible
    assert.equal(db.execute('SELECT * FROM accounts').rows.length, 3);
    tx.rollback();
    // After rollback, row is gone
    assert.equal(db.execute('SELECT * FROM accounts').rows.length, 2);
  });

  it('delete within transaction', () => {
    const tx = txdb.begin();
    // Find Alice's row
    const rows = [...db.tables.get('accounts').heap.scan()];
    const alice = rows.find(r => r.values[1] === 'Alice');
    txdb.deleteInTx(tx, 'accounts', alice.pageId, alice.slotIdx);
    tx.commit();
    assert.equal(db.execute('SELECT * FROM accounts').rows.length, 1);
  });

  it('rollback undoes delete', () => {
    const tx = txdb.begin();
    const rows = [...db.tables.get('accounts').heap.scan()];
    const alice = rows.find(r => r.values[1] === 'Alice');
    txdb.deleteInTx(tx, 'accounts', alice.pageId, alice.slotIdx);
    tx.rollback();
    assert.equal(db.execute('SELECT * FROM accounts').rows.length, 2);
  });

  it('WAL records all operations', () => {
    const tx = txdb.begin();
    txdb.insertInTx(tx, 'accounts', [3, 'Charlie', 750]);
    tx.commit();
    // WAL should have BEGIN, INSERT, COMMIT
    assert.ok(txdb.wal.size >= 3);
  });

  it('crash recovery finds uncommitted tx', () => {
    const tx = txdb.begin();
    txdb.insertInTx(tx, 'accounts', [3, 'Charlie', 750]);
    // Simulate crash: don't commit
    const recovery = txdb.txManager.recover();
    assert.equal(recovery.undoneTransactions.length, 1);
    assert.equal(recovery.undoneTransactions[0], tx.txId);
  });

  it('multiple concurrent transactions', () => {
    const tx1 = txdb.begin();
    const tx2 = txdb.begin();
    txdb.insertInTx(tx1, 'accounts', [3, 'Charlie', 750]);
    txdb.insertInTx(tx2, 'accounts', [4, 'Diana', 800]);
    tx1.commit();
    tx2.commit();
    assert.equal(db.execute('SELECT * FROM accounts').rows.length, 4);
  });
});
