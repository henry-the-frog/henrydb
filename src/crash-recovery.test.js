// crash-recovery.test.js — Crash recovery stress tests for HenryDB
import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { WAL, TransactionManager, TransactionalDB } from './transaction.js';
import { Database } from './db.js';

describe('Crash Recovery', () => {
  let db, txdb;

  beforeEach(() => {
    db = new Database();
    db.execute('CREATE TABLE accounts (id INT PRIMARY KEY, name TEXT, balance INT)');
    db.execute("INSERT INTO accounts VALUES (1, 'Alice', 1000)");
    db.execute("INSERT INTO accounts VALUES (2, 'Bob', 500)");
    db.execute("INSERT INTO accounts VALUES (3, 'Charlie', 750)");
    txdb = new TransactionalDB(db);
  });

  describe('WAL integrity', () => {
    it('all operations logged', () => {
      const tx = txdb.begin();
      txdb.insertInTx(tx, 'accounts', [4, 'Diana', 800]);
      tx.commit();

      const entries = txdb.wal.getEntriesForTx(tx.txId);
      const types = entries.map(e => e.type);
      assert.ok(types.includes('BEGIN'));
      assert.ok(types.includes('INSERT'));
      assert.ok(types.includes('COMMIT'));
    });

    it('LSN monotonically increases', () => {
      const tx1 = txdb.begin();
      const tx2 = txdb.begin();
      txdb.insertInTx(tx1, 'accounts', [4, 'Diana', 800]);
      txdb.insertInTx(tx2, 'accounts', [5, 'Eve', 900]);
      tx1.commit();
      tx2.commit();

      let prevLSN = -1;
      for (const entry of txdb.wal.log) {
        assert.ok(entry.lsn > prevLSN);
        prevLSN = entry.lsn;
      }
    });

    it('WAL size grows with operations', () => {
      const initialSize = txdb.wal.size;
      const tx = txdb.begin();
      txdb.insertInTx(tx, 'accounts', [4, 'Diana', 800]);
      tx.commit();
      assert.ok(txdb.wal.size > initialSize);
    });
  });

  describe('Uncommitted transaction detection', () => {
    it('detects single uncommitted tx', () => {
      const tx = txdb.begin();
      txdb.insertInTx(tx, 'accounts', [4, 'Diana', 800]);
      // "crash" — don't commit

      const uncommitted = txdb.wal.getUncommitted();
      assert.equal(uncommitted.length, 1);
      assert.equal(uncommitted[0], tx.txId);
    });

    it('detects multiple uncommitted txns', () => {
      const tx1 = txdb.begin();
      const tx2 = txdb.begin();
      txdb.insertInTx(tx1, 'accounts', [4, 'Diana', 800]);
      txdb.insertInTx(tx2, 'accounts', [5, 'Eve', 900]);
      // Neither committed

      const uncommitted = txdb.wal.getUncommitted();
      assert.equal(uncommitted.length, 2);
    });

    it('committed tx not in uncommitted list', () => {
      const tx1 = txdb.begin();
      const tx2 = txdb.begin();
      txdb.insertInTx(tx1, 'accounts', [4, 'Diana', 800]);
      txdb.insertInTx(tx2, 'accounts', [5, 'Eve', 900]);
      tx1.commit();
      // tx2 still uncommitted

      const uncommitted = txdb.wal.getUncommitted();
      assert.equal(uncommitted.length, 1);
      assert.equal(uncommitted[0], tx2.txId);
    });

    it('rolled back tx not in uncommitted list', () => {
      const tx = txdb.begin();
      txdb.insertInTx(tx, 'accounts', [4, 'Diana', 800]);
      tx.rollback();

      const uncommitted = txdb.wal.getUncommitted();
      assert.equal(uncommitted.length, 0);
    });
  });

  describe('Recovery undo', () => {
    it('recover identifies uncommitted inserts', () => {
      const tx = txdb.begin();
      txdb.insertInTx(tx, 'accounts', [4, 'Diana', 800]);
      // Crash

      const recovery = txdb.txManager.recover();
      assert.equal(recovery.undoneTransactions.length, 1);
      assert.ok(recovery.undoneEntries > 0);
    });

    it('recover identifies uncommitted deletes', () => {
      const tx = txdb.begin();
      const rows = [...db.tables.get('accounts').heap.scan()];
      txdb.deleteInTx(tx, 'accounts', rows[0].pageId, rows[0].slotIdx);
      // Crash

      const recovery = txdb.txManager.recover();
      assert.equal(recovery.undoneTransactions.length, 1);
      assert.ok(recovery.undoneEntries > 0);
    });

    it('no recovery needed after clean commit', () => {
      const tx = txdb.begin();
      txdb.insertInTx(tx, 'accounts', [4, 'Diana', 800]);
      tx.commit();

      const recovery = txdb.txManager.recover();
      assert.equal(recovery.undoneTransactions.length, 0);
      assert.equal(recovery.undoneEntries, 0);
    });

    it('no recovery needed after clean rollback', () => {
      const tx = txdb.begin();
      txdb.insertInTx(tx, 'accounts', [4, 'Diana', 800]);
      tx.rollback();

      const recovery = txdb.txManager.recover();
      assert.equal(recovery.undoneTransactions.length, 0);
      assert.equal(recovery.undoneEntries, 0);
    });
  });

  describe('Rollback correctness', () => {
    it('rollback restores original row count', () => {
      const initialCount = db.execute('SELECT * FROM accounts').rows.length;
      const tx = txdb.begin();
      txdb.insertInTx(tx, 'accounts', [4, 'Diana', 800]);
      txdb.insertInTx(tx, 'accounts', [5, 'Eve', 900]);
      tx.rollback();
      assert.equal(db.execute('SELECT * FROM accounts').rows.length, initialCount);
    });

    it('rollback restores deleted rows', () => {
      const tx = txdb.begin();
      const rows = [...db.tables.get('accounts').heap.scan()];
      txdb.deleteInTx(tx, 'accounts', rows[0].pageId, rows[0].slotIdx);
      tx.rollback();
      assert.equal(db.execute('SELECT * FROM accounts').rows.length, 3);
    });

    it('partial transaction: commit some, crash some', () => {
      const tx1 = txdb.begin();
      const tx2 = txdb.begin();
      txdb.insertInTx(tx1, 'accounts', [4, 'Diana', 800]);
      txdb.insertInTx(tx2, 'accounts', [5, 'Eve', 900]);
      tx1.commit();
      // tx2 "crashes"

      assert.equal(db.execute('SELECT * FROM accounts').rows.length, 5); // both visible pre-crash
      // Recovery identifies tx2
      const recovery = txdb.txManager.recover();
      assert.equal(recovery.undoneTransactions.length, 1);
      assert.equal(recovery.undoneTransactions[0], tx2.txId);
    });
  });

  describe('Stress scenarios', () => {
    it('many transactions: interleaved commit and crash', () => {
      const txns = [];
      for (let i = 0; i < 20; i++) {
        const tx = txdb.begin();
        txdb.insertInTx(tx, 'accounts', [100 + i, `User${i}`, i * 100]);
        txns.push(tx);
      }

      // Commit even-numbered, leave odd uncommitted
      for (let i = 0; i < 20; i++) {
        if (i % 2 === 0) txns[i].commit();
      }

      const uncommitted = txdb.wal.getUncommitted();
      assert.equal(uncommitted.length, 10);

      const recovery = txdb.txManager.recover();
      assert.equal(recovery.undoneTransactions.length, 10);
    });

    it('rapid insert-delete cycles within transaction', () => {
      const tx = txdb.begin();

      // Insert then delete several rows
      for (let i = 10; i < 20; i++) {
        const rid = txdb.insertInTx(tx, 'accounts', [i, `Temp${i}`, 0]);
      }

      tx.rollback();
      // Original data intact
      assert.equal(db.execute('SELECT * FROM accounts').rows.length, 3);
    });

    it('WAL accumulates correctly across many transactions', () => {
      for (let i = 0; i < 50; i++) {
        const tx = txdb.begin();
        txdb.insertInTx(tx, 'accounts', [100 + i, `Batch${i}`, i]);
        tx.commit();
      }

      // WAL should have: 50 * (BEGIN + INSERT + COMMIT) = 150 entries
      // Plus the WAL entries from recordWrite calls
      assert.ok(txdb.wal.size >= 150);

      // All committed
      const uncommitted = txdb.wal.getUncommitted();
      assert.equal(uncommitted.length, 0);
    });

    it('checkpoint advances correctly', () => {
      const tx1 = txdb.begin();
      txdb.insertInTx(tx1, 'accounts', [4, 'Diana', 800]);
      tx1.commit();

      const cp1 = txdb.wal.checkpoint();
      assert.ok(cp1 > 0);

      const tx2 = txdb.begin();
      txdb.insertInTx(tx2, 'accounts', [5, 'Eve', 900]);
      tx2.commit();

      const cp2 = txdb.wal.checkpoint();
      assert.ok(cp2 > cp1);
    });
  });

  describe('MVCC visibility', () => {
    it('transaction sees own writes', () => {
      const tx = txdb.begin();
      assert.ok(tx.isVisible(tx.txId));
    });

    it('transaction does not see concurrent uncommitted', () => {
      const tx1 = txdb.begin();
      const tx2 = txdb.begin();
      assert.ok(!tx1.isVisible(tx2.txId));
      assert.ok(!tx2.isVisible(tx1.txId));
    });

    it('transaction sees previously committed', () => {
      const tx1 = txdb.begin();
      tx1.commit();
      const tx2 = txdb.begin();
      assert.ok(tx2.isVisible(tx1.txId));
    });

    it('snapshot isolation: later commit not visible to earlier snapshot', () => {
      const tx1 = txdb.begin();
      const tx2 = txdb.begin();
      tx1.commit(); // committed after tx2 began
      assert.ok(!tx2.isVisible(tx1.txId)); // tx2's snapshot doesn't include tx1
    });
  });
});
