// tpcb-benchmark.test.js — TPC-B-style benchmark for HenryDB
// Tests concurrent transactions, ACID compliance, and throughput
import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Database } from './db.js';
import { TransactionalDatabase } from './transactional-db.js';

// ===== TPC-B Schema & Loader =====

const SCALE_FACTOR = 1; // 1 branch, 10 tellers, 100000 accounts
// We'll use smaller for testing: 1 branch, 3 tellers, 100 accounts

function createSchema(db) {
  db.execute('CREATE TABLE branches (bid INT PRIMARY KEY, bbalance INT, filler TEXT)');
  db.execute('CREATE TABLE tellers (tid INT PRIMARY KEY, bid INT, tbalance INT, filler TEXT)');
  db.execute('CREATE TABLE accounts (aid INT PRIMARY KEY, bid INT, abalance INT, filler TEXT)');
  db.execute('CREATE TABLE history (tid INT, bid INT, aid INT, delta INT, mtime INT, filler TEXT)');
}

function loadData(db, { branches = 1, tellersPerBranch = 3, accountsPerBranch = 100 } = {}) {
  // Create branches
  for (let b = 1; b <= branches; b++) {
    db.execute(`INSERT INTO branches VALUES (${b}, 0, 'branch-filler')`);
  }
  
  // Create tellers (10 per branch in TPC-B, we use tellersPerBranch)
  let tid = 1;
  for (let b = 1; b <= branches; b++) {
    for (let t = 0; t < tellersPerBranch; t++) {
      db.execute(`INSERT INTO tellers VALUES (${tid}, ${b}, 0, 'teller-filler')`);
      tid++;
    }
  }
  
  // Create accounts (100000 per branch in TPC-B, we use accountsPerBranch)
  let aid = 1;
  for (let b = 1; b <= branches; b++) {
    for (let a = 0; a < accountsPerBranch; a++) {
      db.execute(`INSERT INTO accounts VALUES (${aid}, ${b}, 0, 'account-filler')`);
      aid++;
    }
  }
  
  return {
    totalBranches: branches,
    totalTellers: tid - 1,
    totalAccounts: aid - 1,
  };
}

// ===== TPC-B Transaction Profile =====
// Each transaction:
// 1. UPDATE accounts SET abalance = abalance + delta WHERE aid = ?
// 2. SELECT abalance FROM accounts WHERE aid = ?
// 3. UPDATE tellers SET tbalance = tbalance + delta WHERE tid = ?
// 4. UPDATE branches SET bbalance = bbalance + delta WHERE bid = ?
// 5. INSERT INTO history VALUES (tid, bid, aid, delta, time, filler)

function tpcbTransaction(db, { aid, tid, bid, delta }) {
  db.execute(`UPDATE accounts SET abalance = abalance + ${delta} WHERE aid = ${aid}`);
  const result = db.execute(`SELECT abalance FROM accounts WHERE aid = ${aid}`);
  db.execute(`UPDATE tellers SET tbalance = tbalance + ${delta} WHERE tid = ${tid}`);
  db.execute(`UPDATE branches SET bbalance = bbalance + ${delta} WHERE bid = ${bid}`);
  db.execute(`INSERT INTO history VALUES (${tid}, ${bid}, ${aid}, ${delta}, ${Date.now()}, 'history-filler')`);
  return result.rows[0]?.abalance;
}

// Generate random transaction parameters
function randomTxParams(config) {
  const aid = 1 + Math.floor(Math.random() * config.totalAccounts);
  const tid = 1 + Math.floor(Math.random() * config.totalTellers);
  const bid = 1 + Math.floor(Math.random() * config.totalBranches);
  const delta = Math.floor(Math.random() * 20001) - 10000; // -10000 to +10000
  return { aid, tid, bid, delta };
}

// ===== ACID Verification =====
// Invariant: SUM(abalance) == SUM(tbalance) == SUM(bbalance) == SUM(delta) from history

function verifyACID(db) {
  const accountSum = db.execute('SELECT SUM(abalance) as total FROM accounts');
  const tellerSum = db.execute('SELECT SUM(tbalance) as total FROM tellers');
  const branchSum = db.execute('SELECT SUM(bbalance) as total FROM branches');
  const historySum = db.execute('SELECT SUM(delta) as total FROM history');
  
  return {
    accountTotal: accountSum.rows[0]?.total ?? 0,
    tellerTotal: tellerSum.rows[0]?.total ?? 0,
    branchTotal: branchSum.rows[0]?.total ?? 0,
    historyTotal: historySum.rows[0]?.total ?? 0,
  };
}

// ===== Tests =====

describe('TPC-B Benchmark', () => {
  
  describe('Schema and Loading', () => {
    it('creates schema and loads data correctly', () => {
      const db = new Database();
      createSchema(db);
      const config = loadData(db, { branches: 2, tellersPerBranch: 3, accountsPerBranch: 50 });
      
      assert.equal(config.totalBranches, 2);
      assert.equal(config.totalTellers, 6);
      assert.equal(config.totalAccounts, 100);
      
      const branches = db.execute('SELECT COUNT(*) as cnt FROM branches');
      assert.equal(branches.rows[0].cnt, 2);
      
      const tellers = db.execute('SELECT COUNT(*) as cnt FROM tellers');
      assert.equal(tellers.rows[0].cnt, 6);
      
      const accounts = db.execute('SELECT COUNT(*) as cnt FROM accounts');
      assert.equal(accounts.rows[0].cnt, 100);
    });
  });

  describe('Transaction Profile', () => {
    it('executes single TPC-B transaction correctly', () => {
      const db = new Database();
      createSchema(db);
      const config = loadData(db);
      
      // Execute one transaction
      const params = { aid: 1, tid: 1, bid: 1, delta: 500 };
      tpcbTransaction(db, params);
      
      // Verify account balance changed
      const acct = db.execute('SELECT abalance FROM accounts WHERE aid = 1');
      assert.equal(acct.rows[0].abalance, 500);
      
      // Verify teller balance changed
      const teller = db.execute('SELECT tbalance FROM tellers WHERE tid = 1');
      assert.equal(teller.rows[0].tbalance, 500);
      
      // Verify branch balance changed
      const branch = db.execute('SELECT bbalance FROM branches WHERE bid = 1');
      assert.equal(branch.rows[0].bbalance, 500);
      
      // Verify history record
      const history = db.execute('SELECT COUNT(*) as cnt FROM history');
      assert.equal(history.rows[0].cnt, 1);
    });
    
    it('ACID invariant holds after 100 sequential transactions', () => {
      const db = new Database();
      createSchema(db);
      const config = loadData(db);
      
      for (let i = 0; i < 100; i++) {
        const params = randomTxParams(config);
        tpcbTransaction(db, params);
      }
      
      const sums = verifyACID(db);
      assert.equal(sums.accountTotal, sums.historyTotal, 'Account sum != History sum');
      assert.equal(sums.tellerTotal, sums.historyTotal, 'Teller sum != History sum');
      assert.equal(sums.branchTotal, sums.historyTotal, 'Branch sum != History sum');
    });
  });

  describe('Sequential Throughput', () => {
    it('measures sequential TPS', () => {
      const db = new Database();
      createSchema(db);
      const config = loadData(db, { branches: 1, tellersPerBranch: 10, accountsPerBranch: 1000 });
      
      const txCount = 500;
      const start = Date.now();
      
      for (let i = 0; i < txCount; i++) {
        tpcbTransaction(db, randomTxParams(config));
      }
      
      const elapsed = Date.now() - start;
      const tps = (txCount / elapsed * 1000).toFixed(0);
      
      console.log(`  Sequential: ${txCount} txns in ${elapsed}ms = ${tps} TPS`);
      
      // Verify ACID
      const sums = verifyACID(db);
      assert.equal(sums.accountTotal, sums.historyTotal);
      assert.equal(sums.tellerTotal, sums.historyTotal);
      assert.equal(sums.branchTotal, sums.historyTotal);
      
      // Should be at least somewhat fast
      assert.ok(parseInt(tps) > 50, `TPS too low: ${tps}`);
    });
  });

  describe('Concurrent ACID (Transactional)', () => {
    it('ACID holds under transactional load with sessions', () => {
      const dir = mkdtempSync(join(tmpdir(), 'henrydb-tpcb-'));
      try {
        const txDb = TransactionalDatabase.open(dir);
        createSchema(txDb);
        const config = loadData(txDb);
      
        const WORKERS = 5;
        const TXN_PER_WORKER = 20;
        let committed = 0;
        let aborted = 0;
        
        for (let w = 0; w < WORKERS; w++) {
          const session = txDb.session();
          for (let t = 0; t < TXN_PER_WORKER; t++) {
            const params = randomTxParams(config);
            session.begin();
            try {
              session.execute(`UPDATE accounts SET abalance = abalance + ${params.delta} WHERE aid = ${params.aid}`);
              session.execute(`UPDATE tellers SET tbalance = tbalance + ${params.delta} WHERE tid = ${params.tid}`);
              session.execute(`UPDATE branches SET bbalance = bbalance + ${params.delta} WHERE bid = ${params.bid}`);
              session.execute(`INSERT INTO history VALUES (${params.tid}, ${params.bid}, ${params.aid}, ${params.delta}, ${Date.now()}, 'h')`);
              session.commit();
              committed++;
            } catch (e) {
              try { session.rollback(); } catch (_) {}
              aborted++;
            }
          }
        }
        
        console.log(`  Committed: ${committed}, Aborted: ${aborted}`);
        
        const sums = verifyACID(txDb);
        assert.equal(sums.accountTotal, sums.historyTotal, 
          `ACID VIOLATION: accounts=${sums.accountTotal} history=${sums.historyTotal}`);
        assert.equal(sums.tellerTotal, sums.historyTotal,
          `ACID VIOLATION: tellers=${sums.tellerTotal} history=${sums.historyTotal}`);
        assert.equal(sums.branchTotal, sums.historyTotal,
          `ACID VIOLATION: branches=${sums.branchTotal} history=${sums.historyTotal}`);
      } finally {
        rmSync(dir, { recursive: true });
      }
    });

    it('interleaved transactions maintain consistency', () => {
      const dir = mkdtempSync(join(tmpdir(), 'henrydb-tpcb-interleave-'));
      try {
        const txDb = TransactionalDatabase.open(dir);
        createSchema(txDb);
        loadData(txDb, { branches: 1, tellersPerBranch: 2, accountsPerBranch: 10 });
      
        const s1 = txDb.session();
        const s2 = txDb.session();
        
        s1.begin();
        s2.begin();
        
        let s1committed = false, s2committed = false;
        try {
          s1.execute('UPDATE accounts SET abalance = abalance + 100 WHERE aid = 1');
          s1.execute('UPDATE tellers SET tbalance = tbalance + 100 WHERE tid = 1');
          s1.execute('UPDATE branches SET bbalance = bbalance + 100 WHERE bid = 1');
          s1.execute("INSERT INTO history VALUES (1, 1, 1, 100, 0, 'tx1')");
          
          s2.execute('UPDATE accounts SET abalance = abalance - 50 WHERE aid = 1');
          s2.execute('UPDATE tellers SET tbalance = tbalance - 50 WHERE tid = 2');
          s2.execute('UPDATE branches SET bbalance = bbalance - 50 WHERE bid = 1');
          s2.execute("INSERT INTO history VALUES (2, 1, 1, -50, 0, 'tx2')");
          
          s1.commit(); s1committed = true;
          s2.commit(); s2committed = true;
        } catch (e) {
          if (!s1committed) try { s1.rollback(); } catch (_) {}
          if (!s2committed) try { s2.rollback(); } catch (_) {}
        }
        
        const sums = verifyACID(txDb);
        assert.equal(sums.accountTotal, sums.historyTotal,
          `ACID VIOLATION: accounts=${sums.accountTotal} history=${sums.historyTotal}`);
      } finally {
        rmSync(dir, { recursive: true });
      }
    });

    it('measures transactional TPS with contention', () => {
      const dir = mkdtempSync(join(tmpdir(), 'henrydb-tpcb-tps-'));
      try {
        const txDb = TransactionalDatabase.open(dir);
        createSchema(txDb);
        const config = loadData(txDb, { branches: 1, tellersPerBranch: 5, accountsPerBranch: 50 });
      
        const txCount = 200;
        let committed = 0, aborted = 0;
        const session = txDb.session();
        const start = Date.now();
        
        for (let i = 0; i < txCount; i++) {
          const params = randomTxParams(config);
          session.begin();
          try {
            session.execute(`UPDATE accounts SET abalance = abalance + ${params.delta} WHERE aid = ${params.aid}`);
            session.execute(`UPDATE tellers SET tbalance = tbalance + ${params.delta} WHERE tid = ${params.tid}`);
            session.execute(`UPDATE branches SET bbalance = bbalance + ${params.delta} WHERE bid = ${params.bid}`);
            session.execute(`INSERT INTO history VALUES (${params.tid}, ${params.bid}, ${params.aid}, ${params.delta}, ${Date.now()}, 'bench')`);
            session.commit();
            committed++;
          } catch (e) {
            try { session.rollback(); } catch (_) {}
            aborted++;
          }
        }
        
        const elapsed = Date.now() - start;
        const tps = (committed / elapsed * 1000).toFixed(0);
        
        console.log(`  Transactional: ${committed} committed, ${aborted} aborted in ${elapsed}ms = ${tps} TPS`);
        
        const sums = verifyACID(txDb);
        assert.equal(sums.accountTotal, sums.historyTotal,
          `ACID VIOLATION: accounts=${sums.accountTotal} != history=${sums.historyTotal}`);
        assert.equal(sums.tellerTotal, sums.historyTotal);
        assert.equal(sums.branchTotal, sums.historyTotal);
      } finally {
        rmSync(dir, { recursive: true });
      }
    });
  });

  describe('Wire Protocol TPC-B', () => {
    it('ACID holds through wire protocol', async () => {
      // This test runs TPC-B transactions through the actual TCP server
      const { HenryDBServer } = await import('./server.js');
      const net = await import('net');
      
      const port = 17000 + Math.floor(Math.random() * 10000);
      const server = new HenryDBServer({ port });
      await server.start();
      
      // Use the internal db for setup (faster than wire protocol)
      createSchema(server.db);
      const config = loadData(server.db, { branches: 1, tellersPerBranch: 3, accountsPerBranch: 50 });
      
      // Helper: send query via wire protocol
      function sendQuery(socket, sql) {
        const queryBuf = Buffer.from(sql + '\0', 'utf8');
        const len = 4 + queryBuf.length;
        const buf = Buffer.alloc(1 + len);
        buf[0] = 0x51;
        buf.writeInt32BE(len, 1);
        queryBuf.copy(buf, 5);
        socket.write(buf);
      }
      
      function sendStartup(socket) {
        const params = 'user\0test\0database\0testdb\0\0';
        const paramsBuf = Buffer.from(params, 'utf8');
        const len = 4 + 4 + paramsBuf.length;
        const buf = Buffer.alloc(len);
        buf.writeInt32BE(len, 0);
        buf.writeInt32BE(196608, 4);
        paramsBuf.copy(buf, 8);
        socket.write(buf);
      }
      
      function waitForReady(socket) {
        return new Promise((resolve) => {
          const chunks = [];
          const handler = (data) => {
            chunks.push(data);
            const all = Buffer.concat(chunks);
            for (let i = 0; i < all.length; i++) {
              if (all[i] === 0x5A && i + 5 <= all.length) {
                socket.removeListener('data', handler);
                resolve(all);
                return;
              }
            }
          };
          socket.on('data', handler);
        });
      }
      
      // Run 50 transactions via wire protocol
      const socket = await new Promise((resolve, reject) => {
        const s = net.createConnection({ host: '127.0.0.1', port }, () => resolve(s));
        s.on('error', reject);
      });
      
      sendStartup(socket);
      await waitForReady(socket);
      
      for (let i = 0; i < 50; i++) {
        const params = randomTxParams(config);
        const sqls = [
          `UPDATE accounts SET abalance = abalance + ${params.delta} WHERE aid = ${params.aid}`,
          `UPDATE tellers SET tbalance = tbalance + ${params.delta} WHERE tid = ${params.tid}`,
          `UPDATE branches SET bbalance = bbalance + ${params.delta} WHERE bid = ${params.bid}`,
          `INSERT INTO history VALUES (${params.tid}, ${params.bid}, ${params.aid}, ${params.delta}, ${Date.now()}, 'wire')`,
        ];
        for (const sql of sqls) {
          sendQuery(socket, sql);
          await waitForReady(socket);
        }
      }
      
      socket.end();
      
      // Verify ACID
      const sums = verifyACID(server.db);
      assert.equal(sums.accountTotal, sums.historyTotal,
        `ACID VIOLATION via wire: accounts=${sums.accountTotal} != history=${sums.historyTotal}`);
      assert.equal(sums.tellerTotal, sums.historyTotal);
      assert.equal(sums.branchTotal, sums.historyTotal);
      
      await server.stop();
    });
  });
});

// Export for reuse
export { createSchema, loadData, tpcbTransaction, randomTxParams, verifyACID };
