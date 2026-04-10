// tpcb-persistent.test.js — TPC-B benchmark with persistent storage + crash recovery
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync, existsSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { HenryDBServer } from './server.js';
import pg from 'pg';

const { Client } = pg;

function getPort() {
  return 18000 + Math.floor(Math.random() * 10000);
}

async function connect(port) {
  const client = new Client({ host: '127.0.0.1', port, user: 'test', database: 'testdb' });
  await client.connect();
  return client;
}

function randomDelta() {
  return Math.floor(Math.random() * 20001) - 10000;
}

describe('TPC-B Persistent Benchmark', () => {
  
  it('ACID holds across persistent server restarts', async () => {
    const port = getPort();
    const dir = mkdtempSync(join(tmpdir(), 'henrydb-tpcb-persist-'));
    
    try {
      // Session 1: Create schema and run 50 transactions
      const s1 = new HenryDBServer({ port, dataDir: dir });
      await s1.start();
      const c1 = await connect(port);
      
      await c1.query('CREATE TABLE branches (bid INT PRIMARY KEY, bbalance INT)');
      await c1.query('CREATE TABLE tellers (tid INT PRIMARY KEY, bid INT, tbalance INT)');
      await c1.query('CREATE TABLE accounts (aid INT PRIMARY KEY, bid INT, abalance INT)');
      await c1.query('CREATE TABLE history (tid INT, bid INT, aid INT, delta INT)');
      
      await c1.query('INSERT INTO branches VALUES (1, 0)');
      for (let t = 1; t <= 5; t++) await c1.query(`INSERT INTO tellers VALUES (${t}, 1, 0)`);
      for (let a = 1; a <= 50; a++) await c1.query(`INSERT INTO accounts VALUES (${a}, 1, 0)`);
      
      // Run 50 TPC-B transactions
      for (let i = 0; i < 50; i++) {
        const aid = 1 + Math.floor(Math.random() * 50);
        const tid = 1 + Math.floor(Math.random() * 5);
        const delta = randomDelta();
        await c1.query(`UPDATE accounts SET abalance = abalance + ${delta} WHERE aid = ${aid}`);
        await c1.query(`UPDATE tellers SET tbalance = tbalance + ${delta} WHERE tid = ${tid}`);
        await c1.query(`UPDATE branches SET bbalance = bbalance + ${delta} WHERE bid = 1`);
        await c1.query(`INSERT INTO history VALUES (${tid}, 1, ${aid}, ${delta})`);
      }
      
      // Check ACID before restart
      const sums1 = await getACIDSums(c1);
      assert.equal(sums1.account, sums1.history, `Pre-restart ACID fail: acct=${sums1.account} hist=${sums1.history}`);
      
      await c1.end();
      await s1.stop();
      
      // Session 2: Restart and verify ACID still holds
      const s2 = new HenryDBServer({ port, dataDir: dir });
      await s2.start();
      const c2 = await connect(port);
      
      const sums2 = await getACIDSums(c2);
      assert.equal(sums2.account, sums2.history, `Post-restart ACID fail: acct=${sums2.account} hist=${sums2.history}`);
      assert.equal(sums2.teller, sums2.history, `Post-restart teller fail: teller=${sums2.teller} hist=${sums2.history}`);
      assert.equal(sums2.branch, sums2.history, `Post-restart branch fail: branch=${sums2.branch} hist=${sums2.history}`);
      
      // Run 50 more transactions after restart
      for (let i = 0; i < 50; i++) {
        const aid = 1 + Math.floor(Math.random() * 50);
        const tid = 1 + Math.floor(Math.random() * 5);
        const delta = randomDelta();
        await c2.query(`UPDATE accounts SET abalance = abalance + ${delta} WHERE aid = ${aid}`);
        await c2.query(`UPDATE tellers SET tbalance = tbalance + ${delta} WHERE tid = ${tid}`);
        await c2.query(`UPDATE branches SET bbalance = bbalance + ${delta} WHERE bid = 1`);
        await c2.query(`INSERT INTO history VALUES (${tid}, 1, ${aid}, ${delta})`);
      }
      
      const sums3 = await getACIDSums(c2);
      assert.equal(sums3.account, sums3.history, `Post-2nd-batch ACID fail: acct=${sums3.account} hist=${sums3.history}`);
      
      // Verify row count accumulated correctly
      const histCount = await c2.query('SELECT COUNT(*) as cnt FROM history');
      assert.equal(String(histCount.rows[0].cnt), '100');
      
      await c2.end();
      await s2.stop();
    } finally {
      rmSync(dir, { recursive: true });
    }
  });

  it('persistent TPC-B throughput measurement', async () => {
    const port = getPort();
    const dir = mkdtempSync(join(tmpdir(), 'henrydb-tpcb-tps-'));
    
    try {
      const server = new HenryDBServer({ port, dataDir: dir });
      await server.start();
      const client = await connect(port);
      
      // Setup
      await client.query('CREATE TABLE branches (bid INT PRIMARY KEY, bbalance INT)');
      await client.query('CREATE TABLE tellers (tid INT PRIMARY KEY, bid INT, tbalance INT)');
      await client.query('CREATE TABLE accounts (aid INT PRIMARY KEY, bid INT, abalance INT)');
      await client.query('CREATE TABLE history (tid INT, bid INT, aid INT, delta INT)');
      
      await client.query('INSERT INTO branches VALUES (1, 0)');
      for (let t = 1; t <= 10; t++) await client.query(`INSERT INTO tellers VALUES (${t}, 1, 0)`);
      for (let a = 1; a <= 200; a++) await client.query(`INSERT INTO accounts VALUES (${a}, 1, 0)`);
      
      // Benchmark: 100 TPC-B transactions
      const txCount = 100;
      const start = Date.now();
      
      for (let i = 0; i < txCount; i++) {
        const aid = 1 + Math.floor(Math.random() * 200);
        const tid = 1 + Math.floor(Math.random() * 10);
        const delta = randomDelta();
        await client.query(`UPDATE accounts SET abalance = abalance + ${delta} WHERE aid = ${aid}`);
        await client.query(`UPDATE tellers SET tbalance = tbalance + ${delta} WHERE tid = ${tid}`);
        await client.query(`UPDATE branches SET bbalance = bbalance + ${delta} WHERE bid = 1`);
        await client.query(`INSERT INTO history VALUES (${tid}, 1, ${aid}, ${delta})`);
      }
      
      const elapsed = Date.now() - start;
      const tps = (txCount / elapsed * 1000).toFixed(0);
      console.log(`  Persistent TPC-B: ${txCount} txns in ${elapsed}ms = ${tps} TPS (via pg client + disk)`);
      
      // Verify ACID
      const sums = await getACIDSums(client);
      assert.equal(sums.account, sums.history, `ACID fail: acct=${sums.account} hist=${sums.history}`);
      
      await client.end();
      await server.stop();
    } finally {
      rmSync(dir, { recursive: true });
    }
  });
});

async function getACIDSums(client) {
  const [acct, teller, branch, hist] = await Promise.all([
    client.query('SELECT SUM(abalance) as total FROM accounts'),
    client.query('SELECT SUM(tbalance) as total FROM tellers'),
    client.query('SELECT SUM(bbalance) as total FROM branches'),
    client.query('SELECT SUM(delta) as total FROM history'),
  ]);
  return {
    account: String(acct.rows[0].total || 0),
    teller: String(teller.rows[0].total || 0),
    branch: String(branch.rows[0].total || 0),
    history: String(hist.rows[0].total || 0),
  };
}
