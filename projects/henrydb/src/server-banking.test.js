// server-banking.test.js — Banking/financial data patterns
import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import pg from 'pg';
import { HenryDBServer } from './server.js';

const { Client } = pg;
const PORT = 15581;

describe('Banking & Finance', () => {
  let server;

  before(async () => {
    server = new HenryDBServer({ port: PORT });
    await server.start();

    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();
    
    await client.query('CREATE TABLE accounts (id INTEGER, holder TEXT, type TEXT, balance REAL, currency TEXT, opened_at TEXT)');
    await client.query('CREATE TABLE transactions (id INTEGER, from_acct INTEGER, to_acct INTEGER, amount REAL, type TEXT, description TEXT, ts TEXT)');
    
    await client.query("INSERT INTO accounts VALUES (1, 'Alice', 'checking', 5000.00, 'USD', '2025-01-01')");
    await client.query("INSERT INTO accounts VALUES (2, 'Bob', 'savings', 15000.00, 'USD', '2025-03-15')");
    await client.query("INSERT INTO accounts VALUES (3, 'Charlie', 'checking', 2500.00, 'USD', '2025-06-01')");
    await client.query("INSERT INTO accounts VALUES (4, 'Alice', 'savings', 25000.00, 'USD', '2025-01-01')");
    
    // Transactions
    await client.query("INSERT INTO transactions VALUES (1, 1, 2, 500.00, 'transfer', 'Monthly savings', '2026-04-01')");
    await client.query("INSERT INTO transactions VALUES (2, 3, 1, 250.00, 'transfer', 'Rent payment', '2026-04-02')");
    await client.query("INSERT INTO transactions VALUES (3, NULL, 1, 3000.00, 'deposit', 'Salary', '2026-04-03')");
    await client.query("INSERT INTO transactions VALUES (4, 2, NULL, 1000.00, 'withdrawal', 'Cash withdrawal', '2026-04-04')");
    await client.query("INSERT INTO transactions VALUES (5, 1, 3, 100.00, 'transfer', 'Dinner split', '2026-04-05')");
    
    await client.end();
  });

  after(async () => {
    await server.stop();
  });

  it('account balances', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query('SELECT holder, type, balance FROM accounts ORDER BY balance DESC');
    assert.strictEqual(result.rows.length, 4);

    await client.end();
  });

  it('transaction history for account', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query(
      'SELECT t.type, t.amount, t.description, t.ts FROM transactions t WHERE t.from_acct = 1 OR t.to_acct = 1 ORDER BY t.ts'
    );
    assert.ok(result.rows.length >= 3);

    await client.end();
  });

  it('total deposits and withdrawals', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query(
      "SELECT type, SUM(amount) AS total, COUNT(*) AS count FROM transactions GROUP BY type ORDER BY total DESC"
    );
    assert.ok(result.rows.length >= 3);

    await client.end();
  });

  it('customer total assets', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query(
      'SELECT holder, SUM(balance) AS total_assets, COUNT(*) AS num_accounts FROM accounts GROUP BY holder ORDER BY total_assets DESC'
    );
    assert.ok(result.rows.length >= 3);
    assert.strictEqual(result.rows[0].holder, 'Alice'); // Highest total

    await client.end();
  });

  it('transfer between accounts', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    // Simulate a transfer
    await client.query('BEGIN');
    await client.query('UPDATE accounts SET balance = balance - 200 WHERE id = 1');
    await client.query('UPDATE accounts SET balance = balance + 200 WHERE id = 3');
    await client.query("INSERT INTO transactions VALUES (6, 1, 3, 200.00, 'transfer', 'Gift', '2026-04-06')");
    await client.query('COMMIT');

    // Verify
    const result = await client.query('SELECT * FROM transactions WHERE id = 6');
    assert.strictEqual(result.rows.length, 1);

    await client.end();
  });
});
