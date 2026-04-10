// analytics-timeseries.test.js — Realistic time-series analytics
import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import pg from 'pg';
import { HenryDBServer } from './server.js';

const { Client } = pg;

describe('Time-Series Analytics', () => {
  let server, port, c;
  
  before(async () => {
    port = 36500 + Math.floor(Math.random() * 2000);
    server = new HenryDBServer({ port });
    await server.start();
    c = new Client({ host: '127.0.0.1', port, user: 'test', database: 'testdb' });
    await c.connect();
    
    // Stock prices table
    await c.query('CREATE TABLE prices (day INT, symbol TEXT, close_price INT, volume INT)');
    
    // AAPL prices
    const aapl = [150, 152, 148, 155, 157, 153, 160, 162, 158, 165];
    for (let i = 0; i < aapl.length; i++) {
      await c.query('INSERT INTO prices VALUES ($1, $2, $3, $4)', [i+1, 'AAPL', aapl[i], 1000000 + i * 100000]);
    }
    
    // GOOG prices  
    const goog = [100, 103, 99, 105, 108, 104, 110, 112, 109, 115];
    for (let i = 0; i < goog.length; i++) {
      await c.query('INSERT INTO prices VALUES ($1, $2, $3, $4)', [i+1, 'GOOG', goog[i], 500000 + i * 50000]);
    }
  });
  
  after(async () => {
    if (c) await c.end();
    if (server) await server.stop();
  });

  it('daily returns using LAG', async () => {
    const r = await c.query(`
      SELECT day, symbol, close_price,
        LAG(close_price) OVER (PARTITION BY symbol ORDER BY day) as prev_close
      FROM prices
      WHERE symbol = 'AAPL'
      ORDER BY day
    `);
    assert.equal(r.rows.length, 10);
    assert.equal(r.rows[0].prev_close, null); // First day has no previous
    assert.equal(String(r.rows[1].prev_close), '150'); // Day 2's prev = day 1
  });

  it('cumulative volume using SUM OVER', async () => {
    const r = await c.query(`
      SELECT day, symbol, volume,
        SUM(volume) OVER (PARTITION BY symbol ORDER BY day) as cumulative_vol
      FROM prices
      WHERE symbol = 'AAPL'
      ORDER BY day
    `);
    assert.equal(r.rows.length, 10);
    // First day: cumulative = volume itself
    assert.equal(String(r.rows[0].cumulative_vol), String(r.rows[0].volume));
  });

  it('ranking stocks by closing price', async () => {
    const r = await c.query(`
      SELECT day, symbol, close_price,
        RANK() OVER (PARTITION BY day ORDER BY close_price DESC) as price_rank
      FROM prices
      WHERE day = 10
    `);
    assert.equal(r.rows.length, 2);
    // AAPL (165) should rank higher than GOOG (115)
    const aapl = r.rows.find(r => r.symbol === 'AAPL');
    assert.equal(String(aapl.price_rank), '1');
  });

  it('total and average price per symbol', async () => {
    const r = await c.query(`
      SELECT symbol,
        SUM(close_price) as total_price,
        AVG(close_price) as avg_price,
        MIN(close_price) as min_price,
        MAX(close_price) as max_price,
        COUNT(*) as trading_days
      FROM prices
      GROUP BY symbol
      ORDER BY symbol
    `);
    assert.equal(r.rows.length, 2);
    assert.equal(r.rows[0].symbol, 'AAPL');
    assert.equal(String(r.rows[0].trading_days), '10');
    assert.equal(String(r.rows[0].min_price), '148');
    assert.equal(String(r.rows[0].max_price), '165');
  });

  it('above-average days using subquery', async () => {
    const r = await c.query(`
      SELECT day, close_price
      FROM prices
      WHERE symbol = 'AAPL'
        AND close_price > (SELECT AVG(close_price) FROM prices WHERE symbol = 'AAPL')
      ORDER BY day
    `);
    assert.ok(r.rows.length > 0);
    assert.ok(r.rows.length < 10);
  });

  it('day-over-day with LEAD', async () => {
    const r = await c.query(`
      SELECT day, close_price,
        LEAD(close_price) OVER (ORDER BY day) as next_close
      FROM prices
      WHERE symbol = 'GOOG'
      ORDER BY day
    `);
    assert.equal(r.rows.length, 10);
    assert.equal(r.rows[9].next_close, null); // Last day has no next
    assert.equal(String(r.rows[0].next_close), '103'); // Day 1's next = day 2
  });

  it('ROW_NUMBER for pagination', async () => {
    const r = await c.query(`
      SELECT * FROM (
        SELECT day, symbol, close_price,
          ROW_NUMBER() OVER (ORDER BY close_price DESC) as rn
        FROM prices
      ) ranked
      WHERE rn <= 5
    `);
    assert.equal(r.rows.length, 5);
  });

  it('GENERATE_SERIES for gap analysis', async () => {
    const r = await c.query('SELECT * FROM GENERATE_SERIES(1, 10) g');
    assert.equal(r.rows.length, 10);
  });
});
