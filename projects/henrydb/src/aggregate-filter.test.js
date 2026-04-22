// aggregate-filter.test.js — Aggregate FILTER clause tests
import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Aggregate FILTER clause', () => {
  let db;
  
  beforeEach(() => {
    db = new Database();
    db.execute('CREATE TABLE sales (id INT, amount DECIMAL, region TEXT, status TEXT, product TEXT)');
    db.execute("INSERT INTO sales VALUES (1,100,'East','shipped','A'),(2,200,'West','pending','B'),(3,150,'East','shipped','A'),(4,50,'West','cancelled','C'),(5,300,'East','shipped','B'),(6,75,'West','shipped','A'),(7,400,'East','pending','C'),(8,25,'West','shipped','B')");
  });

  it('COUNT with FILTER', () => {
    const r = db.execute("SELECT COUNT(*) FILTER (WHERE status = 'shipped') as shipped, COUNT(*) as total FROM sales");
    assert.equal(r.rows[0].shipped, 5);
    assert.equal(r.rows[0].total, 8);
  });

  it('SUM with FILTER', () => {
    const r = db.execute("SELECT SUM(amount) FILTER (WHERE status = 'shipped') as shipped_total, SUM(amount) as total FROM sales");
    assert.equal(r.rows[0].shipped_total, 650);
    assert.equal(r.rows[0].total, 1300);
  });

  it('AVG with FILTER', () => {
    const r = db.execute("SELECT AVG(amount) FILTER (WHERE status = 'shipped') as avg_shipped FROM sales");
    assert.equal(r.rows[0].avg_shipped, 130); // (100+150+300+75+25)/5 = 650/5 = 130
  });

  it('MIN/MAX with FILTER', () => {
    const r = db.execute("SELECT MIN(amount) FILTER (WHERE region = 'East') as min_east, MAX(amount) FILTER (WHERE region = 'West') as max_west FROM sales");
    assert.equal(r.rows[0].min_east, 100);
    assert.equal(r.rows[0].max_west, 200);
  });

  it('FILTER with GROUP BY', () => {
    const r = db.execute(`
      SELECT region, 
        COUNT(*) FILTER (WHERE status = 'shipped') as shipped_count,
        COUNT(*) as total_count,
        SUM(amount) FILTER (WHERE status = 'shipped') as shipped_total
      FROM sales GROUP BY region ORDER BY region
    `);
    assert.equal(r.rows.length, 2);
    assert.equal(r.rows[0].region, 'East');
    assert.equal(r.rows[0].shipped_count, 3);
    assert.equal(r.rows[0].total_count, 4);
    assert.equal(r.rows[0].shipped_total, 550);
    assert.equal(r.rows[1].region, 'West');
    assert.equal(r.rows[1].shipped_count, 2);
  });

  it('FILTER with complex condition', () => {
    const r = db.execute("SELECT COUNT(*) FILTER (WHERE amount > 100 AND status = 'shipped') as big_shipped FROM sales");
    assert.equal(r.rows[0].big_shipped, 2); // 150+300
  });

  it('FILTER with empty result', () => {
    const r = db.execute("SELECT COUNT(*) FILTER (WHERE amount > 9999) as none FROM sales");
    assert.equal(r.rows[0].none, 0);
  });

  it('SUM FILTER on empty match returns null', () => {
    const r = db.execute("SELECT SUM(amount) FILTER (WHERE amount > 9999) as none FROM sales");
    assert.equal(r.rows[0].none, null);
  });

  it('multiple different FILTERs', () => {
    const r = db.execute(`
      SELECT 
        COUNT(*) FILTER (WHERE region = 'East') as east_count,
        COUNT(*) FILTER (WHERE region = 'West') as west_count,
        SUM(amount) FILTER (WHERE status = 'shipped') as shipped_sum,
        SUM(amount) FILTER (WHERE status = 'pending') as pending_sum
      FROM sales
    `);
    assert.equal(r.rows[0].east_count, 4);
    assert.equal(r.rows[0].west_count, 4);
    assert.equal(r.rows[0].shipped_sum, 650);
    assert.equal(r.rows[0].pending_sum, 600);
  });

  it('Volcano vs Legacy parity', () => {
    const sql = `SELECT region, COUNT(*) FILTER (WHERE status = 'shipped') as cnt FROM sales GROUP BY region ORDER BY region`;
    const volcanoResult = db.execute(sql);
    db._useVolcano = false;
    const legacyResult = db.execute(sql);
    
    assert.equal(volcanoResult.rows.length, legacyResult.rows.length);
    for (let i = 0; i < volcanoResult.rows.length; i++) {
      assert.equal(volcanoResult.rows[i].region, legacyResult.rows[i].region);
      assert.equal(volcanoResult.rows[i].cnt, legacyResult.rows[i].cnt);
    }
  });
});
