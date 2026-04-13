// json-combined.test.js — JSON + GROUP BY + HAVING + ORDER BY combined tests
import { describe, it, before } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('JSON combined with SQL features', () => {
  let db;
  before(() => {
    db = new Database();
    db.execute('CREATE TABLE logs (id INT, metadata TEXT, severity TEXT)');
    db.execute(`INSERT INTO logs VALUES (1, '{"service": "auth", "latency": 50, "status": 200}', 'info')`);
    db.execute(`INSERT INTO logs VALUES (2, '{"service": "auth", "latency": 150, "status": 500}', 'error')`);
    db.execute(`INSERT INTO logs VALUES (3, '{"service": "api", "latency": 30, "status": 200}', 'info')`);
    db.execute(`INSERT INTO logs VALUES (4, '{"service": "api", "latency": 200, "status": 503}', 'error')`);
    db.execute(`INSERT INTO logs VALUES (5, '{"service": "auth", "latency": 80, "status": 200}', 'info')`);
    db.execute(`INSERT INTO logs VALUES (6, '{"service": "worker", "latency": 10, "status": 200}', 'info')`);
    db.execute(`INSERT INTO logs VALUES (7, '{"service": "api", "latency": 45, "status": 200}', 'info')`);
    db.execute(`INSERT INTO logs VALUES (8, '{"service": "auth", "latency": 300, "status": 500}', 'error')`);
  });

  it('GROUP BY extracted JSON service name', () => {
    const r = db.execute(`
      SELECT JSON_EXTRACT(metadata, '$.service') AS service, COUNT(*) AS cnt
      FROM logs
      GROUP BY service
      ORDER BY cnt DESC
    `);
    assert.equal(r.rows.length, 3);
    assert.equal(r.rows[0].service, 'auth');
    assert.equal(r.rows[0].cnt, 4);
  });

  it('GROUP BY + HAVING with JSON data', () => {
    const r = db.execute(`
      SELECT JSON_EXTRACT(metadata, '$.service') AS service, COUNT(*) AS error_count
      FROM logs
      WHERE JSON_EXTRACT(metadata, '$.status') > 400
      GROUP BY service
      HAVING COUNT(*) > 1
      ORDER BY error_count DESC
    `);
    assert.equal(r.rows.length, 1);
    assert.equal(r.rows[0].service, 'auth');
    assert.equal(r.rows[0].error_count, 2);
  });

  it('AVG of JSON numeric field by group', () => {
    const r = db.execute(`
      SELECT JSON_EXTRACT(metadata, '$.service') AS service,
             AVG(JSON_EXTRACT(metadata, '$.latency')) AS avg_latency
      FROM logs
      GROUP BY service
      ORDER BY avg_latency DESC
    `);
    assert.equal(r.rows.length, 3);
    // auth: (50+150+80+300)/4 = 145
    const auth = r.rows.find(r => r.service === 'auth');
    assert.ok(Math.abs(auth.avg_latency - 145) < 0.1);
  });

  it('ORDER BY JSON extracted numeric field', () => {
    const r = db.execute(`
      SELECT id, JSON_EXTRACT(metadata, '$.latency') AS latency
      FROM logs
      ORDER BY latency DESC
      LIMIT 3
    `);
    assert.equal(r.rows.length, 3);
    assert.equal(r.rows[0].latency, 300);
  });

  it('JSON_EXTRACT in subquery', () => {
    const r = db.execute(`
      SELECT id, JSON_EXTRACT(metadata, '$.service') AS service
      FROM logs
      WHERE JSON_EXTRACT(metadata, '$.latency') > (
        SELECT AVG(JSON_EXTRACT(metadata, '$.latency')) FROM logs
      )
    `);
    // Avg latency: (50+150+30+200+80+10+45+300)/8 = 108.125
    // Rows with latency > 108: id 2 (150), 4 (200), 8 (300)
    assert.equal(r.rows.length, 3);
  });

  it('JSON with CTE', () => {
    const r = db.execute(`
      WITH error_logs AS (
        SELECT JSON_EXTRACT(metadata, '$.service') AS service,
               JSON_EXTRACT(metadata, '$.status') AS status
        FROM logs
        WHERE severity = 'error'
      )
      SELECT service, COUNT(*) AS cnt FROM error_logs GROUP BY service ORDER BY cnt DESC
    `);
    assert.ok(r.rows.length >= 1);
    assert.equal(r.rows[0].service, 'auth');
  });

  it('JSON in CASE expression', () => {
    const r = db.execute(`
      SELECT id,
        CASE
          WHEN JSON_EXTRACT(metadata, '$.status') < 300 THEN 'success'
          WHEN JSON_EXTRACT(metadata, '$.status') < 500 THEN 'redirect'
          ELSE 'error'
        END AS status_class
      FROM logs
      ORDER BY id
    `);
    assert.equal(r.rows.length, 8);
    assert.equal(r.rows[0].status_class, 'success'); // id 1: 200
    assert.equal(r.rows[1].status_class, 'error');   // id 2: 500
  });

  it('JSON_OBJECT construction from query', () => {
    const r = db.execute(`
      SELECT JSON_OBJECT(
        'service', JSON_EXTRACT(metadata, '$.service'),
        'status', JSON_EXTRACT(metadata, '$.status')
      ) AS summary
      FROM logs
      WHERE id = 1
    `);
    const parsed = JSON.parse(r.rows[0].summary);
    assert.equal(parsed.service, 'auth');
    assert.equal(parsed.status, 200);
  });

  it('multiple JSON functions combined', () => {
    const r = db.execute(`
      SELECT 
        JSON_EXTRACT(metadata, '$.service') AS service,
        JSON_TYPE(metadata) AS json_type,
        JSON_VALID(metadata) AS is_valid,
        JSON_EXTRACT(metadata, '$.latency') AS latency
      FROM logs
      WHERE id = 1
    `);
    assert.equal(r.rows[0].service, 'auth');
    assert.equal(r.rows[0].json_type, 'object');
    assert.equal(r.rows[0].is_valid, 1);
    assert.equal(r.rows[0].latency, 50);
  });
});
