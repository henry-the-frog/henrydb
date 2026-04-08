// predicate-pushdown.test.js — Tests for predicate pushdown with zone maps
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { PushdownScanner } from './predicate-pushdown.js';
import { ZoneMapIndex } from './zone-maps.js';

function setupData(n = 10000) {
  const data = { id: [], score: [], region: [], amount: [] };
  const schema = [
    { name: 'id', type: 'INT' },
    { name: 'score', type: 'INT' },
    { name: 'region', type: 'TEXT' },
    { name: 'amount', type: 'INT' },
  ];
  const zmIdx = new ZoneMapIndex(schema, 1000);

  for (let i = 0; i < n; i++) {
    const row = { id: i, score: i * 7 % 1000, region: ['US', 'EU', 'APAC'][i % 3], amount: (i * 13 + 50) % 5000 };
    data.id.push(row.id);
    data.score.push(row.score);
    data.region.push(row.region);
    data.amount.push(row.amount);
    zmIdx.addRow(row);
  }

  return { data, schema, zmIdx };
}

describe('PushdownScanner', () => {

  it('single predicate with zone map skip', () => {
    const { data, schema, zmIdx } = setupData(10000);
    const scanner = new PushdownScanner(data, schema, zmIdx);

    const rows = scanner.scan({ column: 'id', op: 'GT', value: 9000 }, ['id', 'score']);
    assert.equal(rows.length, 999); // 9001..9999
    assert.ok(rows.every(r => r.id > 9000));

    const stats = scanner.getStats();
    assert.ok(stats.skippedPages > 0, 'Should skip pages');
    assert.ok(stats.scannedRows < 10000, 'Should scan fewer rows than total');
  });

  it('multi-predicate pushdown', () => {
    const { data, schema, zmIdx } = setupData(10000);
    const scanner = new PushdownScanner(data, schema, zmIdx);

    const rows = scanner.scanMulti(
      [
        { column: 'id', op: 'GT', value: 5000 },
        { column: 'id', op: 'LT', value: 6000 },
      ],
      ['id', 'score'],
    );

    assert.equal(rows.length, 999); // 5001..5999
    assert.ok(rows.every(r => r.id > 5000 && r.id < 6000));
  });

  it('limit works', () => {
    const { data, schema, zmIdx } = setupData(10000);
    const scanner = new PushdownScanner(data, schema, zmIdx);

    const rows = scanner.scan({ column: 'id', op: 'GT', value: 0 }, ['id'], 10);
    assert.equal(rows.length, 10);
  });

  it('no matches returns empty', () => {
    const { data, schema, zmIdx } = setupData(10000);
    const scanner = new PushdownScanner(data, schema, zmIdx);

    const rows = scanner.scan({ column: 'id', op: 'GT', value: 99999 }, ['id']);
    assert.equal(rows.length, 0);
  });

  it('equality pushdown', () => {
    const { data, schema, zmIdx } = setupData(10000);
    const scanner = new PushdownScanner(data, schema, zmIdx);

    const rows = scanner.scan({ column: 'id', op: 'EQ', value: 5000 }, ['id', 'score', 'region']);
    assert.equal(rows.length, 1);
    assert.equal(rows[0].id, 5000);

    const stats = scanner.getStats();
    assert.ok(stats.skippedPages >= 9, 'Should skip most pages for EQ');
  });

  it('benchmark: pushdown vs full scan on 100K sorted rows', () => {
    const n = 100000;
    const data = { val: [], label: [] };
    const schema = [{ name: 'val', type: 'INT' }, { name: 'label', type: 'TEXT' }];
    const zmIdx = new ZoneMapIndex(schema, 1000);

    for (let i = 0; i < n; i++) {
      data.val.push(i);
      data.label.push(`label_${i}`);
      zmIdx.addRow({ val: i });
    }

    // Pushdown scan
    const scanner = new PushdownScanner(data, schema, zmIdx);
    const t0 = Date.now();
    const pushdownResult = scanner.scan({ column: 'val', op: 'GT', value: 95000 }, ['val', 'label']);
    const pushdownMs = Date.now() - t0;

    // Full scan
    const t1 = Date.now();
    const fullResult = [];
    for (let i = 0; i < n; i++) {
      if (data.val[i] > 95000) fullResult.push({ val: data.val[i], label: data.label[i] });
    }
    const fullMs = Date.now() - t1;

    const stats = scanner.getStats();
    console.log(`    Pushdown: ${pushdownMs}ms (${stats.skippedPages} pages skipped) vs Full: ${fullMs}ms (${(fullMs / Math.max(pushdownMs, 0.1)).toFixed(1)}x)`);
    assert.equal(pushdownResult.length, fullResult.length);
  });

  it('non-zone-mapped column falls back to full scan', () => {
    const { data, schema, zmIdx } = setupData(1000);
    const scanner = new PushdownScanner(data, schema, zmIdx);

    // region is TEXT — no zone map
    const rows = scanner.scan({ column: 'region', op: 'EQ', value: 'US' }, ['id', 'region']);
    assert.ok(rows.length > 0);
    assert.ok(rows.every(r => r.region === 'US'));
  });
});
