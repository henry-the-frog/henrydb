// zone-maps.test.js — Tests for zone maps (min/max skip-scan)
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { ZoneMap, ZoneMapIndex } from './zone-maps.js';

describe('ZoneMap', () => {

  it('tracks min/max per page', () => {
    const zm = new ZoneMap(4); // 4 rows per page
    zm.push(10); zm.push(30); zm.push(20); zm.push(40);
    zm.push(100); zm.push(110); zm.push(90); zm.push(120);

    assert.equal(zm.pageCount, 2);
    assert.equal(zm.pages[0].min, 10);
    assert.equal(zm.pages[0].max, 40);
    assert.equal(zm.pages[1].min, 90);
    assert.equal(zm.pages[1].max, 120);
  });

  it('pagesForEquals: skip pages that cant contain value', () => {
    const zm = new ZoneMap(100);
    // Page 0: values 0..99
    for (let i = 0; i < 100; i++) zm.push(i);
    // Page 1: values 1000..1099
    for (let i = 1000; i < 1100; i++) zm.push(i);
    // Page 2: values 500..599
    for (let i = 500; i < 600; i++) zm.push(i);

    const pages50 = zm.pagesForEquals(50);
    assert.equal(pages50.length, 1); // Only page 0
    assert.equal(pages50[0].offset, 0);

    const pages1050 = zm.pagesForEquals(1050);
    assert.equal(pages1050.length, 1); // Only page 1

    const pages9999 = zm.pagesForEquals(9999);
    assert.equal(pages9999.length, 0); // Not in any page
  });

  it('pagesForGT: skip pages with all small values', () => {
    const zm = new ZoneMap(100);
    for (let i = 0; i < 100; i++) zm.push(i); // max 99
    for (let i = 100; i < 200; i++) zm.push(i); // max 199
    for (let i = 200; i < 300; i++) zm.push(i); // max 299

    const pages = zm.pagesForGT(150);
    assert.equal(pages.length, 2); // Pages 1 and 2 (max > 150)
  });

  it('pagesForRange: overlap check', () => {
    const zm = new ZoneMap(100);
    for (let i = 0; i < 100; i++) zm.push(i * 10); // 0..990
    for (let i = 0; i < 100; i++) zm.push(i * 10 + 2000); // 2000..2990
    for (let i = 0; i < 100; i++) zm.push(i * 10 + 5000); // 5000..5990

    const pages = zm.pagesForRange(500, 2500);
    assert.equal(pages.length, 2); // Pages 0 and 1 overlap with [500, 2500]
  });

  it('scanWithSkip: faster than full scan', () => {
    const n = 10000;
    const pageSize = 100;
    const zm = new ZoneMap(pageSize);
    const data = [];

    // Insert sorted data so zone maps have tight ranges
    for (let i = 0; i < n; i++) {
      const val = i;
      data.push(val);
      zm.push(val);
    }

    // Full scan
    const t0 = Date.now();
    const fullResult = [];
    for (let i = 0; i < n; i++) {
      if (data[i] > 9500) fullResult.push(i);
    }
    const fullMs = Date.now() - t0;

    // Zone map skip scan
    const t1 = Date.now();
    const skipResult = zm.scanWithSkip(
      data,
      v => v > 9500,
      (min, max) => max > 9500
    );
    const skipMs = Date.now() - t1;

    const stats = zm.skipStats((min, max) => max > 9500);
    console.log(`    Skip: ${skipMs}ms vs Full: ${fullMs}ms | Skipped ${stats.skippedPages}/${stats.totalPages} pages (${stats.skipRate})`);
    assert.equal(skipResult.length, fullResult.length);
  });

  it('skipStats reports effectiveness', () => {
    const zm = new ZoneMap(100);
    for (let i = 0; i < 1000; i++) zm.push(i);

    const stats = zm.skipStats((min, max) => max > 800);
    assert.equal(stats.totalPages, 10);
    assert.equal(stats.candidatePages, 2); // Pages with max > 800: pages 8 and 9
    assert.equal(stats.skippedPages, 8);
    assert.equal(stats.skipRate, '80.0%');
  });
});

describe('ZoneMapIndex', () => {

  it('tracks zone maps for numeric columns', () => {
    const idx = new ZoneMapIndex([
      { name: 'id', type: 'INT' },
      { name: 'name', type: 'TEXT' },
      { name: 'score', type: 'INT' },
    ], 100);

    for (let i = 0; i < 500; i++) {
      idx.addRow({ id: i, name: `user_${i}`, score: i * 7 % 100 });
    }

    assert.deepEqual(idx.columns.sort(), ['id', 'score']);
    assert.ok(idx.getZoneMap('id'));
    assert.ok(idx.getZoneMap('score'));
    assert.equal(idx.getZoneMap('name'), undefined); // TEXT not zone-mapped
  });

  it('getCandidatePages for EQ', () => {
    const idx = new ZoneMapIndex([{ name: 'val', type: 'INT' }], 100);
    for (let i = 0; i < 1000; i++) idx.addRow({ val: i });

    const pages = idx.getCandidatePages('val', 'EQ', 550);
    assert.equal(pages.length, 1); // Only page 5 (rows 500-599)
  });

  it('getCandidatePages for GT', () => {
    const idx = new ZoneMapIndex([{ name: 'val', type: 'INT' }], 100);
    for (let i = 0; i < 1000; i++) idx.addRow({ val: i });

    const pages = idx.getCandidatePages('val', 'GT', 800);
    assert.equal(pages.length, 2); // Pages 8 and 9
  });

  it('benchmark: zone map scan on 100K sorted rows', () => {
    const n = 100000;
    const idx = new ZoneMapIndex([{ name: 'val', type: 'INT' }], 1000);
    const data = [];
    for (let i = 0; i < n; i++) {
      data.push(i);
      idx.addRow({ val: i });
    }

    const zm = idx.getZoneMap('val');
    
    // Skip scan
    const t0 = Date.now();
    const skipResult = zm.scanWithSkip(data, v => v > 95000, (min, max) => max > 95000);
    const skipMs = Date.now() - t0;

    // Full scan
    const t1 = Date.now();
    const fullResult = [];
    for (let i = 0; i < n; i++) if (data[i] > 95000) fullResult.push(i);
    const fullMs = Date.now() - t1;

    const stats = zm.skipStats((min, max) => max > 95000);
    console.log(`    100K: Skip ${skipMs}ms vs Full ${fullMs}ms | ${stats.skipRate} pages skipped (${stats.skippedPages}/${stats.totalPages})`);
    assert.equal(skipResult.length, fullResult.length);
    assert.equal(skipResult.length, 4999);
  });
});
