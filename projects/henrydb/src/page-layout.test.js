// page-layout.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { SlottedPage } from './page-layout.js';

describe('SlottedPage', () => {
  it('insert and retrieve rows', () => {
    const page = new SlottedPage(4096);
    const id0 = page.insertRow({ name: 'Alice', age: 30 });
    const id1 = page.insertRow({ name: 'Bob', age: 25 });
    
    assert.equal(id0, 0);
    assert.deepEqual(page.getRow(0), { name: 'Alice', age: 30 });
    assert.deepEqual(page.getRow(1), { name: 'Bob', age: 25 });
  });

  it('returns -1 when page full', () => {
    const page = new SlottedPage(64); // Tiny page
    const id = page.insertRow({ x: 'a very long string that fills up the tiny page completely yes it does' });
    assert.equal(id, -1);
  });

  it('many rows', () => {
    const page = new SlottedPage(4096);
    let count = 0;
    while (page.insertRow({ id: count }) >= 0) count++;
    assert.ok(count > 50, `Only fit ${count} rows`);
    console.log(`  ${count} rows in 4KB page`);
  });
});
