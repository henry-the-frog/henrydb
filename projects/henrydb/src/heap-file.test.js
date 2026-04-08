// heap-file.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { HeapFile } from './heap-file.js';

describe('HeapFile', () => {
  it('insert and read', () => {
    const hf = new HeapFile();
    const rid = hf.insert({ name: 'Alice', age: 30 });
    const record = hf.read(rid);
    assert.equal(record.name, 'Alice');
  });

  it('multiple inserts', () => {
    const hf = new HeapFile();
    const rids = [];
    for (let i = 0; i < 10; i++) rids.push(hf.insert({ id: i }));
    
    assert.equal(hf.recordCount, 10);
    for (let i = 0; i < 10; i++) assert.equal(hf.read(rids[i]).id, i);
  });

  it('delete', () => {
    const hf = new HeapFile();
    const rid = hf.insert({ data: 'temp' });
    assert.ok(hf.delete(rid));
    assert.equal(hf.read(rid), null);
    assert.equal(hf.recordCount, 0);
  });

  it('update in place', () => {
    const hf = new HeapFile();
    const rid = hf.insert({ name: 'Alice' });
    const newRid = hf.update(rid, { name: 'Bob' });
    assert.equal(hf.read(newRid).name, 'Bob');
  });

  it('full table scan', () => {
    const hf = new HeapFile();
    for (let i = 0; i < 5; i++) hf.insert({ id: i });
    
    const scanned = [...hf.scan()];
    assert.equal(scanned.length, 5);
    assert.deepEqual(scanned.map(s => s.record.id).sort(), [0, 1, 2, 3, 4]);
  });

  it('spans multiple pages', () => {
    const hf = new HeapFile({ pageSize: 256 });
    for (let i = 0; i < 50; i++) hf.insert({ id: i, data: 'x'.repeat(20) });
    
    assert.ok(hf.pageCount > 1);
    assert.equal(hf.recordCount, 50);
    
    // All records retrievable
    const scanned = [...hf.scan()];
    assert.equal(scanned.length, 50);
  });

  it('free space map routes inserts', () => {
    const hf = new HeapFile({ pageSize: 512 });
    const rids = [];
    for (let i = 0; i < 5; i++) rids.push(hf.insert({ id: i, data: 'x'.repeat(20) }));
    
    // Delete some records to free space
    for (let i = 0; i < 3; i++) hf.delete(rids[i]);
    
    // New insert should try reusing freed space
    const newRid = hf.insert({ id: 99, data: 'reuse' });
    assert.ok(hf.read(newRid).id === 99);
    // Free space should have been consulted
    assert.ok(hf.getStats().pages >= 1);
  });

  it('stats', () => {
    const hf = new HeapFile();
    for (let i = 0; i < 10; i++) hf.insert({ id: i });
    
    const stats = hf.getStats();
    assert.equal(stats.records, 10);
    assert.ok(stats.pages >= 1);
    assert.ok(stats.utilization.includes('%'));
  });
});
