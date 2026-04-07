// docstore.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { DocumentStore } from './docstore.js';

describe('DocumentStore', () => {
  it('insert and find', () => {
    const store = new DocumentStore('users');
    store.insert({ name: 'Alice', age: 30 });
    store.insert({ name: 'Bob', age: 25 });
    
    const all = store.find();
    assert.equal(all.length, 2);
  });

  it('find with equality query', () => {
    const store = new DocumentStore('users');
    store.insert({ name: 'Alice', age: 30 });
    store.insert({ name: 'Bob', age: 25 });
    
    const result = store.find({ name: 'Alice' });
    assert.equal(result.length, 1);
    assert.equal(result[0].age, 30);
  });

  it('find with comparison operators', () => {
    const store = new DocumentStore('users');
    store.insert({ name: 'Alice', age: 30 });
    store.insert({ name: 'Bob', age: 25 });
    store.insert({ name: 'Charlie', age: 35 });
    
    assert.equal(store.find({ age: { $gt: 28 } }).length, 2);
    assert.equal(store.find({ age: { $lte: 30 } }).length, 2);
    assert.equal(store.find({ age: { $ne: 30 } }).length, 2);
  });

  it('find with $in operator', () => {
    const store = new DocumentStore('items');
    store.insert({ status: 'active' });
    store.insert({ status: 'inactive' });
    store.insert({ status: 'pending' });
    
    const result = store.find({ status: { $in: ['active', 'pending'] } });
    assert.equal(result.length, 2);
  });

  it('nested document access with dot notation', () => {
    const store = new DocumentStore('users');
    store.insert({ name: 'Alice', address: { city: 'NYC', zip: '10001' } });
    store.insert({ name: 'Bob', address: { city: 'LA', zip: '90001' } });
    
    const result = store.find({ 'address.city': 'NYC' });
    assert.equal(result.length, 1);
    assert.equal(result[0].name, 'Alice');
  });

  it('update with $set', () => {
    const store = new DocumentStore('users');
    store.insert({ name: 'Alice', age: 30 });
    
    store.update({ name: 'Alice' }, { $set: { age: 31 } });
    assert.equal(store.findOne({ name: 'Alice' }).age, 31);
  });

  it('update with $inc', () => {
    const store = new DocumentStore('counters');
    store.insert({ name: 'views', count: 100 });
    
    store.update({ name: 'views' }, { $inc: { count: 1 } });
    assert.equal(store.findOne({ name: 'views' }).count, 101);
  });

  it('deleteMany', () => {
    const store = new DocumentStore('items');
    store.insert({ status: 'active' });
    store.insert({ status: 'inactive' });
    store.insert({ status: 'active' });
    
    const deleted = store.deleteMany({ status: 'inactive' });
    assert.equal(deleted, 1);
    assert.equal(store.size, 2);
  });

  it('aggregate pipeline', () => {
    const store = new DocumentStore('orders');
    store.insert({ product: 'A', amount: 100 });
    store.insert({ product: 'A', amount: 200 });
    store.insert({ product: 'B', amount: 150 });
    
    const result = store.aggregate([
      { $group: { _id: '$product', total: { $sum: '$amount' } } },
      { $sort: { total: -1 } },
    ]);
    
    assert.equal(result[0]._id, 'A');
    assert.equal(result[0].total, 300);
  });

  it('insertMany', () => {
    const store = new DocumentStore('batch');
    const ids = store.insertMany([
      { name: 'A' }, { name: 'B' }, { name: 'C' },
    ]);
    assert.equal(ids.length, 3);
    assert.equal(store.size, 3);
  });

  it('count', () => {
    const store = new DocumentStore('items');
    store.insertMany([{ x: 1 }, { x: 2 }, { x: 3 }]);
    assert.equal(store.count(), 3);
    assert.equal(store.count({ x: { $gt: 1 } }), 2);
  });
});
