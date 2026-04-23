// deserialize-handler.js — Extracted from db.js (2026-04-23)
// Database deserialization from JSON

import { BPlusTree } from './btree.js';

/**
 * Restore a database from a serialized JSON object.
 * @param {function} DatabaseClass - The Database class to instantiate
 * @param {string|object} data - JSON string or object from serialize()
 * @returns {object} Restored Database instance
 */
export function fromSerialized(DatabaseClass, data) {
  const obj = typeof data === 'string' ? JSON.parse(data) : data;
  const db = new DatabaseClass();
  
  // Restore tables
  for (const [name, tableData] of Object.entries(obj.tables)) {
    const schema = tableData.schema;
    const heap = db._heapFactory(name);
    const indexes = new Map();
    const tableObj = { schema, heap, indexes };
    db.tables.set(name, tableObj);
    
    for (const values of tableData.rows) {
      heap.insert(values);
    }
    
    for (const colName of tableData.indexes || []) {
      const colIdx = schema.findIndex(c => c.name === colName);
      if (colIdx >= 0) {
        const index = new BPlusTree(32);
        for (const { pageId, slotIdx, values } of heap.scan()) {
          index.insert(values[colIdx], { pageId, slotIdx });
        }
        indexes.set(colName, index);
      }
    }
    
    if (tableData.indexMeta) {
      if (!tableObj.indexMeta) tableObj.indexMeta = new Map();
      for (const [key, meta] of Object.entries(tableData.indexMeta)) {
        tableObj.indexMeta.set(key, meta);
      }
    }
  }
  
  // Restore views
  for (const [name, view] of Object.entries(obj.views || {})) {
    db.views.set(name, view);
  }
  
  // Restore triggers
  db.triggers = obj.triggers || [];
  
  // Restore sequences
  for (const [name, seq] of Object.entries(obj.sequences || {})) {
    db.sequences.set(name, { ...seq });
  }
  
  // Restore materialized views
  for (const [name, mv] of Object.entries(obj.materializedViews || {})) {
    if (!db.materializedViews) db.materializedViews = new Map();
    db.materializedViews.set(name, mv);
  }
  
  // Restore comments
  for (const [key, val] of Object.entries(obj.comments || {})) {
    if (!db._comments) db._comments = new Map();
    db._comments.set(key, val);
  }
  
  // Restore indexCatalog
  for (const [name, meta] of Object.entries(obj.indexCatalog || {})) {
    db.indexCatalog.set(name, meta);
  }
  
  return db;
}
