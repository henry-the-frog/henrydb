// catalog.js — Database catalog (system tables)
export class Catalog {
  constructor() {
    this._tables = new Map();
    this._indexes = new Map();
  }

  createTable(name, columns, options = {}) {
    this._tables.set(name, { name, columns, options, createdAt: Date.now() });
  }

  dropTable(name) { this._tables.delete(name); this._indexes.delete(name); }
  getTable(name) { return this._tables.get(name); }
  listTables() { return [...this._tables.keys()]; }

  createIndex(tableName, indexName, columns, type = 'btree') {
    if (!this._indexes.has(tableName)) this._indexes.set(tableName, []);
    this._indexes.get(tableName).push({ name: indexName, columns, type, createdAt: Date.now() });
  }

  getIndexes(tableName) { return this._indexes.get(tableName) || []; }
  
  getColumnType(tableName, columnName) {
    const table = this._tables.get(tableName);
    if (!table) return null;
    const col = table.columns.find(c => c.name === columnName);
    return col ? col.type : null;
  }
}
