// schema-registry.js — Type-safe schema registry for table definitions
export class SchemaRegistry {
  constructor() { this._schemas = new Map(); }

  register(name, columns) {
    this._schemas.set(name, { name, columns, createdAt: Date.now() });
  }

  get(name) { return this._schemas.get(name); }
  has(name) { return this._schemas.has(name); }
  drop(name) { return this._schemas.delete(name); }
  
  validate(name, row) {
    const schema = this._schemas.get(name);
    if (!schema) return { valid: false, error: `Schema ${name} not found` };
    if (row.length !== schema.columns.length) return { valid: false, error: 'Column count mismatch' };
    for (let i = 0; i < schema.columns.length; i++) {
      const col = schema.columns[i];
      if (col.type === 'int' && typeof row[i] !== 'number') return { valid: false, error: `Column ${col.name}: expected int` };
      if (col.type === 'string' && typeof row[i] !== 'string') return { valid: false, error: `Column ${col.name}: expected string` };
    }
    return { valid: true };
  }

  list() { return [...this._schemas.keys()]; }
}
