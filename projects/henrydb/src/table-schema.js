// table-schema.js — CREATE TABLE DDL with type checking

const VALID_TYPES = new Set(['INT', 'INTEGER', 'FLOAT', 'DOUBLE', 'VARCHAR', 'TEXT', 'BOOLEAN', 'DATE', 'TIMESTAMP']);

export class TableSchema {
  constructor(name, columns) {
    this.name = name;
    this.columns = columns.map(c => ({
      name: c.name,
      type: c.type.toUpperCase(),
      nullable: c.nullable !== false,
      primaryKey: c.primaryKey === true,
      defaultValue: c.defaultValue ?? null,
      unique: c.unique === true,
    }));
    this._validate();
  }

  _validate() {
    for (const col of this.columns) {
      if (!VALID_TYPES.has(col.type)) throw new Error(`Invalid type: ${col.type}`);
    }
    const names = this.columns.map(c => c.name);
    if (new Set(names).size !== names.length) throw new Error('Duplicate column names');
  }

  /** Validate a row against the schema */
  validateRow(row) {
    const errors = [];
    for (const col of this.columns) {
      const value = row[col.name];
      
      if (value == null) {
        if (!col.nullable && col.defaultValue == null) errors.push(`${col.name}: NOT NULL violation`);
        continue;
      }
      
      switch (col.type) {
        case 'INT': case 'INTEGER':
          if (!Number.isInteger(value)) errors.push(`${col.name}: expected INTEGER, got ${typeof value}`);
          break;
        case 'FLOAT': case 'DOUBLE':
          if (typeof value !== 'number') errors.push(`${col.name}: expected FLOAT, got ${typeof value}`);
          break;
        case 'VARCHAR': case 'TEXT':
          if (typeof value !== 'string') errors.push(`${col.name}: expected VARCHAR, got ${typeof value}`);
          break;
        case 'BOOLEAN':
          if (typeof value !== 'boolean') errors.push(`${col.name}: expected BOOLEAN, got ${typeof value}`);
          break;
      }
    }
    return errors;
  }

  /** Apply defaults to a row */
  applyDefaults(row) {
    const result = { ...row };
    for (const col of this.columns) {
      if (result[col.name] == null && col.defaultValue != null) {
        result[col.name] = col.defaultValue;
      }
    }
    return result;
  }

  get primaryKey() { return this.columns.filter(c => c.primaryKey).map(c => c.name); }
  get columnNames() { return this.columns.map(c => c.name); }
  
  toSQL() {
    const cols = this.columns.map(c => {
      let def = `${c.name} ${c.type}`;
      if (c.primaryKey) def += ' PRIMARY KEY';
      if (!c.nullable) def += ' NOT NULL';
      if (c.unique) def += ' UNIQUE';
      if (c.defaultValue != null) def += ` DEFAULT ${JSON.stringify(c.defaultValue)}`;
      return def;
    });
    return `CREATE TABLE ${this.name} (\n  ${cols.join(',\n  ')}\n);`;
  }
}
