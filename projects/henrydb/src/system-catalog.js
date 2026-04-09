// system-catalog.js — PostgreSQL-compatible system catalog for HenryDB
// Provides pg_catalog tables and information_schema views.

/**
 * SystemCatalog — virtual system tables that reflect database metadata.
 */
export class SystemCatalog {
  constructor(db) {
    this.db = db;
    this._oidCounter = 16384; // Start above reserved OIDs
    this._typeOids = new Map([
      ['INTEGER', 23], ['INT', 23], ['INT4', 23],
      ['BIGINT', 20], ['INT8', 20],
      ['SMALLINT', 21], ['INT2', 21],
      ['TEXT', 25], ['VARCHAR', 1043],
      ['BOOLEAN', 16], ['BOOL', 16],
      ['FLOAT', 701], ['DOUBLE', 701], ['FLOAT8', 701],
      ['REAL', 700], ['FLOAT4', 700],
      ['NUMERIC', 1700], ['DECIMAL', 1700],
      ['DATE', 1082],
      ['TIMESTAMP', 1114],
      ['JSON', 114], ['JSONB', 3802],
      ['BYTEA', 17],
      ['UUID', 2950],
    ]);
  }

  nextOid() {
    return this._oidCounter++;
  }

  /**
   * Query a system catalog table.
   */
  query(tableName, filter = null) {
    const lowerName = tableName.toLowerCase();

    switch (lowerName) {
      case 'pg_class': return this._pgClass(filter);
      case 'pg_attribute': return this._pgAttribute(filter);
      case 'pg_type': return this._pgType(filter);
      case 'pg_index': return this._pgIndex(filter);
      case 'pg_namespace': return this._pgNamespace(filter);
      case 'pg_database': return this._pgDatabase(filter);
      case 'pg_stat_user_tables': return this._pgStatUserTables(filter);
      case 'information_schema.tables': return this._ischemaTables(filter);
      case 'information_schema.columns': return this._ischemaColumns(filter);
      default:
        throw new Error(`System table '${tableName}' not found`);
    }
  }

  // --- pg_class: Table/relation metadata ---
  _pgClass(filter) {
    const tables = this.db._tables || this.db.tables || new Map();
    const rows = [];
    let oid = 16384;

    for (const [name, table] of tables) {
      const row = {
        oid: oid++,
        relname: name,
        relnamespace: 2200, // public schema
        reltype: 0,
        relkind: 'r', // regular table
        reltuples: table.rows ? table.rows.length : (table._rows ? table._rows.length : 0),
        relpages: Math.ceil((table.rows?.length || table._rows?.length || 0) / 100),
        relhasindex: false,
        relispartition: false,
      };
      if (!filter || this._matchFilter(row, filter)) {
        rows.push(row);
      }
    }

    // Add indexes
    const indexes = this.db._indexes || new Map();
    for (const [name, index] of indexes) {
      const row = {
        oid: oid++,
        relname: name,
        relnamespace: 2200,
        reltype: 0,
        relkind: 'i', // index
        reltuples: 0,
        relpages: 0,
        relhasindex: false,
        relispartition: false,
      };
      if (!filter || this._matchFilter(row, filter)) {
        rows.push(row);
      }
    }

    return { rows };
  }

  // --- pg_attribute: Column metadata ---
  _pgAttribute(filter) {
    const tables = this.db._tables || this.db.tables || new Map();
    const rows = [];

    for (const [tableName, table] of tables) {
      const columns = table.columns || table._columns || [];
      let attnum = 1;
      for (const col of columns) {
        const colName = typeof col === 'string' ? col : col.name;
        const colType = typeof col === 'object' ? (col.type || 'TEXT') : 'TEXT';
        const row = {
          attrelid: tableName,
          attname: colName,
          atttypid: this._typeOids.get(colType.toUpperCase()) || 25,
          attnum: attnum++,
          attnotnull: col.notNull || false,
          atthasdef: col.default !== undefined,
          atttypmod: -1,
          attlen: -1,
        };
        if (!filter || this._matchFilter(row, filter)) {
          rows.push(row);
        }
      }
    }

    return { rows };
  }

  // --- pg_type: Data type catalog ---
  _pgType(filter) {
    const rows = [];
    for (const [typeName, oid] of this._typeOids) {
      const row = {
        oid,
        typname: typeName.toLowerCase(),
        typnamespace: 11, // pg_catalog
        typlen: -1,
        typtype: 'b', // base type
        typisdefined: true,
      };
      if (!filter || this._matchFilter(row, filter)) {
        rows.push(row);
      }
    }
    return { rows };
  }

  // --- pg_index: Index metadata ---
  _pgIndex(filter) {
    const indexes = this.db._indexes || new Map();
    const rows = [];

    for (const [name, index] of indexes) {
      const row = {
        indexrelid: name,
        indrelid: index.table || index.tableName || '',
        indisunique: index.unique || false,
        indisprimary: index.primary || false,
        indkey: index.columns ? index.columns.join(',') : '',
      };
      if (!filter || this._matchFilter(row, filter)) {
        rows.push(row);
      }
    }

    return { rows };
  }

  // --- pg_namespace ---
  _pgNamespace() {
    return {
      rows: [
        { oid: 11, nspname: 'pg_catalog', nspowner: 10 },
        { oid: 2200, nspname: 'public', nspowner: 10 },
        { oid: 12500, nspname: 'information_schema', nspowner: 10 },
      ],
    };
  }

  // --- pg_database ---
  _pgDatabase() {
    return {
      rows: [{
        oid: 1,
        datname: 'henrydb',
        datdba: 10,
        encoding: 6, // UTF8
        datcollate: 'en_US.UTF-8',
        datctype: 'en_US.UTF-8',
      }],
    };
  }

  // --- pg_stat_user_tables ---
  _pgStatUserTables() {
    const tables = this.db._tables || this.db.tables || new Map();
    const rows = [];

    for (const [name, table] of tables) {
      const rowCount = table.rows?.length || table._rows?.length || 0;
      rows.push({
        relname: name,
        schemaname: 'public',
        n_live_tup: rowCount,
        n_dead_tup: 0,
        seq_scan: 0,
        idx_scan: 0,
        last_analyze: null,
        last_autoanalyze: null,
      });
    }

    return { rows };
  }

  // --- information_schema.tables ---
  _ischemaTables(filter) {
    const tables = this.db._tables || this.db.tables || new Map();
    const rows = [];

    for (const name of tables.keys()) {
      const row = {
        table_catalog: 'henrydb',
        table_schema: 'public',
        table_name: name,
        table_type: 'BASE TABLE',
        is_insertable_into: 'YES',
      };
      if (!filter || this._matchFilter(row, filter)) {
        rows.push(row);
      }
    }

    return { rows };
  }

  // --- information_schema.columns ---
  _ischemaColumns(filter) {
    const tables = this.db._tables || this.db.tables || new Map();
    const rows = [];

    for (const [tableName, table] of tables) {
      const columns = table.columns || table._columns || [];
      let pos = 1;
      for (const col of columns) {
        const colName = typeof col === 'string' ? col : col.name;
        const colType = typeof col === 'object' ? (col.type || 'text') : 'text';
        const row = {
          table_catalog: 'henrydb',
          table_schema: 'public',
          table_name: tableName,
          column_name: colName,
          ordinal_position: pos++,
          data_type: colType.toLowerCase(),
          is_nullable: col.notNull ? 'NO' : 'YES',
          column_default: col.default ?? null,
        };
        if (!filter || this._matchFilter(row, filter)) {
          rows.push(row);
        }
      }
    }

    return { rows };
  }

  _matchFilter(row, filter) {
    return Object.entries(filter).every(([key, value]) => row[key] === value);
  }
}
