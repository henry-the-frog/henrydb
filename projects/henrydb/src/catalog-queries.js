// catalog-queries.js — Information schema + pg_catalog queries extracted from db.js
// Functions take "db" as first parameter

export function selectInfoSchema(db, ast) {
  const tableName = (ast.from.table || '').toLowerCase().replace('information_schema.', '');
  
  if (tableName === 'tables' || tableName === 'information_schema.tables') {
    const rows = [];
    for (const [name, table] of db.tables) {
      rows.push({
        table_catalog: 'henrydb',
        table_schema: 'public',
        table_name: name,
        table_type: 'BASE TABLE',
        column_count: table.schema.length,
      });
    }
    for (const [name] of db.views) {
      rows.push({
        table_catalog: 'henrydb',
        table_schema: 'public',
        table_name: name,
        table_type: 'VIEW',
        column_count: 0,
      });
    }
    let filtered = rows;
    if (ast.where) {
      filtered = rows.filter(r => db._evalExpr(ast.where, r));
    }
    if (ast.orderBy) {
      filtered.sort((a, b) => {
        for (const o of ast.orderBy) {
          const col = typeof o.column === 'string' ? o.column : o.column.name;
          if (a[col] < b[col]) return o.direction === 'DESC' ? 1 : -1;
          if (a[col] > b[col]) return o.direction === 'DESC' ? -1 : 1;
        }
        return 0;
      });
    }
    return { rows: filtered, columns: Object.keys(rows[0] || {}) };
  }

  if (tableName === 'columns' || tableName === 'information_schema.columns') {
    const rows = [];
    for (const [tname, table] of db.tables) {
      for (let i = 0; i < table.schema.length; i++) {
        const col = table.schema[i];
        rows.push({
          table_catalog: 'henrydb',
          table_schema: 'public',
          table_name: tname,
          column_name: col.name,
          ordinal_position: i + 1,
          data_type: col.type || 'TEXT',
          is_nullable: col.notNull ? 'NO' : 'YES',
          column_default: col.defaultValue,
        });
      }
    }
    let filtered = rows;
    if (ast.where) {
      filtered = rows.filter(r => db._evalExpr(ast.where, r));
    }
    return { rows: filtered, columns: Object.keys(rows[0] || {}) };
  }

  throw new Error(`Unknown information_schema table: ${tableName}`);
}

export function selectPgCatalog(db, ast) {
  const rawName = (ast.from.table || '').toLowerCase().replace('pg_catalog.', '');
  
  if (rawName === 'pg_tables') {
    const rows = [];
    for (const [name, table] of db.tables) {
      rows.push({
        schemaname: 'public',
        tablename: name,
        tableowner: 'henrydb',
        tablespace: null,
        hasindexes: table.indexes && table.indexes.size > 0,
        hasrules: false,
        hastriggers: false,
        rowsecurity: false,
      });
    }
    return filterPgCatalogRows(db, rows, ast);
  }
  
  if (rawName === 'pg_indexes') {
    const rows = [];
    for (const [tableName, table] of db.tables) {
      if (!table.indexMeta) continue;
      for (const [colKey, meta] of table.indexMeta) {
        const unique = meta.unique ? 'UNIQUE ' : '';
        const using = meta.indexType === 'HASH' ? 'USING hash ' : '';
        rows.push({
          schemaname: 'public',
          tablename: tableName,
          indexname: meta.name,
          tablespace: null,
          indexdef: `CREATE ${unique}INDEX ${meta.name} ON public.${tableName} ${using}(${meta.columns.join(', ')})`,
        });
      }
    }
    return filterPgCatalogRows(db, rows, ast);
  }
  
  if (rawName === 'pg_stat_user_tables') {
    const rows = [];
    for (const [name, table] of db.tables) {
      const stats = db._tableStats?.get(name);
      rows.push({
        schemaname: 'public',
        relname: name,
        seq_scan: 0, // Would need tracking
        seq_tup_read: 0,
        idx_scan: 0,
        idx_tup_fetch: 0,
        n_tup_ins: 0,
        n_tup_upd: 0,
        n_tup_del: 0,
        n_live_tup: table.heap?.tupleCount || 0,
        n_dead_tup: 0,
        last_analyze: stats?.analyzedAt ? new Date(stats.analyzedAt).toISOString() : null,
      });
    }
    return filterPgCatalogRows(db, rows, ast);
  }
  
  throw new Error(`Unknown pg_catalog table: ${rawName}`);
}

export function filterPgCatalogRows(db, rows, ast) {
  if (ast.where) {
    rows = rows.filter(row => db._evalExpr(ast.where, row));
  }
  if (ast.orderBy) {
    rows.sort((a, b) => {
      for (const ob of ast.orderBy) {
        const col = ob.column || ob.expr?.name;
        if (!col) continue;
        const va = a[col], vb = b[col];
        const cmp = va < vb ? -1 : va > vb ? 1 : 0;
        if (cmp !== 0) return ob.desc ? -cmp : cmp;
      }
      return 0;
    });
  }
  if (ast.limit != null) rows = rows.slice(0, ast.limit);
  return { type: 'ROWS', rows };
}
