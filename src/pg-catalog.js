// pg-catalog.js — PostgreSQL catalog virtual tables (pg_catalog + information_schema)
// Extracted from db.js. Mixin pattern: installPgCatalog(Database) adds methods to prototype.

/**
 * PostgreSQL type OID mapping
 */
export const PG_TYPE_OIDS = {
  'INT': 23, 'INTEGER': 23, 'SERIAL': 23, 'AUTOINCREMENT': 23,
  'BIGINT': 20, 'SMALLINT': 21,
  'FLOAT': 701, 'DOUBLE': 701, 'REAL': 700, 'NUMERIC': 1700, 'DECIMAL': 1700,
  'TEXT': 25, 'VARCHAR': 1043, 'CHAR': 1042, 'STRING': 25,
  'BOOLEAN': 16, 'BOOL': 16,
  'DATE': 1082, 'TIMESTAMP': 1114, 'TIME': 1083,
  'JSON': 114, 'JSONB': 3802,
  'BYTEA': 17, 'BLOB': 17,
  'UUID': 2950,
};

/**
 * Install pg_catalog and information_schema virtual table methods on Database.
 * @param {Function} DatabaseClass — the Database constructor
 */
export function installPgCatalog(DatabaseClass) {
  // Store type OIDs as static property for backward compatibility
  DatabaseClass._PG_TYPE_OIDS = PG_TYPE_OIDS;

  DatabaseClass.prototype._getPgCatalog = function _getPgCatalog(tableName) {
    // Generate stable OIDs from table names
    const _oid = (name, salt = 0) => {
      let h = salt;
      for (let i = 0; i < name.length; i++) h = ((h << 5) - h + name.charCodeAt(i)) | 0;
      return Math.abs(h) % 1000000 + 16384; // Start above system OIDs
    };
  
    switch (tableName) {
      case 'pg_namespace': {
        return [
          { oid: 11, nspname: 'pg_catalog', nspowner: 10, 'pg_namespace.oid': 11, 'pg_namespace.nspname': 'pg_catalog', 'pg_namespace.nspowner': 10 },
          { oid: 2200, nspname: 'public', nspowner: 10, 'pg_namespace.oid': 2200, 'pg_namespace.nspname': 'public', 'pg_namespace.nspowner': 10 },
          { oid: 13200, nspname: 'information_schema', nspowner: 10, 'pg_namespace.oid': 13200, 'pg_namespace.nspname': 'information_schema', 'pg_namespace.nspowner': 10 },
        ];
      }
  
      case 'pg_class': {
        const rows = [];
        let oidCounter = 16384;
        for (const [name, table] of this.tables) {
          const tableOid = _oid(name);
          const row = {
            oid: tableOid,
            relname: name,
            relnamespace: 2200,
            reltype: 0,
            reloftype: 0,
            relowner: 10,
            relam: 2, // heap
            relfilenode: tableOid,
            reltablespace: 0,
            relpages: 1,
            reltuples: table.heap ? table.heap.scan().length : -1,
            relallvisible: 0,
            reltoastrelid: 0,
            relhasindex: (table.indexes && table.indexes.size > 0) ? true : false,
            relisshared: false,
            relpersistence: 'p',
            relkind: 'r', // ordinary table
            relnatts: table.schema.length,
            relchecks: 0,
            relhasrules: false,
            relhastriggers: (this._triggers && this._triggers.some(t => t.table === name)) || false,
            relhassubclass: false,
            relrowsecurity: false,
            relforcerowsecurity: false,
            relispopulated: true,
            relreplident: 'd',
            relispartition: false,
          };
          // Add qualified names
          for (const [k, v] of Object.entries(row)) {
            row[`pg_class.${k}`] = v;
          }
          rows.push(row);
        }
        // Add indexes as pg_class entries (use indexCatalog for proper names)
        for (const [idxName, idxMeta] of this.indexCatalog) {
              const idxOid = _oid(idxName, 1);
              const row = {
                oid: idxOid,
                relname: idxName,
                relnamespace: 2200,
                reltype: 0,
                reloftype: 0,
                relowner: 10,
                relam: 403, // btree
                relfilenode: idxOid,
                reltablespace: 0,
                relpages: 1,
                reltuples: -1,
                relallvisible: 0,
                reltoastrelid: 0,
                relhasindex: false,
                relisshared: false,
                relpersistence: 'p',
                relkind: 'i', // index
                relnatts: 1,
                relchecks: 0,
                relhasrules: false,
                relhastriggers: false,
                relhassubclass: false,
                relrowsecurity: false,
                relforcerowsecurity: false,
                relispopulated: true,
                relreplident: 'n',
                relispartition: false,
              };
              for (const [k, v] of Object.entries(row)) {
                row[`pg_class.${k}`] = v;
              }
              rows.push(row);
        }
        // Add views
        for (const [name] of this.views) {
          const viewOid = _oid(name, 2);
          const row = {
            oid: viewOid,
            relname: name,
            relnamespace: 2200,
            reltype: 0,
            reloftype: 0,
            relowner: 10,
            relam: 0,
            relfilenode: 0,
            reltablespace: 0,
            relpages: 0,
            reltuples: -1,
            relallvisible: 0,
            reltoastrelid: 0,
            relhasindex: false,
            relisshared: false,
            relpersistence: 'p',
            relkind: 'v', // view
            relnatts: 0,
            relchecks: 0,
            relhasrules: true,
            relhastriggers: false,
            relhassubclass: false,
            relrowsecurity: false,
            relforcerowsecurity: false,
            relispopulated: true,
            relreplident: 'n',
            relispartition: false,
          };
          for (const [k, v] of Object.entries(row)) {
            row[`pg_class.${k}`] = v;
          }
          rows.push(row);
        }
        return rows;
      }
  
      case 'pg_attribute': {
        const rows = [];
        for (const [tblName, table] of this.tables) {
          const tableOid = _oid(tblName);
          for (let i = 0; i < table.schema.length; i++) {
            const col = table.schema[i];
            const typeUpper = (col.type || 'TEXT').toUpperCase();
            const typeOid = PG_TYPE_OIDS[typeUpper] || 25;
            const row = {
              attrelid: tableOid,
              attname: col.name,
              atttypid: typeOid,
              attstattarget: -1,
              attlen: typeOid === 23 ? 4 : typeOid === 20 ? 8 : typeOid === 16 ? 1 : -1,
              attnum: i + 1,
              attndims: 0,
              attcacheoff: -1,
              atttypmod: -1,
              attbyval: [16, 21, 23].includes(typeOid),
              attalign: 'i',
              attstorage: 'p',
              attcompression: '',
              attnotnull: col.notNull || col.primaryKey || false,
              atthasdef: col.defaultValue !== undefined && col.defaultValue !== null,
              atthasmissing: false,
              attidentity: '',
              attgenerated: '',
              attisdropped: false,
              attislocal: true,
              attinhcount: 0,
              attcollation: 0,
            };
            for (const [k, v] of Object.entries(row)) {
              row[`pg_attribute.${k}`] = v;
            }
            rows.push(row);
          }
        }
        return rows;
      }
  
      case 'pg_type': {
        const typeEntries = [
          { oid: 16, typname: 'bool', typnamespace: 11, typlen: 1, typbyval: true, typtype: 'b', typcategory: 'B' },
          { oid: 20, typname: 'int8', typnamespace: 11, typlen: 8, typbyval: true, typtype: 'b', typcategory: 'N' },
          { oid: 21, typname: 'int2', typnamespace: 11, typlen: 2, typbyval: true, typtype: 'b', typcategory: 'N' },
          { oid: 23, typname: 'int4', typnamespace: 11, typlen: 4, typbyval: true, typtype: 'b', typcategory: 'N' },
          { oid: 25, typname: 'text', typnamespace: 11, typlen: -1, typbyval: false, typtype: 'b', typcategory: 'S' },
          { oid: 114, typname: 'json', typnamespace: 11, typlen: -1, typbyval: false, typtype: 'b', typcategory: 'U' },
          { oid: 700, typname: 'float4', typnamespace: 11, typlen: 4, typbyval: true, typtype: 'b', typcategory: 'N' },
          { oid: 701, typname: 'float8', typnamespace: 11, typlen: 8, typbyval: true, typtype: 'b', typcategory: 'N' },
          { oid: 1042, typname: 'bpchar', typnamespace: 11, typlen: -1, typbyval: false, typtype: 'b', typcategory: 'S' },
          { oid: 1043, typname: 'varchar', typnamespace: 11, typlen: -1, typbyval: false, typtype: 'b', typcategory: 'S' },
          { oid: 1082, typname: 'date', typnamespace: 11, typlen: 4, typbyval: true, typtype: 'b', typcategory: 'D' },
          { oid: 1083, typname: 'time', typnamespace: 11, typlen: 8, typbyval: true, typtype: 'b', typcategory: 'D' },
          { oid: 1114, typname: 'timestamp', typnamespace: 11, typlen: 8, typbyval: true, typtype: 'b', typcategory: 'D' },
          { oid: 1700, typname: 'numeric', typnamespace: 11, typlen: -1, typbyval: false, typtype: 'b', typcategory: 'N' },
          { oid: 2950, typname: 'uuid', typnamespace: 11, typlen: 16, typbyval: false, typtype: 'b', typcategory: 'U' },
          { oid: 3802, typname: 'jsonb', typnamespace: 11, typlen: -1, typbyval: false, typtype: 'b', typcategory: 'U' },
        ];
        return typeEntries.map(t => {
          const row = { ...t };
          for (const [k, v] of Object.entries(t)) {
            row[`pg_type.${k}`] = v;
          }
          return row;
        });
      }
  
      case 'pg_index': {
        const rows = [];
        for (const [idxName, idxMeta] of this.indexCatalog) {
          const table = this.tables.get(idxMeta.table);
          const colName = idxMeta.columns ? idxMeta.columns[0] : null;
          const row = {
            indexrelid: _oid(idxName, 1),
            indrelid: _oid(idxMeta.table),
            indnatts: idxMeta.columns ? idxMeta.columns.length : 1,
            indnkeyatts: idxMeta.columns ? idxMeta.columns.length : 1,
            indisunique: idxMeta.unique || false,
            indisprimary: false,
            indisexclusion: false,
            indimmediate: true,
            indisclustered: false,
            indisvalid: true,
            indcheckxmin: false,
            indisready: true,
            indislive: true,
            indisreplident: false,
            indkey: colName && table ? String(table.schema.findIndex(c => c.name === colName) + 1) : '1',
          };
          for (const [k, v] of Object.entries(row)) {
            row[`pg_index.${k}`] = v;
          }
          rows.push(row);
        }
        // Also add PK indexes
        for (const [tblName, table] of this.tables) {
          const pkCol = table.schema.find(c => c.primaryKey);
          if (pkCol && table.indexes.has(pkCol.name)) {
            const pkIdxName = `${tblName}_pkey`;
            const row = {
              indexrelid: _oid(pkIdxName, 1),
              indrelid: _oid(tblName),
              indnatts: 1,
              indnkeyatts: 1,
              indisunique: true,
              indisprimary: true,
              indisexclusion: false,
              indimmediate: true,
              indisclustered: false,
              indisvalid: true,
              indcheckxmin: false,
              indisready: true,
              indislive: true,
              indisreplident: false,
              indkey: String(table.schema.indexOf(pkCol) + 1),
            };
            for (const [k, v] of Object.entries(row)) {
              row[`pg_index.${k}`] = v;
            }
            rows.push(row);
          }
        }
        return rows;
      }
  
      case 'pg_settings': {
        const settings = [];
        const costModel = this._costModel || {};
        const params = {
          'seq_page_cost': costModel.seqPageCost || 1.0,
          'random_page_cost': costModel.randomPageCost || 1.1,
          'cpu_tuple_cost': costModel.cpuTupleCost || 0.01,
          'cpu_index_tuple_cost': costModel.cpuIndexTupleCost || 0.005,
          'cpu_operator_cost': costModel.cpuOperatorCost || 0.0025,
          'effective_cache_size': '4GB',
          'work_mem': '4MB',
          'server_version': '16.0',
          'server_encoding': 'UTF8',
          'client_encoding': 'UTF8',
        };
        for (const [name, setting] of Object.entries(params)) {
          const row = {
            name,
            setting: String(setting),
            unit: typeof setting === 'number' ? '' : null,
            category: 'Query Tuning / Planner Cost Constants',
            short_desc: `${name} parameter`,
            extra_desc: null,
            context: 'user',
            vartype: typeof setting === 'number' ? 'real' : 'string',
            source: 'default',
            min_val: typeof setting === 'number' ? '0' : null,
            max_val: null,
            boot_val: String(setting),
            reset_val: String(setting),
          };
          for (const [k, v] of Object.entries(row)) {
            row[`pg_settings.${k}`] = v;
          }
          settings.push(row);
        }
        return settings;
      }
  
      case 'pg_stat_user_tables': {
        const rows = [];
        for (const [name, table] of this.tables) {
          const scanCount = table.heap ? [...table.heap.scan()].length : 0;
          const row = {
            relid: _oid(name),
            schemaname: 'public',
            relname: name,
            seq_scan: 0,
            seq_tup_read: 0,
            idx_scan: 0,
            idx_tup_fetch: 0,
            n_tup_ins: 0,
            n_tup_upd: 0,
            n_tup_del: 0,
            n_live_tup: scanCount,
            n_dead_tup: 0,
            last_vacuum: null,
            last_autovacuum: null,
            last_analyze: null,
          };
          for (const [k, v] of Object.entries(row)) {
            row[`pg_stat_user_tables.${k}`] = v;
          }
          rows.push(row);
        }
        return rows;
      }
  
      case 'pg_stat_statements': {
        const rows = [];
        for (const [, stats] of this._queryStats) {
          const row = {
            query: stats.query,
            calls: stats.calls,
            total_exec_time: Math.round(stats.total_exec_time * 1000) / 1000,
            mean_exec_time: stats.calls > 0 ? Math.round((stats.total_exec_time / stats.calls) * 1000) / 1000 : 0,
            min_exec_time: stats.min_exec_time === Infinity ? 0 : Math.round(stats.min_exec_time * 1000) / 1000,
            max_exec_time: Math.round(stats.max_exec_time * 1000) / 1000,
            rows: stats.rows,
          };
          for (const [k, v] of Object.entries(row)) {
            row[`pg_stat_statements.${k}`] = v;
          }
          rows.push(row);
        }
        return rows;
      }
  
      default:
        return null;
    }
  }
  
  
  DatabaseClass.prototype._getInformationSchema = function _getInformationSchema(tableName) {
    const schema = tableName.replace('information_schema.', '');
    
    switch (schema) {
      case 'tables': {
        const rows = [];
        for (const [name] of this.tables) {
          rows.push({
            table_catalog: 'henrydb',
            table_schema: 'public',
            table_name: name,
            table_type: 'BASE TABLE',
            'information_schema.tables.table_catalog': 'henrydb',
            'information_schema.tables.table_schema': 'public',
            'information_schema.tables.table_name': name,
            'information_schema.tables.table_type': 'BASE TABLE',
          });
        }
        for (const [name] of this.views) {
          rows.push({
            table_catalog: 'henrydb',
            table_schema: 'public',
            table_name: name,
            table_type: 'VIEW',
            'information_schema.tables.table_catalog': 'henrydb',
            'information_schema.tables.table_schema': 'public',
            'information_schema.tables.table_name': name,
            'information_schema.tables.table_type': 'VIEW',
          });
        }
        return rows;
      }
      
      case 'columns': {
        const rows = [];
        for (const [tableName, table] of this.tables) {
          for (let i = 0; i < table.schema.length; i++) {
            const col = table.schema[i];
            const row = {
              table_catalog: 'henrydb',
              table_schema: 'public',
              table_name: tableName,
              column_name: col.name,
              ordinal_position: i + 1,
              column_default: col.defaultValue,
              is_nullable: col.notNull ? 'NO' : 'YES',
              data_type: col.type,
            };
            // Also add qualified names
            for (const [k, v] of Object.entries(row)) {
              row[`information_schema.columns.${k}`] = v;
            }
            rows.push(row);
          }
        }
        return rows;
      }
      
      case 'table_constraints': {
        const rows = [];
        for (const [tableName, table] of this.tables) {
          for (const col of table.schema) {
            if (col.primaryKey) {
              const row = {
                constraint_catalog: 'henrydb',
                constraint_schema: 'public',
                constraint_name: `${tableName}_${col.name}_pkey`,
                table_catalog: 'henrydb',
                table_schema: 'public',
                table_name: tableName,
                constraint_type: 'PRIMARY KEY',
              };
              for (const [k, v] of Object.entries(row)) {
                row[`information_schema.table_constraints.${k}`] = v;
              }
              rows.push(row);
            }
            if (col.notNull) {
              const row = {
                constraint_catalog: 'henrydb',
                constraint_schema: 'public',
                constraint_name: `${tableName}_${col.name}_notnull`,
                table_catalog: 'henrydb',
                table_schema: 'public',
                table_name: tableName,
                constraint_type: 'NOT NULL',
              };
              for (const [k, v] of Object.entries(row)) {
                row[`information_schema.table_constraints.${k}`] = v;
              }
              rows.push(row);
            }
            if (col.references) {
              const row = {
                constraint_catalog: 'henrydb',
                constraint_schema: 'public',
                constraint_name: `${tableName}_${col.name}_fkey`,
                table_catalog: 'henrydb',
                table_schema: 'public',
                table_name: tableName,
                constraint_type: 'FOREIGN KEY',
              };
              for (const [k, v] of Object.entries(row)) {
                row[`information_schema.table_constraints.${k}`] = v;
              }
              rows.push(row);
            }
          }
        }
        return rows;
      }
      
      case 'key_column_usage': {
        const rows = [];
        for (const [tableName, table] of this.tables) {
          for (let i = 0; i < table.schema.length; i++) {
            const col = table.schema[i];
            if (col.primaryKey) {
              const row = {
                constraint_catalog: 'henrydb',
                constraint_schema: 'public',
                constraint_name: `${tableName}_${col.name}_pkey`,
                table_catalog: 'henrydb',
                table_schema: 'public',
                table_name: tableName,
                column_name: col.name,
                ordinal_position: i + 1,
              };
              for (const [k, v] of Object.entries(row)) {
                row[`information_schema.key_column_usage.${k}`] = v;
              }
              rows.push(row);
            }
            if (col.references) {
              const row = {
                constraint_catalog: 'henrydb',
                constraint_schema: 'public',
                constraint_name: `${tableName}_${col.name}_fkey`,
                table_catalog: 'henrydb',
                table_schema: 'public',
                table_name: tableName,
                column_name: col.name,
                ordinal_position: i + 1,
              };
              for (const [k, v] of Object.entries(row)) {
                row[`information_schema.key_column_usage.${k}`] = v;
              }
              rows.push(row);
            }
          }
        }
        return rows;
      }
      
      default:
        return null;
    }
  }
  
}
