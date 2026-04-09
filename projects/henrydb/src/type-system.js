// type-system.js — SQL type system for HenryDB
// Type checking, inference, and coercion rules.

/**
 * SQL data types.
 */
export const SQLType = {
  INTEGER: 'INTEGER',
  REAL: 'REAL',
  TEXT: 'TEXT',
  BOOLEAN: 'BOOLEAN',
  NULL: 'NULL',
  BLOB: 'BLOB',
  DATE: 'DATE',
  TIMESTAMP: 'TIMESTAMP',
  JSON: 'JSON',
  ANY: 'ANY',
  UNKNOWN: 'UNKNOWN',
};

/**
 * Type affinity rules (SQLite-style).
 */
const AFFINITY = {
  INT: SQLType.INTEGER, INTEGER: SQLType.INTEGER, TINYINT: SQLType.INTEGER,
  SMALLINT: SQLType.INTEGER, MEDIUMINT: SQLType.INTEGER, BIGINT: SQLType.INTEGER,
  UNSIGNED: SQLType.INTEGER, INT2: SQLType.INTEGER, INT8: SQLType.INTEGER,
  REAL: SQLType.REAL, DOUBLE: SQLType.REAL, FLOAT: SQLType.REAL,
  NUMERIC: SQLType.REAL, DECIMAL: SQLType.REAL,
  TEXT: SQLType.TEXT, CHAR: SQLType.TEXT, VARCHAR: SQLType.TEXT, CLOB: SQLType.TEXT,
  BLOB: SQLType.BLOB,
  BOOLEAN: SQLType.BOOLEAN, BOOL: SQLType.BOOLEAN,
  DATE: SQLType.DATE, DATETIME: SQLType.TIMESTAMP, TIMESTAMP: SQLType.TIMESTAMP,
  JSON: SQLType.JSON, JSONB: SQLType.JSON,
};

/**
 * Resolve type affinity from a type name string.
 */
export function resolveAffinity(typeName) {
  if (!typeName) return SQLType.ANY;
  const upper = typeName.toUpperCase().replace(/\(.*\)/, '').trim();
  return AFFINITY[upper] || SQLType.TEXT;
}

/**
 * Type coercion precedence (higher = wins in mixed expressions).
 */
const TYPE_PRECEDENCE = {
  [SQLType.NULL]: 0,
  [SQLType.BOOLEAN]: 1,
  [SQLType.INTEGER]: 2,
  [SQLType.REAL]: 3,
  [SQLType.TEXT]: 4,
  [SQLType.DATE]: 5,
  [SQLType.TIMESTAMP]: 6,
  [SQLType.JSON]: 7,
  [SQLType.BLOB]: 8,
  [SQLType.ANY]: 10,
};

/**
 * Determine the result type of a binary operation.
 */
export function binaryResultType(left, right, op) {
  // NULL propagation
  if (left === SQLType.NULL || right === SQLType.NULL) return SQLType.NULL;
  
  // Comparison operators always return BOOLEAN
  if (['=', '!=', '<', '>', '<=', '>=', 'LIKE', 'IN', 'BETWEEN'].includes(op)) {
    return SQLType.BOOLEAN;
  }
  
  // Arithmetic: promote to REAL if either is REAL
  if (['+', '-', '*', '/'].includes(op)) {
    if (left === SQLType.REAL || right === SQLType.REAL) return SQLType.REAL;
    if (left === SQLType.INTEGER && right === SQLType.INTEGER) return SQLType.INTEGER;
    return SQLType.REAL;
  }
  
  // String concatenation
  if (op === '||') return SQLType.TEXT;
  
  // Boolean operations
  if (['AND', 'OR'].includes(op)) return SQLType.BOOLEAN;
  
  // Default: higher precedence wins
  return (TYPE_PRECEDENCE[left] || 0) >= (TYPE_PRECEDENCE[right] || 0) ? left : right;
}

/**
 * Determine the result type of an aggregate function.
 */
export function aggregateResultType(func, inputType) {
  const f = func.toUpperCase();
  switch (f) {
    case 'COUNT': return SQLType.INTEGER;
    case 'SUM': return inputType === SQLType.INTEGER ? SQLType.INTEGER : SQLType.REAL;
    case 'AVG': return SQLType.REAL;
    case 'MIN': case 'MAX': return inputType;
    case 'GROUP_CONCAT': return SQLType.TEXT;
    case 'JSON_GROUP_ARRAY': return SQLType.JSON;
    case 'TOTAL': return SQLType.REAL;
    default: return SQLType.ANY;
  }
}

/**
 * Determine the result type of a scalar function.
 */
export function functionResultType(func) {
  const f = func.toUpperCase();
  const intFuncs = ['ABS', 'LENGTH', 'UNICODE', 'RANDOM', 'INSTR', 'TYPEOF'];
  const realFuncs = ['ROUND', 'SQRT', 'LOG', 'LOG2', 'LOG10', 'CEIL', 'FLOOR', 'POWER'];
  const textFuncs = ['UPPER', 'LOWER', 'TRIM', 'LTRIM', 'RTRIM', 'SUBSTR', 'REPLACE', 'HEX', 'QUOTE',
                     'TYPEOF', 'DATE', 'TIME', 'DATETIME', 'STRFTIME', 'PRINTF', 'CHAR', 'GROUP_CONCAT'];
  const boolFuncs = ['NULLIF', 'GLOB', 'LIKE'];
  const jsonFuncs = ['JSON', 'JSON_ARRAY', 'JSON_OBJECT', 'JSON_EXTRACT', 'JSON_SET', 'JSON_REMOVE'];
  
  if (intFuncs.includes(f)) return SQLType.INTEGER;
  if (realFuncs.includes(f)) return SQLType.REAL;
  if (textFuncs.includes(f)) return SQLType.TEXT;
  if (boolFuncs.includes(f)) return SQLType.BOOLEAN;
  if (jsonFuncs.includes(f)) return SQLType.JSON;
  if (f === 'COALESCE' || f === 'IIF' || f === 'IFNULL') return SQLType.ANY;
  return SQLType.ANY;
}

/**
 * Check if a type can be implicitly coerced to another.
 */
export function canCoerce(from, to) {
  if (from === to) return true;
  if (from === SQLType.NULL) return true; // NULL coerces to anything
  if (to === SQLType.ANY || from === SQLType.ANY) return true;
  
  // Integer → Real (safe widening)
  if (from === SQLType.INTEGER && to === SQLType.REAL) return true;
  // Boolean → Integer
  if (from === SQLType.BOOLEAN && to === SQLType.INTEGER) return true;
  // Anything → Text (toString)
  if (to === SQLType.TEXT) return true;
  
  return false;
}

/**
 * TypeChecker — validates types in a SQL AST.
 */
export class TypeChecker {
  constructor(schema = {}) {
    // schema: { tableName: { columnName: SQLType } }
    this.schema = schema;
    this.errors = [];
    this.warnings = [];
  }

  /**
   * Register a table schema.
   */
  addTable(name, columns) {
    this.schema[name] = columns;
  }

  /**
   * Infer the type of an expression.
   */
  inferType(expr) {
    if (!expr) return SQLType.UNKNOWN;
    
    switch (expr.type) {
      case 'literal':
        if (expr.value === null) return SQLType.NULL;
        if (typeof expr.value === 'number') return Number.isInteger(expr.value) ? SQLType.INTEGER : SQLType.REAL;
        if (typeof expr.value === 'boolean') return SQLType.BOOLEAN;
        if (typeof expr.value === 'string') return SQLType.TEXT;
        return SQLType.UNKNOWN;
      
      case 'column_ref':
      case 'column':
        return this._lookupColumnType(expr.table, expr.name) || SQLType.ANY;
      
      case 'BINARY':
        return binaryResultType(this.inferType(expr.left), this.inferType(expr.right), expr.op);
      
      case 'COMPARE':
        return SQLType.BOOLEAN;
      
      case 'AND': case 'OR': case 'NOT':
        return SQLType.BOOLEAN;
      
      case 'function_call':
      case 'FUNCTION':
        return functionResultType(expr.name || expr.function);
      
      case 'CASE':
      case 'case_expr':
        if (expr.whens && expr.whens.length > 0) {
          return this.inferType(expr.whens[0].then || expr.whens[0].result);
        }
        return SQLType.ANY;
      
      case 'CAST':
        return resolveAffinity(expr.targetType || expr.dataType);
      
      case 'star':
        return SQLType.ANY;
      
      default:
        return SQLType.ANY;
    }
  }

  _lookupColumnType(table, column) {
    if (table && this.schema[table]) return this.schema[table][column];
    // Search all tables
    for (const schema of Object.values(this.schema)) {
      if (schema[column]) return schema[column];
    }
    return null;
  }
}
