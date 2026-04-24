// expression-evaluator.js — SQL expression evaluation engine
// Extracted from db.js. Mixin pattern: installExpressionEvaluator(Database) adds methods to prototype.

import { tokenize } from './fulltext.js';

/**
 * Install expression evaluation methods on Database.
 * Methods: _collectAggregateExprs, _resolveColumn, _evalExpr, _evalValue,
 *          _evalFunction, _evalSubquery, _computeAggregates
 * @param {Function} DatabaseClass — the Database constructor
 */
export function installExpressionEvaluator(DatabaseClass) {

  DatabaseClass.prototype._collectAggregateExprs = function _collectAggregateExprs(expr) {
    if (!expr) return [];
    if (expr.type === 'aggregate_expr') return [expr];
    const results = [];
    for (const key of ['left', 'right', 'expr']) {
      if (expr[key]) results.push(...this._collectAggregateExprs(expr[key]));
    }
    return results;
  }
  
  DatabaseClass.prototype._resolveColumn = function _resolveColumn(name, row) {
    if (name in row) return row[name];
    // Try without table prefix
    for (const key of Object.keys(row)) {
      if (key.endsWith(`.${name}`)) return row[key];
    }
    // For correlated subqueries: check outer row
    if (this._outerRow) {
      if (name in this._outerRow) return this._outerRow[name];
      for (const key of Object.keys(this._outerRow)) {
        if (key.endsWith(`.${name}`)) return this._outerRow[key];
      }
    }
    // For LATERAL JOINs: check lateral scope (outer row)
    if (this._lateralScope) {
      if (name in this._lateralScope) return this._lateralScope[name];
      for (const key of Object.keys(this._lateralScope)) {
        if (key.endsWith(`.${name}`)) return this._lateralScope[key];
      }
    }
    return undefined;
  }
  
  DatabaseClass.prototype._evalExpr = function _evalExpr(expr, row) {
    if (!expr) return true;
    switch (expr.type) {
      case 'AND': return this._evalExpr(expr.left, row) && this._evalExpr(expr.right, row);
      case 'OR': return this._evalExpr(expr.left, row) || this._evalExpr(expr.right, row);
      case 'NOT': return !this._evalExpr(expr.expr, row);
      case 'MATCH_AGAINST': {
        // Find the fulltext index for this column
        const searchText = this._evalValue(expr.search, row);
        const column = expr.column;
        
        // Find a fulltext index that covers this column
        let ftIdx = null;
        for (const [, idx] of this.fulltextIndexes) {
          if (idx.column === column) { ftIdx = idx; break; }
        }
        if (!ftIdx) throw new Error(`No fulltext index found for column ${column}`);
        
        // Get the text from the current row
        const rowText = String(row[column] || '');
        const rowTokens = tokenize(rowText);
        const searchTokens = tokenize(String(searchText));
        
        // Check if all search terms appear in the row
        return searchTokens.every(st => rowTokens.includes(st));
      }
      case 'EXISTS': {
        const result = this._evalSubquery(expr.subquery, row);
        return result.length > 0;
      }
      case 'IN_SUBQUERY': {
        const leftVal = this._evalValue(expr.left, row);
        const result = this._evalSubquery(expr.subquery, row);
        return result.some(r => {
          const vals = Object.values(r);
          return vals.includes(leftVal);
        });
      }
      case 'IN_HASHSET': {
        const leftVal = this._evalValue(expr.left, row);
        const found = expr.hashSet.has(leftVal);
        return expr.negated ? !found : found;
      }
      case 'NOT_IN_HASHSET': {
        const leftVal = this._evalValue(expr.left, row);
        return !expr.hashSet.has(leftVal);
      }
      case 'IN_COMPOSITE_HASHSET': {
        const vals = expr.outerCols.map(col => this._evalValue({ type: 'column_ref', name: col }, row));
        return expr.hashSet.has(JSON.stringify(vals));
      }
      case 'NOT_IN_COMPOSITE_HASHSET': {
        const vals = expr.outerCols.map(col => this._evalValue({ type: 'column_ref', name: col }, row));
        return !expr.hashSet.has(JSON.stringify(vals));
      }
      case 'LITERAL_BOOL': {
        return expr.value;
      }
      case 'IN_LIST': {
        const leftVal = this._evalValue(expr.left, row);
        return expr.values.some(v => this._evalValue(v, row) === leftVal);
      }
      case 'IS_NULL': {
        const val = this._evalValue(expr.left, row);
        return val === null || val === undefined;
      }
      case 'IS_NOT_NULL': {
        const val = this._evalValue(expr.left, row);
        return val !== null && val !== undefined;
      }
      case 'LIKE':
      case 'ILIKE': {
        const val = this._evalValue(expr.left, row);
        const pattern = this._evalValue(expr.pattern, row);
        if (val == null || pattern == null) return false;
        const regex = '^' + String(pattern)
          .replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
          .replace(/%/g, '.*')
          .replace(/_/g, '.')
          + '$';
        const flags = expr.type === 'ILIKE' ? 'i' : '';
        return new RegExp(regex, flags).test(String(val));
      }
      case 'BETWEEN': {
        const val = this._evalValue(expr.left, row);
        const low = this._evalValue(expr.low, row);
        const high = this._evalValue(expr.high, row);
        return val >= low && val <= high;
      }
      case 'COMPARE': {
        const left = this._evalValue(expr.left, row);
        const right = this._evalValue(expr.right, row);
        // SQL NULL semantics: any comparison with NULL returns false
        if (left === null || left === undefined || right === null || right === undefined) return false;
        switch (expr.op) {
          case 'EQ': return left === right;
          case 'NE': return left !== right;
          case 'LT': return left < right;
          case 'GT': return left > right;
          case 'LE': return left <= right;
          case 'GE': return left >= right;
        }
      }
      default: {
        // For literals, column refs, and other value expressions, evaluate and check truthiness
        const val = this._evalValue(expr, row);
        if (val === null || val === undefined || val === 0 || val === false || val === '') return false;
        return true;
      }
    }
  }
  
  DatabaseClass.prototype._evalValue = function _evalValue(node, row) {
    if (node.type === 'literal') return node.value;
    if (node.type === 'column_ref') return this._resolveColumn(node.name, row);
    if (node.type === 'array_constructor') {
      return node.elements.map(e => this._evalValue(e, row));
    }
    if (node.type === 'MATCH_AGAINST') {
      // Return relevance score
      return this._evalExpr(node, row) ? 1 : 0;
    }
    if (node.type === 'SUBQUERY' || node.type === 'subquery') {
      const subqueryAst = node.subquery || node.query;
      const result = this._evalSubquery(subqueryAst, row);
      if (result.length === 0) return null;
      const firstRow = result[0];
      return Object.values(firstRow)[0];
    }
    if (node.type === 'function_call') {
      return this._evalFunction(node.func, node.args, row);
    }
    if (node.type === 'cast') {
      const val = this._evalValue(node.expr, row);
      if (val == null) return null;
      switch (node.targetType) {
        case 'INT': case 'INTEGER': return parseInt(val, 10) || 0;
        case 'FLOAT': case 'REAL': case 'DOUBLE': return parseFloat(val) || 0;
        case 'TEXT': case 'VARCHAR': case 'CHAR': return String(val);
        case 'BOOLEAN': return Boolean(val);
        default: return val;
      }
    }
    if (node.type === 'IS_NULL') {
      const val = this._evalValue(node.left, row);
      return (val === null || val === undefined) ? 1 : 0;
    }
    if (node.type === 'IS_NOT_NULL') {
      const val = this._evalValue(node.left, row);
      return (val !== null && val !== undefined) ? 1 : 0;
    }
    if (node.type === 'COMPARE') {
      return this._evalExpr(node, row) ? 1 : 0;
    }
    if (node.type === 'case_expr') {
      for (const { condition, result } of node.whens) {
        if (this._evalExpr(condition, row)) {
          return this._evalValue(result, row);
        }
      }
      return node.elseResult ? this._evalValue(node.elseResult, row) : null;
    }
    if (node.type === 'arith') {
      const left = this._evalValue(node.left, row);
      const right = this._evalValue(node.right, row);
      if (left == null || right == null) return null;
      switch (node.op) {
        case '+': return left + right;
        case '-': return left - right;
        case '*': return left * right;
        case '/': {
          if (right === 0) return null;
          const result = left / right;
          // Integer division when both operands are integers AND neither was a float literal
          const leftIsFloat = node.left?.isFloat || !Number.isInteger(left);
          const rightIsFloat = node.right?.isFloat || !Number.isInteger(right);
          if (!leftIsFloat && !rightIsFloat && Number.isInteger(left) && Number.isInteger(right)) return Math.trunc(result);
          return result;
        }
        case '%': return right === 0 ? null : left % right;
      }
    }
    if (node.type === 'aggregate_expr') {
      // In HAVING/ORDER BY context, look up the computed aggregate from the row
      const argStr = typeof node.arg === 'string' ? node.arg : (node.arg?.name || '*');
      const key = `${node.func}(${argStr})`;
      if (key in row) return row[key];
      // Try to find it with any alias pattern
      for (const k of Object.keys(row)) {
        if (k.toUpperCase().includes(node.func) && k.includes(argStr)) return row[k];
      }
      return null;
    }
    return null;
  }
  
  DatabaseClass.prototype._evalFunction = function _evalFunction(func, args, row) {
    switch (func) {
      case 'NEXTVAL': { const v = this._evalValue(args[0], row); return this._nextval(String(v)); }
      case 'CURRVAL': { const v = this._evalValue(args[0], row); return this._currval(String(v)); }
      case 'SETVAL': { const v = this._evalValue(args[0], row); const n = this._evalValue(args[1], row); return this._setval(String(v), Number(n)); }
      case 'PG_STAT_STATEMENTS_RESET': { this._queryStats.clear(); return true; }
      case 'COALESCE': { for (const arg of args) { const v = this._evalValue(arg, row); if (v !== null && v !== undefined) return v; } return null; }
      case 'NULLIF': { const a = this._evalValue(args[0], row); const b = this._evalValue(args[1], row); return a === b ? null : a; }
      case 'GREATEST': { return Math.max(...args.map(a => this._evalValue(a, row)).filter(v => v !== null)); }
      case 'LEAST': { return Math.min(...args.map(a => this._evalValue(a, row)).filter(v => v !== null)); }
      case 'UPPER': { const v = this._evalValue(args[0], row); return v != null ? String(v).toUpperCase() : null; }
      case 'LOWER': { const v = this._evalValue(args[0], row); return v != null ? String(v).toLowerCase() : null; }
      case 'LENGTH': { const v = this._evalValue(args[0], row); return v != null ? String(v).length : null; }
      case 'INITCAP': {
        const v = this._evalValue(args[0], row);
        if (v == null) return null;
        return String(v).replace(/\b\w/g, c => c.toUpperCase());
      }
      case 'TRANSLATE': {
        const str = String(this._evalValue(args[0], row));
        const from = String(this._evalValue(args[1], row));
        const to = String(this._evalValue(args[2], row));
        let result = '';
        for (const c of str) {
          const idx = from.indexOf(c);
          if (idx >= 0) result += idx < to.length ? to[idx] : '';
          else result += c;
        }
        return result;
      }
      case 'CHR': return String.fromCharCode(Number(this._evalValue(args[0], row)));
      case 'ASCII': { const v = this._evalValue(args[0], row); return v != null && String(v).length > 0 ? String(v).charCodeAt(0) : null; }
      case 'MD5': {
        // Simple hash for md5 (not cryptographically secure but functional)
        const str = String(this._evalValue(args[0], row));
        let h = 0;
        for (let i = 0; i < str.length; i++) h = ((h << 5) - h + str.charCodeAt(i)) | 0;
        return Math.abs(h).toString(16).padStart(8, '0');
      }
      case 'ENCODE': case 'DECODE': {
        const val = this._evalValue(args[0], row);
        const fmt = String(this._evalValue(args[1], row)).toLowerCase();
        if (func === 'ENCODE' && fmt === 'base64') return Buffer.from(String(val)).toString('base64');
        if (func === 'DECODE' && fmt === 'base64') return Buffer.from(String(val), 'base64').toString();
        return val;
      }
      case 'CONCAT': return args.map(a => { const v = this._evalValue(a, row); return v != null ? String(v) : ''; }).join('');
      case 'CONCAT_WS': {
        const sep = String(this._evalValue(args[0], row));
        return args.slice(1).map(a => this._evalValue(a, row)).filter(v => v != null).map(String).join(sep);
      }
      case 'REGEXP_REPLACE': {
        const str = String(this._evalValue(args[0], row));
        const pattern = String(this._evalValue(args[1], row));
        const replacement = String(this._evalValue(args[2], row));
        const flags = args[3] ? String(this._evalValue(args[3], row)) : 'g';
        return str.replace(new RegExp(pattern, flags), replacement);
      }
      case 'REGEXP_MATCH': {
        const str = String(this._evalValue(args[0], row));
        const pattern = String(this._evalValue(args[1], row));
        return new RegExp(pattern).test(str) ? 1 : 0;
      }
      case 'REGEXP_MATCHES': {
        const str = String(this._evalValue(args[0], row));
        const pattern = String(this._evalValue(args[1], row));
        const flags = args[2] ? String(this._evalValue(args[2], row)) : '';
        const re = new RegExp(pattern, flags);
        const matches = [];
        if (flags.includes('g')) {
          let m;
          while ((m = re.exec(str)) !== null) {
            matches.push(m[1] || m[0]); // Capture group or full match
          }
        } else {
          const m = str.match(re);
          if (m) matches.push(m[1] || m[0]);
        }
        return matches;
      }
      case 'REGEXP_COUNT': {
        const str = String(this._evalValue(args[0], row));
        const pattern = String(this._evalValue(args[1], row));
        return (str.match(new RegExp(pattern, 'g')) || []).length;
      }
      case 'SPLIT_PART': {
        const str = String(this._evalValue(args[0], row));
        const delim = String(this._evalValue(args[1], row));
        const field = Number(this._evalValue(args[2], row));
        const parts = str.split(delim);
        return field >= 1 && field <= parts.length ? parts[field - 1] : '';
      }
      case 'POSITION': case 'STRPOS': {
        const substr = String(this._evalValue(args[0], row));
        const str = String(this._evalValue(args[1], row));
        const idx = str.indexOf(substr);
        return idx >= 0 ? idx + 1 : 0; // 1-based, 0 if not found
      }
      case 'COALESCE': {
        for (const arg of args) {
          const v = this._evalValue(arg, row);
          if (v !== null && v !== undefined) return v;
        }
        return null;
      }
      case 'NULLIF': {
        const a = this._evalValue(args[0], row);
        const b = this._evalValue(args[1], row);
        return a === b ? null : a;
      }
      case 'SUBSTR':
      case 'SUBSTRING': {
        const str = this._evalValue(args[0], row);
        if (str == null) return null;
        const start = (this._evalValue(args[1], row) || 1) - 1; // SQL is 1-indexed
        const len = args[2] ? this._evalValue(args[2], row) : undefined;
        return String(str).substring(start, len !== undefined ? start + len : undefined);
      }
      case 'REPLACE': {
        const str = this._evalValue(args[0], row);
        if (str == null) return null;
        const search = this._evalValue(args[1], row);
        const replace = this._evalValue(args[2], row);
        return String(str).replaceAll(String(search), String(replace));
      }
      case 'TRIM': {
        const str = this._evalValue(args[0], row);
        return str != null ? String(str).trim() : null;
      }
      case 'LTRIM': {
        const str = this._evalValue(args[0], row);
        return str != null ? String(str).replace(/^\s+/, '') : null;
      }
      case 'RTRIM': {
        const str = this._evalValue(args[0], row);
        return str != null ? String(str).replace(/\s+$/, '') : null;
      }
      case 'INSTR': {
        const str = this._evalValue(args[0], row);
        const sub = this._evalValue(args[1], row);
        if (str == null || sub == null) return null;
        const idx = String(str).indexOf(String(sub));
        return idx >= 0 ? idx + 1 : 0; // SQL INSTR is 1-based, 0 if not found
      }
      case 'PRINTF': {
        // Simplified printf: supports %d, %s, %f, %0Nd
        const fmt = this._evalValue(args[0], row);
        const vals = args.slice(1).map(a => this._evalValue(a, row));
        if (fmt == null) return null;
        let i = 0;
        return String(fmt).replace(/%(\d*)([dsf%])/g, (m, width, type) => {
          if (type === '%') return '%';
          const v = vals[i++];
          if (type === 'd') return width ? String(v || 0).padStart(parseInt(width), '0') : String(v || 0);
          if (type === 's') return String(v ?? '');
          if (type === 'f') return String(v ?? 0);
          return m;
        });
      }
      case 'ABS': {
        const val = this._evalValue(args[0], row);
        return val != null ? Math.abs(val) : null;
      }
      case 'ROUND': {
        const val = this._evalValue(args[0], row);
        if (val == null) return null;
        const decimals = args[1] ? this._evalValue(args[1], row) : 0;
        const factor = Math.pow(10, decimals);
        return Math.round(val * factor) / factor;
      }
      case 'CEIL': case 'CEILING': {
        const val = this._evalValue(args[0], row);
        return val != null ? Math.ceil(val) : null;
      }
      case 'FLOOR': {
        const val = this._evalValue(args[0], row);
        return val != null ? Math.floor(val) : null;
      }
      case 'MOD': {
        const a = Number(this._evalValue(args[0], row));
        const b = Number(this._evalValue(args[1], row));
        return b !== 0 ? a % b : null;
      }
      case 'SIGN': {
        const val = Number(this._evalValue(args[0], row));
        return val > 0 ? 1 : val < 0 ? -1 : 0;
      }
      case 'TRUNC': case 'TRUNCATE': {
        const val = Number(this._evalValue(args[0], row));
        const places = args[1] ? Number(this._evalValue(args[1], row)) : 0;
        const factor = Math.pow(10, places);
        return Math.trunc(val * factor) / factor;
      }
      case 'PI': return Math.PI;
      case 'EXP': return Math.exp(Number(this._evalValue(args[0], row)));
      case 'LN': return Math.log(Number(this._evalValue(args[0], row)));
      case 'LOG10': case 'LOG2': {
        const val = Number(this._evalValue(args[0], row));
        return func === 'LOG10' ? Math.log10(val) : Math.log2(val);
      }
      case 'DEGREES': return Number(this._evalValue(args[0], row)) * (180 / Math.PI);
      case 'RADIANS': return Number(this._evalValue(args[0], row)) * (Math.PI / 180);
      case 'SIN': return Math.sin(Number(this._evalValue(args[0], row)));
      case 'COS': return Math.cos(Number(this._evalValue(args[0], row)));
      case 'TAN': return Math.tan(Number(this._evalValue(args[0], row)));
      case 'ASIN': return Math.asin(Number(this._evalValue(args[0], row)));
      case 'ACOS': return Math.acos(Number(this._evalValue(args[0], row)));
      case 'ATAN': return Math.atan(Number(this._evalValue(args[0], row)));
      case 'ATAN2': return Math.atan2(Number(this._evalValue(args[0], row)), Number(this._evalValue(args[1], row)));
      case 'CBRT': return Math.cbrt(Number(this._evalValue(args[0], row)));
      case 'GCD': {
        let a = Math.abs(Number(this._evalValue(args[0], row)));
        let b = Math.abs(Number(this._evalValue(args[1], row)));
        while (b) { [a, b] = [b, a % b]; }
        return a;
      }
      case 'LCM': {
        const a = Math.abs(Number(this._evalValue(args[0], row)));
        const b = Math.abs(Number(this._evalValue(args[1], row)));
        let gcd_a = a, gcd_b = b;
        while (gcd_b) { [gcd_a, gcd_b] = [gcd_b, gcd_a % gcd_b]; }
        return gcd_a ? (a / gcd_a) * b : 0;
      }
      case 'IFNULL': {
        const val = this._evalValue(args[0], row);
        return val != null ? val : this._evalValue(args[1], row);
      }
      case 'IIF': {
        // IIF(condition, true_val, false_val) — but condition is an expression
        const cond = this._evalExpr(args[0], row);
        return cond ? this._evalValue(args[1], row) : this._evalValue(args[2], row);
      }
      case 'TYPEOF': {
        const val = this._evalValue(args[0], row);
        if (val === null || val === undefined) return 'null';
        if (typeof val === 'number') return Number.isInteger(val) ? 'integer' : 'real';
        if (typeof val === 'string') return 'text';
        if (typeof val === 'boolean') return 'integer';
        return 'blob';
      }
      case 'JSON_EXTRACT': {
        const json = this._evalValue(args[0], row);
        const path = this._evalValue(args[1], row);
        if (json == null) return null;
        try {
          const obj = typeof json === 'string' ? JSON.parse(json) : json;
          if (path === '$') return JSON.stringify(obj);
          const parts = path.replace(/^\$\.?/, '').split('.').filter(Boolean);
          let current = obj;
          for (const part of parts) {
            const arrMatch = part.match(/^(\w*)\[(\d+)\]$/);
            if (arrMatch) {
              if (arrMatch[1]) current = current[arrMatch[1]];
              current = current?.[parseInt(arrMatch[2])];
            } else {
              current = current?.[part];
            }
          }
          return current === undefined ? null : (typeof current === 'object' ? JSON.stringify(current) : current);
        } catch { return null; }
      }
      case 'JSON_SET': {
        const json = this._evalValue(args[0], row);
        const path = this._evalValue(args[1], row);
        const value = this._evalValue(args[2], row);
        if (json == null) return null;
        try {
          const obj = typeof json === 'string' ? JSON.parse(json) : { ...json };
          const parts = path.replace(/^\$\.?/, '').split('.').filter(Boolean);
          let current = obj;
          for (let i = 0; i < parts.length - 1; i++) {
            if (!current[parts[i]]) current[parts[i]] = {};
            current = current[parts[i]];
          }
          current[parts[parts.length - 1]] = value;
          return JSON.stringify(obj);
        } catch { return null; }
      }
      case 'JSON_ARRAY_LENGTH': {
        const json = this._evalValue(args[0], row);
        if (json == null) return null;
        try {
          const arr = typeof json === 'string' ? JSON.parse(json) : json;
          return Array.isArray(arr) ? arr.length : null;
        } catch { return null; }
      }
      case 'JSON_TYPE': {
        const json = this._evalValue(args[0], row);
        if (json == null) return 'null';
        try {
          const val = typeof json === 'string' ? JSON.parse(json) : json;
          if (Array.isArray(val)) return 'array';
          if (typeof val === 'object') return 'object';
          return typeof val;
        } catch { return 'text'; }
      }
      // String functions
      case 'LEFT': { const v = this._evalValue(args[0], row); return v == null ? null : String(v).substring(0, this._evalValue(args[1], row)); }
      case 'RIGHT': { const v = this._evalValue(args[0], row); const n = this._evalValue(args[1], row); return v == null ? null : String(v).slice(-n); }
      case 'LPAD': {
        const str = String(this._evalValue(args[0], row) || '');
        const len = this._evalValue(args[1], row) || 0;
        const pad = args[2] ? String(this._evalValue(args[2], row)) : ' ';
        return str.padStart(len, pad);
      }
      case 'RPAD': {
        const str = String(this._evalValue(args[0], row) || '');
        const len = this._evalValue(args[1], row) || 0;
        const pad = args[2] ? String(this._evalValue(args[2], row)) : ' ';
        return str.padEnd(len, pad);
      }
      case 'REVERSE': { const v = this._evalValue(args[0], row); return v == null ? null : String(v).split('').reverse().join(''); }
      case 'REPEAT': { const v = this._evalValue(args[0], row); const n = this._evalValue(args[1], row); return v == null ? null : String(v).repeat(n || 0); }
      
      // Math functions
      case 'POWER': return Math.pow(this._evalValue(args[0], row), this._evalValue(args[1], row));
      case 'GREATEST': return Math.max(...args.map(a => this._evalValue(a, row)));
      case 'LEAST': return Math.min(...args.map(a => this._evalValue(a, row)));
      case 'SQRT': return Math.sqrt(this._evalValue(args[0], row));
      case 'LOG': return args.length > 1 ? Math.log(this._evalValue(args[1], row)) / Math.log(this._evalValue(args[0], row)) : Math.log(this._evalValue(args[0], row));
      case 'RANDOM': return Math.random();
      
      // Date/time functions
      case 'CURRENT_TIMESTAMP': case 'NOW': return new Date().toISOString();
      case 'CURRENT_DATE': return new Date().toISOString().split('T')[0];
      case 'STRFTIME': {
        const fmt = this._evalValue(args[0], row);
        const dateStr = args[1] ? this._evalValue(args[1], row) : new Date().toISOString();
        const d = new Date(dateStr);
        return String(fmt)
          .replace('%Y', String(d.getUTCFullYear()))
          .replace('%m', String(d.getUTCMonth() + 1).padStart(2, '0'))
          .replace('%d', String(d.getUTCDate()).padStart(2, '0'))
          .replace('%H', String(d.getUTCHours()).padStart(2, '0'))
          .replace('%M', String(d.getUTCMinutes()).padStart(2, '0'))
          .replace('%S', String(d.getUTCSeconds()).padStart(2, '0'));
      }
      
      case 'DATE_TRUNC': {
        const field = String(this._evalValue(args[0], row)).toLowerCase();
        const dateStr = this._evalValue(args[1], row);
        const d = new Date(dateStr);
        switch (field) {
          case 'year': return `${d.getUTCFullYear()}-01-01`;
          case 'month': return `${d.getUTCFullYear()}-${String(d.getUTCMonth() + 1).padStart(2, '0')}-01`;
          case 'day': case 'date': return d.toISOString().split('T')[0];
          case 'hour': return `${d.toISOString().split(':')[0]}:00:00`;
          case 'minute': return `${d.toISOString().split(':').slice(0,2).join(':')}:00`;
          case 'quarter': {
            const q = Math.floor(d.getUTCMonth() / 3) * 3;
            return `${d.getUTCFullYear()}-${String(q + 1).padStart(2, '0')}-01`;
          }
          case 'week': {
            const day = d.getUTCDay();
            const diff = d.getUTCDate() - day + (day === 0 ? -6 : 1);
            const monday = new Date(d);
            monday.setUTCDate(diff);
            return monday.toISOString().split('T')[0];
          }
          default: return dateStr;
        }
      }
      
      case 'EXTRACT': {
        const field = String(this._evalValue(args[0], row)).toLowerCase();
        const dateStr = this._evalValue(args[1], row);
        const d = new Date(dateStr);
        switch (field) {
          case 'year': return d.getUTCFullYear();
          case 'month': return d.getUTCMonth() + 1;
          case 'day': return d.getUTCDate();
          case 'hour': return d.getUTCHours();
          case 'minute': return d.getUTCMinutes();
          case 'second': return d.getUTCSeconds();
          case 'dow': case 'dayofweek': return d.getUTCDay();
          case 'doy': case 'dayofyear': {
            const start = new Date(d.getUTCFullYear(), 0, 0);
            const diff = d - start;
            return Math.floor(diff / 86400000);
          }
          case 'quarter': return Math.floor(d.getUTCMonth() / 3) + 1;
          case 'week': {
            const start = new Date(d.getUTCFullYear(), 0, 1);
            const diff = d - start;
            return Math.ceil((diff / 86400000 + start.getUTCDay()) / 7);
          }
          case 'epoch': return Math.floor(d.getTime() / 1000);
          default: throw new Error(`Unknown extract field: ${field}`);
        }
      }
      
      case 'DATE_PART': {
        // DATE_PART is an alias for EXTRACT
        return this._evalFunction('EXTRACT', args, row);
      }
      
      case 'AGE': {
        const d1 = new Date(this._evalValue(args[0], row));
        const d2 = args.length > 1 ? new Date(this._evalValue(args[1], row)) : new Date();
        const diff = Math.abs(d1 - d2);
        const days = Math.floor(diff / 86400000);
        const years = Math.floor(days / 365);
        const months = Math.floor((days % 365) / 30);
        const remDays = days % 30;
        return `${years} years ${months} mons ${remDays} days`;
      }
      
      case 'DATE_ADD': case 'DATE_SUB': {
        const dateStr = this._evalValue(args[0], row);
        const interval = this._evalValue(args[1], row);
        const d = new Date(dateStr);
        // Parse interval: "N unit" (e.g., "3 days", "1 month")
        const match = String(interval).match(/^(-?\d+)\s*(year|month|day|hour|minute|second)s?$/i);
        if (match) {
          const n = parseInt(match[1]) * (func === 'DATE_SUB' ? -1 : 1);
          const unit = match[2].toLowerCase();
          switch (unit) {
            case 'year': d.setUTCFullYear(d.getUTCFullYear() + n); break;
            case 'month': d.setUTCMonth(d.getUTCMonth() + n); break;
            case 'day': d.setUTCDate(d.getUTCDate() + n); break;
            case 'hour': d.setUTCHours(d.getUTCHours() + n); break;
            case 'minute': d.setUTCMinutes(d.getUTCMinutes() + n); break;
            case 'second': d.setUTCSeconds(d.getUTCSeconds() + n); break;
          }
        }
        return d.toISOString();
      }
      
      case 'TO_CHAR': {
        const val = this._evalValue(args[0], row);
        const fmt = args.length > 1 ? String(this._evalValue(args[1], row)) : '';
        const d = new Date(val);
        if (isNaN(d.getTime())) return String(val);
        return fmt
          .replace('YYYY', String(d.getUTCFullYear()))
          .replace('MM', String(d.getUTCMonth() + 1).padStart(2, '0'))
          .replace('DD', String(d.getUTCDate()).padStart(2, '0'))
          .replace('HH24', String(d.getUTCHours()).padStart(2, '0'))
          .replace('HH', String(d.getUTCHours() % 12 || 12).padStart(2, '0'))
          .replace('MI', String(d.getUTCMinutes()).padStart(2, '0'))
          .replace('SS', String(d.getUTCSeconds()).padStart(2, '0'))
          .replace('Month', d.toLocaleString('en', { month: 'long', timeZone: 'UTC' }))
          .replace('Mon', d.toLocaleString('en', { month: 'short', timeZone: 'UTC' }))
          .replace('Day', d.toLocaleString('en', { weekday: 'long', timeZone: 'UTC' }))
          .replace('Dy', d.toLocaleString('en', { weekday: 'short', timeZone: 'UTC' }));
      }
      
      case 'ARRAY_LENGTH': {
        const arr = this._evalValue(args[0], row);
        if (Array.isArray(arr)) return arr.length;
        if (typeof arr === 'string') {
          try { const parsed = JSON.parse(arr); return Array.isArray(parsed) ? parsed.length : 0; }
          catch { return 0; }
        }
        return 0;
      }
      
      case 'ARRAY_APPEND': {
        let arr = this._evalValue(args[0], row);
        const val = this._evalValue(args[1], row);
        if (typeof arr === 'string') try { arr = JSON.parse(arr); } catch { arr = []; }
        if (!Array.isArray(arr)) arr = [];
        return [...arr, val];
      }
      
      case 'ARRAY_REMOVE': {
        let arr = this._evalValue(args[0], row);
        const val = this._evalValue(args[1], row);
        if (typeof arr === 'string') try { arr = JSON.parse(arr); } catch { arr = []; }
        if (!Array.isArray(arr)) return [];
        return arr.filter(x => x !== val);
      }
      
      case 'ARRAY_CAT': {
        let arr1 = this._evalValue(args[0], row);
        let arr2 = this._evalValue(args[1], row);
        if (typeof arr1 === 'string') try { arr1 = JSON.parse(arr1); } catch { arr1 = []; }
        if (typeof arr2 === 'string') try { arr2 = JSON.parse(arr2); } catch { arr2 = []; }
        return [...(arr1 || []), ...(arr2 || [])];
      }
      
      case 'ARRAY_POSITION': {
        let arr = this._evalValue(args[0], row);
        const val = this._evalValue(args[1], row);
        if (typeof arr === 'string') try { arr = JSON.parse(arr); } catch { arr = []; }
        if (!Array.isArray(arr)) return null;
        const idx = arr.indexOf(val);
        return idx >= 0 ? idx + 1 : null; // 1-based
      }
      
      default: {
        // Check user-defined functions
        const udf = this._functions.get(func.toLowerCase());
        if (udf) {
          return this._callUserFunction(udf, args, row);
        }
        throw new Error(`Unknown function: ${func}`);
      }
    }
  }
  
  DatabaseClass.prototype._evalSubquery = function _evalSubquery(subqueryAst, outerRow) {
    // Execute the subquery, passing outerRow for correlated references
    const savedOuterRow = this._outerRow;
    this._outerRow = outerRow;
    const result = this._select(subqueryAst);
    this._outerRow = savedOuterRow;
    return result.rows;
  }
  
  DatabaseClass.prototype._computeAggregates = function _computeAggregates(columns, rows) {
    const result = {};
    for (const col of columns) {
      if (col.type !== 'aggregate') continue;
      const argStr = typeof col.arg === 'object' ? 'expr' : col.arg;
      const name = col.alias || `${col.func}(${argStr})`;
      let filteredRows = rows;
      if (col.filter) {
        filteredRows = rows.filter(r => this._evalExpr(col.filter, r));
      }
      
      let values;
      if (col.arg === '*') {
        values = filteredRows;
      } else if (typeof col.arg === 'object') {
        values = filteredRows.map(r => this._evalValue(col.arg, r)).filter(v => v != null);
      } else {
        values = filteredRows.map(r => this._resolveColumn(col.arg, r)).filter(v => v != null);
      }
  
      switch (col.func) {
        case 'COUNT': {
          if (col.distinct && col.arg !== '*') {
            result[name] = new Set(values).size;
          } else {
            result[name] = col.arg === '*' ? filteredRows.length : values.length;
          }
          break;
        }
        case 'SUM': {
          const nonNull = values.filter(v => v !== null && v !== undefined);
          result[name] = nonNull.length > 0 ? nonNull.reduce((s, v) => s + v, 0) : null;
          break;
        }
        case 'AVG': result[name] = values.length ? values.reduce((s, v) => s + v, 0) / values.length : null; break;
        case 'MIN': result[name] = values.length ? values.reduce((a, b) => a < b ? a : b) : null; break;
        case 'MAX': result[name] = values.length ? values.reduce((a, b) => a > b ? a : b) : null; break;
        case 'STDDEV': case 'STDDEV_SAMP': {
          if (values.length < 2) { result[name] = null; break; }
          const nums = values.map(Number);
          const mean = nums.reduce((s, v) => s + v, 0) / nums.length;
          const variance = nums.reduce((s, v) => s + (v - mean) ** 2, 0) / (nums.length - 1);
          result[name] = Math.sqrt(variance);
          break;
        }
        case 'STDDEV_POP': {
          if (!values.length) { result[name] = null; break; }
          const nums = values.map(Number);
          const mean = nums.reduce((s, v) => s + v, 0) / nums.length;
          const variance = nums.reduce((s, v) => s + (v - mean) ** 2, 0) / nums.length;
          result[name] = Math.sqrt(variance);
          break;
        }
        case 'VARIANCE': case 'VAR_SAMP': {
          if (values.length < 2) { result[name] = null; break; }
          const nums = values.map(Number);
          const mean = nums.reduce((s, v) => s + v, 0) / nums.length;
          result[name] = nums.reduce((s, v) => s + (v - mean) ** 2, 0) / (nums.length - 1);
          break;
        }
        case 'VAR_POP': {
          if (!values.length) { result[name] = null; break; }
          const nums = values.map(Number);
          const mean = nums.reduce((s, v) => s + v, 0) / nums.length;
          result[name] = nums.reduce((s, v) => s + (v - mean) ** 2, 0) / nums.length;
          break;
        }
        case 'MEDIAN': {
          if (!values.length) { result[name] = null; break; }
          const sorted = values.map(Number).sort((a, b) => a - b);
          const mid = Math.floor(sorted.length / 2);
          result[name] = sorted.length % 2 === 0 ? (sorted[mid - 1] + sorted[mid]) / 2 : sorted[mid];
          break;
        }
        case 'GROUP_CONCAT': {
          const sep = col.separator || ',';
          result[name] = values.map(String).join(sep);
          break;
        }
      }
    }
    return result;
  }
}
