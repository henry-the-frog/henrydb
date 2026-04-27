// sql-functions.js — SQL built-in function evaluator
// Extracted from db.js to reduce monolith size (~700 LOC)

export function jsonExtract(obj, path) {
  if (!path || !path.startsWith('$')) return null;
  const parts = path.substring(1); // Remove leading $
  if (!parts) return obj;
  
  let current = obj;
  // Tokenize path: split on . and [] 
  const tokens = parts.match(/\.([^.\[\]]+)|\[(\d+)\]/g);
  if (!tokens) return obj;
  
  for (const token of tokens) {
    if (current == null) return null;
    if (token.startsWith('.')) {
      const key = token.substring(1);
      if (typeof current !== 'object' || Array.isArray(current)) return null;
      current = current[key];
    } else if (token.startsWith('[')) {
      const idx = parseInt(token.slice(1, -1), 10);
      if (!Array.isArray(current)) return null;
      current = current[idx];
    }
  }
  
  // Return primitives directly, objects as JSON strings
  if (current === null || current === undefined) return null;
  if (typeof current === 'object') return JSON.stringify(current);
  return current;
}

/**
 * Apply a SQLite-style date modifier to a Date object.
 * Modifiers: '+N days', '-N months', '+N years', '+N hours', '+N minutes', '+N seconds',
 *            'start of month', 'start of year', 'start of day', 'now', 'weekday N'
 */
function _applyDateModifier(d, mod) {
  // 'now' — replace with current time
  if (mod === 'now') return new Date();
  
  // 'start of ...'
  if (mod === 'start of month') {
    return new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), 1));
  }
  if (mod === 'start of year') {
    return new Date(Date.UTC(d.getUTCFullYear(), 0, 1));
  }
  if (mod === 'start of day') {
    return new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate()));
  }
  
  // 'localtime' / 'utc' — for now, treat as no-op (we always use UTC)
  if (mod === 'localtime' || mod === 'utc') return d;
  
  // 'weekday N' — advance to the next day that is weekday N (0=Sunday, 6=Saturday)
  const weekdayMatch = mod.match(/^weekday\s+(\d)$/);
  if (weekdayMatch) {
    const target = parseInt(weekdayMatch[1]);
    if (target < 0 || target > 6) return null;
    const result = new Date(d.getTime());
    const current = result.getUTCDay();
    let diff = target - current;
    if (diff <= 0) diff += 7; // Always advance (if already target day, go to next week)
    if (diff === 7 && current === target) diff = 0; // Same day stays
    result.setUTCDate(result.getUTCDate() + diff);
    return result;
  }
  
  // '+N unit' or '-N unit'
  const numMatch = mod.match(/^([+-])\s*(\d+)\s+(day|days|month|months|year|years|hour|hours|minute|minutes|second|seconds)$/);
  if (numMatch) {
    const sign = numMatch[1] === '+' ? 1 : -1;
    const n = parseInt(numMatch[2]) * sign;
    const unit = numMatch[3].replace(/s$/, ''); // normalize to singular
    const result = new Date(d.getTime());
    
    switch (unit) {
      case 'day':
        result.setUTCDate(result.getUTCDate() + n);
        break;
      case 'month':
        result.setUTCMonth(result.getUTCMonth() + n);
        break;
      case 'year':
        result.setUTCFullYear(result.getUTCFullYear() + n);
        break;
      case 'hour':
        result.setUTCHours(result.getUTCHours() + n);
        break;
      case 'minute':
        result.setUTCMinutes(result.getUTCMinutes() + n);
        break;
      case 'second':
        result.setUTCSeconds(result.getUTCSeconds() + n);
        break;
      default:
        return null;
    }
    return result;
  }
  
  return null; // Unknown modifier
}

export function dateArith(dateStr, intervalStr, op) {
  const d = new Date(String(dateStr));
  if (isNaN(d.getTime())) return null;
  const match = String(intervalStr).match(/^(\d+)\s*(year|month|day|hour|minute|second|week)s?$/i);
  if (!match) return null;
  const n = parseInt(match[1]) * (op === '-' ? -1 : 1);
  const unit = match[2].toLowerCase();
  switch (unit) {
    case 'year': d.setUTCFullYear(d.getUTCFullYear() + n); break;
    case 'month': d.setUTCMonth(d.getUTCMonth() + n); break;
    case 'day': d.setUTCDate(d.getUTCDate() + n); break;
    case 'week': d.setUTCDate(d.getUTCDate() + n * 7); break;
    case 'hour': d.setUTCHours(d.getUTCHours() + n); break;
    case 'minute': d.setUTCMinutes(d.getUTCMinutes() + n); break;
    case 'second': d.setUTCSeconds(d.getUTCSeconds() + n); break;
  }
  return d.toISOString();
}


export function likeToRegex(pattern, escapeChar) {
  let regex = '^';
  for (let i = 0; i < pattern.length; i++) {
    const ch = pattern[i];
    if (escapeChar && ch === escapeChar && i + 1 < pattern.length) {
      // Next character is literal (escaped)
      i++;
      regex += pattern[i].replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    } else if (ch === '%') {
      regex += '.*';
    } else if (ch === '_') {
      regex += '.';
    } else {
      regex += ch.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    }
  }
  regex += '$';
  return regex;
}

export function evalFunction(db, func, args, row) {
  // Check user-defined functions first
  const udfName = func.toLowerCase();
  if (db._functions && db._functions.has(udfName)) {
    const funcDef = db._functions.get(udfName);
    const evaluatedArgs = args.map(a => db._evalValue(a, row));
    return db._callUserFunction(funcDef, evaluatedArgs);
  }
  switch (func) {
    case 'UPPER': { const v = db._evalValue(args[0], row); return v != null ? String(v).toUpperCase() : null; }
    case 'LOWER': { const v = db._evalValue(args[0], row); return v != null ? String(v).toLowerCase() : null; }
    case 'INITCAP': { const v = db._evalValue(args[0], row); return v != null ? String(v).replace(/\w\S*/g, t => t.charAt(0).toUpperCase() + t.slice(1).toLowerCase()) : null; }
    case 'SPLIT_PART': {
      const str = db._evalValue(args[0], row);
      const delim = db._evalValue(args[1], row);
      const field = db._evalValue(args[2], row);
      if (str == null || delim == null || field == null) return null;
      const parts = String(str).split(String(delim));
      const idx = Number(field) - 1; // 1-indexed
      return idx >= 0 && idx < parts.length ? parts[idx] : '';
    }
    case 'OVERLAY': {
      // OVERLAY(string PLACING replacement FROM start FOR length)
      const str = String(db._evalValue(args[0], row) ?? '');
      const replacement = String(db._evalValue(args[1], row) ?? '');
      const start = Number(db._evalValue(args[2], row)) - 1; // 1-indexed
      const len = args.length > 3 ? Number(db._evalValue(args[3], row)) : replacement.length;
      return str.substring(0, start) + replacement + str.substring(start + len);
    }
    case 'TRANSLATE': {
      const str = String(db._evalValue(args[0], row) ?? '');
      const from = String(db._evalValue(args[1], row) ?? '');
      const to = String(db._evalValue(args[2], row) ?? '');
      let result = '';
      for (const ch of str) {
        const idx = from.indexOf(ch);
        if (idx >= 0) { if (idx < to.length) result += to[idx]; }
        else result += ch;
      }
      return result;
    }
    case 'CHR': return String.fromCharCode(Number(db._evalValue(args[0], row)));
    case 'ASCII': { const v = db._evalValue(args[0], row); return v != null ? String(v).charCodeAt(0) : null; }
    case 'MD5': {
      const v = db._evalValue(args[0], row);
      if (v == null) return null;
      // Simple hash (not cryptographic, just for compatibility)
      let hash = 0;
      const str = String(v);
      for (let i = 0; i < str.length; i++) {
        hash = ((hash << 5) - hash + str.charCodeAt(i)) | 0;
      }
      return Math.abs(hash).toString(16).padStart(8, '0');
    }
    case 'LENGTH': case 'CHAR_LENGTH': { const v = db._evalValue(args[0], row); return v != null ? String(v).length : null; }
    case 'POSITION': {
      const substr = String(db._evalValue(args[0], row));
      const str = String(db._evalValue(args[1], row));
      const idx = str.indexOf(substr);
      return idx === -1 ? 0 : idx + 1;
    }
    case 'CONCAT': return args.map(a => { const v = db._evalValue(a, row); return v != null ? String(v) : ''; }).join('');
    case 'CONCAT_OP': {
      // SQL || operator: NULL propagates
      const vals = args.map(a => db._evalValue(a, row));
      if (vals.some(v => v === null || v === undefined)) return null;
      return vals.map(v => String(v)).join('');
    }
    case 'COALESCE': {
      for (const arg of args) {
        const v = db._evalValue(arg, row);
        if (v !== null && v !== undefined) return v;
      }
      return null;
    }
    case 'EXTRACT': {
      const field = String(db._evalValue(args[0], row)).toUpperCase();
      const dateStr = String(db._evalValue(args[1], row));
      const d = new Date(dateStr);
      if (isNaN(d.getTime())) return null;
      switch (field) {
        case 'YEAR': return d.getUTCFullYear();
        case 'MONTH': return d.getUTCMonth() + 1;
        case 'DAY': return d.getUTCDate();
        case 'HOUR': return d.getUTCHours();
        case 'MINUTE': return d.getUTCMinutes();
        case 'SECOND': return d.getUTCSeconds();
        case 'DOW': return d.getUTCDay();
        case 'DOY': { const start = new Date(Date.UTC(d.getUTCFullYear(), 0, 0)); return Math.floor((d - start) / 86400000); }
        case 'EPOCH': return Math.floor(d.getTime() / 1000);
        case 'QUARTER': return Math.ceil((d.getMonth() + 1) / 3);
        case 'WEEK': { const start = new Date(d.getFullYear(), 0, 1); return Math.ceil(((d - start) / 86400000 + start.getDay() + 1) / 7); }
        default: return null;
      }
    }
    case 'NULLIF': {
      const a = db._evalValue(args[0], row);
      const b = db._evalValue(args[1], row);
      return a === b ? null : a;
    }
    case 'SUBSTR':
    case 'SUBSTRING': {
      const str = db._evalValue(args[0], row);
      if (str == null) return null;
      const s = String(str);
      let startVal = db._evalValue(args[1], row) || 1;
      const len = args[2] ? db._evalValue(args[2], row) : undefined;
      // SQLite: negative start means from the end
      let start;
      if (startVal < 0) {
        start = s.length + startVal;  // -3 → length-3
        if (start < 0) start = 0;
      } else {
        start = startVal - 1; // SQL is 1-indexed
      }
      return s.substring(start, len !== undefined ? start + len : undefined);
    }
    case 'REPLACE': {
      const str = db._evalValue(args[0], row);
      if (str == null) return null;
      const search = db._evalValue(args[1], row);
      const replace = db._evalValue(args[2], row);
      return String(str).replaceAll(String(search), String(replace));
    }
    case 'TRIM': {
      const str = db._evalValue(args[0], row);
      return str != null ? String(str).trim() : null;
    }
    case 'INSTR': {
      const str = db._evalValue(args[0], row);
      const search = db._evalValue(args[1], row);
      if (str == null || search == null) return null;
      const idx = String(str).indexOf(String(search));
      return idx === -1 ? 0 : idx + 1;  // SQL 1-indexed, 0 = not found
    }
    case 'PRINTF':
    case 'FORMAT': {
      // SQLite printf: %d (int), %f (float), %s (string), %% (literal %)
      const fmt = db._evalValue(args[0], row);
      if (fmt == null) return null;
      const fmtStr = String(fmt);
      let result = '';
      let argIdx = 1;
      for (let i = 0; i < fmtStr.length; i++) {
        if (fmtStr[i] === '%') {
          i++;
          if (i >= fmtStr.length) break;
          if (fmtStr[i] === '%') { result += '%'; continue; }
          // Parse flags, width, precision
          let flags = '';
          while ('-+ 0#'.includes(fmtStr[i])) { flags += fmtStr[i]; i++; }
          let width = '';
          while (fmtStr[i] >= '0' && fmtStr[i] <= '9') { width += fmtStr[i]; i++; }
          let precision = '';
          if (fmtStr[i] === '.') {
            i++;
            while (fmtStr[i] >= '0' && fmtStr[i] <= '9') { precision += fmtStr[i]; i++; }
          }
          const spec = fmtStr[i];
          const val = argIdx < args.length ? db._evalValue(args[argIdx], row) : null;
          argIdx++;
          let formatted = '';
          switch (spec) {
            case 'd': case 'i': formatted = String(parseInt(val) || 0); break;
            case 'f': {
              const prec = precision !== '' ? parseInt(precision) : 6;
              formatted = (parseFloat(val) || 0).toFixed(prec);
              break;
            }
            case 's': formatted = val != null ? String(val) : ''; break;
            case 'x': formatted = ((parseInt(val) || 0) >>> 0).toString(16); break;
            case 'X': formatted = ((parseInt(val) || 0) >>> 0).toString(16).toUpperCase(); break;
            case 'o': formatted = ((parseInt(val) || 0) >>> 0).toString(8); break;
            case 'c': formatted = String.fromCharCode(parseInt(val) || 0); break;
            default: formatted = '%' + spec; break;
          }
          // Apply width padding
          if (width) {
            const w = parseInt(width);
            const padChar = flags.includes('0') && !flags.includes('-') ? '0' : ' ';
            if (flags.includes('-')) {
              formatted = formatted.padEnd(w, ' ');
            } else {
              formatted = formatted.padStart(w, padChar);
            }
          }
          result += formatted;
        } else {
          result += fmtStr[i];
        }
      }
      return result;
    }
    case 'ABS': {
      const val = db._evalValue(args[0], row);
      return val != null ? Math.abs(val) : null;
    }
    case 'ROUND': {
      const val = db._evalValue(args[0], row);
      if (val == null) return null;
      const decimals = args[1] ? db._evalValue(args[1], row) : 0;
      const factor = Math.pow(10, decimals);
      return Math.round(val * factor) / factor;
    }
    case 'CEIL': {
      const val = db._evalValue(args[0], row);
      return val != null ? Math.ceil(val) : null;
    }
    case 'FLOOR': {
      const val = db._evalValue(args[0], row);
      return val != null ? Math.floor(val) : null;
    }
    case 'IFNULL':
    case 'ISNULL':
    case 'NVL': {
      const val = db._evalValue(args[0], row);
      return val != null ? val : db._evalValue(args[1], row);
    }
    case 'IIF': {
      // IIF(condition, true_val, false_val) — but condition is an expression
      const cond = db._evalExpr(args[0], row);
      return cond ? db._evalValue(args[1], row) : db._evalValue(args[2], row);
    }
    case 'TYPEOF': {
      const val = db._evalValue(args[0], row);
      if (val === null || val === undefined) return 'null';
      if (typeof val === 'number') return Number.isInteger(val) ? 'integer' : 'real';
      if (typeof val === 'string') return 'text';
      if (typeof val === 'boolean') return 'integer';
      return 'blob';
    }
    case 'JSON_EXTRACT': {
      const json = db._evalValue(args[0], row);
      const path = db._evalValue(args[1], row);
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
    case 'JSON_EXTRACT_TEXT': {
      // Same as JSON_EXTRACT but always returns text/string
      const json = db._evalValue(args[0], row);
      const path = db._evalValue(args[1], row);
      if (json == null) return null;
      try {
        const obj = typeof json === 'string' ? JSON.parse(json) : json;
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
        return current === undefined ? null : (typeof current === 'object' && current !== null ? JSON.stringify(current) : String(current));
      } catch { return null; }
    }
    case 'JSON_SET': {
      const json = db._evalValue(args[0], row);
      const path = db._evalValue(args[1], row);
      const value = db._evalValue(args[2], row);
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
      const json = db._evalValue(args[0], row);
      if (json == null) return null;
      try {
        const arr = typeof json === 'string' ? JSON.parse(json) : json;
        return Array.isArray(arr) ? arr.length : null;
      } catch { return null; }
    }
    case 'JSON_TYPE': {
      const json = db._evalValue(args[0], row);
      if (json == null) return 'null';
      try {
        const val = typeof json === 'string' ? JSON.parse(json) : json;
        if (Array.isArray(val)) return 'array';
        if (typeof val === 'object') return 'object';
        return typeof val;
      } catch { return 'text'; }
    }
    // json_build_object(key1, val1, key2, val2, ...) → JSON string
    case 'JSON_BUILD_OBJECT': {
      const obj = {};
      for (let i = 0; i < args.length; i += 2) {
        const key = db._evalValue(args[i], row);
        const val = i + 1 < args.length ? db._evalValue(args[i + 1], row) : null;
        obj[key] = val;
      }
      return JSON.stringify(obj);
    }
    // json_build_array(val1, val2, ...) → JSON array string
    case 'JSON_BUILD_ARRAY': {
      const arr = args.map(a => db._evalValue(a, row));
      return JSON.stringify(arr);
    }
    // row_to_json(row) — we return the entire row as JSON
    case 'ROW_TO_JSON': {
      return JSON.stringify(row);
    }
    // to_json(value) — convert value to JSON
    case 'TO_JSON': {
      const v = db._evalValue(args[0], row);
      return JSON.stringify(v);
    }
    // json_object_keys(json) — for aggregate context
    case 'JSON_OBJECT_KEYS': {
      const json = db._evalValue(args[0], row);
      if (json == null) return null;
      try {
        const obj = typeof json === 'string' ? JSON.parse(json) : json;
        return JSON.stringify(Object.keys(obj));
      } catch { return null; }
    }
    // String functions
    case 'LEFT': { const v = db._evalValue(args[0], row); return v == null ? null : String(v).substring(0, db._evalValue(args[1], row)); }
    case 'RIGHT': { const v = db._evalValue(args[0], row); const n = db._evalValue(args[1], row); return v == null ? null : String(v).slice(-n); }
    case 'LPAD': {
      const str = String(db._evalValue(args[0], row) || '');
      const len = db._evalValue(args[1], row) || 0;
      const pad = args[2] ? String(db._evalValue(args[2], row)) : ' ';
      return str.length > len ? str.slice(0, len) : str.padStart(len, pad);
    }
    case 'RPAD': {
      const str = String(db._evalValue(args[0], row) || '');
      const len = db._evalValue(args[1], row) || 0;
      const pad = args[2] ? String(db._evalValue(args[2], row)) : ' ';
      return str.length > len ? str.slice(0, len) : str.padEnd(len, pad);
    }
    case 'REVERSE': { const v = db._evalValue(args[0], row); return v == null ? null : String(v).split('').reverse().join(''); }
    case 'REPEAT': { const v = db._evalValue(args[0], row); const n = db._evalValue(args[1], row); return v == null ? null : String(v).repeat(n || 0); }
    
    // Math functions
    case 'POWER': return Math.pow(db._evalValue(args[0], row), db._evalValue(args[1], row));
    case 'SQRT': return Math.sqrt(db._evalValue(args[0], row));
    case 'LOG': return args.length > 1 ? Math.log(db._evalValue(args[1], row)) / Math.log(db._evalValue(args[0], row)) : Math.log(db._evalValue(args[0], row));
    case 'EXP': return Math.exp(db._evalValue(args[0], row));
    case 'RANDOM': {
      // SQLite random() returns a random integer between -9223372036854775808 and +9223372036854775807
      // In JS we return a random 32-bit signed integer
      return (Math.random() * 4294967296 - 2147483648) | 0;
    }
    case 'GEN_RANDOM_UUID': case 'UUID': {
      // Generate UUID v4
      const hex = 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx';
      return hex.replace(/[xy]/g, c => {
        const r = Math.random() * 16 | 0;
        return (c === 'x' ? r : (r & 0x3 | 0x8)).toString(16);
      });
    }
    case 'GREATEST': { const vals = args.map(a => db._evalValue(a, row)).filter(v => v != null); return vals.length ? Math.max(...vals.map(Number)) : null; }
    case 'LEAST': { const vals = args.map(a => db._evalValue(a, row)).filter(v => v != null); return vals.length ? Math.min(...vals.map(Number)) : null; }
    case 'MOD': { const a = Number(db._evalValue(args[0], row)); const b = Number(db._evalValue(args[1], row)); return b === 0 ? null : a % b; }
    case 'LN': return Math.log(db._evalValue(args[0], row));
    case 'LOG2': return Math.log2(db._evalValue(args[0], row));
    case 'LOG10': return Math.log10(db._evalValue(args[0], row));
    case 'SIGN': { const v = Number(db._evalValue(args[0], row)); return v > 0 ? 1 : v < 0 ? -1 : 0; }
    case 'PI': return Math.PI;
    case 'DEGREES': return db._evalValue(args[0], row) * (180 / Math.PI);
    case 'RADIANS': return db._evalValue(args[0], row) * (Math.PI / 180);
    case 'SIN': return Math.sin(db._evalValue(args[0], row));
    case 'COS': return Math.cos(db._evalValue(args[0], row));
    case 'TAN': return Math.tan(db._evalValue(args[0], row));
    case 'ASIN': return Math.asin(db._evalValue(args[0], row));
    case 'ACOS': return Math.acos(db._evalValue(args[0], row));
    case 'ATAN': return Math.atan(db._evalValue(args[0], row));
    case 'ATAN2': return Math.atan2(db._evalValue(args[0], row), db._evalValue(args[1], row));
    case 'LTRIM': { const v = db._evalValue(args[0], row); return v == null ? null : String(v).trimStart(); }
    case 'RTRIM': { const v = db._evalValue(args[0], row); return v == null ? null : String(v).trimEnd(); }
    
    // Regex functions
    case 'REGEXP_MATCHES': {
      const str = db._evalValue(args[0], row);
      const pattern = db._evalValue(args[1], row);
      if (str == null || pattern == null) return null;
      const flags = args[2] ? String(db._evalValue(args[2], row)) : '';
      try {
        const re = new RegExp(String(pattern), flags);
        const match = String(str).match(re);
        if (!match) return null;
        // If global flag, return all matches
        if (flags.includes('g')) {
          return [...String(str).matchAll(new RegExp(String(pattern), flags))].map(m => m[0]);
        }
        // Return capture groups (or full match if no groups)
        return match.length > 1 ? match.slice(1) : [match[0]];
      } catch (e) {
        throw new Error(`Invalid regex pattern: ${pattern}`);
      }
    }
    case 'REGEXP_REPLACE': {
      const str = db._evalValue(args[0], row);
      const pattern = db._evalValue(args[1], row);
      const replacement = db._evalValue(args[2], row);
      if (str == null) return null;
      const flags = args[3] ? String(db._evalValue(args[3], row)) : '';
      try {
        return String(str).replace(new RegExp(String(pattern), flags), String(replacement || ''));
      } catch (e) {
        throw new Error(`Invalid regex pattern: ${pattern}`);
      }
    }
    case 'REGEXP_COUNT': {
      const str = db._evalValue(args[0], row);
      const pattern = db._evalValue(args[1], row);
      if (str == null || pattern == null) return 0;
      const flags = args[2] ? String(db._evalValue(args[2], row)) : 'g';
      try {
        const matches = String(str).match(new RegExp(String(pattern), flags.includes('g') ? flags : flags + 'g'));
        return matches ? matches.length : 0;
      } catch (e) {
        throw new Error(`Invalid regex pattern: ${pattern}`);
      }
    }
    
    // Date/time functions
    case 'CURRENT_TIMESTAMP': case 'NOW': return new Date().toISOString();
    case 'CURRENT_TIME': return new Date().toISOString().split('T')[1].replace('Z', '');
    case 'DATE_PART': {
      const part = db._evalValue(args[0], row);
      const dateStr = String(db._evalValue(args[1], row));
      const d = new Date(dateStr);
      switch (String(part).toLowerCase()) {
        case 'year': return d.getUTCFullYear();
        case 'month': return d.getUTCMonth() + 1;
        case 'day': return d.getUTCDate();
        case 'hour': return d.getUTCHours();
        case 'minute': return d.getUTCMinutes();
        case 'second': return d.getUTCSeconds();
        case 'dow': case 'dayofweek': return d.getUTCDay();
        case 'doy': case 'dayofyear': {
          const start = new Date(Date.UTC(d.getUTCFullYear(), 0, 0));
          return Math.floor((d - start) / 86400000);
        }
        case 'epoch': return Math.floor(d.getTime() / 1000);
        default: return null;
      }
    }
    case 'CURRENT_DATE': return new Date().toISOString().split('T')[0];
    case 'NEXTVAL': {
      const seqName = String(db._evalValue(args[0], row)).toLowerCase();
      const seq = db.sequences.get(seqName);
      if (!seq) throw new Error(`Sequence ${seqName} not found`);
      seq.current += seq.increment;
      return seq.current;
    }
    case 'CURRVAL': {
      const seqName = String(db._evalValue(args[0], row)).toLowerCase();
      const seq = db.sequences.get(seqName);
      if (!seq) throw new Error(`Sequence ${seqName} not found`);
      return seq.current;
    }
    case 'SETVAL': {
      const seqName = String(db._evalValue(args[0], row)).toLowerCase();
      const seq = db.sequences.get(seqName);
      if (!seq) throw new Error(`Sequence ${seqName} not found`);
      seq.current = db._evalValue(args[1], row);
      return seq.current;
    }
    case 'DATE_ADD': {
      // DATE_ADD(date, interval, unit)
      const date = db._evalValue(args[0], row);
      const interval = db._evalValue(args[1], row);
      const unit = (db._evalValue(args[2], row) || 'day').toLowerCase();
      const d = new Date(date + 'T00:00:00Z');
      switch (unit) {
        case 'day': case 'days': d.setUTCDate(d.getUTCDate() + interval); break;
        case 'month': case 'months': d.setUTCMonth(d.getUTCMonth() + interval); break;
        case 'year': case 'years': d.setUTCFullYear(d.getUTCFullYear() + interval); break;
        case 'hour': case 'hours': d.setUTCHours(d.getUTCHours() + interval); break;
        default: throw new Error(`Unknown date unit: ${unit}`);
      }
      return d.toISOString().split('T')[0];
    }
    case 'DATE_DIFF': {
      // DATE_DIFF(date1, date2, unit) — returns date1 - date2
      const d1 = new Date(db._evalValue(args[0], row));
      const d2 = new Date(db._evalValue(args[1], row));
      const unit = (db._evalValue(args[2], row) || 'day').toLowerCase();
      const diffMs = d1 - d2;
      switch (unit) {
        case 'day': case 'days': return Math.floor(diffMs / 86400000);
        case 'hour': case 'hours': return Math.floor(diffMs / 3600000);
        case 'month': case 'months': return (d1.getFullYear() - d2.getFullYear()) * 12 + d1.getMonth() - d2.getMonth();
        case 'year': case 'years': return d1.getFullYear() - d2.getFullYear();
        default: throw new Error(`Unknown date unit: ${unit}`);
      }
    }
    case 'REGEXP_MATCHES': {
      const str = String(db._evalValue(args[0], row));
      const pattern = String(db._evalValue(args[1], row));
      const flags = args.length > 2 ? String(db._evalValue(args[2], row)) : '';
      const regex = new RegExp(pattern, flags.includes('g') ? 'g' : '');
      const matches = str.match(regex);
      return matches ? JSON.stringify(matches) : null;
    }
    case 'DATE': {
      // DATE(value, modifier1, modifier2, ...) — SQLite-compatible date function
      // Modifiers: '+N days', '-N months', '+N years', '+N hours', '+N minutes', '+N seconds',
      //            'start of month', 'start of year', 'start of day', 'now', 'localtime', 'utc'
      let v = db._evalValue(args[0], row);
      if (v == null) return null;
      let s = String(v);
      
      // Handle 'now' as first argument
      if (s.toLowerCase() === 'now') s = new Date().toISOString();
      
      // Parse initial date
      const dateMatch = s.match(/^(\d{4}-\d{2}-\d{2})/);
      let d;
      if (dateMatch) {
        d = new Date(dateMatch[1] + 'T00:00:00Z');
      } else {
        d = new Date(s);
      }
      if (isNaN(d.getTime())) return null;
      
      // Apply modifiers (args[1], args[2], ...)
      for (let i = 1; i < args.length; i++) {
        const mod = String(db._evalValue(args[i], row)).trim().toLowerCase();
        d = _applyDateModifier(d, mod);
        if (!d || isNaN(d.getTime())) return null;
      }
      
      return d.toISOString().split('T')[0];
    }
    case 'TIME': {
      // TIME(value, modifier1, modifier2, ...) — SQLite-compatible time function
      // Returns HH:MM:SS
      let v = db._evalValue(args[0], row);
      if (v == null) return null;
      let s = String(v);
      if (s.toLowerCase() === 'now') s = new Date().toISOString();
      
      let d;
      // Parse time from various formats
      const timeOnly = s.match(/^(\d{2}):(\d{2}):(\d{2})/);
      if (timeOnly) {
        d = new Date(Date.UTC(2000, 0, 1, parseInt(timeOnly[1]), parseInt(timeOnly[2]), parseInt(timeOnly[3])));
      } else {
        d = new Date(s.includes('T') ? s : s + 'T00:00:00Z');
      }
      if (isNaN(d.getTime())) return null;
      
      for (let i = 1; i < args.length; i++) {
        const mod = String(db._evalValue(args[i], row)).trim().toLowerCase();
        d = _applyDateModifier(d, mod);
        if (!d || isNaN(d.getTime())) return null;
      }
      
      return d.toISOString().split('T')[1].replace('Z', '').replace(/\.\d+$/, '');
    }
    case 'DATETIME': {
      // DATETIME(value, modifier1, modifier2, ...) — SQLite-compatible datetime function
      // Returns YYYY-MM-DD HH:MM:SS
      let v = db._evalValue(args[0], row);
      if (v == null) return null;
      let s = String(v);
      if (s.toLowerCase() === 'now') s = new Date().toISOString();
      
      const dateMatch = s.match(/^(\d{4}-\d{2}-\d{2})/);
      let d;
      if (dateMatch && (s.includes('T') || s.includes(' '))) {
        // Handle both ISO format and space-separated: '2024-01-15 10:30:00'
        d = new Date(s.replace(' ', 'T') + (s.includes('Z') ? '' : 'Z'));
      } else if (dateMatch) {
        d = new Date(s + 'T00:00:00Z');
      } else {
        d = new Date(s);
      }
      if (isNaN(d.getTime())) return null;
      
      for (let i = 1; i < args.length; i++) {
        const mod = String(db._evalValue(args[i], row)).trim().toLowerCase();
        d = _applyDateModifier(d, mod);
        if (!d || isNaN(d.getTime())) return null;
      }
      
      const iso = d.toISOString();
      return iso.split('T')[0] + ' ' + iso.split('T')[1].replace('Z', '').replace(/\.\d+$/, '');
    }
    case 'AGE': {
      // AGE(date1, date2) — interval between two dates (PG-style)
      // AGE(date) — interval from date to CURRENT_DATE
      const d1 = args.length >= 2
        ? new Date(String(db._evalValue(args[0], row)))
        : new Date();
      const d2 = args.length >= 2
        ? new Date(String(db._evalValue(args[1], row)))
        : new Date(String(db._evalValue(args[0], row)));
      if (isNaN(d1.getTime()) || isNaN(d2.getTime())) return null;
      let years = d1.getUTCFullYear() - d2.getUTCFullYear();
      let months = d1.getUTCMonth() - d2.getUTCMonth();
      let days = d1.getUTCDate() - d2.getUTCDate();
      if (days < 0) { months--; days += 30; }
      if (months < 0) { years--; months += 12; }
      const parts = [];
      if (years) parts.push(`${years} year${years !== 1 ? 's' : ''}`);
      if (months) parts.push(`${months} mon${months !== 1 ? 's' : ''}`);
      if (days || parts.length === 0) parts.push(`${days} day${days !== 1 ? 's' : ''}`);
      return parts.join(' ');
    }
    case 'TO_CHAR': {
      // TO_CHAR(value, format) — format a number or date as string
      const val = db._evalValue(args[0], row);
      const fmt = args.length > 1 ? String(db._evalValue(args[1], row)) : '';
      if (val == null) return null;
      // Try as date first
      const d = new Date(String(val));
      if (!isNaN(d.getTime()) && typeof val === 'string' && val.includes('-')) {
        // Date formatting
        return fmt
          .replace('YYYY', String(d.getUTCFullYear()))
          .replace('YY', String(d.getUTCFullYear()).slice(-2))
          .replace('MM', String(d.getUTCMonth() + 1).padStart(2, '0'))
          .replace('DD', String(d.getUTCDate()).padStart(2, '0'))
          .replace('HH24', String(d.getUTCHours()).padStart(2, '0'))
          .replace('HH', String(d.getUTCHours() % 12 || 12).padStart(2, '0'))
          .replace('MI', String(d.getUTCMinutes()).padStart(2, '0'))
          .replace('SS', String(d.getUTCSeconds()).padStart(2, '0'))
          .replace('Month', d.toLocaleString('en-US', { month: 'long', timeZone: 'UTC' }))
          .replace('Mon', d.toLocaleString('en-US', { month: 'short', timeZone: 'UTC' }))
          .replace('Day', d.toLocaleString('en-US', { weekday: 'long', timeZone: 'UTC' }))
          .replace('Dy', d.toLocaleString('en-US', { weekday: 'short', timeZone: 'UTC' }));
      }
      // Number formatting
      const num = Number(val);
      if (!isNaN(num)) {
        if (fmt.includes(',')) {
          // Thousands separator
          const fmtDigits = fmt.replace(/[^9]/g, '').length;
          const formatted = num.toLocaleString('en-US', { minimumIntegerDigits: 1, maximumFractionDigits: 0 });
          return formatted.padStart(fmt.length);
        }
        if (fmt.includes('.')) {
          const decimals = fmt.split('.')[1]?.length || 0;
          return num.toFixed(decimals);
        }
        return String(num).padStart(fmt.length);
      }
      return String(val);
    }
    case 'DATE_FORMAT': {
      // DATE_FORMAT(date, format) — alias for TO_CHAR for dates
      return evalFunction(db, 'TO_CHAR', args, row);
    }
    case 'MAKE_DATE': {
      // MAKE_DATE(year, month, day)
      const y = db._evalValue(args[0], row);
      const m = db._evalValue(args[1], row);
      const d = db._evalValue(args[2], row);
      return `${y}-${String(m).padStart(2, '0')}-${String(d).padStart(2, '0')}`;
    }
    case 'MAKE_TIMESTAMP': {
      // MAKE_TIMESTAMP(year, month, day, hour, min, sec)
      const y = db._evalValue(args[0], row);
      const mo = db._evalValue(args[1], row);
      const d = db._evalValue(args[2], row);
      const h = args.length > 3 ? db._evalValue(args[3], row) : 0;
      const mi = args.length > 4 ? db._evalValue(args[4], row) : 0;
      const s = args.length > 5 ? db._evalValue(args[5], row) : 0;
      const dt = new Date(Date.UTC(y, mo - 1, d, h, mi, s));
      return dt.toISOString();
    }
    case 'EPOCH': case 'TO_TIMESTAMP': {
      // TO_TIMESTAMP(epoch_seconds) or EPOCH(date)
      if (func === 'EPOCH') {
        const v = db._evalValue(args[0], row);
        const d = new Date(String(v));
        return isNaN(d.getTime()) ? null : Math.floor(d.getTime() / 1000);
      }
      const epoch = db._evalValue(args[0], row);
      return new Date(epoch * 1000).toISOString();
    }
    case 'DATE_TRUNC': {
      // DATE_TRUNC(unit, date)
      const unit = (db._evalValue(args[0], row) || 'day').toLowerCase();
      const dateVal = db._evalValue(args[1], row);
      const dateStr = String(dateVal);
      // Parse as UTC to avoid timezone issues
      const d = dateStr.includes('T') ? new Date(dateStr) : new Date(dateStr + 'T00:00:00Z');
      if (isNaN(d.getTime())) return null;
      switch (unit) {
        case 'year': return `${d.getUTCFullYear()}-01-01`;
        case 'month': return `${d.getUTCFullYear()}-${String(d.getUTCMonth() + 1).padStart(2, '0')}-01`;
        case 'day': return d.toISOString().split('T')[0];
        case 'hour': { const iso = d.toISOString(); return iso.slice(0, 14) + '00:00.000Z'; }
        case 'minute': { const iso = d.toISOString(); return iso.slice(0, 17) + '00.000Z'; }
        case 'week': { const dayOfWeek = d.getUTCDay(); d.setUTCDate(d.getUTCDate() - dayOfWeek); return d.toISOString().split('T')[0]; }
        case 'quarter': { const q = Math.floor(d.getUTCMonth() / 3) * 3; return `${d.getUTCFullYear()}-${String(q + 1).padStart(2, '0')}-01`; }
        default: throw new Error(`Unknown date trunc unit: ${unit}`);
      }
    }
    case 'STRFTIME': {
      const fmt = db._evalValue(args[0], row);
      const dateStr = args[1] ? db._evalValue(args[1], row) : new Date().toISOString();
      const d = new Date(dateStr);
      return String(fmt)
        .replace('%Y', String(d.getUTCFullYear()))
        .replace('%m', String(d.getUTCMonth() + 1).padStart(2, '0'))
        .replace('%d', String(d.getUTCDate()).padStart(2, '0'))
        .replace('%H', String(d.getUTCHours()).padStart(2, '0'))
        .replace('%M', String(d.getUTCMinutes()).padStart(2, '0'))
        .replace('%S', String(d.getUTCSeconds()).padStart(2, '0'));
    }
    
    // JSON functions
    case 'JSON_EXTRACT': case 'JSON_VALUE': {
      const jsonStr = db._evalValue(args[0], row);
      const path = db._evalValue(args[1], row);
      if (jsonStr == null || path == null) return null;
      try {
        const obj = typeof jsonStr === 'object' ? jsonStr : JSON.parse(String(jsonStr));
        return jsonExtract(obj, String(path));
      } catch (e) { return null; }
    }
    case 'JSON_ARRAY_LENGTH': {
      const jsonStr = db._evalValue(args[0], row);
      if (jsonStr == null) return null;
      try {
        const arr = typeof jsonStr === 'object' ? jsonStr : JSON.parse(String(jsonStr));
        return Array.isArray(arr) ? arr.length : null;
      } catch (e) { return null; }
    }
    case 'JSON_TYPE': {
      const jsonStr = db._evalValue(args[0], row);
      if (jsonStr == null) return 'null';
      try {
        const val = typeof jsonStr === 'object' ? jsonStr : JSON.parse(String(jsonStr));
        if (Array.isArray(val)) return 'array';
        if (val === null) return 'null';
        return typeof val; // 'object', 'number', 'string', 'boolean'
      } catch (e) { return 'text'; }
    }
    case 'JSON_OBJECT': {
      // JSON_OBJECT('key1', val1, 'key2', val2, ...)
      const obj = {};
      for (let i = 0; i < args.length; i += 2) {
        const key = String(db._evalValue(args[i], row));
        const val = i + 1 < args.length ? db._evalValue(args[i + 1], row) : null;
        obj[key] = val;
      }
      return JSON.stringify(obj);
    }
    case 'JSON_ARRAY': {
      const arr = args.map(a => db._evalValue(a, row));
      return JSON.stringify(arr);
    }
    case 'JSON_VALID': {
      const jsonStr = db._evalValue(args[0], row);
      if (jsonStr == null) return 0;
      try { JSON.parse(String(jsonStr)); return 1; } catch (e) { return 0; }
    }
    
    // Full-text search functions — return the text for TS_MATCH to process
    case 'TO_TSVECTOR': {
      const text = db._evalValue(args[0], row);
      return text != null ? String(text) : null;
    }
    case 'TO_TSQUERY': case 'PLAINTO_TSQUERY': case 'PHRASETO_TSQUERY': {
      const query = db._evalValue(args[0], row);
      return query != null ? String(query) : null;
    }
    
    default: throw new Error(`Unknown function: ${func}`);
  }
}

