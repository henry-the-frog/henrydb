// jsonpath.js — JSON Path query engine for HenryDB
// Implements a subset of SQL/JSON Path (RFC 9535 / PostgreSQL jsonpath)
// Supports: $, .key, [index], [*], .*, .., ?(filter), comparison operators

/**
 * JSONPath — parse and evaluate JSON path expressions.
 * 
 * Syntax:
 *   $              — root object
 *   $.key          — member access
 *   $[0]           — array index
 *   $[*]           — all array elements
 *   $.*            — all object values
 *   $..key         — recursive descent
 *   $[?(@.x > 5)] — filter expression
 *   $.a.b.c        — chained access
 */
export class JSONPath {
  /**
   * Parse a JSON path string into a compiled path.
   */
  static parse(path) {
    return new CompiledPath(tokenizePath(path));
  }

  /**
   * Query a JSON document with a path expression.
   * Returns an array of matching values.
   */
  static query(doc, path) {
    const compiled = typeof path === 'string' ? JSONPath.parse(path) : path;
    return compiled.evaluate(doc);
  }

  /**
   * Query and return the first match, or undefined.
   */
  static first(doc, path) {
    const results = JSONPath.query(doc, path);
    return results.length > 0 ? results[0] : undefined;
  }

  /**
   * Check if a path matches any values in the document.
   */
  static exists(doc, path) {
    return JSONPath.query(doc, path).length > 0;
  }

  /**
   * Check if doc contains other (PostgreSQL @> operator).
   */
  static contains(doc, other) {
    if (typeof doc !== typeof other) return false;
    if (Array.isArray(doc) && Array.isArray(other)) {
      return other.every(item => doc.some(d => JSONPath.contains(d, item)));
    }
    if (typeof doc === 'object' && doc !== null && typeof other === 'object' && other !== null) {
      return Object.keys(other).every(key => 
        key in doc && JSONPath.contains(doc[key], other[key])
      );
    }
    return doc === other;
  }
}

class CompiledPath {
  constructor(segments) {
    this.segments = segments;
  }

  evaluate(doc) {
    let current = [doc];

    for (const seg of this.segments) {
      const next = [];
      for (const val of current) {
        next.push(...this._apply(seg, val));
      }
      current = next;
    }

    return current;
  }

  _apply(seg, val) {
    switch (seg.type) {
      case 'root':
        return [val];

      case 'member':
        if (val && typeof val === 'object' && !Array.isArray(val)) {
          return seg.name in val ? [val[seg.name]] : [];
        }
        return [];

      case 'index':
        if (Array.isArray(val)) {
          const idx = seg.index < 0 ? val.length + seg.index : seg.index;
          return idx >= 0 && idx < val.length ? [val[idx]] : [];
        }
        return [];

      case 'wildcard':
        if (Array.isArray(val)) return [...val];
        if (val && typeof val === 'object') return Object.values(val);
        return [];

      case 'recursive': {
        const results = [];
        this._recursiveSearch(val, seg.name, results);
        return results;
      }

      case 'filter':
        if (Array.isArray(val)) {
          return val.filter(item => this._evalFilter(seg.expression, item));
        }
        return [];

      case 'slice': {
        if (!Array.isArray(val)) return [];
        const start = seg.start ?? 0;
        const end = seg.end ?? val.length;
        const step = seg.step ?? 1;
        const results = [];
        if (step > 0) {
          for (let i = start; i < end && i < val.length; i += step) {
            if (i >= 0) results.push(val[i]);
          }
        }
        return results;
      }

      default:
        return [];
    }
  }

  _recursiveSearch(val, name, results) {
    if (!val || typeof val !== 'object') return;

    if (!Array.isArray(val) && name in val) {
      results.push(val[name]);
    }

    if (Array.isArray(val)) {
      for (const item of val) {
        this._recursiveSearch(item, name, results);
      }
    } else {
      for (const v of Object.values(val)) {
        this._recursiveSearch(v, name, results);
      }
    }
  }

  _evalFilter(expr, item) {
    try {
      if (expr.type === 'comparison') {
        const left = this._resolveFilterValue(expr.left, item);
        const right = this._resolveFilterValue(expr.right, item);
        switch (expr.op) {
          case '==': return left == right;
          case '!=': return left != right;
          case '>': return left > right;
          case '<': return left < right;
          case '>=': return left >= right;
          case '<=': return left <= right;
        }
      }
      if (expr.type === 'exists') {
        const path = JSONPath.parse(expr.path.replace(/^@/, '$'));
        return path.evaluate(item).length > 0;
      }
      if (expr.type === 'and') {
        return this._evalFilter(expr.left, item) && this._evalFilter(expr.right, item);
      }
      if (expr.type === 'or') {
        return this._evalFilter(expr.left, item) || this._evalFilter(expr.right, item);
      }
      if (expr.type === 'not') {
        return !this._evalFilter(expr.operand, item);
      }
      return false;
    } catch {
      return false;
    }
  }

  _resolveFilterValue(val, item) {
    if (val.type === 'literal') return val.value;
    if (val.type === 'path') {
      // @ references current item
      const path = val.value.replace(/^@\.?/, '');
      if (!path) return item;
      const parts = path.split('.');
      let current = item;
      for (const part of parts) {
        if (current && typeof current === 'object') {
          current = current[part];
        } else {
          return undefined;
        }
      }
      return current;
    }
    return val;
  }
}

/**
 * Tokenize a JSON path string into segments.
 */
function tokenizePath(path) {
  const segments = [];
  let i = 0;

  if (path[i] === '$') {
    segments.push({ type: 'root' });
    i++;
  }

  while (i < path.length) {
    // Recursive descent: ..key
    if (path[i] === '.' && path[i + 1] === '.') {
      i += 2;
      let name = '';
      while (i < path.length && /[a-zA-Z0-9_]/.test(path[i])) name += path[i++];
      segments.push({ type: 'recursive', name });
      continue;
    }

    // Member access: .key or .*
    if (path[i] === '.') {
      i++;
      if (path[i] === '*') {
        segments.push({ type: 'wildcard' });
        i++;
      } else {
        let name = '';
        while (i < path.length && /[a-zA-Z0-9_]/.test(path[i])) name += path[i++];
        segments.push({ type: 'member', name });
      }
      continue;
    }

    // Bracket notation: [index], [*], [start:end], [?(filter)]
    if (path[i] === '[') {
      i++;

      // Skip whitespace
      while (path[i] === ' ') i++;

      if (path[i] === '*') {
        segments.push({ type: 'wildcard' });
        i++;
        while (path[i] === ' ') i++;
        i++; // skip ]
        continue;
      }

      if (path[i] === '?') {
        i++; // skip ?
        if (path[i] === '(') i++; // skip (

        const filterExpr = parseFilterExpression(path, i);
        segments.push({ type: 'filter', expression: filterExpr.expr });
        i = filterExpr.end;

        while (i < path.length && path[i] !== ']') i++;
        i++; // skip ]
        continue;
      }

      // Number index or slice
      let numStr = '';
      if (path[i] === '-') { numStr += '-'; i++; }
      while (i < path.length && /\d/.test(path[i])) numStr += path[i++];

      if (path[i] === ':') {
        // Slice: [start:end:step]
        const start = numStr ? parseInt(numStr) : 0;
        i++; // skip :
        let endStr = '';
        while (i < path.length && /\d/.test(path[i])) endStr += path[i++];
        let step = 1;
        if (path[i] === ':') {
          i++;
          let stepStr = '';
          while (i < path.length && /\d/.test(path[i])) stepStr += path[i++];
          step = parseInt(stepStr) || 1;
        }
        segments.push({ type: 'slice', start, end: endStr ? parseInt(endStr) : undefined, step });
      } else {
        segments.push({ type: 'index', index: parseInt(numStr) });
      }

      while (i < path.length && path[i] !== ']') i++;
      i++; // skip ]
      continue;
    }

    i++; // skip unknown chars
  }

  return segments;
}

function parseFilterExpression(path, start) {
  let i = start;
  // Parse simple comparison: @.field op value
  let left = parseFilterOperand(path, i);
  i = left.end;

  while (path[i] === ' ') i++;

  // Check for comparison operator
  let op = '';
  if (path[i] === '=' && path[i + 1] === '=') { op = '=='; i += 2; }
  else if (path[i] === '!' && path[i + 1] === '=') { op = '!='; i += 2; }
  else if (path[i] === '>' && path[i + 1] === '=') { op = '>='; i += 2; }
  else if (path[i] === '<' && path[i + 1] === '=') { op = '<='; i += 2; }
  else if (path[i] === '>') { op = '>'; i++; }
  else if (path[i] === '<') { op = '<'; i++; }

  if (!op) {
    // Just an exists check
    return {
      expr: { type: 'exists', path: left.value },
      end: i,
    };
  }

  while (path[i] === ' ') i++;
  let right = parseFilterOperand(path, i);
  i = right.end;

  while (path[i] === ' ' || path[i] === ')') i++;

  return {
    expr: {
      type: 'comparison',
      op,
      left: { type: left.type, value: left.value },
      right: { type: right.type, value: right.value },
    },
    end: i,
  };
}

function parseFilterOperand(path, start) {
  let i = start;
  while (path[i] === ' ') i++;

  // @ path reference
  if (path[i] === '@') {
    let val = '';
    while (i < path.length && ![')', ']', ' ', '=', '!', '>', '<'].includes(path[i])) {
      val += path[i++];
    }
    return { type: 'path', value: val, end: i };
  }

  // String literal
  if (path[i] === '"' || path[i] === "'") {
    const quote = path[i++];
    let val = '';
    while (i < path.length && path[i] !== quote) val += path[i++];
    i++; // skip closing quote
    return { type: 'literal', value: val, end: i };
  }

  // Number
  let numStr = '';
  if (path[i] === '-') { numStr += '-'; i++; }
  while (i < path.length && /[\d.]/.test(path[i])) numStr += path[i++];
  if (numStr) {
    return { type: 'literal', value: parseFloat(numStr), end: i };
  }

  // Boolean/null
  const rest = path.substring(i, i + 5);
  if (rest.startsWith('true')) return { type: 'literal', value: true, end: i + 4 };
  if (rest.startsWith('false')) return { type: 'literal', value: false, end: i + 5 };
  if (rest.startsWith('null')) return { type: 'literal', value: null, end: i + 4 };

  return { type: 'literal', value: null, end: i };
}
