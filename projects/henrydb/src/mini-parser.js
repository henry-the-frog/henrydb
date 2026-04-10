// mini-parser.js — Lightweight SQL parser for the Query VM
//
// Parses a subset of SQL into an AST compatible with QueryCompiler.
// Supports: SELECT, WHERE (simple conditions), GROUP BY, aggregate functions.
//
// Not a full SQL parser — just enough to feed the bytecode VM.

const KEYWORDS = new Set([
  'SELECT', 'FROM', 'WHERE', 'GROUP', 'BY', 'ORDER', 'ASC', 'DESC',
  'AND', 'OR', 'NOT', 'AS', 'LIMIT', 'HAVING', 'JOIN', 'ON',
  'INSERT', 'INTO', 'VALUES', 'UPDATE', 'SET', 'DELETE',
  'COUNT', 'SUM', 'AVG', 'MIN', 'MAX', 'DISTINCT',
  'NULL', 'TRUE', 'FALSE', 'IS', 'IN', 'LIKE', 'BETWEEN',
]);

const AGG_FUNCS = new Set(['COUNT', 'SUM', 'AVG', 'MIN', 'MAX']);

class Token {
  constructor(type, value) {
    this.type = type;
    this.value = value;
  }
}

/**
 * Tokenize SQL string.
 */
function tokenize(sql) {
  const tokens = [];
  let i = 0;
  const s = sql.trim();
  
  while (i < s.length) {
    // Skip whitespace
    if (/\s/.test(s[i])) { i++; continue; }
    
    // Single-char tokens
    if ('(),*'.includes(s[i])) {
      tokens.push(new Token(s[i], s[i]));
      i++;
      continue;
    }
    
    // Comparison operators
    if (s[i] === '>' || s[i] === '<' || s[i] === '!' || s[i] === '=') {
      if (i + 1 < s.length && s[i + 1] === '=') {
        tokens.push(new Token('OP', s[i] + '='));
        i += 2;
      } else if (s[i] === '!' && s[i + 1] === '>') {
        tokens.push(new Token('OP', '<='));
        i += 2;
      } else {
        tokens.push(new Token('OP', s[i]));
        i++;
      }
      continue;
    }
    
    // Numbers
    if (/\d/.test(s[i]) || (s[i] === '-' && i + 1 < s.length && /\d/.test(s[i + 1]))) {
      let num = '';
      if (s[i] === '-') { num = '-'; i++; }
      while (i < s.length && /[\d.]/.test(s[i])) { num += s[i]; i++; }
      tokens.push(new Token('NUMBER', parseFloat(num)));
      continue;
    }
    
    // Strings (single-quoted)
    if (s[i] === "'") {
      i++;
      let str = '';
      while (i < s.length && s[i] !== "'") {
        if (s[i] === "'" && s[i + 1] === "'") { str += "'"; i += 2; }
        else { str += s[i]; i++; }
      }
      i++; // closing quote
      tokens.push(new Token('STRING', str));
      continue;
    }
    
    // Parameter ($1, $2, ...)
    if (s[i] === '$') {
      i++;
      let num = '';
      while (i < s.length && /\d/.test(s[i])) { num += s[i]; i++; }
      tokens.push(new Token('PARAM', parseInt(num)));
      continue;
    }
    
    // Identifiers and keywords
    if (/[a-zA-Z_]/.test(s[i])) {
      let ident = '';
      while (i < s.length && /[a-zA-Z0-9_]/.test(s[i])) { ident += s[i]; i++; }
      const upper = ident.toUpperCase();
      if (KEYWORDS.has(upper)) {
        tokens.push(new Token('KEYWORD', upper));
      } else {
        tokens.push(new Token('IDENT', ident));
      }
      continue;
    }
    
    // Dot (for table.column)
    if (s[i] === '.') {
      tokens.push(new Token('.', '.'));
      i++;
      continue;
    }
    
    throw new Error(`Unexpected character '${s[i]}' at position ${i}`);
  }
  
  return tokens;
}

/**
 * Parser — turns tokens into a query AST for QueryCompiler.
 */
class Parser {
  constructor(tokens) {
    this.tokens = tokens;
    this.pos = 0;
  }
  
  peek() { return this.pos < this.tokens.length ? this.tokens[this.pos] : null; }
  next() { return this.tokens[this.pos++]; }
  
  expect(type, value) {
    const t = this.next();
    if (!t) throw new Error(`Expected ${type} ${value || ''}, got end of input`);
    if (type && t.type !== type) throw new Error(`Expected ${type}, got ${t.type} '${t.value}'`);
    if (value && t.value !== value) throw new Error(`Expected '${value}', got '${t.value}'`);
    return t;
  }
  
  match(type, value) {
    const t = this.peek();
    if (!t) return false;
    if (type && t.type !== type) return false;
    if (value !== undefined && t.value !== value) return false;
    return true;
  }
  
  consume(type, value) {
    if (this.match(type, value)) { return this.next(); }
    return null;
  }

  /**
   * Parse a SELECT statement.
   */
  parseSelect() {
    this.expect('KEYWORD', 'SELECT');
    
    // Parse select list
    const columns = [];
    const aggregates = [];
    
    do {
      if (this.match('*')) {
        this.next();
        columns.push({ name: '*' });
      } else if (this.matchAggFunc()) {
        const agg = this.parseAggFunc();
        aggregates.push(agg);
      } else {
        const name = this.expect('IDENT').value;
        let alias = name;
        if (this.consume('KEYWORD', 'AS')) {
          alias = this.expect('IDENT').value;
        }
        columns.push({ name, alias });
      }
    } while (this.consume(','));
    
    // FROM
    let table = null;
    if (this.consume('KEYWORD', 'FROM')) {
      table = this.expect('IDENT').value;
    }
    
    // WHERE
    let where = null;
    if (this.consume('KEYWORD', 'WHERE')) {
      where = this.parseCondition();
    }
    
    // GROUP BY
    let groupBy = null;
    if (this.consume('KEYWORD', 'GROUP')) {
      this.expect('KEYWORD', 'BY');
      groupBy = [];
      do {
        groupBy.push(this.expect('IDENT').value);
      } while (this.consume(','));
    }
    
    // ORDER BY
    let orderBy = null;
    if (this.consume('KEYWORD', 'ORDER')) {
      this.expect('KEYWORD', 'BY');
      const col = this.expect('IDENT').value;
      let desc = false;
      if (this.consume('KEYWORD', 'DESC')) desc = true;
      else this.consume('KEYWORD', 'ASC');
      orderBy = { column: col, descending: desc };
    }
    
    // LIMIT
    let limit = null;
    if (this.consume('KEYWORD', 'LIMIT')) {
      limit = this.expect('NUMBER').value;
    }
    
    return { table, columns, aggregates, where, groupBy, orderBy, limit };
  }
  
  matchAggFunc() {
    const t = this.peek();
    return t && t.type === 'KEYWORD' && AGG_FUNCS.has(t.value);
  }
  
  parseAggFunc() {
    const func = this.expect('KEYWORD').value;
    this.expect('(');
    let arg;
    if (this.match('*')) {
      this.next();
      arg = '*';
    } else {
      arg = this.expect('IDENT').value;
    }
    this.expect(')');
    let alias = `${func.toLowerCase()}(${arg})`;
    if (this.consume('KEYWORD', 'AS')) {
      alias = this.expect('IDENT').value;
    }
    return { func, arg, alias };
  }
  
  parseCondition() {
    const col = this.expect('IDENT').value;
    const op = this.expect('OP').value;
    let value;
    const t = this.peek();
    if (t.type === 'NUMBER') {
      value = this.next().value;
    } else if (t.type === 'STRING') {
      value = this.next().value;
    } else if (t.type === 'PARAM') {
      value = { type: 'param', index: this.next().value };
    } else {
      value = this.next().value;
    }
    return { col, op, value };
  }
}

/**
 * Parse SQL string into AST for QueryCompiler.
 */
export function parseSQL(sql) {
  const tokens = tokenize(sql);
  const parser = new Parser(tokens);
  
  const t = parser.peek();
  if (t && t.type === 'KEYWORD' && t.value === 'SELECT') {
    return parser.parseSelect();
  }
  
  throw new Error(`Unsupported statement type: ${t?.value || 'empty'}`);
}

export { tokenize, Parser };
