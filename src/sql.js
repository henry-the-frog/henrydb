// sql.js — SQL tokenizer, parser, and query executor for HenryDB

// ===== Tokenizer =====
const KEYWORDS = new Set([
  'SELECT', 'FROM', 'WHERE', 'INSERT', 'INTO', 'VALUES', 'UPDATE', 'SET',
  'DELETE', 'CREATE', 'TABLE', 'DROP', 'AND', 'OR', 'NOT', 'NULL', 'TRUE',
  'FALSE', 'ORDER', 'BY', 'ASC', 'DESC', 'LIMIT', 'OFFSET', 'AS',
  'INT', 'INTEGER', 'TEXT', 'VARCHAR', 'FLOAT', 'BOOL', 'BOOLEAN',
  'PRIMARY', 'KEY', 'COUNT', 'SUM', 'AVG', 'MIN', 'MAX', 'ARRAY_AGG', 'JSON_AGG', 'BOOL_AND', 'BOOL_OR',
  'JOIN', 'INNER', 'LEFT', 'RIGHT', 'ON', 'GROUP', 'HAVING',
  'INDEX', 'UNIQUE', 'IF', 'EXISTS', 'IN', 'ALTER', 'ADD', 'COLUMN', 'DEFAULT', 'RENAME', 'TO',
  'LIKE', 'ILIKE', 'UPPER', 'LOWER', 'LENGTH', 'CONCAT', 'BETWEEN',
  'OVER', 'PARTITION', 'ROW_NUMBER', 'RANK', 'DENSE_RANK', 'LAG', 'LEAD', 'FIRST_VALUE', 'LAST_VALUE', 'NTILE', 'VIEW', 'DISTINCT',
  'WITH', 'RECURSIVE', 'UNION', 'ALL', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END', 'EXPLAIN', 'ANALYZE', 'COMPILED',
  'INTERSECT', 'EXCEPT',
  'IS', 'COALESCE', 'NULLIF', 'TRUNCATE', 'CROSS', 'SHOW', 'TABLES', 'DESCRIBE',
  'SUBSTRING', 'SUBSTR', 'REPLACE', 'TRIM', 'ABS', 'ROUND', 'CEIL', 'FLOOR', 'IFNULL', 'IIF', 'TYPEOF',
  'LEFT', 'RIGHT', 'LPAD', 'RPAD', 'REVERSE', 'REPEAT',
  'POWER', 'SQRT', 'LOG', 'RANDOM',
  'CURRENT_TIMESTAMP', 'CURRENT_DATE', 'NOW', 'STRFTIME',
  'SHOW', 'TABLES', 'COLUMNS',
  'TRUNCATE', 'RENAME', 'DESCRIBE',
  'BEGIN', 'COMMIT', 'ROLLBACK', 'TRANSACTION', 'VACUUM', 'CHECKPOINT', 'SAVEPOINT', 'RELEASE',
  'SEQUENCE', 'START', 'INCREMENT', 'MINVALUE', 'MAXVALUE', 'CYCLE',
  'NEXTVAL', 'CURRVAL', 'SETVAL', 'TRUNCATE',
  'OVER', 'PARTITION', 'RANK', 'ROW_NUMBER', 'DENSE_RANK', 'NTILE', 'LAG', 'LEAD',
  'INCLUDE', 'ALTER', 'ADD', 'COLUMN', 'RENAME', 'TO', 'CHECK',
  'REFERENCES', 'FOREIGN', 'CASCADE', 'RESTRICT', 'SET',
  'CAST', 'INT', 'INTEGER', 'TEXT', 'FLOAT', 'BOOLEAN',
  'GROUP_CONCAT', 'STRING_AGG', 'SEPARATOR',
  'FUNCTION', 'RETURNS', 'LANGUAGE', 'CALL', 'PROCEDURE',
  'CONFLICT', 'DO', 'NOTHING',
  'ANALYZE', 'RETURNING',
  'MATERIALIZED', 'REFRESH',
  'TRIGGER', 'BEFORE', 'AFTER', 'EACH', 'ROW', 'EXECUTE',
  'IF', 'EXISTS', 'PREPARE', 'DEALLOCATE', 'COPY', 'STDIN', 'STDOUT',
  'FORMAT', 'CSV', 'HEADER', 'DELIMITER', 'CURSOR', 'DECLARE', 'FETCH',
  'CLOSE', 'LISTEN', 'NOTIFY', 'FORWARD', 'NEXT', 'FIRST', 'SCROLL', 'FOR', 'TO',
  'JSON_EXTRACT', 'JSON_SET', 'JSON_ARRAY_LENGTH', 'JSON_TYPE', 'JSON_OBJECT', 'JSON_ARRAY',
  'FULLTEXT', 'MATCH', 'AGAINST',
  'GENERATE_SERIES', 'LATERAL',
  'GENERATED', 'ALWAYS', 'STORED', 'VIRTUAL',
  'NO',
  'LTRIM', 'RTRIM', 'INSTR', 'PRINTF',
  'USING',
  'MERGE', 'MATCHED',
  'IGNORE', 'NULLS', 'FIRST', 'LAST',
  'GREATEST', 'LEAST', 'CONCAT_WS', 'REGEXP_REPLACE', 'REGEXP_MATCH',
]);

export function tokenize(sql) {
  const tokens = [];
  let i = 0;
  const src = sql.trim();

  while (i < src.length) {
    // Whitespace
    if (/\s/.test(src[i])) { i++; continue; }

    // String literal
    if (src[i] === "'") {
      i++;
      let str = '';
      while (i < src.length) {
        if (src[i] === "'" && src[i + 1] === "'") { str += "'"; i += 2; continue; }
        if (src[i] === "'") break;
        str += src[i++];
      }
      if (i >= src.length) throw new Error('Unterminated string literal');
      i++; // closing quote
      tokens.push({ type: 'STRING', value: str });
      continue;
    }

    // Number
    if (/[0-9]/.test(src[i]) || (src[i] === '-' && /[0-9]/.test(src[i + 1]))) {
      let num = '';
      if (src[i] === '-') num += src[i++];
      while (i < src.length && /[0-9.]/.test(src[i])) num += src[i++];
      tokens.push({ type: 'NUMBER', value: num.includes('.') ? parseFloat(num) : parseInt(num) });
      continue;
    }

    // Operators
    if (src[i] === '>' && src[i + 1] === '=') { tokens.push({ type: 'GE' }); i += 2; continue; }
    if (src[i] === '<' && src[i + 1] === '=') { tokens.push({ type: 'LE' }); i += 2; continue; }
    if (src[i] === '!' && src[i + 1] === '=') { tokens.push({ type: 'NE' }); i += 2; continue; }
    if (src[i] === '<' && src[i + 1] === '>') { tokens.push({ type: 'NE' }); i += 2; continue; }
    if (src[i] === '|' && src[i + 1] === '|') { tokens.push({ type: 'CONCAT_OP' }); i += 2; continue; }
    if (src[i] === '=') { tokens.push({ type: 'EQ' }); i++; continue; }
    if (src[i] === '<') { tokens.push({ type: 'LT' }); i++; continue; }
    if (src[i] === '>') { tokens.push({ type: 'GT' }); i++; continue; }

    // Punctuation
    if ('(),;'.includes(src[i])) {
      tokens.push({ type: src[i] }); i++; continue;
    }
    if (src[i] === '*') { tokens.push({ type: '*' }); i++; continue; }
    if (src[i] === '+') { tokens.push({ type: 'PLUS' }); i++; continue; }
    if (src[i] === '-' && (i + 1 < src.length) && /[0-9]/.test(src[i+1]) && (tokens.length === 0 || ['(', ',', 'EQ', 'NE', 'LT', 'GT', 'LE', 'GE', 'PLUS', 'MINUS', 'KEYWORD'].includes(tokens[tokens.length-1]?.type))) {
      // Negative number literal
      let num = '-';
      i++;
      while (i < src.length && /[0-9.]/.test(src[i])) num += src[i++];
      tokens.push({ type: 'NUMBER', value: num.includes('.') ? parseFloat(num) : parseInt(num) });
      continue;
    }
    if (src[i] === '-') { tokens.push({ type: 'MINUS' }); i++; continue; }
    if (src[i] === '/') { tokens.push({ type: 'SLASH' }); i++; continue; }
    if (src[i] === '%') { tokens.push({ type: 'MOD' }); i++; continue; }

    // Dollar-quoted string: $$body$$
    if (src[i] === '$' && i + 1 < src.length && src[i + 1] === '$') {
      i += 2; // skip opening $$
      let body = '';
      while (i + 1 < src.length && !(src[i] === '$' && src[i + 1] === '$')) {
        body += src[i++];
      }
      if (i + 1 < src.length) i += 2; // skip closing $$
      tokens.push({ type: 'STRING', value: body.trim() });
      continue;
    }
    // Parameter placeholder: $1, $2, etc.
    if (src[i] === '$' && i + 1 < src.length && /[0-9]/.test(src[i + 1])) {
      i++; // skip $
      let num = '';
      while (i < src.length && /[0-9]/.test(src[i])) num += src[i++];
      tokens.push({ type: 'PARAM', index: parseInt(num) });
      continue;
    }

    // Identifier / keyword
    if (/[a-zA-Z_]/.test(src[i])) {
      let ident = '';
      while (i < src.length && /[a-zA-Z0-9_.]/.test(src[i])) ident += src[i++];
      const upper = ident.toUpperCase();
      if (KEYWORDS.has(upper)) tokens.push({ type: 'KEYWORD', value: upper, originalValue: ident });
      else tokens.push({ type: 'IDENT', value: ident });
      continue;
    }

    i++; // skip unknown
  }

  tokens.push({ type: 'EOF' });
  return tokens;
}

// ===== Parser =====
const SCALAR_FUNCTIONS = new Set([
  'UPPER', 'LOWER', 'LENGTH', 'CONCAT', 'COALESCE', 'NULLIF', 'SUBSTRING', 'SUBSTR',
  'REPLACE', 'TRIM', 'ABS', 'ROUND', 'CEIL', 'FLOOR', 'IFNULL', 'IIF', 'TYPEOF',
  'JSON_EXTRACT', 'JSON_SET', 'JSON_ARRAY_LENGTH', 'JSON_TYPE', 'JSON_OBJECT', 'JSON_ARRAY',
  'LEFT', 'RIGHT', 'LPAD', 'RPAD', 'REVERSE', 'REPEAT', 'POWER', 'SQRT', 'LOG',
  'RANDOM', 'STRFTIME', 'NOW', 'GREATEST', 'LEAST', 'CONCAT_WS',
  'REGEXP_REPLACE', 'REGEXP_MATCH', 'NEXTVAL', 'CURRVAL', 'SETVAL',
  'PG_STAT_STATEMENTS_RESET',
]);

export function parse(sql) {
  const tokens = tokenize(sql);
  let pos = 0;

  function peek() { return tokens[pos]; }
  function advance() { return tokens[pos++]; }
  function expect(type, value) {
    const t = advance();
    if (t.type !== type || (value && t.value !== value))
      throw new Error(`Expected ${type} ${value || ''}, got ${t.type} ${t.value || ''}`);
    return t;
  }
  function match(type, value) {
    if (peek().type === type && (!value || peek().value === value)) { advance(); return true; }
    return false;
  }
  // Read an alias name after AS — preserves original case even for keywords
  function readAlias() {
    const t = advance();
    return t.originalValue || t.value;
  }
  function isKeyword(val) { return peek().type === 'KEYWORD' && peek().value === val; }

  // EXPLAIN
  if (isKeyword('EXPLAIN')) {
    advance();
    const analyze = isKeyword('ANALYZE') ? (advance(), true) : false;
    const compiled = isKeyword('COMPILED') ? (advance(), true) : false;
    // Parse the underlying statement
    let statement;
    if (isKeyword('WITH')) statement = parseWith();
    else if (isKeyword('SELECT')) statement = parseSelect();
    else throw new Error('EXPLAIN requires a SELECT statement');
    return { type: 'EXPLAIN', statement, analyze, compiled };
  }

  // SELECT or WITH
  if (isKeyword('WITH')) return parseWith();
  if (isKeyword('SELECT')) return parseSelect();
  if (isKeyword('VALUES') && !isKeyword('INSERT')) return parseValuesClause();
  if (isKeyword('INSERT')) return parseInsert();
  if (isKeyword('UPDATE')) return parseUpdate();
  if (isKeyword('DELETE')) return parseDelete();
  if (isKeyword('MERGE')) return parseMerge();
  if (isKeyword('ALTER')) return parseAlter();
  if (isKeyword('CREATE')) return parseCreate();
  if (isKeyword('REFRESH')) {
    advance(); // REFRESH
    expect('KEYWORD', 'MATERIALIZED');
    expect('KEYWORD', 'VIEW');
    const name = advance().value;
    return { type: 'REFRESH_MATVIEW', name };
  }
  if (isKeyword('DROP')) return parseDrop();
  if (isKeyword('ALTER')) return parseAlter();
  if (isKeyword('SHOW')) {
    advance(); // SHOW
    if (isKeyword('TABLES')) {
      advance();
      return { type: 'SHOW_TABLES' };
    }
    if (isKeyword('COLUMNS') || isKeyword('CREATE')) {
      const what = advance().value;
      if (what === 'CREATE') { expect('KEYWORD', 'TABLE'); }
      else { expect('KEYWORD', 'FROM'); }
      const table = advance().value;
      return { type: what === 'CREATE' ? 'SHOW_CREATE_TABLE' : 'SHOW_COLUMNS', table };
    }
    throw new Error('Expected TABLES, COLUMNS, or CREATE after SHOW');
  }
  if (isKeyword('TRUNCATE')) {
    advance(); if (isKeyword('TABLE')) advance();
    return { type: 'TRUNCATE_TABLE', table: advance().value };
  }
  if (isKeyword('RENAME')) {
    advance(); expect('KEYWORD', 'TABLE');
    const from = advance().value;
    expect('KEYWORD', 'TO');
    const to = advance().value;
    return { type: 'RENAME_TABLE', from, to };
  }
  if (isKeyword('DESCRIBE')) {
    advance();
    return { type: 'SHOW_COLUMNS', table: advance().value };
  }
  if (isKeyword('TRUNCATE')) { advance(); if (isKeyword('TABLE')) advance(); return { type: 'TRUNCATE', table: advance().value }; }
  if (isKeyword('SHOW')) { advance(); expect('KEYWORD', 'TABLES'); return { type: 'SHOW_TABLES' }; }
  if (isKeyword('DESCRIBE')) { advance(); return { type: 'DESCRIBE', table: advance().value }; }
  if (isKeyword('BEGIN')) { advance(); if (isKeyword('TRANSACTION')) advance(); return { type: 'BEGIN' }; }
  if (isKeyword('COMMIT')) { advance(); return { type: 'COMMIT' }; }
  if (isKeyword('ROLLBACK')) {
    advance();
    if (isKeyword('TO')) {
      advance();
      if (isKeyword('SAVEPOINT')) advance();
      const name = (advance().originalValue || tokens[pos-1].value);
      return { type: 'ROLLBACK_TO', savepoint: name };
    }
    return { type: 'ROLLBACK' };
  }
  
  // SAVEPOINT name
  if (isKeyword('SAVEPOINT')) {
    advance();
    const name = (advance().originalValue || tokens[pos-1].value);
    return { type: 'SAVEPOINT', name };
  }
  
  // RELEASE [SAVEPOINT] name
  if (isKeyword('RELEASE')) {
    advance();
    if (isKeyword('SAVEPOINT')) advance();
    const name = (advance().originalValue || tokens[pos-1].value);
    return { type: 'RELEASE_SAVEPOINT', name };
  }
  if (isKeyword('VACUUM')) { advance(); let table = null; let incremental = false; let maxPages = null; if (peek().type === 'IDENT' && peek().value === 'INCREMENTAL') { advance(); incremental = true; if (peek().type === 'NUMBER') { maxPages = parseInt(advance().value, 10); } } if (peek().type === 'IDENT' || peek().type === 'KEYWORD') table = (advance().originalValue || tokens[pos-1].value); return { type: 'VACUUM', table, incremental, maxPages }; }
  if (isKeyword('CHECKPOINT')) { advance(); return { type: 'CHECKPOINT' }; }

  // COPY table FROM 'data' | STDIN [WITH (FORMAT CSV, HEADER true, DELIMITER ',')]
  // COPY table TO STDOUT [WITH (FORMAT CSV, HEADER true)]
  // COPY (query) TO STDOUT [WITH ...]
  if (isKeyword('COPY')) {
    advance();
    let table = null, query = null;
    if (peek().type === '(' || peek().value === '(') {
      // COPY (query) TO ...
      advance(); // (
      // Simple: collect tokens until matching )
      const queryTokens = [];
      let depth = 1;
      while (depth > 0 && peek().type !== 'EOF') {
        if (peek().type === '(' || peek().value === '(') depth++;
        if (peek().type === ')' || peek().value === ')') depth--;
        if (depth > 0) queryTokens.push(advance());
      }
      if (peek().type === ')' || peek().value === ')') advance();
      const queryOpMap = { 'EQ': '=', 'NE': '!=', 'LT': '<', 'GT': '>', 'LE': '<=', 'GE': '>=' };
      query = queryTokens.map(t => {
        if (t.type === 'STRING') return `'${t.value}'`;
        if (t.type === 'NUMBER') return String(t.value);
        if (queryOpMap[t.type]) return queryOpMap[t.type];
        return t.originalValue || t.value || t.type;
      }).join(' ');
    } else {
      table = (advance().originalValue || tokens[pos-1].value);
    }
    
    let direction = null, source = null;
    if (isKeyword('FROM')) { advance(); direction = 'FROM'; }
    else if (isKeyword('TO')) { advance(); direction = 'TO'; }
    
    if (isKeyword('STDIN')) { advance(); source = 'STDIN'; }
    else if (isKeyword('STDOUT')) { advance(); source = 'STDOUT'; }
    else if (peek().type === 'STRING') { source = advance().value; }
    
    // Parse WITH options
    const options = { format: 'text', header: false, delimiter: '\t' };
    if (isKeyword('WITH')) {
      advance();
      if (peek().type === '(' || peek().value === '(') advance();
      while (peek().type !== ')' && peek().value !== ')' && peek().type !== 'EOF') {
        if (isKeyword('FORMAT')) { advance(); options.format = (advance().value || '').toLowerCase(); if (options.format === 'csv') options.delimiter = ','; }
        else if (isKeyword('CSV')) { options.format = 'csv'; options.delimiter = ','; }
        else if (isKeyword('HEADER')) { advance(); options.header = true; if (peek().value === 'true' || peek().value === 'TRUE') advance(); }
        else if (isKeyword('DELIMITER')) { advance(); options.delimiter = advance().value; }
        else advance();
        if (peek().type === ',' || peek().value === ',') advance();
      }
      if (peek().type === ')' || peek().value === ')') advance();
    }
    
    return { type: 'COPY', table, query, direction, source, options };
  }

  // PREPARE name [(type, type, ...)] AS statement
  if (isKeyword('PREPARE')) {
    advance();
    const name = (advance().originalValue || tokens[pos-1].value);
    let paramTypes = [];
    if (peek().type === '(' || peek().value === '(') {
      advance(); // (
      while (peek().type !== ')' && peek().value !== ')' && peek().type !== 'EOF') {
        paramTypes.push(advance().value || advance().type);
        if (peek().type === ',' || peek().value === ',') advance();
      }
      if (peek().type === ')' || peek().value === ')') advance();
    }
    if (isKeyword('AS')) advance();
    // Collect remaining tokens as the statement SQL
    const stmtTokens = [];
    while (peek().type !== 'EOF') {
      stmtTokens.push(advance());
    }
    // Reconstruct SQL from tokens
    const opMap = { 'EQ': '=', 'NE': '!=', 'LT': '<', 'GT': '>', 'LE': '<=', 'GE': '>=' };
    const stmtSql = stmtTokens.map(t => {
      if (t.type === 'STRING') return `'${t.value}'`;
      if (t.type === 'PARAM') return `$${t.index}`;
      if (t.type === 'NUMBER') return String(t.value);
      if (opMap[t.type]) return opMap[t.type];
      return t.originalValue || t.value || t.type;
    }).join(' ');
    return { type: 'PREPARE', name, paramTypes, sql: stmtSql };
  }

  // EXECUTE name [(param, param, ...)]
  if (isKeyword('EXECUTE')) {
    advance();
    const name = (advance().originalValue || tokens[pos-1].value);
    let params = [];
    if (peek().type === '(' || peek().value === '(') {
      advance(); // (
      while (peek().type !== ')' && peek().value !== ')' && peek().type !== 'EOF') {
        params.push(parseExpr());
        if (peek().type === ',' || peek().value === ',') advance();
      }
      if (peek().type === ')' || peek().value === ')') advance();
    }
    return { type: 'EXECUTE_PREPARED', name, params };
  }

  // DEALLOCATE [PREPARE] name | ALL
  if (isKeyword('DEALLOCATE')) {
    advance();
    if (isKeyword('PREPARE')) advance();
    if (isKeyword('ALL')) { advance(); return { type: 'DEALLOCATE', name: 'ALL' }; }
    const name = (advance().originalValue || tokens[pos-1].value);
    return { type: 'DEALLOCATE', name };
  }

  // TRUNCATE [TABLE] name [, name2, ...]
  if (isKeyword('TRUNCATE')) {
    advance();
    if (isKeyword('TABLE')) advance();
    const tables = [];
    tables.push((advance().originalValue || tokens[pos-1].value));
    while (peek().type === ',' || peek().value === ',') {
      advance();
      tables.push((advance().originalValue || tokens[pos-1].value));
    }
    let cascade = false;
    if (isKeyword('CASCADE')) { advance(); cascade = true; }
    return { type: 'TRUNCATE', tables, cascade };
  }

  // DECLARE name CURSOR FOR query
  if (isKeyword('DECLARE')) {
    advance();
    const name = (advance().originalValue || tokens[pos-1].value);
    // Skip optional SCROLL / NO SCROLL
    while (isKeyword('SCROLL') || (peek().type === 'IDENT' && peek().value === 'NO')) advance();
    if (isKeyword('CURSOR')) advance();
    if (isKeyword('FOR')) advance();
    // Collect remaining tokens as query SQL
    const queryOpMap = { 'EQ': '=', 'NE': '!=', 'LT': '<', 'GT': '>', 'LE': '<=', 'GE': '>=' };
    const queryTokens = [];
    while (peek().type !== 'EOF') queryTokens.push(advance());
    const querySql = queryTokens.map(t => {
      if (t.type === 'STRING') return `'${t.value}'`;
      if (t.type === 'NUMBER') return String(t.value);
      if (t.type === 'PARAM') return `$${t.index}`;
      if (queryOpMap[t.type]) return queryOpMap[t.type];
      return t.originalValue || t.value || t.type;
    }).join(' ');
    return { type: 'DECLARE_CURSOR', name, query: querySql };
  }

  // FETCH [FORWARD|NEXT|ALL|n] [FROM|IN] name
  if (isKeyword('FETCH')) {
    advance();
    let count = 1;
    let direction = 'FORWARD';
    if (isKeyword('ALL')) { advance(); count = Infinity; }
    else if (isKeyword('FORWARD')) { advance(); if (peek().type === 'NUMBER') count = parseInt(advance().value, 10); }
    else if (isKeyword('NEXT')) { advance(); count = 1; }
    else if (isKeyword('FIRST')) { advance(); count = 1; direction = 'FIRST'; }
    else if (peek().type === 'NUMBER') { count = parseInt(advance().value, 10); }
    
    if (isKeyword('FROM') || isKeyword('IN')) advance();
    const name = (advance().originalValue || tokens[pos-1].value);
    return { type: 'FETCH', name, count, direction };
  }

  // CLOSE name | ALL
  if (isKeyword('CLOSE')) {
    advance();
    if (isKeyword('ALL')) { advance(); return { type: 'CLOSE_CURSOR', name: 'ALL' }; }
    const name = (advance().originalValue || tokens[pos-1].value);
    return { type: 'CLOSE_CURSOR', name };
  }

  // LISTEN channel
  if (isKeyword('LISTEN')) {
    advance();
    const channel = (advance().originalValue || tokens[pos-1].value);
    return { type: 'LISTEN', channel };
  }

  // NOTIFY channel [, 'payload']
  if (isKeyword('NOTIFY')) {
    advance();
    const channel = (advance().originalValue || tokens[pos-1].value);
    let payload = '';
    if (peek().type === ',' || peek().value === ',') {
      advance(); // skip comma
      if (peek().type === 'STRING') {
        payload = advance().value;
      } else {
        payload = (advance().originalValue || tokens[pos-1].value);
      }
    }
    return { type: 'NOTIFY', channel, payload };
  }

  // UNLISTEN channel | *
  if (peek().type === 'IDENT' && (peek().value === 'UNLISTEN' || peek().value === 'unlisten')) {
    advance();
    let channel = '*';
    if (peek().value === '*') { advance(); }
    else if (peek().type === 'IDENT' || peek().type === 'KEYWORD') {
      channel = (advance().originalValue || tokens[pos-1].value);
    }
    return { type: 'UNLISTEN', channel };
  }

  if (isKeyword('ANALYZE') && !isKeyword('EXPLAIN')) {
    advance();
    let table = null;
    if (peek()?.type === 'IDENT') table = advance().value;
    return { type: 'ANALYZE_TABLE', table };
  }
  throw new Error(`Unexpected token: ${peek().type} ${peek().value || ''}`);

  function parseWith() {
    advance(); // WITH
    let recursive = false;
    if (isKeyword('RECURSIVE')) { recursive = true; advance(); }

    const ctes = [];
    do {
      const cteTok = advance();
      const name = cteTok.originalValue || cteTok.value;
      if (isKeyword('AS')) advance(); // optional AS
      expect('(');
      const baseQuery = parseSelect();
      // Check for UNION ALL (recursive CTEs)
      let unionQuery = null;
      if (isKeyword('UNION')) {
        advance(); // UNION
        const all = isKeyword('ALL') ? (advance(), true) : false;
        unionQuery = parseSelect();
        unionQuery.unionAll = all;
      }
      expect(')');
      ctes.push({ name, query: baseQuery, unionQuery, recursive });
    } while (match(','));

    // Main query
    const mainQuery = parseSelect();
    mainQuery.ctes = ctes;
    return mainQuery;
  }

  function parseSelect() {
    advance(); // SELECT
    let distinct = false;
    let distinctOn = null;
    if (isKeyword('DISTINCT')) {
      distinct = true;
      advance();
      if (isKeyword('ON')) {
        advance(); // ON
        expect('(');
        distinctOn = [];
        do { distinctOn.push(advance().value); } while (match(','));
        expect(')');
      }
    }
    const columns = parseSelectList();
    let from = null;
    let where = null, orderBy = null, limit = null, offset = null;
    let joins = [];
    let groupBy = null, having = null;

    if (isKeyword('FROM')) {
      advance(); // FROM
      from = parseFromClause();

      // Implicit CROSS JOINs from comma-separated tables in FROM
      while (match(',')) {
        // Handle LATERAL subquery: , LATERAL (SELECT ...) alias
        if (isKeyword('LATERAL')) {
          advance(); // LATERAL
          expect('(');
          const subquery = parseSelect();
          expect(')');
          let subAlias = null;
          if (isKeyword('AS')) { advance(); subAlias = readAlias(); }
          else if (peek().type === 'IDENT') subAlias = advance().value;
          subAlias = subAlias || '__lateral';
          joins.push({ joinType: 'CROSS', lateral: true, subquery, alias: subAlias, on: null });
        } else {
          const nextTable = advance().value;
          let nextAlias = null;
          if (peek().type === 'IDENT') nextAlias = advance().value;
          else if (isKeyword('AS')) { advance(); nextAlias = readAlias(); }
          joins.push({ joinType: 'CROSS', table: nextTable, alias: nextAlias, on: null });
        }
      }

      // JOINs
      while (isKeyword('JOIN') || isKeyword('INNER') || isKeyword('LEFT') || isKeyword('RIGHT') || isKeyword('CROSS')) {
        joins.push(parseJoin());
      }
    }

    if (isKeyword('WHERE')) { advance(); where = parseExpr(); }
    if (isKeyword('GROUP')) { advance(); expect('KEYWORD', 'BY'); groupBy = parseGroupBy(); }
    if (isKeyword('HAVING')) { advance(); having = parseExpr(); }
    if (isKeyword('ORDER')) { advance(); expect('KEYWORD', 'BY'); orderBy = parseOrderBy(); }
    if (isKeyword('LIMIT')) { advance(); limit = advance().value; }
    if (isKeyword('OFFSET')) { advance(); offset = advance().value; }

    let result = { type: 'SELECT', distinct, distinctOn, columns, from, joins, where, groupBy, having, orderBy, limit, offset };

    // UNION / UNION ALL / INTERSECT / EXCEPT
    if (isKeyword('UNION')) {
      advance();
      let all = false;
      if (isKeyword('ALL')) { all = true; advance(); }
      const right = parseSelect();
      result = { type: 'UNION', left: result, right, all };
      // If the right SELECT has ORDER BY or LIMIT, they should apply
      // to the entire UNION (standard SQL behavior)
      if (right.orderBy) {
        result.orderBy = right.orderBy;
        delete right.orderBy;
      }
      if (right.limit != null) {
        result.limit = right.limit;
        delete right.limit;
      }
      if (right.offset != null) {
        result.offset = right.offset;
        delete right.offset;
      }
    } else if (isKeyword('INTERSECT')) {
      advance();
      const all = isKeyword('ALL') ? (advance(), true) : false;
      const right = parseSelect();
      result = { type: 'INTERSECT', left: result, right, all };
    } else if (isKeyword('EXCEPT')) {
      advance();
      const all = isKeyword('ALL') ? (advance(), true) : false;
      const right = parseSelect();
      result = { type: 'EXCEPT', left: result, right, all };
    }

    return result;
  }

  // UNION not yet handled at this layer — keeping for later

  function parseSelectList() {
    if (match('*')) return [{ type: 'star' }];
    const cols = [parseSelectColumn()];
    while (match(',')) cols.push(parseSelectColumn());
    return cols;
  }

  function parseSelectColumn() {
    // CURRENT_TIMESTAMP, CURRENT_DATE (no parens)
    if (peek().type === 'KEYWORD' && (peek().value === 'CURRENT_TIMESTAMP' || peek().value === 'CURRENT_DATE')) {
      const func = advance().value;
      let alias = null;
      if (isKeyword('AS')) { advance(); alias = readAlias(); }
      return { type: 'function', func, args: [], alias: alias || func };
    }

    // Check for CAST expression in SELECT
    if (peek().type === 'KEYWORD' && peek().value === 'CAST') {
      advance(); // CAST
      expect('(');
      const expr = parseExpr();
      expect('KEYWORD', 'AS');
      const targetType = advance().value;
      expect(')');
      let alias = null;
      if (isKeyword('AS')) { advance(); alias = readAlias(); }
      return { type: 'expression', expr: { type: 'cast', expr, targetType }, alias };
    }

    // Check for scalar subquery: (SELECT ...)
    if (peek().type === '(' && tokens[pos + 1]?.type === 'KEYWORD' && tokens[pos + 1]?.value === 'SELECT') {
      advance(); // (
      const subquery = parseSelect();
      expect(')');
      let alias = null;
      if (isKeyword('AS')) { advance(); alias = readAlias(); }
      return { type: 'scalar_subquery', subquery, alias };
    }

    // Check for general function call: FUNC(args) — may be part of larger expression
    if (peek().type === 'KEYWORD' && SCALAR_FUNCTIONS.has(peek().value) && tokens[pos + 1]?.type === '(') {
      // Parse as expression to handle FUNC(...) + expr, FUNC(...) * expr, etc.
      const expr = parseExpr();
      let alias = null;
      if (isKeyword('AS')) { advance(); alias = readAlias(); }
      if (expr.type === 'function_call') {
        return { type: 'function', func: expr.func, args: expr.args, alias: alias || `${expr.func}(...)` };
      }
      return { type: 'expression', expr, alias: alias || null };
    }

    // Check for identifier function call: ident(args) — may be part of larger expression
    if (peek().type === 'IDENT' && tokens[pos + 1]?.type === '(') {
      const expr = parseExpr();
      let alias = null;
      if (isKeyword('AS')) { advance(); alias = readAlias(); }
      if (expr.type === 'function_call') {
        return { type: 'function', func: expr.func, args: expr.args, alias: alias || `${expr.func}(...)` };
      }
      return { type: 'expression', expr, alias: alias || null };
    }

    // Check for aggregate: COUNT, SUM, AVG, MIN, MAX
    if (peek().type === 'KEYWORD' && ['COUNT', 'SUM', 'AVG', 'MIN', 'MAX', 'GROUP_CONCAT', 'STRING_AGG', 'ARRAY_AGG', 'JSON_AGG', 'BOOL_AND', 'BOOL_OR'].includes(peek().value) && tokens[pos + 1]?.type === '(') {
      const func = advance().value;
      expect('(');
      let distinct = false;
      if (isKeyword('DISTINCT')) { distinct = true; advance(); }
      let arg;
      if (match('*')) arg = '*';
      else {
        // Parse full expression for aggregate argument (e.g., SUM(qty * price))
        const argExpr = parseExpr();
        // If it's a simple column ref, use just the name for backward compat
        if (argExpr.type === 'column_ref') arg = argExpr.name;
        else arg = argExpr; // store the full expression node
      }
      // Optional SEPARATOR for GROUP_CONCAT
      let separator = ',';
      if ((func === 'GROUP_CONCAT' || func === 'STRING_AGG') && match(',')) {
        // SQLite-style: GROUP_CONCAT(val, ',')
        separator = advance().value;
      } else if (isKeyword('SEPARATOR')) {
        advance();
        separator = advance().value; // STRING literal
      }
      expect(')');

      // Add separator info for GROUP_CONCAT/STRING_AGG
      const aggExtra = (func === 'GROUP_CONCAT' || func === 'STRING_AGG') ? { separator } : {};
      // Check for window function: aggregate OVER (...)
      if (isKeyword('OVER')) {
        const over = parseOverClause();
        let alias = null;
        if (isKeyword('AS')) { advance(); alias = readAlias(); }
        return { type: 'window', func, arg, distinct, over, alias };
      }

      let alias = null;
      if (isKeyword('AS')) { advance(); alias = readAlias(); }
      return { type: 'aggregate', func, arg, distinct, alias, ...aggExtra };
    }

    // Window functions: ROW_NUMBER, RANK, DENSE_RANK (no arguments)
    if (peek().type === 'KEYWORD' && ['ROW_NUMBER', 'RANK', 'DENSE_RANK'].includes(peek().value)) {
      const func = advance().value;
      expect('(');
      expect(')');
      const over = parseOverClause();
      let alias = null;
      if (isKeyword('AS')) { advance(); alias = readAlias(); }
      return { type: 'window', func, arg: null, over, alias };
    }

    // Window functions with arguments: LEAD, LAG, FIRST_VALUE, LAST_VALUE, NTILE
    if (peek().type === 'KEYWORD' && ['LEAD', 'LAG', 'FIRST_VALUE', 'LAST_VALUE', 'NTILE'].includes(peek().value)) {
      const func = advance().value;
      expect('(');
      const args = [];
      if (!match(')')) {
        args.push(parseExpr());
        while (match(',')) args.push(parseExpr());
        expect(')');
      }
      const over = parseOverClause();
      let alias = null;
      if (isKeyword('AS')) { advance(); alias = readAlias(); }
      // First arg is the column, rest are additional args
      const arg = args.length > 0 && args[0].type === 'column_ref' ? args[0].name : null;
      return { type: 'window', func, arg, args, over, alias };
    }

    // String functions in SELECT
    if (peek().type === 'KEYWORD' && ['UPPER', 'LOWER', 'LENGTH', 'CONCAT', 'COALESCE', 'NULLIF', 'SUBSTRING', 'SUBSTR', 'REPLACE', 'TRIM', 'LTRIM', 'RTRIM', 'ABS', 'ROUND', 'CEIL', 'FLOOR', 'IFNULL', 'IIF', 'TYPEOF', 'INSTR', 'PRINTF',
      'JSON_EXTRACT', 'JSON_SET', 'JSON_ARRAY_LENGTH', 'JSON_TYPE', 'JSON_OBJECT', 'JSON_ARRAY', 'LEFT', 'RIGHT', 'LPAD', 'RPAD', 'REVERSE', 'REPEAT', 'POWER', 'SQRT', 'LOG', 'RANDOM', 'STRFTIME', 'NOW', 'GREATEST', 'LEAST', 'CONCAT_WS', 'REGEXP_REPLACE', 'REGEXP_MATCH'].includes(peek().value)) {
      const func = advance().value;
      expect('(');
      const args = [];
      if (!match(')')) {
        args.push(parseExpr());
        while (match(',')) args.push(parseExpr());
        expect(')');
      }
      // Check for arithmetic after function call — use parsePrimaryWithConcat for precedence
      let node = { type: 'function_call', func, args };
      node = parseSelectArithExpr(node);
      let alias = null;
      if (isKeyword('AS')) { advance(); alias = readAlias(); }
      if (node.type === 'function_call') {
        return { type: 'function', func, args, alias };
      }
      return { type: 'expression', expr: node, alias };
    }
    // CASE expression in SELECT
    if (peek().type === 'KEYWORD' && peek().value === 'CASE') {
      const expr = parseCaseExpr();
      let alias = null;
      if (isKeyword('AS')) { advance(); alias = readAlias(); }
      return { type: 'expression', expr, alias };
    }
    // Literal number or string as expression
    if (peek().type === 'NUMBER' || peek().type === 'STRING') {
      const tok = advance();
      let node = { type: 'literal', value: tok.value };
      // Check for arithmetic
      const nt = peek().type;
      if (nt === 'CONCAT_OP' || nt === 'PLUS' || nt === 'MINUS' || nt === '*' || nt === 'SLASH' || nt === 'MOD') {
        node = parseSelectArithExpr(node);
      }
      let alias = null;
      if (isKeyword('AS')) { advance(); alias = readAlias(); }
      if (node.type === 'literal') {
        return { type: 'expression', expr: node, alias };
      }
      return { type: 'expression', expr: node, alias };
    }
    // Parenthesized expression in SELECT
    if (peek().type === '(') {
      advance(); // (
      // Could be a subquery or a parenthesized expression
      if (peek().type === 'KEYWORD' && peek().value === 'SELECT') {
        const subquery = parseSelect();
        expect(')');
        let alias = null;
        if (isKeyword('AS')) { advance(); alias = readAlias(); }
        return { type: 'expression', expr: { type: 'subquery', query: subquery }, alias };
      }
      // Parenthesized expression
      const expr = parsePrimaryWithConcat();
      expect(')');
      // Check for arithmetic after parens
      let node = expr;
      const nt = peek().type;
      if (nt === 'PLUS' || nt === 'MINUS' || nt === '*' || nt === 'SLASH' || nt === 'MOD' || nt === 'CONCAT_OP') {
        node = parseSelectArithExpr(node);
      }
      let alias = null;
      if (isKeyword('AS')) { advance(); alias = readAlias(); }
      return { type: 'expression', expr: node, alias };
    }
    const col = advance().value;
    // Check for || concatenation or arithmetic operators
    const nextType = peek().type;
    if (nextType === 'CONCAT_OP' || nextType === 'PLUS' || nextType === 'MINUS' || nextType === '*' || nextType === 'SLASH' || nextType === 'MOD') {
      let left = { type: 'column_ref', name: col };
      left = parseSelectArithExpr(left);
      let alias = null;
      if (isKeyword('AS')) { advance(); alias = readAlias(); }
      return { type: 'expression', expr: left, alias };
    }
    let alias = null;
    if (isKeyword('AS')) { advance(); alias = readAlias(); }
    return { type: 'column', name: col, alias };
  }

  // Parse arithmetic in SELECT columns with proper precedence (left is already parsed)
  function parseSelectArithExpr(left) {
    // First handle multiplicative (higher precedence) — left is already the first operand
    while (peek().type === '*' || peek().type === 'SLASH' || peek().type === 'MOD') {
      const t = peek().type;
      const op = t === '*' ? '*' : t === 'SLASH' ? '/' : '%';
      advance();
      const right = parsePrimary();
      left = { type: 'arith', op, left, right };
    }
    // Then handle additive (lower precedence)
    while (['PLUS', 'MINUS'].includes(peek().type) || peek().type === 'CONCAT_OP') {
      const t = peek().type;
      advance();
      if (t === 'CONCAT_OP') {
        const right = parsePrimary();
        left = { type: 'function_call', func: 'CONCAT', args: [left, right] };
      } else {
        const op = t === 'PLUS' ? '+' : '-';
        // Right side: parse multiplicative first
        let right = parsePrimary();
        while (peek().type === '*' || peek().type === 'SLASH' || peek().type === 'MOD') {
          const mt = peek().type;
          const mop = mt === '*' ? '*' : mt === 'SLASH' ? '/' : '%';
          advance();
          const mr = parsePrimary();
          right = { type: 'arith', op: mop, left: right, right: mr };
        }
        left = { type: 'arith', op, left, right };
      }
    }
    return left;
  }

  function parseFromClause() {
    // GENERATE_SERIES(start, stop[, step])
    if (isKeyword('GENERATE_SERIES')) {
      advance();
      expect('(');
      const start = parsePrimary();
      expect(',');
      const stop = parsePrimary();
      let step = null;
      if (match(',')) step = parsePrimary();
      expect(')');
      let alias = null;
      if (isKeyword('AS')) { advance(); alias = readAlias(); }
      else if (peek().type === 'IDENT') alias = advance().value;
      return { table: '__generate_series', alias, start, stop, step };
    }
    // Subquery in FROM
    if (peek().type === '(') {
      advance(); // (
      const subquery = parseSelect();
      expect(')');
      let alias = null;
      if (isKeyword('AS')) { advance(); alias = readAlias(); }
      else if (peek().type === 'IDENT') alias = advance().value;
      return { table: '__subquery', alias, subquery };
    }
    const table = advance().value;
    let alias = null;
    if (peek().type === 'IDENT') alias = advance().value;
    else if (isKeyword('AS')) { advance(); alias = readAlias(); }
    return { table, alias };
  }

  function parseJoin() {
    let joinType = 'INNER';
    if (isKeyword('LEFT')) { joinType = 'LEFT'; advance(); }
    else if (isKeyword('RIGHT')) { joinType = 'RIGHT'; advance(); }
    else if (isKeyword('CROSS')) { joinType = 'CROSS'; advance(); }
    else if (isKeyword('INNER')) { advance(); }
    expect('KEYWORD', 'JOIN');
    
    // LATERAL JOIN: for each outer row, re-evaluate the subquery
    let lateral = false;
    if (isKeyword('LATERAL')) {
      lateral = true;
      advance();
    }
    
    if (lateral) {
      // Expect a parenthesized subquery: ( SELECT ... )
      expect('(');
      const subquery = parseSelect();
      expect(')');
      
      let alias = null;
      // Optional: AS alias or just alias
      if (isKeyword('AS')) { advance(); alias = advance().value; }
      else if (peek().type === 'IDENT' && !isKeyword('ON') && !isKeyword('WHERE') && !isKeyword('ORDER') && !isKeyword('GROUP') && !isKeyword('LIMIT')) {
        alias = advance().value;
      }
      
      let on = null;
      if (isKeyword('ON')) {
        advance();
        on = parseExpr();
      }
      
      return { type: 'JOIN', joinType, lateral: true, subquery, alias: alias || '__lateral', on };
    }
    
    const table = advance().value;
    let alias = null;
    if (peek().type === 'IDENT' && !isKeyword('ON')) alias = advance().value;
    let on = null;
    if (isKeyword('ON')) {
      advance();
      on = parseExpr();
    }
    return { type: 'JOIN', joinType, table, alias, on };
  }

  function parseExpr() { return parseOr(); }

  function parseOr() {
    let left = parseAnd();
    while (isKeyword('OR')) { advance(); left = { type: 'OR', left, right: parseAnd() }; }
    return left;
  }

  function parseAnd() {
    let left = parseComparison();
    while (isKeyword('AND')) { advance(); left = { type: 'AND', left, right: parseComparison() }; }
    return left;
  }

  function parseComparison() {
    if (isKeyword('NOT')) {
      advance();
      const expr = parseComparison();
      return { type: 'NOT', expr };
    }
    if (isKeyword('EXISTS')) {
      advance();
      expect('(');
      const subquery = parseSelect();
      expect(')');
      return { type: 'EXISTS', subquery };
    }
    if (match('(')) {
      // Could be subquery or grouped expression
      if (isKeyword('SELECT')) {
        const subquery = parseSelect();
        expect(')');
        return { type: 'SUBQUERY', subquery };
      }
      const expr = parseExpr();
      expect(')');
      return expr;
    }

    const left = parsePrimaryWithConcat();

    // NOT IN / NOT LIKE / NOT BETWEEN
    if (isKeyword('NOT') && tokens[pos + 1]?.type === 'KEYWORD' && tokens[pos + 1]?.value === 'IN') {
      advance(); // NOT
      advance(); // IN
      expect('(');
      if (isKeyword('SELECT')) {
        const subquery = parseSelect();
        expect(')');
        return { type: 'NOT', expr: { type: 'IN_SUBQUERY', left, subquery } };
      }
      const values = [];
      do { values.push(parsePrimary()); } while (match(','));
      expect(')');
      return { type: 'NOT', expr: { type: 'IN_LIST', left, values } };
    }

    // NOT LIKE
    if (isKeyword('NOT') && tokens[pos + 1]?.type === 'KEYWORD' && tokens[pos + 1]?.value === 'LIKE') {
      advance(); // NOT
      advance(); // LIKE
      const pattern = parsePrimary();
      return { type: 'NOT', expr: { type: 'LIKE', left, pattern } };
    }
    // NOT ILIKE
    if (isKeyword('NOT') && tokens[pos + 1]?.type === 'KEYWORD' && tokens[pos + 1]?.value === 'ILIKE') {
      advance(); // NOT
      advance(); // ILIKE
      const pattern = parsePrimary();
      return { type: 'NOT', expr: { type: 'ILIKE', left, pattern } };
    }

    // NOT BETWEEN
    if (isKeyword('NOT') && tokens[pos + 1]?.type === 'KEYWORD' && tokens[pos + 1]?.value === 'BETWEEN') {
      advance(); // NOT
      advance(); // BETWEEN
      const low = parsePrimary();
      expect('KEYWORD', 'AND');
      const high = parsePrimary();
      return { type: 'NOT', expr: { type: 'BETWEEN', left, low, high } };
    }

    if (isKeyword('IN')) {
      advance();
      expect('(');
      if (isKeyword('SELECT')) {
        const subquery = parseSelect();
        expect(')');
        return { type: 'IN_SUBQUERY', left, subquery };
      }
      const values = [];
      do { values.push(parsePrimary()); } while (match(','));
      expect(')');
      return { type: 'IN_LIST', left, values };
    }

    if (isKeyword('LIKE')) {
      advance();
      const pattern = parsePrimary();
      return { type: 'LIKE', left, pattern };
    }
    if (isKeyword('ILIKE')) {
      advance();
      const pattern = parsePrimary();
      return { type: 'ILIKE', left, pattern };
    }

    if (isKeyword('IS')) {
      advance();
      if (isKeyword('NOT')) {
        advance();
        expect('KEYWORD', 'NULL');
        return { type: 'IS_NOT_NULL', left };
      }
      expect('KEYWORD', 'NULL');
      return { type: 'IS_NULL', left };
    }

    if (isKeyword('BETWEEN')) {
      advance();
      const low = parsePrimary();
      expect('KEYWORD', 'AND');
      const high = parsePrimary();
      return { type: 'BETWEEN', left, low, high };
    }

    const op = peek().type;
    if (['EQ', 'NE', 'LT', 'GT', 'LE', 'GE'].includes(op)) {
      advance();
      // Right side could be a scalar subquery
      if (match('(')) {
        if (isKeyword('SELECT')) {
          const subquery = parseSelect();
          expect(')');
          return { type: 'COMPARE', op, left, right: { type: 'SUBQUERY', subquery } };
        }
        // Parenthesized expression
        const expr = parseExpr();
        expect(')');
        return { type: 'COMPARE', op, left, right: expr };
      }
      const right = parsePrimary();
      return { type: 'COMPARE', op, left, right };
    }
    return left;
  }

  function parsePrimaryWithConcat() {
    let left = parseMultiplicative();
    while (true) {
      const t = peek().type;
      if (t === 'CONCAT_OP') {
        advance();
        const right = parseMultiplicative();
        left = { type: 'function_call', func: 'CONCAT', args: [left, right] };
      } else if (t === 'PLUS') {
        advance();
        const right = parseMultiplicative();
        left = { type: 'arith', op: '+', left, right };
      } else if (t === 'MINUS') {
        advance();
        const right = parseMultiplicative();
        left = { type: 'arith', op: '-', left, right };
      } else break;
    }
    return left;
  }

  function parseMultiplicative() {
    let left = parsePrimary();
    while (true) {
      const t = peek().type;
      if (t === '*') {
        advance();
        const right = parsePrimary();
        left = { type: 'arith', op: '*', left, right };
      } else if (t === 'SLASH') {
        advance();
        const right = parsePrimary();
        left = { type: 'arith', op: '/', left, right };
      } else if (t === 'MOD') {
        advance();
        const right = parsePrimary();
        left = { type: 'arith', op: '%', left, right };
      } else break;
    }
    return left;
  }

  function parsePrimary() {
    const t = peek();
    if (t.type === 'NUMBER') { advance(); return { type: 'literal', value: t.value }; }
    if (t.type === 'STRING') { advance(); return { type: 'literal', value: t.value }; }
    if (t.type === 'PARAM') { advance(); return { type: 'PARAM', index: t.index }; }
    // Parenthesized expression or subquery
    if (t.type === '(') {
      advance();
      // Check for subquery
      if (isKeyword('SELECT')) {
        const subquery = parseSelect();
        expect(')');
        return { type: 'subquery', query: subquery };
      }
      const expr = parsePrimaryWithConcat();
      expect(')');
      return expr;
    }
    // MATCH(column) AGAINST('text')
    if (t.type === 'KEYWORD' && t.value === 'MATCH') {
      advance(); // MATCH
      expect('(');
      const column = advance().value;
      expect(')');
      expect('KEYWORD', 'AGAINST');
      expect('(');
      const searchExpr = parsePrimary();
      expect(')');
      return { type: 'MATCH_AGAINST', column, search: searchExpr };
    }
    if (t.type === 'KEYWORD' && t.value === 'NULL') { advance(); return { type: 'literal', value: null }; }

    // CAST(expr AS type)
    if (t.type === 'KEYWORD' && t.value === 'CAST') {
      advance(); // CAST
      expect('(');
      const expr = parseExpr();
      expect('KEYWORD', 'AS');
      const targetType = advance().value; // INT, TEXT, FLOAT, etc.
      expect(')');
      return { type: 'cast', expr, targetType };
    }
    if (t.type === 'KEYWORD' && t.value === 'TRUE') { advance(); return { type: 'literal', value: true }; }
    if (t.type === 'KEYWORD' && t.value === 'FALSE') { advance(); return { type: 'literal', value: false }; }
    if (t.type === 'KEYWORD' && t.value === 'CURRENT_TIMESTAMP') { advance(); return { type: 'function_call', func: 'CURRENT_TIMESTAMP', args: [] }; }
    if (t.type === 'KEYWORD' && t.value === 'CURRENT_DATE') { advance(); return { type: 'function_call', func: 'CURRENT_DATE', args: [] }; }

    // CASE expression
    if (t.type === 'KEYWORD' && t.value === 'CASE') {
      return parseCaseExpr();
    }

    // Built-in string/null functions
    if (t.type === 'KEYWORD' &&
      SCALAR_FUNCTIONS.has(t.value)) {
      const func = advance().value;
      expect('(');
      const args = [];
      if (!match(')')) {
        args.push(parseExpr());
        while (match(',')) args.push(parseExpr());
        expect(')');
      }
      return { type: 'function_call', func, args };
    }

    // Aggregate functions in expressions (HAVING, subqueries) — only if followed by (
    if (t.type === 'KEYWORD' && ['COUNT', 'SUM', 'AVG', 'MIN', 'MAX'].includes(t.value) && tokens[pos + 1]?.type === '(') {
      const func = advance().value;
      expect('(');
      let distinct = false;
      if (isKeyword('DISTINCT')) { distinct = true; advance(); }
      let arg;
      if (peek().type === '*') { advance(); arg = '*'; } else { arg = parseExpr(); }
      expect(')');
      return { type: 'aggregate_expr', func, arg, distinct };
    }

    if (t.type === 'IDENT') {
      // Check if this is a function call: identifier followed by (
      if (tokens[pos + 1]?.type === '(') {
        const func = advance().value.toUpperCase();
        expect('(');
        const args = [];
        if (!match(')')) {
          args.push(parseExpr());
          while (match(',')) args.push(parseExpr());
          expect(')');
        }
        return { type: 'function_call', func, args };
      }
      advance(); return { type: 'column_ref', name: t.value };
    }
    // Allow keywords used as column names (e.g., column named "count")
    if (t.type === 'KEYWORD' && tokens[pos + 1]?.type !== '(') {
      advance();
      return { type: 'column_ref', name: t.originalValue || t.value.toLowerCase() };
    }
    throw new Error(`Unexpected in expression: ${t.type} ${t.value || ''}`);
  }

  function parseCaseExpr() {
    advance(); // CASE
    // Check for simple CASE form: CASE expr WHEN val THEN result ...
    let operand = null;
    if (!isKeyword('WHEN')) {
      operand = parsePrimaryWithConcat();
    }
    const whens = [];
    while (isKeyword('WHEN')) {
      advance(); // WHEN
      const condition = parseExpr();
      expect('KEYWORD', 'THEN');
      const result = parsePrimaryWithConcat();
      if (operand) {
        // Convert simple CASE to searched CASE: WHEN operand = condition
        whens.push({ condition: { type: 'COMPARE', op: 'EQ', left: operand, right: condition }, result });
      } else {
        whens.push({ condition, result });
      }
    }
    let elseResult = null;
    if (isKeyword('ELSE')) {
      advance();
      elseResult = parsePrimaryWithConcat();
    }
    expect('KEYWORD', 'END');
    return { type: 'case_expr', whens, elseResult };
  }

  function parseOrderBy() {
    const cols = [];
    do {
      const expr = parseExpr();
      let column;
      if (expr.type === 'literal' && typeof expr.value === 'number' && Number.isInteger(expr.value)) {
        // Integer literal in ORDER BY = column position (1-based, SQL standard)
        column = expr.value;
      } else if (expr.type === 'column_ref') {
        column = expr.name;
      } else {
        column = expr; // Expression-based ORDER BY
      }
      let dir = 'ASC';
      if (isKeyword('DESC')) { dir = 'DESC'; advance(); }
      else if (isKeyword('ASC')) { advance(); }
      let nulls = null; // null = default behavior
      if (isKeyword('NULLS')) {
        advance();
        if (isKeyword('FIRST')) { nulls = 'FIRST'; advance(); }
        else if (isKeyword('LAST')) { nulls = 'LAST'; advance(); }
      }
      cols.push({ column, direction: dir, nulls });
    } while (match(','));
    return cols;
  }

  function parseGroupBy() {
    const cols = [];
    do {
      // Try to parse as expression (handles col % 3, function calls, etc.)
      const expr = parseExpr();
      if (expr.type === 'column_ref') {
        cols.push(expr.name); // Simple column name
      } else {
        cols.push(expr); // Expression
      }
    } while (match(','));
    return cols;
  }

  function parseOverClause() {
    expect('KEYWORD', 'OVER');
    expect('(');
    let partitionBy = null;
    let orderBy = null;
    if (isKeyword('PARTITION')) {
      advance(); // PARTITION
      expect('KEYWORD', 'BY');
      partitionBy = [];
      do { partitionBy.push(advance().value); } while (match(','));
    }
    if (isKeyword('ORDER')) {
      advance(); // ORDER
      expect('KEYWORD', 'BY');
      orderBy = parseOrderBy();
    }
    expect(')');
    return { partitionBy, orderBy };
  }

  function parseInsert() {
    advance(); // INSERT
    
    // INSERT OR REPLACE / INSERT OR IGNORE
    let orReplace = false;
    let orIgnore = false;
    if (isKeyword('OR')) {
      advance(); // OR
      if (isKeyword('REPLACE')) { advance(); orReplace = true; }
      else if (isKeyword('IGNORE')) { advance(); orIgnore = true; }
    }
    
    expect('KEYWORD', 'INTO');
    const table = advance().value;

    let columns = null;
    if (match('(')) {
      columns = [];
      do { columns.push(advance().value); } while (match(','));
      expect(')');
    }

    // INSERT INTO ... SELECT
    if (isKeyword('SELECT') || isKeyword('WITH')) {
      const selectStmt = isKeyword('WITH') ? parseWith() : parseSelect();
      return { type: 'INSERT_SELECT', table, columns, query: selectStmt };
    }

    expect('KEYWORD', 'VALUES');
    const rows = [];
    do {
      expect('(');
      const values = [];
      do { values.push(parsePrimary()); } while (match(','));
      expect(')');
      rows.push(values);
    } while (match(','));

    // ON CONFLICT clause (UPSERT)
    let onConflict = null;
    if (isKeyword('ON')) {
      advance(); // ON
      expect('KEYWORD', 'CONFLICT');
      let conflictColumns = null;
      if (match('(')) {
        conflictColumns = [];
        do { conflictColumns.push(advance().value); } while (match(','));
        expect(')');
      }
      if (isKeyword('DO')) {
        advance(); // DO
        if (isKeyword('NOTHING')) {
          advance();
          onConflict = { action: 'NOTHING', columns: conflictColumns };
        } else if (isKeyword('UPDATE')) {
          advance(); // UPDATE
          expect('KEYWORD', 'SET');
          const sets = [];
          do {
            const col = advance().value;
            expect('EQ');
            const val = parseExpr();
            sets.push({ column: col, value: val });
          } while (match(','));
          onConflict = { action: 'UPDATE', columns: conflictColumns, sets };
        }
      }
    }

    // RETURNING clause
    let returning = null;
    if (isKeyword('RETURNING')) {
      advance();
      if (match('*')) {
        returning = '*';
      } else {
        returning = [];
        do { returning.push(advance().value); } while (match(','));
      }
    }

    return { type: 'INSERT', table, columns, rows, onConflict, returning, orReplace, orIgnore };
  }

  function parseUpdate() {
    advance(); // UPDATE
    const table = advance().value;
    expect('KEYWORD', 'SET');
    const assignments = [];
    do {
      const col = advance().value;
      expect('EQ');
      const value = parsePrimaryWithConcat();
      assignments.push({ column: col, value });
    } while (match(','));
    
    // FROM clause (PostgreSQL-style UPDATE ... FROM)
    let from = null;
    if (isKeyword('FROM')) {
      advance(); // FROM
      const fromTable = advance().value;
      let alias = null;
      if (peek() && peek().type === 'IDENT' && !isKeyword('WHERE') && !isKeyword('SET')) {
        alias = advance().value;
      }
      from = { table: fromTable, alias: alias || fromTable };
    }
    
    let where = null;
    if (isKeyword('WHERE')) { advance(); where = parseExpr(); }
    
    let returning = null;
    if (isKeyword('RETURNING')) {
      advance();
      if (match('*')) { returning = '*'; }
      else {
        returning = [];
        do { returning.push(advance().value); } while (match(','));
      }
    }
    return { type: 'UPDATE', table, assignments, where, from, returning };
  }

  function parseDelete() {
    advance(); // DELETE
    expect('KEYWORD', 'FROM');
    const table = advance().value;
    
    // USING clause (multi-table DELETE)
    let using = null;
    if (isKeyword('USING')) {
      advance(); // USING
      const usingTable = advance().value;
      let alias = null;
      if (peek() && peek().type === 'IDENT' && !isKeyword('WHERE')) {
        alias = advance().value;
      }
      using = { table: usingTable, alias: alias || usingTable };
    }
    
    let where = null;
    if (isKeyword('WHERE')) { advance(); where = parseExpr(); }
    let returning = null;
    if (isKeyword('RETURNING')) {
      advance();
      if (match('*')) { returning = '*'; }
      else {
        returning = [];
        do { returning.push(advance().value); } while (match(','));
      }
    }
    return { type: 'DELETE', table, where, using, returning };
  }

  function parseValuesClause() {
    advance(); // VALUES
    const rows = [];
    do {
      expect('(');
      const values = [];
      do {
        values.push(parsePrimaryWithConcat());
      } while (match(','));
      expect(')');
      rows.push(values);
    } while (match(','));
    return { type: 'VALUES_QUERY', rows };
  }

  function parseMerge() {
    advance(); // MERGE
    expect('KEYWORD', 'INTO');
    const target = advance().value;
    let targetAlias = null;
    if (peek() && peek().type === 'IDENT' && !isKeyword('USING')) {
      targetAlias = advance().value;
    }
    
    expect('KEYWORD', 'USING');
    const source = advance().value;
    let sourceAlias = null;
    if (peek() && peek().type === 'IDENT' && !isKeyword('ON')) {
      sourceAlias = advance().value;
    }
    
    expect('KEYWORD', 'ON');
    const condition = parseExpr();
    
    const whenClauses = [];
    while (isKeyword('WHEN')) {
      advance(); // WHEN
      let matched;
      if (isKeyword('NOT')) {
        advance(); // NOT
        expect('KEYWORD', 'MATCHED');
        matched = false;
      } else {
        expect('KEYWORD', 'MATCHED');
        matched = true;
      }
      expect('KEYWORD', 'THEN');
      
      if (matched && isKeyword('UPDATE')) {
        advance(); // UPDATE
        expect('KEYWORD', 'SET');
        const assignments = [];
        do {
          const col = advance().value;
          expect('EQ');
          const value = parsePrimaryWithConcat();
          assignments.push({ column: col, value });
        } while (match(','));
        whenClauses.push({ matched: true, action: 'UPDATE', assignments });
      } else if (matched && isKeyword('DELETE')) {
        advance(); // DELETE
        whenClauses.push({ matched: true, action: 'DELETE' });
      } else if (!matched && isKeyword('INSERT')) {
        advance(); // INSERT
        let columns = null;
        if (match('(')) {
          columns = [];
          do { columns.push(advance().value); } while (match(','));
          expect(')');
        }
        expect('KEYWORD', 'VALUES');
        expect('(');
        const values = [];
        do { values.push(parsePrimaryWithConcat()); } while (match(','));
        expect(')');
        whenClauses.push({ matched: false, action: 'INSERT', columns, values });
      }
    }
    
    return {
      type: 'MERGE',
      target, targetAlias: targetAlias || target,
      source, sourceAlias: sourceAlias || source,
      condition, whenClauses
    };
  }

  function parseAlter() {
    advance(); // ALTER
    expect('KEYWORD', 'TABLE');
    const table = advance().value;
    
    if (isKeyword('ADD')) {
      advance(); // ADD
      if (isKeyword('COLUMN')) advance(); // optional COLUMN
      const tok = advance();
      const colName = tok.originalValue || tok.value;
      const colType = advance().value;
      let defaultVal = null;
      if (isKeyword('DEFAULT')) {
        advance();
        const t = advance();
        defaultVal = t.type === 'NUMBER' ? t.value : t.type === 'STRING' ? t.value : null;
      }
      return { type: 'ALTER_TABLE', table, action: 'ADD_COLUMN', column: colName, dataType: colType, defaultValue: defaultVal };
    }
    
    if (isKeyword('DROP')) {
      advance(); // DROP
      if (isKeyword('COLUMN')) advance(); // optional COLUMN
      const colName = advance().value;
      return { type: 'ALTER_TABLE', table, action: 'DROP_COLUMN', column: colName };
    }
    
    if (isKeyword('RENAME')) {
      advance(); // RENAME
      if (isKeyword('COLUMN')) advance(); // optional COLUMN
      const oldName = advance().value;
      expect('KEYWORD', 'TO');
      const tok = advance();
      const newName = tok.originalValue || tok.value;
      return { type: 'ALTER_TABLE', table, action: 'RENAME_COLUMN', oldName, newName };
    }
    
    throw new Error('Expected ADD, DROP, or RENAME after ALTER TABLE');
  }

  function parseCreate() {
    advance(); // CREATE
    let unique = false;
    if (isKeyword('UNIQUE')) { unique = true; advance(); }
    if (isKeyword('FULLTEXT')) {
      advance(); // FULLTEXT
      expect('KEYWORD', 'INDEX');
      const name = advance().value;
      expect('KEYWORD', 'ON');
      const table = advance().value;
      expect('(');
      const column = advance().value;
      expect(')');
      return { type: 'CREATE_FULLTEXT_INDEX', name, table, column };
    }
    if (isKeyword('INDEX')) return parseCreateIndex(unique);
    if (isKeyword('VIEW')) return parseCreateView();
    if (isKeyword('SEQUENCE')) {
      advance(); // SEQUENCE
      const name = (advance().originalValue || tokens[pos-1].value);
      const options = { start: 1, increment: 1, minValue: 1, maxValue: Number.MAX_SAFE_INTEGER, cycle: false };
      while (peek().type !== 'EOF' && peek().type !== ';') {
        if (isKeyword('START')) { advance(); if (isKeyword('WITH')) advance(); options.start = parseInt(advance().value, 10); }
        else if (isKeyword('INCREMENT')) { advance(); if (isKeyword('BY')) advance(); options.increment = parseInt(advance().value, 10); }
        else if (isKeyword('MINVALUE')) { advance(); options.minValue = parseInt(advance().value, 10); }
        else if (isKeyword('MAXVALUE')) { advance(); options.maxValue = parseInt(advance().value, 10); }
        else if (isKeyword('CYCLE')) { advance(); options.cycle = true; }
        else if (peek().type === 'IDENT' && peek().value === 'NO') { advance(); if (isKeyword('CYCLE')) { advance(); options.cycle = false; } else if (isKeyword('MINVALUE')) advance(); else if (isKeyword('MAXVALUE')) advance(); }
        else advance();
      }
      return { type: 'CREATE_SEQUENCE', name, options };
    }
    if (isKeyword('TRIGGER')) {
      advance(); // TRIGGER
      const name = advance().value;
      const timing = advance().value; // BEFORE or AFTER
      const event = advance().value; // INSERT, UPDATE, DELETE
      expect('KEYWORD', 'ON');
      const table = advance().value;
      if (isKeyword('FOR')) { advance(); expect('KEYWORD', 'EACH'); expect('KEYWORD', 'ROW'); }
      if (isKeyword('EXECUTE')) advance();
      const bodyTokens = [];
      while (peek().type !== 'EOF') {
        const tok = advance();
        if (tok.type === 'STRING') bodyTokens.push(`'${tok.value}'`);
        else if (tok.type === 'NUMBER') bodyTokens.push(String(tok.value));
        else if (tok.type === 'KEYWORD' || tok.type === 'IDENT') bodyTokens.push(tok.originalValue || tok.value);
        else if (['(', ')', ',', ';'].includes(tok.type)) bodyTokens.push(tok.type);
        else if (tok.type === 'EQ') bodyTokens.push('=');
        else if (tok.type === 'PLUS') bodyTokens.push('+');
        else if (tok.type === 'MINUS') bodyTokens.push('-');
        else if (tok.type === '*') bodyTokens.push('*');
        else bodyTokens.push(tok.value || tok.type);
      }
      return { type: 'CREATE_TRIGGER', name, timing, event, table, bodySql: bodyTokens.join(' ') };
    }
    if (isKeyword('MATERIALIZED')) {
      advance(); // MATERIALIZED
      expect('KEYWORD', 'VIEW');
      const name = advance().value;
      expect('KEYWORD', 'AS');
      const query = parseSelect();
      return { type: 'CREATE_MATVIEW', name, query };
    }
    if (isKeyword('FUNCTION') || isKeyword('OR')) {
      // CREATE [OR REPLACE] FUNCTION
      let orReplace = false;
      if (isKeyword('OR')) {
        advance(); // OR
        expect('KEYWORD', 'REPLACE');
        orReplace = true;
        expect('KEYWORD', 'FUNCTION');
      } else {
        advance(); // FUNCTION
      }
      const name = advance().value.toLowerCase();
      expect('(');
      const params = [];
      while (peek().type !== ')') {
        const paramName = advance().value.toLowerCase();
        const paramType = advance().value.toUpperCase();
        params.push({ name: paramName, type: paramType });
        if (peek().type === ',') advance();
      }
      expect(')');
      let returnType = null;
      if (isKeyword('RETURNS')) {
        advance(); // RETURNS
        returnType = advance().value.toUpperCase();
      }
      // Parse body: AS $$ body $$ or AS 'body'
      expect('KEYWORD', 'AS');
      let body;
      if (peek().type === 'STRING') {
        body = advance().value;
      } else if (peek().value === '$$' || peek().type === '$$') {
        // Dollar-quoted string: consume everything between $$ and $$
        advance(); // opening $$
        const bodyTokens = [];
        while (pos < tokens.length && !(peek().value === '$$' || peek().type === '$$')) {
          const t = peek();
          bodyTokens.push(t.originalValue || t.value || t.type);
          advance();
        }
        if (pos < tokens.length) advance(); // closing $$
        body = bodyTokens.join(' ');
      } else {
        throw new Error('Expected function body after AS');
      }
      // Optional: LANGUAGE specifier (ignore for now)
      let language = 'sql';
      if (isKeyword('LANGUAGE')) {
        advance();
        language = advance().value.toLowerCase();
      }
      return { type: 'CREATE_FUNCTION', name, params, returnType, body, language, orReplace };
    }
    expect('KEYWORD', 'TABLE');
    let ifNotExists = false;
    if (isKeyword('IF')) { advance(); expect('KEYWORD', 'NOT'); expect('KEYWORD', 'EXISTS'); ifNotExists = true; }
    const tableTok = advance();
    const table = tableTok.originalValue || tableTok.value;
    
    // CREATE TABLE ... AS SELECT (CTAS)
    if (isKeyword('AS')) {
      advance(); // AS
      const query = parseSelect();
      return { type: 'CREATE_TABLE_AS', table, query };
    }
    
    expect('(');
    const columns = [];
    const tableConstraints = [];
    do {
      // Check for table-level constraints
      if (isKeyword('FOREIGN')) {
        advance(); // FOREIGN
        expect('KEYWORD', 'KEY');
        expect('(');
        const fkCols = [];
        do { fkCols.push(advance().value); } while (match(','));
        expect(')');
        expect('KEYWORD', 'REFERENCES');
        const refTable = advance().value;
        expect('(');
        const refCols = [];
        do { refCols.push(advance().value); } while (match(','));
        expect(')');
        let onDelete = 'RESTRICT';
        let onUpdate = 'RESTRICT';
        while (isKeyword('ON')) {
          advance();
          if (isKeyword('DELETE')) {
            advance();
            if (isKeyword('CASCADE')) { advance(); onDelete = 'CASCADE'; }
            else if (isKeyword('SET')) { advance(); expect('KEYWORD', 'NULL'); onDelete = 'SET NULL'; }
            else if (isKeyword('RESTRICT')) { advance(); onDelete = 'RESTRICT'; }
            else if (isKeyword('NO')) { advance(); advance() /* ACTION */; onDelete = 'NO ACTION'; }
          } else if (isKeyword('UPDATE')) {
            advance();
            if (isKeyword('CASCADE')) { advance(); onUpdate = 'CASCADE'; }
            else if (isKeyword('SET')) { advance(); expect('KEYWORD', 'NULL'); onUpdate = 'SET NULL'; }
            else if (isKeyword('RESTRICT')) { advance(); onUpdate = 'RESTRICT'; }
            else if (isKeyword('NO')) { advance(); advance() /* ACTION */; onUpdate = 'NO ACTION'; }
          }
        }
        tableConstraints.push({ type: 'FOREIGN_KEY', columns: fkCols, references: { table: refTable, columns: refCols, onDelete, onUpdate } });
        continue;
      }
      if (isKeyword('PRIMARY')) {
        advance(); // PRIMARY
        expect('KEYWORD', 'KEY');
        expect('(');
        const pkCols = [];
        do { pkCols.push(advance().value); } while (match(','));
        expect(')');
        tableConstraints.push({ type: 'PRIMARY_KEY', columns: pkCols });
        continue;
      }
      if (isKeyword('CHECK')) {
        advance(); // CHECK
        expect('(');
        const checkExpr = parseExpr();
        expect(')');
        tableConstraints.push({ type: 'CHECK', expression: checkExpr });
        continue;
      }
      if (isKeyword('UNIQUE')) {
        advance(); // UNIQUE
        expect('(');
        const uqCols = [];
        do { uqCols.push(advance().value); } while (match(','));
        expect(')');
        tableConstraints.push({ type: 'UNIQUE', columns: uqCols });
        continue;
      }
      const tok = advance();
      const name = tok.originalValue || tok.value;
      const dataType = advance().value;
      let primaryKey = false;
      let notNull = false;
      let check = null;
      let defaultVal = null;
      let references = null;
      let generated = null;
      // Parse column constraints
      while (true) {
        if (isKeyword('PRIMARY')) { advance(); expect('KEYWORD', 'KEY'); primaryKey = true; }
        else if (isKeyword('NOT')) { advance(); expect('KEYWORD', 'NULL'); notNull = true; }
        else if (isKeyword('CHECK')) {
          advance();
          expect('(');
          check = parseExpr();
          expect(')');
        }
        else if (isKeyword('GENERATED')) {
          advance(); // GENERATED
          expect('KEYWORD', 'ALWAYS');
          expect('KEYWORD', 'AS');
          expect('(');
          const genExpr = parseExpr();
          expect(')');
          let mode = 'VIRTUAL'; // default
          if (isKeyword('STORED')) { advance(); mode = 'STORED'; }
          else if (isKeyword('VIRTUAL')) { advance(); mode = 'VIRTUAL'; }
          generated = { expression: genExpr, mode };
        }
        else if (isKeyword('DEFAULT')) {
          advance();
          const t = advance();
          defaultVal = t.type === 'NUMBER' ? t.value : t.type === 'STRING' ? t.value : null;
        }
        else if (isKeyword('REFERENCES')) {
          advance();
          const refTable = advance().value;
          expect('(');
          const refColumn = advance().value;
          expect(')');
          let onDelete = 'RESTRICT';
          let onUpdate = 'RESTRICT';
          while (isKeyword('ON')) {
            advance();
            if (isKeyword('DELETE')) {
              advance();
              if (isKeyword('CASCADE')) { advance(); onDelete = 'CASCADE'; }
              else if (isKeyword('SET')) { advance(); expect('KEYWORD', 'NULL'); onDelete = 'SET NULL'; }
              else if (isKeyword('RESTRICT')) { advance(); onDelete = 'RESTRICT'; }
              else if (isKeyword('NO')) { advance(); advance() /* ACTION */; onDelete = 'NO ACTION'; }
            } else if (isKeyword('UPDATE')) {
              advance();
              if (isKeyword('CASCADE')) { advance(); onUpdate = 'CASCADE'; }
              else if (isKeyword('SET')) { advance(); expect('KEYWORD', 'NULL'); onUpdate = 'SET NULL'; }
              else if (isKeyword('RESTRICT')) { advance(); onUpdate = 'RESTRICT'; }
              else if (isKeyword('NO')) { advance(); advance() /* ACTION */; onUpdate = 'NO ACTION'; }
            }
          }
          references = { table: refTable, column: refColumn, onDelete, onUpdate };
        }
        else break;
      }
      columns.push({ name, type: dataType, primaryKey, notNull, check, defaultValue: defaultVal, references, generated });
    } while (match(','));
    expect(')');
    return { type: 'CREATE_TABLE', table, columns, ifNotExists, constraints: tableConstraints.length > 0 ? tableConstraints : null };
  }

  function parseCreateIndex(unique) {
    advance(); // INDEX
    const name = advance().value;
    expect('KEYWORD', 'ON');
    const table = advance().value;

    // CREATE TABLE ... AS SELECT
    if (isKeyword('AS')) {
      advance(); // AS
      const query = parseSelect();
      return { type: 'CREATE_TABLE_AS', table, query, ifNotExists };
    }

    expect('(');
    const columns = [];
    const expressions = [];
    let hasExpressions = false;
    do {
      // Try to parse as an expression — if it's a simple identifier, treat as column name
      const expr = parseExpr();
      if (expr.type === 'column_ref' && !expr.table) {
        columns.push(expr.name || expr.column);
        expressions.push(null);
      } else {
        // Expression index: store the expression
        columns.push(null);
        expressions.push(expr);
        hasExpressions = true;
      }
    } while (match(','));
    expect(')');
    // INCLUDE clause for covering indexes
    let include = null;
    if (isKeyword('INCLUDE')) {
      advance();
      expect('(');
      include = [];
      do { include.push(advance().value); } while (match(','));
      expect(')');
    }
    // WHERE clause for partial indexes
    let where = null;
    if (isKeyword('WHERE')) {
      advance();
      where = parseExpr();
    }
    return { type: 'CREATE_INDEX', name, table, columns, unique, include, where, expressions: hasExpressions ? expressions : null };
  }

  function parseCreateView() {
    advance(); // VIEW
    const name = advance().value;
    if (isKeyword('AS')) advance(); // optional AS
    const query = parseSelect();
    return { type: 'CREATE_VIEW', name, query };
  }

  function parseDrop() {
    advance(); // DROP
    if (isKeyword('INDEX')) {
      advance();
      const name = advance().value;
      return { type: 'DROP_INDEX', name };
    }
    if (isKeyword('VIEW')) {
      advance();
      const name = advance().value;
      return { type: 'DROP_VIEW', name };
    }
    if (isKeyword('SEQUENCE')) {
      advance();
      let ifExists = false;
      if (isKeyword('IF')) { advance(); if (isKeyword('EXISTS')) advance(); ifExists = true; }
      const name = (advance().originalValue || tokens[pos-1].value);
      return { type: 'DROP_SEQUENCE', name, ifExists };
    }
    if (isKeyword('FUNCTION')) {
      advance();
      let ifExists = false;
      if (isKeyword('IF')) { advance(); if (isKeyword('EXISTS')) advance(); ifExists = true; }
      const name = advance().value.toLowerCase();
      // Optional parameter list (ignored — we match by name only)
      if (peek().type === '(') {
        advance();
        while (peek().type !== ')' && pos < tokens.length) advance();
        if (peek().type === ')') advance();
      }
      return { type: 'DROP_FUNCTION', name, ifExists };
    }
    expect('KEYWORD', 'TABLE');
    let ifExists = false;
    if (isKeyword('IF')) { advance(); expect('KEYWORD', 'EXISTS'); ifExists = true; }
    const table = advance().value;
    return { type: 'DROP_TABLE', table, ifExists };
  }

  function parseAlter() {
    advance(); // ALTER
    expect('KEYWORD', 'TABLE');
    const table = advance().value;

    if (isKeyword('ADD')) {
      advance(); // ADD
      if (isKeyword('COLUMN')) advance(); // optional COLUMN keyword
      const name = advance().value;
      const dataType = advance().value;
      let defaultValue = null;
      if (isKeyword('DEFAULT')) {
        advance();
        defaultValue = parsePrimary();
      }
      return { type: 'ALTER_TABLE', table, action: 'ADD_COLUMN', column: { name, type: dataType, default: defaultValue } };
    }

    if (isKeyword('DROP')) {
      advance(); // DROP
      if (isKeyword('COLUMN')) advance(); // optional COLUMN keyword
      const name = advance().value;
      return { type: 'ALTER_TABLE', table, action: 'DROP_COLUMN', column: { name } };
    }

    if (isKeyword('RENAME')) {
      advance(); // RENAME
      if (isKeyword('COLUMN')) {
        advance(); // COLUMN
        const oldName = advance().value;
        expect('KEYWORD', 'TO');
        const newName = advance().value;
        return { type: 'ALTER_TABLE', table, action: 'RENAME_COLUMN', column: { oldName, newName } };
      }
      expect('KEYWORD', 'TO');
      const newName = advance().value;
      return { type: 'ALTER_TABLE', table, action: 'RENAME_TABLE', newName };
    }

    throw new Error('ALTER TABLE requires ADD, DROP, or RENAME');
  }
}
