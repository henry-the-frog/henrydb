// sql.js — SQL tokenizer, parser, and query executor for HenryDB

// ===== Tokenizer =====
const KEYWORDS = new Set([
  'SELECT', 'FROM', 'WHERE', 'INSERT', 'INTO', 'VALUES', 'UPDATE', 'SET',
  'DELETE', 'CREATE', 'TABLE', 'DROP', 'AND', 'OR', 'NOT', 'NULL', 'TRUE',
  'FALSE', 'ORDER', 'BY', 'ASC', 'DESC', 'LIMIT', 'OFFSET', 'FETCH', 'FIRST', 'NEXT', 'ROWS', 'ROW', 'ONLY', 'AS',
  'INT', 'INTEGER', 'TEXT', 'VARCHAR', 'FLOAT', 'BOOL', 'BOOLEAN',
  'PRIMARY', 'KEY', 'COUNT', 'SUM', 'AVG', 'MIN', 'MAX',
  'JOIN', 'INNER', 'LEFT', 'RIGHT', 'ON', 'GROUP', 'HAVING',
  'INDEX', 'UNIQUE', 'IF', 'EXISTS', 'IN', 'ALTER', 'ADD', 'COLUMN', 'DEFAULT', 'RENAME', 'TO',
  'LIKE', 'ILIKE', 'SIMILAR', 'UPPER', 'LOWER', 'INITCAP', 'LENGTH', 'CHAR_LENGTH', 'CONCAT', 'BETWEEN', 'SYMMETRIC', 'TABLESAMPLE', 'POSITION',
  'OVER', 'PARTITION', 'ROW_NUMBER', 'RANK', 'DENSE_RANK', 'LAG', 'LEAD', 'VIEW', 'DISTINCT',
  'WITH', 'RECURSIVE', 'UNION', 'ALL', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END', 'EXPLAIN', 'ANALYZE', 'COMPILED', 'FORMAT',
  'INTERSECT', 'EXCEPT', 'GENERATED', 'ALWAYS', 'STORED', 'ROLLUP', 'CUBE', 'GROUPING', 'SETS', 'MERGE', 'USING', 'MATCHED',
  'IS', 'COALESCE', 'NULLIF', 'TRUNCATE', 'CROSS', 'FULL', 'OUTER', 'NATURAL', 'USING', 'SHOW', 'TABLES', 'DESCRIBE',
  'SUBSTRING', 'SUBSTR', 'REPLACE', 'TRIM', 'ABS', 'ROUND', 'CEIL', 'FLOOR', 'IFNULL', 'IIF', 'TYPEOF',
  'LEFT', 'RIGHT', 'LPAD', 'RPAD', 'REVERSE', 'REPEAT',
  'POWER', 'SQRT', 'LOG', 'EXP', 'RANDOM',
  'CURRENT_TIMESTAMP', 'CURRENT_DATE', 'NOW', 'STRFTIME',
  'SHOW', 'TABLES', 'COLUMNS',
  'TRUNCATE', 'RENAME', 'DESCRIBE',
  'BEGIN', 'COMMIT', 'ROLLBACK', 'TRANSACTION', 'VACUUM', 'CHECKPOINT',
  'OVER', 'PARTITION', 'RANK', 'ROW_NUMBER', 'DENSE_RANK', 'NTILE', 'LAG', 'LEAD',
  'INCLUDE', 'ALTER', 'ADD', 'COLUMN', 'RENAME', 'TO', 'CHECK',
  'UNBOUNDED', 'PRECEDING', 'FOLLOWING', 'RANGE', 'CURRENT',
  'REFERENCES', 'FOREIGN', 'CASCADE', 'RESTRICT', 'SET',
  'CAST', 'INT', 'INTEGER', 'TEXT', 'FLOAT', 'BOOLEAN',
  'GROUP_CONCAT', 'STRING_AGG', 'SEPARATOR',
  'JSON_AGG', 'JSONB_AGG', 'ARRAY_AGG',
  'JSON_BUILD_OBJECT', 'JSON_BUILD_ARRAY', 'ROW_TO_JSON', 'TO_JSON', 'JSON_OBJECT_KEYS', 'DATE_ADD', 'DATE_DIFF', 'DATE_TRUNC',
  'CONFLICT', 'DO', 'NOTHING',
  'ANALYZE', 'RETURNING', 'USING', 'FIRST_VALUE', 'LAST_VALUE',
  'MATERIALIZED', 'REFRESH',
  'TRIGGER', 'BEFORE', 'AFTER', 'EACH', 'ROW', 'EXECUTE', 'PREPARE', 'DEALLOCATE',
  'IF', 'EXISTS',
  'JSON_EXTRACT', 'JSON_SET', 'JSON_ARRAY_LENGTH', 'JSON_TYPE', 'JSON_OBJECT', 'JSON_ARRAY', 'JSON_VALID', 'JSON_VALUE',
  'FULLTEXT', 'MATCH', 'AGAINST',
  'GENERATE_SERIES', 'LATERAL',
  'EXTRACT', 'DATE_PART', 'LTRIM', 'RTRIM', 'INTERVAL', 'GREATEST', 'LEAST', 'MOD', 'FOR',
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
        if (src[i] === "'" && i + 1 < src.length && src[i + 1] === "'") {
          // Escaped single quote ('')
          str += "'";
          i += 2;
        } else if (src[i] === "'") {
          // End of string
          break;
        } else {
          str += src[i++];
        }
      }
      if (i >= src.length) throw new Error('Unterminated string literal');
      i++; // closing quote
      tokens.push({ type: 'STRING', value: str });
      continue;
    }

    // Number
    if (/[0-9]/.test(src[i]) || (src[i] === '-' && /[0-9]/.test(src[i + 1]) && (tokens.length === 0 || !['NUMBER', 'IDENT', ')', 'STRING'].includes(tokens[tokens.length-1]?.type)))) {
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
    if (src[i] === ':' && src[i + 1] === ':') { tokens.push({ type: 'CAST_OP' }); i += 2; continue; }
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
      // Check for qualified star: table.* 
      if (ident.endsWith('.') && i < src.length && src[i] === '*') {
        i++; // consume *
        tokens.push({ type: 'QUALIFIED_STAR', table: ident.slice(0, -1) });
        continue;
      }
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
    let analyze = false, compiled = false, format = 'text';
    // Parse options: ANALYZE, COMPILED, (FORMAT JSON|YAML|DOT|TEXT)
    while (true) {
      if (isKeyword('ANALYZE')) { advance(); analyze = true; continue; }
      if (isKeyword('COMPILED')) { advance(); compiled = true; continue; }
      if (peek() && peek().type === '(') {
        advance(); // skip '('
        if (isKeyword('FORMAT')) {
          advance();
          const fmtToken = peek();
          const fmt = (fmtToken.value || '').toUpperCase();
          if (['JSON', 'YAML', 'DOT', 'TEXT', 'TREE', 'HTML'].includes(fmt)) {
            format = fmt.toLowerCase();
            advance();
          }
        }
        if (peek() && peek().type === ')') advance(); // skip ')'
        continue;
      }
      break;
    }
    // Parse the underlying statement
    let statement;
    if (isKeyword('WITH')) statement = parseWith();
    else if (isKeyword('SELECT')) statement = parseSelect();
    else throw new Error('EXPLAIN requires a SELECT statement');
    return { type: 'EXPLAIN', statement, analyze, compiled, format };
  }

  // SELECT or WITH
  if (isKeyword('WITH')) return parseWith();
  if (isKeyword('SELECT')) return parseSelect();
  if (isKeyword('VALUES')) return parseValuesClause();
  if (isKeyword('MERGE')) return parseMerge();
  if (isKeyword('INSERT')) return parseInsert();
  if (isKeyword('UPDATE')) return parseUpdate();
  if (isKeyword('DELETE')) return parseDelete();
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
    return { type: 'TRUNCATE_TABLE', table: (function(){ const t = advance(); return t.originalValue || t.value; })() };
  }
  if (isKeyword('RENAME')) {
    advance(); expect('KEYWORD', 'TABLE');
    const _renFrom = advance(); const from = _renFrom.originalValue || _renFrom.value;
    expect('KEYWORD', 'TO');
    const _renTo = advance(); const to = _renTo.originalValue || _renTo.value;
    return { type: 'RENAME_TABLE', from, to };
  }
  if (isKeyword('DESCRIBE')) {
    advance();
    return { type: 'SHOW_COLUMNS', table: advance().value };
  }
  if (isKeyword('TRUNCATE')) { advance(); if (isKeyword('TABLE')) advance(); const _tt = advance(); return { type: 'TRUNCATE', table: _tt.originalValue || _tt.value }; }
  if (isKeyword('SHOW')) { advance(); expect('KEYWORD', 'TABLES'); return { type: 'SHOW_TABLES' }; }
  if (isKeyword('DESCRIBE')) { advance(); return { type: 'DESCRIBE', table: advance().value }; }
  if (isKeyword('BEGIN')) { advance(); if (isKeyword('TRANSACTION')) advance(); return { type: 'BEGIN' }; }
  if (isKeyword('COMMIT')) { advance(); return { type: 'COMMIT' }; }
  if (isKeyword('ROLLBACK')) { advance(); return { type: 'ROLLBACK' }; }
  if (isKeyword('VACUUM')) { advance(); let table = null; if (peek().type === 'IDENT' || peek().type === 'KEYWORD') table = (advance().originalValue || tokens[pos-1].value); return { type: 'VACUUM', table }; }
  if (isKeyword('CHECKPOINT')) { advance(); return { type: 'CHECKPOINT' }; }
  if (isKeyword('ANALYZE') && !isKeyword('EXPLAIN')) {
    advance();
    let table = null;
    if (peek()?.type === 'IDENT') table = advance().value;
    return { type: 'ANALYZE_TABLE', table };
  }
  // PREPARE name AS query
  if (isKeyword('PREPARE')) {
    advance(); // PREPARE
    const name = advance().value;
    if (isKeyword('AS')) advance(); // AS
    // Parse the inner statement (SELECT, INSERT, UPDATE, DELETE)
    let query;
    if (isKeyword('SELECT') || isKeyword('WITH')) {
      query = isKeyword('WITH') ? parseWith() : parseSelect();
    } else if (isKeyword('INSERT')) {
      query = parseInsert();
    } else if (isKeyword('UPDATE')) {
      query = parseUpdate();
    } else if (isKeyword('DELETE')) {
      query = parseDelete();
    } else {
      throw new Error(`PREPARE body must be SELECT, INSERT, UPDATE, or DELETE`);
    }
    return { type: 'PREPARE', name, query };
  }
  // EXECUTE name(param1, param2, ...)
  if (isKeyword('EXECUTE') && peek()?.type !== '(') {
    advance(); // EXECUTE
    const name = advance().value;
    const params = [];
    if (match('(')) {
      if (!match(')')) {
        params.push(parseExpr());
        while (match(',')) params.push(parseExpr());
        expect(')');
      }
    }
    return { type: 'EXECUTE_PREPARED', name, params };
  }
  // DEALLOCATE name
  if (isKeyword('DEALLOCATE')) {
    advance();
    if (isKeyword('ALL')) { advance(); return { type: 'DEALLOCATE', name: null, all: true }; }
    const name = advance().value;
    return { type: 'DEALLOCATE', name, all: false };
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
      // Optional column aliases: cte_name(col1, col2, ...) AS (...)
      let cteColumns = null;
      if (peek() && peek().type === '(' && !isKeyword('AS')) {
        // Check if this is column aliases (identifiers) or the body (SELECT)
        // Peek ahead to see if after ( we get an identifier, not SELECT/WITH
        const saved = pos;
        advance(); // consume (
        if (!isKeyword('SELECT') && !isKeyword('WITH')) {
          // Column aliases
          cteColumns = [];
          do {
            const colTok = advance();
            cteColumns.push(colTok.originalValue || colTok.value);
          } while (match(','));
          expect(')');
        } else {
          // Not column aliases — go back
          pos = saved;
        }
      }
      if (isKeyword('AS')) advance(); // optional AS
      expect('(');
      let baseQuery = parseSelect();
      // Check for UNION ALL (recursive CTEs) — parseSelect may or may not consume this
      let unionQuery = null;
      if (baseQuery.type === 'UNION') {
        // parseSelect already parsed the UNION ALL
        unionQuery = baseQuery.right;
        unionQuery.unionAll = baseQuery.all;
        baseQuery = baseQuery.left;
      } else if (isKeyword('UNION')) {
        advance(); // UNION
        const all = isKeyword('ALL') ? (advance(), true) : false;
        unionQuery = parseSelect();
        unionQuery.unionAll = all;
      }
      expect(')');
      ctes.push({ name, query: baseQuery, unionQuery, recursive, columns: cteColumns });
    } while (match(','));

    // Main query
    const mainQuery = parseSelect();
    mainQuery.ctes = ctes;
    return mainQuery;
  }

  function parseValuesClause() {
    advance(); // VALUES
    const tuples = [];
    do {
      expect('(');
      const values = [];
      do {
        values.push(parseExpr());
      } while (match(','));
      expect(')');
      tuples.push(values);
    } while (match(','));
    return { type: 'VALUES', tuples };
  }

  function parseSelect() {
    advance(); // SELECT
    let distinct = false;
    let distinctOn = null;
    if (isKeyword('DISTINCT')) {
      distinct = true; advance();
      if (isKeyword('ON') && tokens[pos + 1] && tokens[pos + 1].type === '(') {
        advance(); // ON
        advance(); // (
        distinctOn = [];
        // Parse column references
        distinctOn.push({ type: 'column_ref', name: advance().value });
        while (peek().type === ',') { advance(); distinctOn.push({ type: 'column_ref', name: advance().value }); }
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
        const nextTable = advance().value;
        let nextAlias = null;
        if (peek().type === 'IDENT') nextAlias = advance().value;
        else if (isKeyword('AS')) { advance(); nextAlias = readAlias(); }
        joins.push({ joinType: 'CROSS', table: nextTable, alias: nextAlias, on: null });
      }

      // JOINs
      while (isKeyword('JOIN') || isKeyword('INNER') || isKeyword('LEFT') || isKeyword('RIGHT') || isKeyword('CROSS') || isKeyword('FULL') || isKeyword('NATURAL')) {
        joins.push(parseJoin());
      }
    }

    if (isKeyword('WHERE')) { advance(); where = parseExpr(); }
    if (isKeyword('GROUP')) { advance(); expect('KEYWORD', 'BY'); groupBy = parseGroupBy(); }
    if (isKeyword('HAVING')) { advance(); having = parseExpr(); }
    if (isKeyword('ORDER')) { advance(); expect('KEYWORD', 'BY'); orderBy = parseOrderBy(); }
    if (isKeyword('LIMIT')) {
      advance();
      if (isKeyword('ALL')) { advance(); }
      else {
        const e = parseExpr();
        limit = e.type === 'literal' ? e.value : null;
        // Store expression for evaluation if not a literal
        if (limit == null && e.type === 'arith') {
          // Simple constant expression evaluation
          try {
            const evalArith = (n) => {
              if (n.type === 'literal') return n.value;
              if (n.type === 'arith') {
                const l = evalArith(n.left), r = evalArith(n.right);
                switch(n.op) { case '+': return l+r; case '-': return l-r; case '*': return l*r; case '/': return l/r; case '%': return l%r; }
              }
              return null;
            };
            limit = evalArith(e);
          } catch { limit = null; }
        }
      }
    }
    if (isKeyword('OFFSET')) {
      advance();
      const e = parseExpr();
      offset = e.type === 'literal' ? e.value : null;
      if (offset == null && e.type === 'arith') {
        try {
          const evalArith = (n) => {
            if (n.type === 'literal') return n.value;
            if (n.type === 'arith') {
              const l = evalArith(n.left), r = evalArith(n.right);
              switch(n.op) { case '+': return l+r; case '-': return l-r; case '*': return l*r; case '/': return l/r; case '%': return l%r; }
            }
            return null;
          };
          offset = evalArith(e);
        } catch { offset = null; }
      }
      if (isKeyword('ROWS') || isKeyword('ROW')) advance();
    }
    // SQL standard: FETCH FIRST N ROWS ONLY
    if (isKeyword('FETCH')) {
      advance(); // FETCH
      if (isKeyword('FIRST') || isKeyword('NEXT')) advance();
      if (peek().type === 'NUMBER') limit = advance().value;
      else limit = 1; // FETCH FIRST ROW ONLY = 1
      if (isKeyword('ROWS') || isKeyword('ROW')) advance();
      if (isKeyword('ONLY')) advance();
    }

    // FOR UPDATE / FOR SHARE / FOR NO KEY UPDATE / FOR KEY SHARE
    let forUpdate = null;
    if (isKeyword('FOR')) {
      advance(); // FOR
      const lockToken = advance();
      const lockMode = lockToken.value.toUpperCase();
      if (lockMode === 'UPDATE') {
        forUpdate = 'UPDATE';
      } else if (lockMode === 'SHARE') {
        forUpdate = 'SHARE';
      } else if (lockMode === 'NO') {
        // FOR NO KEY UPDATE
        advance(); // KEY
        advance(); // UPDATE
        forUpdate = 'NO KEY UPDATE';
      } else if (lockMode === 'KEY') {
        advance(); // SHARE
        forUpdate = 'KEY SHARE';
      }
      // Optional: NOWAIT / SKIP LOCKED
      if (isKeyword('NOWAIT')) {
        advance();
        forUpdate += ' NOWAIT';
      } else if (isKeyword('SKIP')) {
        advance(); // SKIP
        advance(); // LOCKED
        forUpdate += ' SKIP LOCKED';
      }
    }

    let result = { type: 'SELECT', distinct, distinctOn, columns, from, joins, where, groupBy, having, orderBy, limit, offset, forUpdate };

    // UNION / UNION ALL / INTERSECT / EXCEPT
    if (isKeyword('UNION')) {
      advance();
      let all = false;
      if (isKeyword('ALL')) { all = true; advance(); }
      const right = parseSelect();
      result = { type: 'UNION', left: result, right, all };
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
    if (peek().type === 'QUALIFIED_STAR') {
      const t = advance();
      const cols = [{ type: 'qualified_star', table: t.table }];
      while (match(',')) cols.push(parseSelectColumn());
      return cols;
    }
    const cols = [parseSelectColumn()];
    while (match(',')) {
      if (peek().type === 'QUALIFIED_STAR') {
        const t = advance();
        cols.push({ type: 'qualified_star', table: t.table });
      } else {
        cols.push(parseSelectColumn());
      }
    }
    return cols;
  }

  function parseSelectColumn() {
    // CURRENT_TIMESTAMP, CURRENT_DATE (no parens)
    if (peek().type === 'KEYWORD' && (peek().value === 'CURRENT_TIMESTAMP' || peek().value === 'CURRENT_DATE')) {
      const func = advance().value;
      // Check for arithmetic after (e.g., CURRENT_DATE + INTERVAL '1 day')
      let node = { type: 'function', func, args: [] };
      while (['PLUS', 'MINUS'].includes(peek().type)) {
        const op = peek().type === 'PLUS' ? '+' : '-';
        advance();
        const right = parsePrimary();
        node = { type: 'arith', op, left: node, right };
      }
      let alias = null;
      if (isKeyword('AS')) { advance(); alias = readAlias(); }
      if (node.type === 'arith') {
        return { type: 'expression', expr: node, alias };
      }
      return { ...node, alias: alias || func };
    }

    // Check for CAST expression in SELECT
    if (peek().type === 'KEYWORD' && peek().value === 'CAST') {
      advance(); // CAST
      expect('(');
      const expr = parseExpr();
      expect('KEYWORD', 'AS');
      const targetType = advance().value;
      expect(')');
      let castNode = { type: 'cast', expr, targetType };
      // Handle operator chaining after CAST: CAST(x AS TEXT) || 'suffix'
      if (peek().type === 'CONCAT_OP' || peek().type === 'CONCAT') {
        let left = castNode;
        while (match('CONCAT_OP') || match('CONCAT')) {
          const right = parsePrimaryWithConcat();
          left = { type: 'function_call', func: 'CONCAT', args: [left, right] };
        }
        castNode = left;
      } else if (peek().type === 'PLUS' || peek().type === 'MINUS') {
        let left = castNode;
        while (peek().type === 'PLUS' || peek().type === 'MINUS') {
          const op = advance().type === 'PLUS' ? '+' : '-';
          const right = parsePrimaryWithConcat();
          left = { type: 'arith', op, left, right };
        }
        castNode = left;
      }
      let alias = null;
      if (isKeyword('AS')) { advance(); alias = readAlias(); }
      else if (peek().type === 'IDENT' && !isKeyword('FROM') && !isKeyword('WHERE') && !isKeyword('JOIN') && !isKeyword('ON') && !isKeyword('GROUP') && !isKeyword('ORDER') && !isKeyword('HAVING') && !isKeyword('LIMIT') && !isKeyword('UNION') && !isKeyword('INTERSECT') && !isKeyword('EXCEPT')) {
        alias = readAlias();
      }
      return { type: 'expression', expr: castNode, alias };
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

    // Parenthesized expression: (2 + 3) * 4, ((1+2)*3), etc.
    // Use the standard expression parser which handles paren nesting and operator precedence correctly.
    if (peek().type === '(' && !(tokens[pos + 1]?.type === 'KEYWORD' && tokens[pos + 1]?.value === 'SELECT')) {
      const expr = parseExpr();
      let alias = null;
      if (isKeyword('AS')) { advance(); alias = readAlias(); }
      return { type: 'expression', expr, alias };
    }

    // Check for aggregate: COUNT, SUM, AVG, MIN, MAX
    if (peek().type === 'KEYWORD' && ['COUNT', 'SUM', 'AVG', 'MIN', 'MAX', 'GROUP_CONCAT', 'STRING_AGG', 'JSON_AGG', 'JSONB_AGG', 'ARRAY_AGG'].includes(peek().value) && tokens[pos + 1]?.type === '(') {
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
      // Optional SEPARATOR for GROUP_CONCAT / STRING_AGG
      let separator = ',';
      if (isKeyword('SEPARATOR')) {
        advance();
        separator = advance().value; // STRING literal
      } else if (func === 'STRING_AGG' && peek().type === ',') {
        // PostgreSQL STRING_AGG(expr, delimiter) syntax
        advance(); // skip comma
        separator = advance().value; // STRING literal
      }
      expect(')');

      // Add separator info for GROUP_CONCAT / STRING_AGG
      const aggExtra = (func === 'GROUP_CONCAT' || func === 'STRING_AGG') ? { separator } : {};
      // Check for window function: aggregate OVER (...)
      if (isKeyword('OVER')) {
        const over = parseOverClause();
        let alias = null;
        if (isKeyword('AS')) { advance(); alias = readAlias(); }
        return { type: 'window', func, arg, distinct, over, alias };
      }

      let alias = null;
      // Check for arithmetic after aggregate: SUM(a) * 100 / SUM(b)
      let node = { type: 'aggregate', func, arg, distinct, ...aggExtra };
      if (['PLUS', 'MINUS', 'SLASH', 'MOD'].includes(peek().type) || (peek().type === '*' && tokens[pos+1]?.type !== ')')) {
        // Parse arithmetic with the aggregate as left operand
        let left = { type: 'aggregate_expr', func, arg: typeof arg === 'string' ? { type: 'column_ref', name: arg } : (arg === '*' ? '*' : arg), distinct };
        // Handle operator precedence
        while (true) {
          const t = peek().type;
          if (t === '*' && tokens[pos+1]?.type !== ')') {
            advance(); const right = parsePrimary(); left = { type: 'arith', op: '*', left, right };
          } else if (t === 'SLASH') {
            advance(); const right = parsePrimary(); left = { type: 'arith', op: '/', left, right };
          } else if (t === 'MOD') {
            advance(); const right = parsePrimary(); left = { type: 'arith', op: '%', left, right };
          } else if (t === 'PLUS' || t === 'MINUS') {
            const op = t === 'PLUS' ? '+' : '-';
            advance();
            let right = parsePrimary();
            while (true) {
              const rt = peek().type;
              if (rt === '*' && tokens[pos+1]?.type !== ')') { advance(); const rr = parsePrimary(); right = { type: 'arith', op: '*', left: right, right: rr }; }
              else if (rt === 'SLASH') { advance(); const rr = parsePrimary(); right = { type: 'arith', op: '/', left: right, right: rr }; }
              else if (rt === 'MOD') { advance(); const rr = parsePrimary(); right = { type: 'arith', op: '%', left: right, right: rr }; }
              else break;
            }
            left = { type: 'arith', op, left, right };
          } else break;
        }
        if (isKeyword('AS')) { advance(); alias = readAlias(); }
        return { type: 'expression', expr: left, alias };
      }
      if (isKeyword('AS')) { advance(); alias = readAlias(); }
      return { type: 'aggregate', func, arg, distinct, alias, ...aggExtra };
    }

    // Window functions: ROW_NUMBER, RANK, DENSE_RANK
    if (peek().type === 'KEYWORD' && ['ROW_NUMBER', 'RANK', 'DENSE_RANK'].includes(peek().value)) {
      const func = advance().value;
      expect('(');
      expect(')');
      const over = parseOverClause();
      let alias = null;
      if (isKeyword('AS')) { advance(); alias = readAlias(); }
      return { type: 'window', func, arg: null, over, alias };
    }

    // LAG/LEAD window functions with arguments
    if (peek().type === 'KEYWORD' && ['LAG', 'LEAD', 'NTILE', 'FIRST_VALUE', 'LAST_VALUE'].includes(peek().value)) {
      const func = advance().value;
      expect('(');
      let arg = null;
      let offset = 1;
      let defaultValue = null;
      if (!match(')')) {
        arg = parseExpr();
        if (match(',')) {
          offset = parseExpr().value || 1;
          if (match(',')) {
            defaultValue = parseExpr().value ?? null;
          }
        }
        expect(')');
      }
      const over = parseOverClause();
      let alias = null;
      if (isKeyword('AS')) { advance(); alias = readAlias(); }
      return { type: 'window', func, arg, offset, defaultValue, over, alias };
    }

    // EXTRACT(field FROM expr)
    if (isKeyword('EXTRACT')) {
      advance(); expect('(');
      const field = advance().value.toUpperCase(); // YEAR, MONTH, DAY, HOUR, MINUTE, SECOND
      if (!isKeyword('FROM')) throw new Error('Expected FROM in EXTRACT');
      advance(); // consume FROM
      const expr = parseExpr();
      expect(')');
      let alias = null;
      if (isKeyword('AS')) { advance(); alias = readAlias(); }
      let node = { type: 'function', func: 'EXTRACT', args: [{ type: 'literal', value: field }, expr], alias };
      // Handle arithmetic after EXTRACT
      while (peek() && ['+', '-', '*', '/', '%'].includes(peek().value)) {
        const op = advance().value;
        const right = parseExpr();
        node = { type: 'binary', op, left: node, right };
      }
      return node;
    }

    // DATE_PART('field', expr)
    if (isKeyword('DATE_PART')) {
      advance(); expect('(');
      const field = parseExpr();
      expect(',');
      const expr = parseExpr();
      expect(')');
      let alias = null;
      if (isKeyword('AS')) { advance(); alias = readAlias(); }
      let node = { type: 'function', func: 'EXTRACT', args: [field, expr], alias };
      while (peek() && ['+', '-', '*', '/', '%'].includes(peek().value)) {
        const op = advance().value;
        const right = parseExpr();
        node = { type: 'binary', op, left: node, right };
      }
      return node;
    }

    // SUBSTRING with FROM...FOR syntax: SUBSTRING(str FROM pos FOR len)
    if (isKeyword('SUBSTRING') || isKeyword('SUBSTR')) {
      const savedPos = pos;
      advance(); // SUBSTRING
      expect('(');
      const str = parseExpr();
      if (isKeyword('FROM')) {
        advance(); // FROM
        const fromPos = parseExpr();
        let forLen = null;
        if (isKeyword('FOR')) {
          advance(); // FOR
          forLen = parseExpr();
        }
        expect(')');
        const args = forLen ? [str, fromPos, forLen] : [str, fromPos];
        let alias = null;
        if (isKeyword('AS')) { advance(); alias = readAlias(); }
        let node = { type: 'function', func: 'SUBSTRING', args, alias };
        while (peek() && ['+', '-', '*', '/', '%'].includes(peek().value)) {
          const op = advance().value;
          const right = parseExpr();
          node = { type: 'binary', op, left: node, right };
        }
        return node;
      }
      // Not FROM syntax — backtrack to regular function parsing
      pos = savedPos;
    }

    // POSITION(substr IN str)
    if (isKeyword('POSITION')) {
      advance(); expect('(');
      const substr = parsePrimaryWithConcat();
      if (!isKeyword('IN')) throw new Error('Expected IN in POSITION');
      advance(); // consume IN
      const str = parsePrimaryWithConcat();
      expect(')');
      let alias = null;
      if (isKeyword('AS')) { advance(); alias = readAlias(); }
      return { type: 'function', func: 'POSITION', args: [substr, str], alias };
    }

    // String functions in SELECT
    if (peek().type === 'KEYWORD' && ['UPPER', 'LOWER', 'INITCAP', 'LENGTH', 'CHAR_LENGTH', 'CONCAT', 'COALESCE', 'NULLIF', 'SUBSTRING', 'SUBSTR', 'REPLACE', 'TRIM', 'ABS', 'ROUND', 'CEIL', 'FLOOR', 'IFNULL', 'IIF', 'TYPEOF',
      'JSON_EXTRACT', 'JSON_SET', 'JSON_ARRAY_LENGTH', 'JSON_TYPE', 'JSON_OBJECT', 'JSON_ARRAY', 'JSON_VALID', 'JSON_VALUE', 'LEFT', 'RIGHT', 'LPAD', 'RPAD', 'REVERSE', 'REPEAT', 'POWER', 'SQRT', 'LOG', 'EXP', 'RANDOM', 'STRFTIME', 'NOW', 'GREATEST', 'LEAST', 'MOD', 'LTRIM', 'RTRIM',
      'JSON_BUILD_OBJECT', 'JSON_BUILD_ARRAY', 'ROW_TO_JSON', 'TO_JSON', 'JSON_OBJECT_KEYS', 'DATE_ADD', 'DATE_DIFF', 'DATE_TRUNC'].includes(peek().value)) {
      const func = advance().value;
      expect('(');
      const args = [];
      if (!match(')')) {
        args.push(parseExpr());
        while (match(',')) args.push(parseExpr());
        expect(')');
      }
      // Check for arithmetic after function call
      let node = { type: 'function_call', func, args };
      while (['PLUS', 'MINUS', 'SLASH', 'MOD'].includes(peek().type) || (peek().type === '*' && tokens[pos+1]?.type !== ')')) {
        const t = peek().type;
        const op = t === 'PLUS' ? '+' : t === 'MINUS' ? '-' : t === 'SLASH' ? '/' : t === 'MOD' ? '%' : '*';
        advance();
        const right = parsePrimary();
        node = { type: 'arith', op, left: node, right };
      }
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
    // Unary minus in SELECT: -val, -SUM(...), -(expr)
    if (peek().type === 'MINUS') {
      advance(); // consume MINUS
      const operand = parsePrimary();
      let expr;
      if (operand.type === 'literal' && typeof operand.value === 'number') {
        expr = { type: 'literal', value: -operand.value };
      } else {
        expr = { type: 'unary_minus', operand };
      }
      // Check for arithmetic after: -val + 100, -val * 2
      while (true) {
        const t = peek().type;
        if (t === '*' && tokens[pos+1]?.type !== ')') {
          advance(); const right = parsePrimary(); expr = { type: 'arith', op: '*', left: expr, right };
        } else if (t === 'SLASH') {
          advance(); const right = parsePrimary(); expr = { type: 'arith', op: '/', left: expr, right };
        } else if (t === 'MOD') {
          advance(); const right = parsePrimary(); expr = { type: 'arith', op: '%', left: expr, right };
        } else if (t === 'PLUS' || t === 'MINUS') {
          const op = t === 'PLUS' ? '+' : '-';
          advance();
          const right = parsePrimary();
          expr = { type: 'arith', op, left: expr, right };
        } else break;
      }
      let alias = null;
      if (isKeyword('AS')) { advance(); alias = readAlias(); }
      return { type: 'expression', expr, alias };
    }
    const colTok = advance();
    const col = colTok.originalValue || colTok.value;
    // Check for || concatenation or arithmetic operators
    const nextType = peek().type;
    if (nextType === 'CONCAT_OP' || nextType === 'PLUS' || nextType === 'MINUS' || nextType === '*' || nextType === 'SLASH' || nextType === 'MOD') {
      let seed = colTok.type === 'STRING' || colTok.type === 'NUMBER'
        ? { type: 'literal', value: col }
        : { type: 'column_ref', name: col };
      // Parse with correct operator precedence
      // First, consume mul/div/mod that directly follow the seed
      let left = seed;
      while (true) {
        const t = peek().type;
        if (t === '*' && tokens[pos+1]?.type !== ')') {
          advance(); const right = parsePrimary(); left = { type: 'arith', op: '*', left, right };
        } else if (t === 'SLASH') {
          advance(); const right = parsePrimary(); left = { type: 'arith', op: '/', left, right };
        } else if (t === 'MOD') {
          advance(); const right = parsePrimary(); left = { type: 'arith', op: '%', left, right };
        } else break;
      }
      // Then, consume add/sub (lower precedence)
      while (true) {
        const t = peek().type;
        if (t === 'PLUS' || t === 'MINUS') {
          const op = t === 'PLUS' ? '+' : '-';
          advance();
          // Parse right side as mul/div chain
          let right = parsePrimary();
          while (true) {
            const rt = peek().type;
            if (rt === '*' && tokens[pos+1]?.type !== ')') {
              advance(); const rr = parsePrimary(); right = { type: 'arith', op: '*', left: right, right: rr };
            } else if (rt === 'SLASH') {
              advance(); const rr = parsePrimary(); right = { type: 'arith', op: '/', left: right, right: rr };
            } else if (rt === 'MOD') {
              advance(); const rr = parsePrimary(); right = { type: 'arith', op: '%', left: right, right: rr };
            } else break;
          }
          left = { type: 'arith', op, left, right };
        } else if (t === 'CONCAT_OP') {
          advance();
          const right = parsePrimary();
          left = { type: 'function_call', func: 'CONCAT', args: [left, right] };
        } else break;
      }
      let alias = null;
      if (isKeyword('AS')) { advance(); alias = readAlias(); }
      return { type: 'expression', expr: left, alias };
    }
    let alias = null;
    if (isKeyword('AS')) { advance(); alias = readAlias(); }
    // NUMBER and STRING tokens without operators should be literals, not column refs
    if (colTok.type === 'NUMBER' || colTok.type === 'STRING') {
      return { type: 'expression', expr: { type: 'literal', value: col }, alias };
    }
    return { type: 'column', name: col, alias };
  }

  function parseFromClause() {
    // GENERATE_SERIES(start, stop[, step])
    if (isKeyword('GENERATE_SERIES')) {
      advance();
      expect('(');
      const start = parseExpr();
      expect(',');
      const stop = parseExpr();
      let step = null;
      if (match(',')) step = parseExpr();
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
    const fromTok = advance();
    const table = fromTok.originalValue || fromTok.value;
    let alias = null;
    if (peek().type === 'IDENT') alias = advance().value;
    else if (isKeyword('AS')) { advance(); alias = readAlias(); }
    let tablesample = null;
    if (isKeyword('TABLESAMPLE')) {
      advance(); // TABLESAMPLE
      const method = advance().value; // BERNOULLI or SYSTEM
      expect('(');
      const pct = advance().value;
      expect(')');
      tablesample = { method, percentage: pct };
    }
    return { table, alias, tablesample };
  }

  function parseJoin() {
    let joinType = 'INNER';
    let isNatural = false;
    if (isKeyword('NATURAL')) { isNatural = true; advance(); }
    if (isKeyword('LEFT')) { joinType = 'LEFT'; advance(); }
    else if (isKeyword('RIGHT')) { joinType = 'RIGHT'; advance(); }
    else if (isKeyword('FULL')) { joinType = 'FULL'; advance(); }
    else if (isKeyword('CROSS')) { joinType = 'CROSS'; advance(); }
    else if (isKeyword('INNER')) { advance(); }
    // Skip optional OUTER keyword (LEFT OUTER JOIN, RIGHT OUTER JOIN, FULL OUTER JOIN)
    if (isKeyword('OUTER')) advance();
    expect('KEYWORD', 'JOIN');
    let lateral = false;
    let subquery = null;
    if (isKeyword('LATERAL')) {
      lateral = true;
      advance();
    }
    if (peek().type === '(') {
      // Subquery as join source
      advance(); // (
      if (isKeyword('SELECT')) {
        subquery = parseSelect();
      }
      expect(')');
      let alias = null;
      if (peek().type === 'IDENT' && !isKeyword('ON') && !isKeyword('WHERE')) alias = advance().value;
      // Optional AS
      if (isKeyword('AS') || (peek().type === 'IDENT' && peek().value.toUpperCase() === 'AS')) {
        advance(); // AS
        alias = advance().value;
      }
      let on = null;
      if (isKeyword('ON')) { advance(); on = parseExpr(); }
      return { type: 'JOIN', joinType, table: null, alias, on, lateral, subquery };
    }
    const joinTok = advance();
    const table = joinTok.originalValue || joinTok.value;
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
      // Not a subquery — put the '(' back and let parsePrimary handle it
      // (parsePrimary correctly handles nested parens with operator precedence)
      pos--;
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
      do { values.push(parseExpr()); } while (match(','));
      expect(')');
      return { type: 'NOT', expr: { type: 'IN_LIST', left, values } };
    }

    // NOT LIKE
    if (isKeyword('NOT') && tokens[pos + 1]?.type === 'KEYWORD' && tokens[pos + 1]?.value === 'LIKE') {
      advance(); // NOT
      advance(); // LIKE
      const pattern = parsePrimaryWithConcat();
      return { type: 'NOT', expr: { type: 'LIKE', left, pattern } };
    }

    // NOT BETWEEN
    if (isKeyword('NOT') && tokens[pos + 1]?.type === 'KEYWORD' && tokens[pos + 1]?.value === 'BETWEEN') {
      advance(); // NOT
      advance(); // BETWEEN
      const low = parsePrimaryWithConcat();
      expect('KEYWORD', 'AND');
      const high = parsePrimaryWithConcat();
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
      do { values.push(parseExpr()); } while (match(','));
      expect(')');
      return { type: 'IN_LIST', left, values };
    }

    if (isKeyword('LIKE')) {
      advance();
      const pattern = parsePrimaryWithConcat();
      return { type: 'LIKE', left, pattern };
    }

    if (isKeyword('ILIKE')) {
      advance();
      const pattern = parsePrimaryWithConcat();
      return { type: 'ILIKE', left, pattern };
    }

    // SIMILAR TO
    if (isKeyword('SIMILAR')) {
      advance(); // SIMILAR
      if (isKeyword('TO')) advance(); // TO
      const pattern = parsePrimaryWithConcat();
      return { type: 'SIMILAR_TO', left, pattern };
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
      let symmetric = false;
      if (isKeyword('SYMMETRIC')) { advance(); symmetric = true; }
      const low = parsePrimaryWithConcat();
      expect('KEYWORD', 'AND');
      const high = parsePrimaryWithConcat();
      return { type: 'BETWEEN', left, low, high, symmetric };
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
      const right = parsePrimaryWithConcat();
      return { type: 'COMPARE', op, left, right };
    }
    return left;
  }

  function parsePrimaryWithConcat() {
    let left = parseAddSub();
    while (true) {
      const t = peek().type;
      if (t === 'CONCAT_OP') {
        advance();
        const right = parseAddSub();
        left = { type: 'function_call', func: 'CONCAT', args: [left, right] };
      } else break;
    }
    return left;
  }

  function parseAddSub() {
    let left = parseMulDivMod();
    while (true) {
      const t = peek().type;
      if (t === 'PLUS') {
        advance();
        const right = parseMulDivMod();
        left = { type: 'arith', op: '+', left, right };
      } else if (t === 'MINUS') {
        advance();
        const right = parseMulDivMod();
        left = { type: 'arith', op: '-', left, right };
      } else break;
    }
    return left;
  }

  function parseMulDivMod() {
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
    // Unary minus: -expr
    if (t.type === 'MINUS') {
      advance();
      const operand = parsePrimary();
      // Fold literal numbers: -5 → literal(-5)
      if (operand.type === 'literal' && typeof operand.value === 'number') {
        return { type: 'literal', value: -operand.value };
      }
      return { type: 'unary_minus', operand };
    }
    // Unary plus: +expr (no-op, just parse the operand)
    if (t.type === 'PLUS') {
      advance();
      return parsePrimary();
    }
    if (t.type === 'NUMBER') { advance(); return { type: 'literal', value: t.value }; }
    if (t.type === 'STRING') { advance(); return { type: 'literal', value: t.value }; }
    if (t.type === 'PARAM') { advance(); return { type: 'PARAM', index: t.index }; }
    // INTERVAL 'N unit'
    if (t.type === 'KEYWORD' && t.value === 'INTERVAL') {
      advance(); // consume INTERVAL
      const strTok = peek();
      if (strTok.type === 'STRING') {
        advance();
        return { type: 'interval', value: strTok.value };
      }
      throw new Error('INTERVAL requires a string literal');
    }
    // Parenthesized expression
    if (t.type === '(') {
      advance(); // consume '('
      const expr = parseExpr();
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

    // SUBSTRING/SUBSTR with FROM...FOR syntax (must handle before generic function parser)
    if (t.type === 'KEYWORD' && (t.value === 'SUBSTRING' || t.value === 'SUBSTR') && tokens[pos + 1]?.type === '(') {
      const funcName = advance().value;
      expect('(');
      const arg1 = parseExpr();
      if (isKeyword('FROM')) {
        advance(); // FROM
        const fromPos = parsePrimaryWithConcat();
        let forLen = null;
        if (isKeyword('FOR')) { advance(); forLen = parsePrimaryWithConcat(); }
        expect(')');
        const args = forLen ? [arg1, fromPos, forLen] : [arg1, fromPos];
        return { type: 'function_call', func: 'SUBSTRING', args };
      }
      // Comma syntax: SUBSTRING(str, pos, len)
      const args = [arg1];
      while (match(',')) args.push(parseExpr());
      expect(')');
      return { type: 'function_call', func: funcName, args };
    }
// Built-in string/null functions
    if (t.type === 'KEYWORD' && ['UPPER', 'LOWER', 'INITCAP', 'LENGTH', 'CHAR_LENGTH', 'CONCAT', 'COALESCE', 'NULLIF', 'SUBSTRING', 'SUBSTR', 'REPLACE', 'TRIM', 'ABS', 'ROUND', 'CEIL', 'FLOOR', 'IFNULL', 'IIF', 'TYPEOF',
      'JSON_EXTRACT', 'JSON_SET', 'JSON_ARRAY_LENGTH', 'JSON_TYPE', 'JSON_OBJECT', 'JSON_ARRAY', 'JSON_VALID', 'JSON_VALUE', 'LEFT', 'RIGHT', 'LPAD', 'RPAD', 'REVERSE', 'REPEAT', 'POWER', 'SQRT', 'LOG', 'EXP', 'RANDOM', 'STRFTIME', 'NOW', 'GREATEST', 'LEAST', 'MOD', 'LTRIM', 'RTRIM',
      'JSON_BUILD_OBJECT', 'JSON_BUILD_ARRAY', 'ROW_TO_JSON', 'TO_JSON', 'JSON_OBJECT_KEYS', 'DATE_ADD', 'DATE_DIFF', 'DATE_TRUNC'].includes(t.value)) {
      // Only parse as function call if next token is '(' — otherwise treat as identifier
      if (tokens[pos + 1]?.type === '(') {
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

    if (t.type === 'IDENT') { advance(); return { type: 'column_ref', name: t.value }; }
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
      // Parse as expression to support: ORDER BY -val, ORDER BY col + 1, etc.
      const expr = parseExpr();
      let column;
      if (expr.type === 'column_ref') {
        column = expr.name; // Simple column name (backward compat)
      } else if (expr.type === 'literal' && typeof expr.value === 'number') {
        column = expr.value; // Numeric column reference: ORDER BY 1
      } else {
        column = expr; // Expression node
      }
      let dir = 'ASC';
      if (isKeyword('DESC')) { dir = 'DESC'; advance(); }
      else if (isKeyword('ASC')) { advance(); }
      let nulls = null; // null = default, 'FIRST' or 'LAST'
      if (peek().type === 'IDENT' && peek().value.toUpperCase() === 'NULLS') {
        advance(); // consume NULLS
        const nv = peek().value?.toUpperCase();
        if (nv === 'FIRST') {
          nulls = 'FIRST'; advance();
        } else if (nv === 'LAST') {
          nulls = 'LAST'; advance();
        }
      }
      cols.push({ column, direction: dir, nulls });
    } while (match(','));
    return cols;
  }

  function parseGroupBy() {
    // Check for ROLLUP, CUBE, GROUPING SETS
    if (isKeyword('ROLLUP')) {
      advance();
      expect('(');
      const cols = [];
      do { cols.push(parseGroupByItem()); } while (match(','));
      expect(')');
      return { type: 'ROLLUP', columns: cols };
    }
    if (isKeyword('CUBE')) {
      advance();
      expect('(');
      const cols = [];
      do { cols.push(parseGroupByItem()); } while (match(','));
      expect(')');
      return { type: 'CUBE', columns: cols };
    }
    if (isKeyword('GROUPING') && tokens[pos + 1] && tokens[pos + 1].value === 'SETS') {
      advance(); // GROUPING
      advance(); // SETS
      expect('(');
      const sets = [];
      do {
        if (peek().type === '(') {
          advance(); // (
          const cols = [];
          if (peek().type !== ')') {
            do { cols.push(parseGroupByItem()); } while (match(','));
          }
          expect(')');
          sets.push(cols);
        } else {
          sets.push([parseGroupByItem()]);
        }
      } while (match(','));
      expect(')');
      return { type: 'GROUPING_SETS', sets };
    }
    
    // Regular GROUP BY
    const cols = [];
    do {
      cols.push(parseGroupByItem());
    } while (match(','));
    return cols;
  }

  function parseGroupByItem() {
    const expr = parseExpr();
    if (expr.type === 'column_ref') return expr.name;
    return expr;
  }

  function parseOverClause() {
    expect('KEYWORD', 'OVER');
    expect('(');
    let partitionBy = null;
    let orderBy = null;
    let frame = null;
    if (isKeyword('PARTITION')) {
      advance(); // PARTITION
      expect('KEYWORD', 'BY');
      partitionBy = [];
      do { partitionBy.push(parseExpr()); } while (match(','));
    }
    if (isKeyword('ORDER')) {
      advance(); // ORDER
      expect('KEYWORD', 'BY');
      orderBy = parseOrderBy();
    }
    // Optional frame clause: ROWS|RANGE BETWEEN ... AND ...
    if (isKeyword('ROWS') || isKeyword('RANGE')) {
      const frameType = advance().value; // ROWS or RANGE
      if (isKeyword('BETWEEN')) {
        advance(); // BETWEEN
        const start = parseFrameBound();
        expect('KEYWORD', 'AND');
        const end = parseFrameBound();
        frame = { type: frameType, start, end };
      } else {
        // Single bound: ROWS UNBOUNDED PRECEDING or ROWS N PRECEDING
        const bound = parseFrameBound();
        frame = { type: frameType, start: bound, end: { type: 'CURRENT ROW' } };
      }
    }
    expect(')');
    return { partitionBy, orderBy, frame };
  }

  function parseFrameBound() {
    if (isKeyword('UNBOUNDED')) {
      advance(); // UNBOUNDED
      const dir = advance().value; // PRECEDING or FOLLOWING
      return { type: 'UNBOUNDED', direction: dir };
    }
    if (isKeyword('CURRENT')) {
      advance(); // CURRENT
      expect('KEYWORD', 'ROW');
      return { type: 'CURRENT ROW' };
    }
    // N PRECEDING or N FOLLOWING
    const n = advance().value;
    const dir = advance().value;
    return { type: 'OFFSET', offset: Number(n), direction: dir };
  }

  function parseReturningClause() {
    advance(); // RETURNING
    if (match('*')) return '*';
    const cols = [];
    do {
      const expr = parseExpr();
      let alias = null;
      if (isKeyword('AS')) { advance(); alias = advance().value; }
      else if (peek().type === 'IDENT' && !isKeyword('FROM') && !isKeyword('WHERE')) {
        alias = advance().value;
      }
      cols.push(alias ? { expr, alias } : expr);
    } while (match(','));
    return cols;
  }

  function parseMerge() {
    advance(); // MERGE
    expect('KEYWORD', 'INTO');
    const targetTok = advance();
    const target = targetTok.originalValue || targetTok.value;
    let targetAlias = null;
    if (peek().type === 'IDENT' && !isKeyword('USING')) targetAlias = advance().value;
    
    expect('KEYWORD', 'USING');
    const sourceTok = advance();
    const source = sourceTok.originalValue || sourceTok.value;
    let sourceAlias = null;
    if (peek().type === 'IDENT' && !isKeyword('ON')) sourceAlias = advance().value;
    
    expect('KEYWORD', 'ON');
    const onCondition = parseExpr();
    
    const whenClauses = [];
    while (isKeyword('WHEN')) {
      advance(); // WHEN
      const matched = isKeyword('MATCHED');
      if (matched) {
        advance(); // MATCHED
      } else {
        expect('KEYWORD', 'NOT');
        expect('KEYWORD', 'MATCHED');
      }
      expect('KEYWORD', 'THEN');
      
      if (matched) {
        expect('KEYWORD', 'UPDATE');
        expect('KEYWORD', 'SET');
        const assignments = [];
        do {
          const colTok = advance();
          const colName = colTok.originalValue || colTok.value;
          if (peek().type === 'EQ') advance();
          else expect('=');
          const value = parseExpr();
          assignments.push({ column: colName, value });
        } while (match(','));
        whenClauses.push({ type: 'MATCHED', action: 'UPDATE', assignments });
      } else {
        expect('KEYWORD', 'INSERT');
        let columns = null;
        if (peek().type === '(') {
          advance();
          columns = [];
          do { const ct = advance(); columns.push(ct.originalValue || ct.value); } while (match(','));
          expect(')');
        }
        expect('KEYWORD', 'VALUES');
        expect('(');
        const values = [];
        do { values.push(parseExpr()); } while (match(','));
        expect(')');
        whenClauses.push({ type: 'NOT_MATCHED', action: 'INSERT', columns, values });
      }
    }
    
    return { type: 'MERGE', target, targetAlias, source, sourceAlias, on: onCondition, whenClauses };
  }

  function parseInsert() {
    advance(); // INSERT
    expect('KEYWORD', 'INTO');
    const tableTok = advance();
    const table = tableTok.originalValue || tableTok.value;

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
      do { values.push(parseExpr()); } while (match(','));
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
      returning = parseReturningClause();
    }

    return { type: 'INSERT', table, columns, rows, onConflict, returning };
  }

  function parseUpdate() {
    advance(); // UPDATE
    const updateTok = advance();
    const table = updateTok.originalValue || updateTok.value;
    expect('KEYWORD', 'SET');
    const assignments = [];
    do {
      const col = advance().value;
      expect('EQ');
      const value = parseExpr();
      assignments.push({ column: col, value });
    } while (match(','));
    // PostgreSQL-style UPDATE ... FROM
    let from = null;
    let fromAlias = null;
    if (isKeyword('FROM')) {
      advance(); // FROM
      const fromTok = advance();
      from = fromTok.originalValue || fromTok.value;
      if (peek().type === 'IDENT' && !isKeyword('WHERE') && !isKeyword('RETURNING')) {
        fromAlias = advance().value;
      }
    }
    let where = null;
    if (isKeyword('WHERE')) { advance(); where = parseExpr(); }
    let returning = null;
    if (isKeyword('RETURNING')) {
      returning = parseReturningClause();
    }
    return { type: 'UPDATE', table, assignments, where, from, fromAlias, returning };
  }

  function parseDelete() {
    advance(); // DELETE
    expect('KEYWORD', 'FROM');
    const delTok = advance();
    const table = delTok.originalValue || delTok.value;
    // USING clause (PostgreSQL extension)
    let using = null;
    let usingAlias = null;
    if (isKeyword('USING')) {
      advance();
      const usingTok = advance();
      using = usingTok.originalValue || usingTok.value;
      if (peek().type === 'IDENT' && !isKeyword('WHERE')) usingAlias = advance().value;
    }
    let where = null;
    if (isKeyword('WHERE')) { advance(); where = parseExpr(); }
    let returning = null;
    if (isKeyword('RETURNING')) {
      returning = parseReturningClause();
    }
    return { type: 'DELETE', table, using, usingAlias, where, returning };
  }

  function parseAlter() {
    advance(); // ALTER
    expect('KEYWORD', 'TABLE');
    const _altTok1 = advance(); const table = _altTok1.originalValue || _altTok1.value;
    
    if (isKeyword('ADD')) {
      advance(); // ADD
      if (isKeyword('COLUMN')) advance(); // optional COLUMN
      const tok = advance();
      const colName = tok.originalValue || tok.value;
      const colType = advance().value;
      let defaultVal = null;
      if (isKeyword('DEFAULT')) {
        advance();
        const defExpr = parseExpr();
        defaultVal = defExpr.type === 'literal' ? defExpr.value : defExpr;
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
    let orReplace = false;
    if (isKeyword('OR')) { advance(); expect('KEYWORD', 'REPLACE'); orReplace = true; }
    if (isKeyword('VIEW')) return parseCreateView(orReplace);
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
    expect('KEYWORD', 'TABLE');
    let ifNotExists = false;
    if (isKeyword('IF')) { advance(); expect('KEYWORD', 'NOT'); expect('KEYWORD', 'EXISTS'); ifNotExists = true; }
    const tableTok = advance();
    const table = tableTok.originalValue || tableTok.value;
    
    // CREATE TABLE ... AS SELECT/WITH (CTAS)
    if (isKeyword('AS')) {
      advance(); // AS
      // Support both SELECT and WITH (for CTEs)
      const query = isKeyword('WITH') ? parseWith() : parseSelect();
      return { type: 'CREATE_TABLE_AS', table, query };
    }
    
    expect('(');
    const columns = [];
    do {
      const tok = advance();
      const name = tok.originalValue || tok.value;
      const dataType = advance().value;
      let primaryKey = false;
      let notNull = false;
      let unique = false;
      let check = null;
      let defaultVal = null;
      let references = null;
      // Parse column constraints
      while (true) {
        if (isKeyword('PRIMARY')) { advance(); expect('KEYWORD', 'KEY'); primaryKey = true; }
        else if (isKeyword('UNIQUE')) { advance(); unique = true; }
        else if (isKeyword('NOT')) { advance(); expect('KEYWORD', 'NULL'); notNull = true; }
        else if (isKeyword('CHECK')) {
          advance();
          expect('(');
          check = parseExpr();
          expect(')');
        }
        else if (isKeyword('DEFAULT')) {
          advance();
          const defExpr = parseExpr();
          // Extract value from literal or evaluate simple expressions
          if (defExpr.type === 'literal') {
            defaultVal = defExpr.value;
          } else {
            defaultVal = defExpr; // Store expression node for later evaluation
          }
        }
        else if (isKeyword('REFERENCES')) {
          advance();
          const refTable = advance().value;
          expect('(');
          const refColumn = advance().value;
          expect(')');
          let onDelete = 'RESTRICT';
          let onUpdate = 'RESTRICT';
          // Parse ON DELETE and ON UPDATE in any order
          for (let k = 0; k < 2; k++) {
            if (isKeyword('ON')) {
              advance();
              if (isKeyword('DELETE')) {
                advance();
                if (isKeyword('CASCADE')) { advance(); onDelete = 'CASCADE'; }
                else if (isKeyword('SET')) { advance(); expect('KEYWORD', 'NULL'); onDelete = 'SET NULL'; }
                else if (isKeyword('RESTRICT')) { advance(); onDelete = 'RESTRICT'; }
              } else if (isKeyword('UPDATE')) {
                advance();
                if (isKeyword('CASCADE')) { advance(); onUpdate = 'CASCADE'; }
                else if (isKeyword('SET')) { advance(); expect('KEYWORD', 'NULL'); onUpdate = 'SET NULL'; }
                else if (isKeyword('RESTRICT')) { advance(); onUpdate = 'RESTRICT'; }
              }
            }
          }
          references = { table: refTable, column: refColumn, onDelete, onUpdate };
        }
        else break;
      }
      // Generated column: GENERATED ALWAYS AS (expr) STORED
      let generated = null;
      if (isKeyword('GENERATED')) {
        advance(); // GENERATED
        if (isKeyword('ALWAYS')) advance(); // ALWAYS (optional)
        expect('KEYWORD', 'AS');
        expect('(');
        generated = parseExpr();
        expect(')');
        if (isKeyword('STORED')) advance(); // STORED (optional)
      }
      // Alternative shorthand: AS (expr) STORED
      if (!generated && isKeyword('AS') && tokens[pos + 1] && tokens[pos + 1].type === '(') {
        advance(); // AS
        expect('(');
        generated = parseExpr();
        expect(')');
        if (isKeyword('STORED')) advance();
      }
      columns.push({ name, type: dataType, primaryKey, notNull, unique, check, defaultValue: defaultVal, references, generated });
    } while (match(','));
    expect(')');
    // Optional: USING BTREE | USING HEAP (default: HEAP)
    let engine = null;
    if (isKeyword('USING')) {
      advance(); // USING
      const engineTok = advance();
      engine = (engineTok.originalValue || engineTok.value).toUpperCase();
    }
    return { type: 'CREATE_TABLE', table, columns, ifNotExists, engine };
  }

  function parseCreateIndex(unique) {
    advance(); // INDEX
    let ifNotExists = false;
    if (isKeyword('IF')) {
      advance(); // IF
      expect('KEYWORD', 'NOT');
      expect('KEYWORD', 'EXISTS');
      ifNotExists = true;
    }
    const name = advance().value;
    expect('KEYWORD', 'ON');
    const table = advance().value;
    // Optional: USING HASH | USING BTREE (default: BTREE)
    let indexType = null;
    if (isKeyword('USING')) {
      advance(); // USING
      const typeTok = advance();
      indexType = (typeTok.originalValue || typeTok.value).toUpperCase();
    }
    expect('(');
    const columns = [];
    do { columns.push(advance().value); } while (match(','));
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
    return { type: 'CREATE_INDEX', name, table, columns, unique, include, where, ifNotExists, indexType };
  }

  function parseCreateView(orReplace = false) {
    advance(); // VIEW
    const name = advance().value;
    if (isKeyword('AS')) advance(); // optional AS
    const query = isKeyword('WITH') ? parseWith() : parseSelect();
    return { type: 'CREATE_VIEW', name, query, orReplace };
  }

  function parseDrop() {
    advance(); // DROP
    if (isKeyword('INDEX')) {
      advance();
      let ifExists = false;
      if (isKeyword('IF')) {
        advance(); expect('KEYWORD', 'EXISTS');
        ifExists = true;
      }
      const name = advance().value;
      return { type: 'DROP_INDEX', name, ifExists };
    }
    if (isKeyword('VIEW')) {
      advance();
      const name = advance().value;
      return { type: 'DROP_VIEW', name };
    }
    expect('KEYWORD', 'TABLE');
    let ifExists = false;
    if (isKeyword('IF')) { advance(); expect('KEYWORD', 'EXISTS'); ifExists = true; }
    const _dropTok = advance();
    const table = _dropTok.originalValue || _dropTok.value;
    return { type: 'DROP_TABLE', table, ifExists };
  }

  function parseAlter() {
    advance(); // ALTER
    expect('KEYWORD', 'TABLE');
    const _altTok2 = advance();
    const table = _altTok2.originalValue || _altTok2.value;

    if (isKeyword('ADD')) {
      advance(); // ADD
      if (isKeyword('COLUMN')) advance(); // optional COLUMN keyword
      const name = advance().value;
      const dataType = advance().value;
      let defaultValue = null;
      if (isKeyword('DEFAULT')) {
        advance();
        defaultValue = parseExpr();
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
