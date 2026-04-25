// sql.js — SQL tokenizer, parser, and query executor for HenryDB

// ===== Tokenizer =====
const KEYWORDS = new Set([
  'SELECT', 'FROM', 'WHERE', 'INSERT', 'INTO', 'VALUES', 'UPDATE', 'SET',
  'DELETE', 'CREATE', 'TABLE', 'DROP', 'AND', 'OR', 'NOT', 'NULL', 'TRUE',
  'FALSE', 'ORDER', 'BY', 'ASC', 'DESC', 'LIMIT', 'OFFSET', 'FETCH', 'FIRST', 'NEXT', 'ROWS', 'ROW', 'ONLY', 'AS',
  'INT', 'INTEGER', 'TEXT', 'VARCHAR', 'FLOAT', 'BOOL', 'BOOLEAN',
  'PRIMARY', 'KEY', 'COUNT', 'SUM', 'AVG', 'MIN', 'MAX', 'MEDIAN', 'BOOL_AND', 'BOOL_OR', 'EVERY',
  'PERCENTILE_CONT', 'PERCENTILE_DISC', 'MODE',
  'STDDEV', 'STDDEV_POP', 'STDDEV_SAMP', 'VARIANCE', 'VAR_POP', 'VAR_SAMP',
  'CORR', 'COVAR_POP', 'COVAR_SAMP', 'REGR_SLOPE', 'REGR_INTERCEPT', 'REGR_R2', 'REGR_COUNT',
  'JOIN', 'INNER', 'LEFT', 'RIGHT', 'ON', 'GROUP', 'HAVING',
  'INDEX', 'INDEXES', 'UNIQUE', 'IF', 'EXISTS', 'IN', 'ALTER', 'ADD', 'COLUMN', 'DEFAULT', 'RENAME', 'TO',
  'LIKE', 'ILIKE', 'SIMILAR', 'ESCAPE', 'UPPER', 'LOWER', 'INITCAP', 'LENGTH', 'CHAR_LENGTH', 'CONCAT', 'BETWEEN', 'SYMMETRIC', 'TABLESAMPLE', 'POSITION',
  'OVERLAY', 'PLACING', 'SPLIT_PART', 'TRANSLATE', 'CHR', 'ASCII', 'MD5', 'DATE_FORMAT', 'MAKE_DATE', 'MAKE_TIMESTAMP', 'EPOCH', 'TO_TIMESTAMP',
  'OVER', 'PARTITION', 'ROW_NUMBER', 'RANK', 'DENSE_RANK', 'LAG', 'LEAD', 'FIRST_VALUE', 'LAST_VALUE', 'CUME_DIST', 'PERCENT_RANK', 'NTH_VALUE', 'VIEW', 'DISTINCT',
  'WITH', 'RECURSIVE', 'UNION', 'ALL', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END', 'EXPLAIN', 'ANALYZE', 'COMPILED', 'FORMAT',
  'INTERSECT', 'EXCEPT', 'GENERATED', 'ALWAYS', 'STORED', 'ROLLUP', 'CUBE', 'GROUPING', 'SETS', 'MERGE', 'USING', 'MATCHED', 'FILTER', 'SEQUENCE', 'START', 'INCREMENT', 'RESTART', 'NEXTVAL', 'CURRVAL',
  'IS', 'COALESCE', 'NULLIF', 'TRUNCATE', 'CROSS', 'FULL', 'OUTER', 'NATURAL', 'USING', 'SHOW', 'TABLES', 'DESCRIBE',
  'SUBSTRING', 'SUBSTR', 'REPLACE', 'TRIM', 'INSTR', 'ABS', 'ROUND', 'CEIL', 'FLOOR', 'IFNULL', 'ISNULL', 'NVL', 'IIF', 'TYPEOF',
  'LEFT', 'RIGHT', 'LPAD', 'RPAD', 'REVERSE', 'REPEAT',
  'POWER', 'SQRT', 'LOG', 'EXP', 'RANDOM',
  'CURRENT_TIMESTAMP', 'CURRENT_DATE', 'CURRENT_TIME', 'NOW', 'STRFTIME',
  'SHOW', 'TABLES', 'COLUMNS',
  'TRUNCATE', 'RENAME', 'DESCRIBE',
  'BEGIN', 'COMMIT', 'ROLLBACK', 'TRANSACTION', 'VACUUM', 'CHECKPOINT',
  'OVER', 'PARTITION', 'RANK', 'ROW_NUMBER', 'DENSE_RANK', 'NTILE', 'LAG', 'LEAD', 'FIRST_VALUE', 'LAST_VALUE', 'CUME_DIST', 'PERCENT_RANK', 'NTH_VALUE',
  'INCLUDE', 'ALTER', 'ADD', 'COLUMN', 'RENAME', 'TO', 'CHECK',
  'UNBOUNDED', 'PRECEDING', 'FOLLOWING', 'RANGE', 'GROUPS', 'EXCLUDE', 'TIES', 'OTHERS', 'CURRENT',
  'REFERENCES', 'FOREIGN', 'CASCADE', 'RESTRICT', 'SET', 'TEMPORARY', 'TEMP',
  'CAST', 'INT', 'INTEGER', 'TEXT', 'FLOAT', 'BOOLEAN',
  'GROUP_CONCAT', 'STRING_AGG', 'SEPARATOR',
  'JSON_AGG', 'JSONB_AGG', 'ARRAY_AGG',
  'JSON_BUILD_OBJECT', 'JSON_BUILD_ARRAY', 'ROW_TO_JSON', 'TO_JSON', 'JSON_OBJECT_KEYS', 'DATE_ADD', 'DATE_DIFF', 'DATE_TRUNC',
  'CONFLICT', 'DO', 'NOTHING',
  'ANALYZE', 'RETURNING', 'USING', 'FIRST_VALUE', 'LAST_VALUE', 'CUME_DIST', 'PERCENT_RANK', 'NTH_VALUE',
  'MATERIALIZED', 'REFRESH',
  'TRIGGER', 'BEFORE', 'AFTER', 'EACH', 'ROW', 'EXECUTE', 'PREPARE', 'DEALLOCATE',
  'ANY', 'SOME',
  'IF', 'EXISTS',
  'JSON_EXTRACT', 'JSON_SET', 'JSON_ARRAY_LENGTH', 'JSON_TYPE', 'JSON_OBJECT', 'JSON_ARRAY', 'JSON_VALID', 'JSON_VALUE',
  'FULLTEXT', 'MATCH', 'AGAINST',
  'GENERATE_SERIES', 'LATERAL', 'UNNEST',
  'EXTRACT', 'DATE_PART', 'LTRIM', 'RTRIM', 'INTERVAL', 'GREATEST', 'LEAST', 'MOD', 'FOR',
  'PIVOT', 'UNPIVOT', 'CONCURRENTLY', 'REGEXP', 'RLIKE', 'REGEXP_MATCHES', 'REGEXP_REPLACE', 'REGEXP_COUNT', 'APPLY',
  'CYCLE', 'SEARCH', 'DEPTH', 'BREADTH', 'WINDOW', 'COMMENT',
  'FUNCTION', 'RETURNS', 'LANGUAGE', 'PROCEDURE', 'CALL', 'IMMUTABLE', 'VOLATILE', 'STABLE',
  'NOWAIT', 'LOCKED', 'SKIP',
  'EXTENSION', 'SCHEMA', 'GRANT', 'REVOKE', 'AUTHORIZATION', 'PRIVILEGES',
]);

export function tokenize(sql) {
  const tokens = [];
  let i = 0;
  const src = sql.trim();

  while (i < src.length) {
    // Whitespace
    if (/\s/.test(src[i])) { i++; continue; }

    // Line comment: -- to end of line
    if (src[i] === '-' && src[i + 1] === '-') {
      i += 2;
      while (i < src.length && src[i] !== '\n') i++;
      continue;
    }

    // Block comment: /* ... */
    if (src[i] === '/' && src[i + 1] === '*') {
      i += 2;
      while (i < src.length - 1 && !(src[i] === '*' && src[i + 1] === '/')) i++;
      i += 2; // skip */
      continue;
    }

    // Dollar-quoted string ($$ body $$ or $tag$ body $tag$)
    if (src[i] === '$') {
      let tag = '$';
      let j = i + 1;
      // Read optional tag between $ chars
      while (j < src.length && src[j] !== '$') {
        if (/[a-zA-Z0-9_]/.test(src[j])) { tag += src[j]; j++; }
        else break;
      }
      if (j < src.length && src[j] === '$') {
        tag += '$';
        j++;
        // Now find the closing tag
        const endIdx = src.indexOf(tag, j);
        if (endIdx !== -1) {
          const body = src.substring(j, endIdx);
          i = endIdx + tag.length;
          tokens.push({ type: 'DOLLAR_STRING', value: body });
          continue;
        }
      }
      // Not a dollar-quote — fall through to operator handling
    }

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

    // Backtick-quoted identifier: `table name`
    if (src[i] === '`') {
      i++;
      let ident = '';
      while (i < src.length && src[i] !== '`') {
        if (src[i] === '`' && i + 1 < src.length && src[i + 1] === '`') {
          ident += '`'; i += 2;  // escaped backtick
        } else {
          ident += src[i++];
        }
      }
      if (i >= src.length) throw new Error('Unterminated backtick identifier');
      i++;  // closing backtick
      tokens.push({ type: 'IDENT', value: ident.toUpperCase(), originalValue: ident });
      continue;
    }

    // Double-quoted identifier: "column name"
    if (src[i] === '"') {
      i++;
      let ident = '';
      while (i < src.length && src[i] !== '"') {
        if (src[i] === '"' && i + 1 < src.length && src[i + 1] === '"') {
          ident += '"'; i += 2;  // escaped double-quote
        } else {
          ident += src[i++];
        }
      }
      if (i >= src.length) throw new Error('Unterminated double-quoted identifier');
      i++;  // closing double-quote
      tokens.push({ type: 'IDENT', value: ident.toUpperCase(), originalValue: ident });
      continue;
    }

    // Number
    if (/[0-9]/.test(src[i]) || (src[i] === '-' && /[0-9]/.test(src[i + 1]) && (tokens.length === 0 || !['NUMBER', 'IDENT', ')', 'STRING'].includes(tokens[tokens.length-1]?.type)))) {
      let num = '';
      if (src[i] === '-') num += src[i++];
      while (i < src.length && /[0-9.]/.test(src[i])) num += src[i++];
      tokens.push({ type: 'NUMBER', value: num.includes('.') ? parseFloat(num) : parseInt(num), isFloat: num.includes('.') });
      continue;
    }

    // Operators
    if (src[i] === '>' && src[i + 1] === '=') { tokens.push({ type: 'GE' }); i += 2; continue; }
    if (src[i] === '<' && src[i + 1] === '=') { tokens.push({ type: 'LE' }); i += 2; continue; }
    if (src[i] === '!' && src[i + 1] === '=') { tokens.push({ type: 'NE' }); i += 2; continue; }
    if (src[i] === '<' && src[i + 1] === '>') { tokens.push({ type: 'NE' }); i += 2; continue; }
    if (src[i] === '|' && src[i + 1] === '|') { tokens.push({ type: 'CONCAT_OP' }); i += 2; continue; }
    if (src[i] === ':' && src[i + 1] === ':') { tokens.push({ type: 'CAST_OP' }); i += 2; continue; }
    if (src[i] === '@' && src[i + 1] === '@') { tokens.push({ type: 'TS_MATCH' }); i += 2; continue; }
    if (src[i] === '=') { tokens.push({ type: 'EQ' }); i++; continue; }
    if (src[i] === '<') { tokens.push({ type: 'LT' }); i++; continue; }
    if (src[i] === '>') { tokens.push({ type: 'GT' }); i++; continue; }

    // Punctuation
    if ('(),;'.includes(src[i])) {
      tokens.push({ type: src[i] }); i++; continue;
    }
    if (src[i] === '*') { tokens.push({ type: '*' }); i++; continue; }
    if (src[i] === '+') { tokens.push({ type: 'PLUS' }); i++; continue; }
    if (src[i] === '-' && src[i + 1] === '>' && src[i + 2] === '>') { tokens.push({ type: 'JSON_ARROW_TEXT' }); i += 3; continue; }
    if (src[i] === '-' && src[i + 1] === '>') { tokens.push({ type: 'JSON_ARROW' }); i += 2; continue; }
    if (src[i] === '#' && src[i + 1] === '>' && src[i + 2] === '>') { tokens.push({ type: 'JSON_PATH_TEXT' }); i += 3; continue; }
    if (src[i] === '#' && src[i + 1] === '>') { tokens.push({ type: 'JSON_PATH' }); i += 2; continue; }
    if (src[i] === '-' && (i + 1 < src.length) && /[0-9]/.test(src[i+1]) && (tokens.length === 0 || ['(', ',', 'EQ', 'NE', 'LT', 'GT', 'LE', 'GE', 'PLUS', 'MINUS', 'KEYWORD'].includes(tokens[tokens.length-1]?.type))) {
      // Negative number literal
      let num = '-';
      i++;
      while (i < src.length && /[0-9.]/.test(src[i])) num += src[i++];
      tokens.push({ type: 'NUMBER', value: num.includes('.') ? parseFloat(num) : parseInt(num), isFloat: num.includes('.') });
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

    // Square brackets for ARRAY literals
    if (src[i] === '[') { tokens.push({ type: '[' }); i++; continue; }
    if (src[i] === ']') { tokens.push({ type: ']' }); i++; continue; }

    i++; // skip unknown
  }

  tokens.push({ type: 'EOF' });
  return tokens;
}

// ===== Parser =====
export function parse(sql) {
  const tokens = tokenize(sql);
  let pos = 0;

  const EOF_TOKEN = { type: 'EOF', value: 'EOF' };
  function peek() { return tokens[pos] || EOF_TOKEN; }
  function advance() { 
    if (pos >= tokens.length) throw new Error('Unexpected end of SQL');
    return tokens[pos++]; 
  }
  function expect(type, value) {
    if (pos >= tokens.length) throw new Error(`Expected ${type} ${value || ''}, got end of SQL`);
    const t = advance();
    if (t.type !== type || (value && t.value !== value))
      throw new Error(`Expected ${type} ${value || ''}, got ${t.type} ${t.value || ''}`);
    return t;
  }
  function match(type, value) {
    const p = peek();
    if (p && p.type === type && (!value || p.value === value)) { advance(); return true; }
    return false;
  }
  // Read an alias name after AS — preserves original case even for keywords
  function readAlias() {
    const t = advance();
    return t.originalValue || t.value;
  }
  function isKeyword(val) { const t = peek(); return t && t.type === 'KEYWORD' && t.value === val; }

  // Shared keyword lists (must be before any code that calls parse functions)
  var ZERO_ARG_WINDOW_FUNCS = ['ROW_NUMBER', 'RANK', 'DENSE_RANK', 'CUME_DIST', 'PERCENT_RANK'];
  var ARG_WINDOW_FUNCS = ['LAG', 'LEAD', 'FIRST_VALUE', 'LAST_VALUE', 'NTH_VALUE', 'NTILE'];
  var AGGREGATE_FUNCS = ['COUNT', 'SUM', 'AVG', 'MIN', 'MAX', 'MEDIAN', 'BOOL_AND', 'BOOL_OR', 'EVERY', 'GROUP_CONCAT', 'STRING_AGG', 'JSON_AGG', 'JSONB_AGG', 'ARRAY_AGG', 'PERCENTILE_CONT', 'PERCENTILE_DISC', 'MODE', 'STDDEV', 'STDDEV_POP', 'STDDEV_SAMP', 'VARIANCE', 'VAR_POP', 'VAR_SAMP', 'CORR', 'COVAR_POP', 'COVAR_SAMP', 'REGR_SLOPE', 'REGR_INTERCEPT', 'REGR_R2', 'REGR_COUNT'];

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
          if (['JSON', 'YAML', 'DOT', 'TEXT', 'TREE', 'HTML', 'VOLCANO'].includes(fmt)) {
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
  if (isKeyword('REPLACE')) return parseReplace();
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
    if (isKeyword('COLUMNS') || isKeyword('CREATE') || isKeyword('INDEXES')) {
      const what = advance().value;
      if (what === 'CREATE') { expect('KEYWORD', 'TABLE'); }
      else { expect('KEYWORD', 'FROM'); }
      const table = advance().value;
      if (what === 'INDEXES') return { type: 'SHOW_INDEXES', table };
      return { type: what === 'CREATE' ? 'SHOW_CREATE_TABLE' : 'SHOW_COLUMNS', table };
    }
    throw new Error('Expected TABLES, COLUMNS, INDEXES, or CREATE after SHOW');
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
  if (isKeyword('COMMENT')) {
    advance(); // COMMENT
    expect('KEYWORD', 'ON');
    if (isKeyword('TABLE')) {
      advance();
      const table = (advance()).originalValue || tokens[pos - 1].value;
      expect('KEYWORD', 'IS');
      const comment = advance().value;
      return { type: 'COMMENT_ON', objectType: 'TABLE', table, comment };
    } else if (isKeyword('COLUMN')) {
      advance();
      const colRef = (advance()).originalValue || tokens[pos - 1].value;
      let table, column;
      if (colRef.includes('.')) {
        const parts = colRef.split('.');
        table = parts.slice(0, -1).join('.');
        column = parts[parts.length - 1];
      } else {
        table = null;
        column = colRef;
      }
      expect('KEYWORD', 'IS');
      const comment = advance().value;
      return { type: 'COMMENT_ON', objectType: 'COLUMN', table, column, comment };
    }
    throw new Error('Expected TABLE or COLUMN after COMMENT ON');
  }
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
  if (isKeyword('GRANT') || isKeyword('REVOKE')) {
    const action = advance().value; // GRANT or REVOKE
    // Skip everything until end of statement — these are no-ops in HenryDB
    while (peek() && peek().type !== ';' && peek().type !== 'EOF') advance();
    return { type: action };
  }

  if (isKeyword('CALL')) {
    advance(); // CALL
    const funcTok = advance();
    const name = funcTok.originalValue || funcTok.value;
    expect('(');
    const args = [];
    if (!match(')')) {
      do { args.push(parseExpr()); } while (match(','));
      expect(')');
    }
    return { type: 'CALL', name, args };
  }

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
      
      // Parse CYCLE clause: CYCLE col1, col2 SET is_cycle_col [TO 'Y' DEFAULT 'N'] USING path_col
      let cycle = null;
      if (isKeyword('CYCLE')) {
        advance(); // CYCLE
        const cycleCols = [];
        do {
          const tok = advance();
          cycleCols.push(tok.originalValue || tok.value);
        } while (match(','));
        expect('KEYWORD', 'SET');
        const setCycleCol = (advance()).originalValue || tokens[pos - 1].value;
        let cycleMarkVal = true, defaultVal = false;
        if (isKeyword('TO')) {
          advance();
          cycleMarkVal = advance().value;
          expect('KEYWORD', 'DEFAULT');
          defaultVal = advance().value;
        }
        expect('KEYWORD', 'USING');
        const pathCol = (advance()).originalValue || tokens[pos - 1].value;
        cycle = { columns: cycleCols, setCycleCol, cycleMarkVal, defaultVal, pathCol };
      }
      
      // Parse SEARCH clause: SEARCH {DEPTH|BREADTH} FIRST BY col1, col2 SET ordering_col
      let search = null;
      if (isKeyword('SEARCH')) {
        advance(); // SEARCH
        const mode = advance().value.toUpperCase(); // DEPTH or BREADTH
        expect('KEYWORD', 'FIRST');
        expect('KEYWORD', 'BY');
        const searchCols = [];
        do {
          const tok = advance();
          searchCols.push(tok.originalValue || tok.value);
        } while (match(','));
        expect('KEYWORD', 'SET');
        const orderCol = (advance()).originalValue || tokens[pos - 1].value;
        search = { mode, columns: searchCols, orderCol };
      }
      
      ctes.push({ name, query: baseQuery, unionQuery, recursive, columns: cteColumns, cycle, search });
    } while (match(','));

    // Main query — can be SELECT, DELETE, UPDATE, or INSERT
    let mainQuery;
    if (isKeyword('DELETE')) {
      mainQuery = parseDelete();
    } else if (isKeyword('UPDATE')) {
      mainQuery = parseUpdate();
    } else if (isKeyword('INSERT')) {
      mainQuery = parseInsert();
    } else {
      mainQuery = parseSelect();
    }
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
    let where = null, orderBy = null, limit = null, offset = null, limitExpr = null;
    let joins = [];
    let groupBy = null, having = null;

    if (isKeyword('FROM')) {
      advance(); // FROM
      from = parseFromClause();

      // Implicit CROSS JOINs from comma-separated tables in FROM
      while (match(',')) {
        // Check for LATERAL subquery
        if (isKeyword('LATERAL')) {
          advance(); // LATERAL
          expect('(');
          const subquery = parseSelect();
          expect(')');
          let alias = null;
          if (isKeyword('AS')) { advance(); alias = readAlias(); }
          else if (peek() && peek().type === 'IDENT') alias = advance().value;
          joins.push({ joinType: 'CROSS', lateral: true, subquery, alias, on: null });
        } else if (peek() && peek().type === '(') {
          // Subquery as cross-joined table
          advance(); // (
          const subquery = parseSelect();
          expect(')');
          let alias = null;
          if (isKeyword('AS')) { advance(); alias = readAlias(); }
          else if (peek() && peek().type === 'IDENT') alias = advance().value;
          joins.push({ joinType: 'CROSS', table: { table: '__subquery', subquery }, alias, on: null });
        } else {
          const nextTable = advance().value;
          let nextAlias = null;
          if (peek() && peek().type === 'IDENT') nextAlias = advance().value;
          else if (isKeyword('AS')) { advance(); nextAlias = readAlias(); }
          joins.push({ joinType: 'CROSS', table: nextTable, alias: nextAlias, on: null });
        }
      }

      // JOINs
      while (isKeyword('JOIN') || isKeyword('INNER') || isKeyword('LEFT') || isKeyword('RIGHT') || isKeyword('CROSS') || isKeyword('FULL') || isKeyword('NATURAL') || (isKeyword('OUTER') && tokens[pos + 1]?.value?.toUpperCase() === 'APPLY')) {
        joins.push(parseJoin());
      }
    }

    // PIVOT / UNPIVOT
    let pivot = null, unpivot = null;
    if (isKeyword('PIVOT')) {
      advance(); // PIVOT
      expect('(');
      // AGG(value_col)
      const aggFunc = advance().value.toUpperCase(); // e.g. SUM, COUNT, AVG, MAX, MIN
      expect('(');
      const aggColTok = advance();
      const aggCol = aggColTok.originalValue || aggColTok.value;
      expect(')');
      // FOR category_col IN (val1, val2, ...)
      expect('KEYWORD', 'FOR');
      const pivotColTok = advance();
      const pivotCol = pivotColTok.originalValue || pivotColTok.value;
      expect('KEYWORD', 'IN');
      expect('(');
      const pivotValues = [];
      do {
        const tok = advance();
        pivotValues.push(tok.type === 'STRING' ? tok.value : (tok.originalValue || tok.value));
      } while (match(','));
      expect(')');
      expect(')');
      let pivotAlias = null;
      if (isKeyword('AS')) { advance(); const t = advance(); pivotAlias = t.originalValue || t.value; }
      else if (peek() && peek().type === 'IDENT' && !isKeyword('WHERE') && !isKeyword('ORDER') && !isKeyword('GROUP') && !isKeyword('HAVING') && !isKeyword('LIMIT')) {
        const t = advance(); pivotAlias = t.originalValue || t.value;
      }
      pivot = { aggFunc, aggCol, pivotCol, pivotValues, alias: pivotAlias };
    }
    if (isKeyword('UNPIVOT')) {
      advance(); // UNPIVOT
      expect('(');
      const valueColTok = advance();
      const valueCol = valueColTok.originalValue || valueColTok.value;
      expect('KEYWORD', 'FOR');
      const nameColTok = advance();
      const nameCol = nameColTok.originalValue || nameColTok.value;
      expect('KEYWORD', 'IN');
      expect('(');
      const sourceCols = [];
      do {
        const tok = advance();
        sourceCols.push(tok.originalValue || tok.value);
      } while (match(','));
      expect(')');
      expect(')');
      let unpivotAlias = null;
      if (isKeyword('AS')) { advance(); const t = advance(); unpivotAlias = t.originalValue || t.value; }
      else if (peek() && peek().type === 'IDENT' && !isKeyword('WHERE') && !isKeyword('ORDER') && !isKeyword('GROUP') && !isKeyword('HAVING') && !isKeyword('LIMIT')) {
        const t = advance(); unpivotAlias = t.originalValue || t.value;
      }
      unpivot = { valueCol, nameCol, sourceCols, alias: unpivotAlias };
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
        // Store full expression for runtime evaluation (subqueries, etc.)
        if (limit == null) {
          limitExpr = e;
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

    // WINDOW clause: WINDOW w AS (PARTITION BY ... ORDER BY ...)
    let windowDefs = null;
    if (isKeyword('WINDOW')) {
      advance(); // WINDOW
      windowDefs = {};
      do {
        const wname = advance().value;
        expect('KEYWORD', 'AS');
        expect('(');
        let partitionBy = null;
        let orderBy = null;
        let frame = null;
        if (isKeyword('PARTITION')) {
          advance(); expect('KEYWORD', 'BY');
          partitionBy = [];
          do { partitionBy.push(parseExpr()); } while (match(','));
        }
        if (isKeyword('ORDER')) {
          advance(); expect('KEYWORD', 'BY');
          orderBy = parseOrderBy();
        }
        if (isKeyword('ROWS') || isKeyword('RANGE')) {
          const frameType = advance().value;
          if (isKeyword('BETWEEN')) {
            advance();
            const start = parseFrameBound();
            expect('KEYWORD', 'AND');
            const end = parseFrameBound();
            frame = { type: frameType, start, end };
          } else {
            const bound = parseFrameBound();
            frame = { type: frameType, start: bound, end: { type: 'CURRENT ROW' } };
          }
        }
        expect(')');
        windowDefs[wname] = { partitionBy, orderBy, frame };
      } while (match(','));
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

    let result = { type: 'SELECT', distinct, distinctOn, columns, from, joins, where, groupBy, having, orderBy, limit, limitExpr, offset, forUpdate, pivot, unpivot, windowDefs };

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

    // ORDER BY / LIMIT / OFFSET on combined result (after UNION/INTERSECT/EXCEPT)
    // Note: parseSelect() on the right side may have consumed ORDER BY/LIMIT
    // that actually belongs to the UNION. Steal them from right if present.
    if (result.type === 'UNION' || result.type === 'INTERSECT' || result.type === 'EXCEPT') {
      // First check if there are more tokens to parse
      if (isKeyword('ORDER')) {
        advance(); expect('KEYWORD', 'BY');
        result.orderBy = parseOrderBy();
      }
      if (isKeyword('LIMIT')) {
        advance();
        if (isKeyword('ALL')) { advance(); result.limit = null; }
        else { result.limit = Number(advance().value); }
      }
      if (isKeyword('OFFSET')) {
        advance();
        result.offset = Number(advance().value);
        if (isKeyword('ROWS') || isKeyword('ROW')) advance();
      }
      if (isKeyword('FETCH')) {
        advance();
        if (isKeyword('FIRST') || isKeyword('NEXT')) advance();
        result.limit = Number(advance().value);
        if (isKeyword('ROWS') || isKeyword('ROW')) advance();
        if (isKeyword('ONLY')) advance();
      }
      
      // If the UNION still has no ORDER BY/LIMIT but the right SELECT does,
      // it was likely intended for the UNION. Move them up.
      if (result.right && result.right.type === 'SELECT') {
        if (!result.orderBy && result.right.orderBy) {
          result.orderBy = result.right.orderBy;
          result.right.orderBy = null;
        }
        if (result.limit == null && result.right.limit != null) {
          result.limit = result.right.limit;
          result.right.limit = null;
        }
        if (!result.offset && result.right.offset) {
          result.offset = result.right.offset;
          result.right.offset = null;
        }
      }
    }

    return result;
  }

  // UNION not yet handled at this layer — keeping for later

  function parseSelectList() {
    // SELECT requires at least one column expression
    const nxt = peek();
    if (!nxt || nxt.type === 'EOF' || nxt.type === ';' || 
        (nxt.type === 'KEYWORD' && ['FROM', 'WHERE', 'ORDER', 'GROUP', 'HAVING', 'LIMIT', 'OFFSET', 'UNION', 'INTERSECT', 'EXCEPT', 'INTO', 'FOR'].includes(nxt.value))) {
      throw new Error('Parse error: SELECT requires at least one column or expression');
    }
    if (match('*')) {
      const cols = [{ type: 'star' }];
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
    // Boolean literals TRUE/FALSE
    if (peek().type === 'KEYWORD' && (peek().value === 'TRUE' || peek().value === 'FALSE')) {
      const val = advance().value === 'TRUE';
      let expr = { type: 'literal', value: val };
      // Handle trailing arithmetic: TRUE AND x, etc.
      expr = parseTrailingArithmetic(expr);
      let alias = null;
      if (isKeyword('AS')) { advance(); alias = readAlias(); }
      else if (peek().type === 'IDENT' && !isKeyword('FROM') && !isKeyword('WHERE') && !isKeyword('JOIN') && !isKeyword('ON') && !isKeyword('GROUP') && !isKeyword('ORDER') && !isKeyword('HAVING') && !isKeyword('LIMIT') && !isKeyword('UNION') && !isKeyword('INTERSECT') && !isKeyword('EXCEPT')) {
        alias = readAlias();
      }
      return { type: 'expression', expr, alias: alias || (val ? 'TRUE' : 'FALSE') };
    }
    // CURRENT_TIMESTAMP, CURRENT_DATE (no parens)
    if (peek().type === 'KEYWORD' && (peek().value === 'CURRENT_TIMESTAMP' || peek().value === 'CURRENT_DATE' || peek().value === 'CURRENT_TIME')) {
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
      // Handle operator chaining after CAST
      castNode = parseTrailingArithmetic(castNode);
      // Handle CONCAT
      if (peek().type === 'CONCAT_OP' || peek().type === 'CONCAT') {
        let left = castNode;
        while (match('CONCAT_OP') || match('CONCAT')) {
          const right = parsePrimaryWithConcat();
          left = { type: 'function_call', func: 'CONCAT_OP', args: [left, right] };
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

    // Check for EXISTS / NOT expression in SELECT (handles NOT EXISTS, NOT NULL, NOT NOT TRUE, etc.)
    if (isKeyword('EXISTS') || isKeyword('NOT')) {
      const expr = parseExpr();
      let alias = null;
      if (isKeyword('AS')) { advance(); alias = readAlias(); }
      return { type: 'expression', expr, alias };
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
    if (peek().type === 'KEYWORD' && AGGREGATE_FUNCS.includes(peek().value) && tokens[pos + 1]?.type === '(') {
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
      } else if ((func === 'STRING_AGG' || func === 'GROUP_CONCAT') && peek().type === ',') {
        // PostgreSQL STRING_AGG(expr, delimiter) / GROUP_CONCAT(expr, delimiter) syntax
        advance(); // skip comma
        separator = advance().value; // STRING literal
      }
      // Parse percentile fraction for PERCENTILE_CONT/PERCENTILE_DISC
      let percentile = null;
      if ((func === 'PERCENTILE_CONT' || func === 'PERCENTILE_DISC') && peek().type === ',') {
        advance(); // skip comma
        const fracExpr = parseExpr();
        percentile = fracExpr.type === 'literal' ? fracExpr.value : fracExpr;
      }
      // Parse second argument for two-arg aggregate functions (CORR, COVAR_*, REGR_*)
      let arg2 = null;
      const TWO_ARG_AGGS = ['CORR', 'COVAR_POP', 'COVAR_SAMP', 'REGR_SLOPE', 'REGR_INTERCEPT', 'REGR_R2', 'REGR_COUNT'];
      if (TWO_ARG_AGGS.includes(func) && peek().type === ',') {
        advance(); // skip comma
        const arg2Expr = parseExpr();
        arg2 = arg2Expr.type === 'column_ref' ? arg2Expr.name : arg2Expr;
      }
      // Optional ORDER BY inside aggregate (STRING_AGG, ARRAY_AGG, etc.)
      let aggOrderBy = null;
      if (isKeyword('ORDER')) {
        advance(); // ORDER
        expect('KEYWORD', 'BY');
        aggOrderBy = [];
        do {
          const col = parseExpr();
          let dir = 'ASC';
          if (isKeyword('ASC')) { advance(); dir = 'ASC'; }
          else if (isKeyword('DESC')) { advance(); dir = 'DESC'; }
          aggOrderBy.push({ column: col, direction: dir });
        } while (match(','));
      }
      expect(')');

      // Optional FILTER clause: AGG(...) FILTER (WHERE condition)
      let filterClause = null;
      if (isKeyword('FILTER')) {
        advance(); // FILTER
        expect('(');
        expect('KEYWORD', 'WHERE');
        filterClause = parseExpr();
        expect(')');
      }

      // Add separator info for GROUP_CONCAT / STRING_AGG
      const aggExtra = (func === 'GROUP_CONCAT' || func === 'STRING_AGG') ? { separator, aggOrderBy, filter: filterClause, percentile, arg2 } : { aggOrderBy, filter: filterClause, percentile, arg2 };
      // Check for window function: aggregate OVER (...)
      if (isKeyword('OVER')) {
        const over = parseOverClause();
        let node = { type: 'window', func, arg, distinct, over };
        const withArith = parseTrailingArithmetic(node);
        if (withArith !== node) {
          let alias = null;
          if (isKeyword('AS')) { advance(); alias = readAlias(); }
          return { type: 'expression', expr: withArith, alias };
        }
        let alias = null;
        if (isKeyword('AS')) { advance(); alias = readAlias(); }
        return { type: 'window', func, arg, distinct, over, alias };
      }

      let alias = null;
      // Check for arithmetic/concat after aggregate: SUM(a) * 100 / SUM(b)
      let node = { type: 'aggregate', func, arg, distinct, ...aggExtra };
      if (['PLUS', 'MINUS', 'SLASH', 'MOD', 'CONCAT_OP', 'CONCAT'].includes(peek().type) || (peek().type === '*' && tokens[pos+1]?.type !== ')')) {
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

    // Window functions: ROW_NUMBER, RANK, DENSE_RANK, CUME_DIST, PERCENT_RANK
    if (peek().type === 'KEYWORD' && ZERO_ARG_WINDOW_FUNCS.includes(peek().value) && tokens[pos + 1]?.type === '(') {
      let node = parseWindowCall();
      const withArith = parseTrailingArithmetic(node);
      if (withArith !== node) {
        let alias = null;
        if (isKeyword('AS')) { advance(); alias = readAlias(); }
        return { type: 'expression', expr: withArith, alias };
      }
      let alias = null;
      if (isKeyword('AS')) { advance(); alias = readAlias(); }
      return { ...node, alias };
    }

    // LAG/LEAD window functions with arguments
    if (peek().type === 'KEYWORD' && ARG_WINDOW_FUNCS.includes(peek().value) && tokens[pos + 1]?.type === '(') {
      let node = parseWindowCall();
      const withArith = parseTrailingArithmetic(node);
      if (withArith !== node) {
        let alias = null;
        if (isKeyword('AS')) { advance(); alias = readAlias(); }
        return { type: 'expression', expr: withArith, alias };
      }
      let alias = null;
      if (isKeyword('AS')) { advance(); alias = readAlias(); }
      return { ...node, alias };
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

    // OVERLAY(string PLACING replacement FROM start [FOR length])
    if (isKeyword('OVERLAY')) {
      advance(); // OVERLAY
      expect('(');
      const str = parseExpr();
      expect('KEYWORD', 'PLACING');
      const replacement = parseExpr();
      expect('KEYWORD', 'FROM');
      const start = parseExpr();
      let len = null;
      if (isKeyword('FOR')) { advance(); len = parseExpr(); }
      expect(')');
      let alias = null;
      if (isKeyword('AS')) { advance(); alias = readAlias(); }
      const args = len ? [str, replacement, start, len] : [str, replacement, start];
      return { type: 'function', func: 'OVERLAY', args, alias };
    }

    // String functions in SELECT
    if (peek().type === 'KEYWORD' && ['UPPER', 'LOWER', 'INITCAP', 'LENGTH', 'CHAR_LENGTH', 'CONCAT', 'COALESCE', 'NULLIF', 'SUBSTRING', 'SUBSTR', 'REPLACE', 'TRIM', 'INSTR', 'ABS', 'ROUND', 'CEIL', 'FLOOR', 'IFNULL', 'ISNULL', 'NVL', 'IIF', 'TYPEOF',
      'JSON_EXTRACT', 'JSON_SET', 'JSON_ARRAY_LENGTH', 'JSON_TYPE', 'JSON_OBJECT', 'JSON_ARRAY', 'JSON_VALID', 'JSON_VALUE', 'LEFT', 'RIGHT', 'LPAD', 'RPAD', 'REVERSE', 'REPEAT', 'POWER', 'SQRT', 'LOG', 'EXP', 'RANDOM', 'STRFTIME', 'NOW', 'GREATEST', 'LEAST', 'MOD', 'LTRIM', 'RTRIM',
      'JSON_BUILD_OBJECT', 'JSON_BUILD_ARRAY', 'ROW_TO_JSON', 'TO_JSON', 'JSON_OBJECT_KEYS', 'DATE_ADD', 'DATE_DIFF', 'DATE_TRUNC', 'NEXTVAL', 'CURRVAL', 'SETVAL', 'REGEXP_MATCHES', 'REGEXP_REPLACE', 'REGEXP_COUNT',
      'SPLIT_PART', 'TRANSLATE', 'CHR', 'ASCII', 'MD5', 'DATE', 'AGE', 'TO_CHAR', 'DATE_FORMAT', 'MAKE_DATE', 'MAKE_TIMESTAMP', 'EPOCH', 'TO_TIMESTAMP'].includes(peek().value)) {
      const func = advance().value;
      expect('(');
      const args = [];
      if (!match(')')) {
        args.push(parseExpr());
        while (match(',')) args.push(parseExpr());
        expect(')');
      }
      // Check for arithmetic and concat after function call
      let node = { type: 'function_call', func, args };
      while (['PLUS', 'MINUS', 'SLASH', 'MOD', 'CONCAT_OP', 'CONCAT'].includes(peek().type) || (peek().type === '*' && tokens[pos+1]?.type !== ')')) {
        const t = peek().type;
        if (t === 'CONCAT_OP' || t === 'CONCAT') {
          advance();
          const right = parsePrimary();
          node = { type: 'function_call', func: 'CONCAT_OP', args: [node, right] };
        } else {
          const op = t === 'PLUS' ? '+' : t === 'MINUS' ? '-' : t === 'SLASH' ? '/' : t === 'MOD' ? '%' : '*';
          advance();
          const right = parsePrimary();
          node = { type: 'arith', op, left: node, right };
        }
      }
      let alias = null;
      if (isKeyword('AS')) { advance(); alias = readAlias(); }
      if (node.type === 'function_call') {
        return { type: 'function', func: node.func, args: node.args, alias };
      }
      return { type: 'expression', expr: node, alias };
    }
    // CASE expression in SELECT
    if (peek().type === 'KEYWORD' && peek().value === 'CASE') {
      let expr = parseCaseExpr();
      // Handle trailing arithmetic, comparison, and logical operators after CASE
      expr = parseTrailingArithmetic(expr);
      const compOps2 = { 'EQ': '=', 'NE': '!=', 'LT': '<', 'GT': '>', 'LE': '<=', 'GE': '>=' };
      if (compOps2[peek().type]) {
        const op = peek().type;
        advance();
        const right = parsePrimaryWithConcat();
        expr = { type: 'COMPARE', op, left: expr, right };
      }
      while (isKeyword('AND') || isKeyword('OR')) {
        const logicOp = advance().value.toUpperCase();
        const right = parseComparison();
        expr = { type: logicOp, left: expr, right };
      }
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
        expr = { type: 'literal', value: -operand.value, ...(operand.isFloat ? { isFloat: true } : {}) };
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
    // User-defined or unknown function call: ident(args) in SELECT columns
    if (peek().type === 'IDENT' && tokens[pos + 1]?.type === '(') {
      const func = advance().value;
      expect('(');
      const args = [];
      if (!match(')')) {
        args.push(parseExpr());
        while (match(',')) args.push(parseExpr());
        expect(')');
      }
      let node = { type: 'function_call', func: func.toUpperCase(), args };
      // Handle arithmetic and concat after function call
      while (['PLUS', 'MINUS', 'SLASH', 'MOD', 'CONCAT_OP', 'CONCAT'].includes(peek().type) || (peek().type === '*' && tokens[pos+1]?.type !== ')')) {
        const t = peek().type;
        if (t === 'CONCAT_OP' || t === 'CONCAT') {
          advance();
          const right = parsePrimary();
          node = { type: 'function_call', func: 'CONCAT_OP', args: [node, right] };
        } else {
          const op = t === 'PLUS' ? '+' : t === 'MINUS' ? '-' : t === 'SLASH' ? '/' : t === 'MOD' ? '%' : '*';
          advance();
          const right = parsePrimary();
          node = { type: 'arith', op, left: node, right };
        }
      }
      let alias = null;
      if (isKeyword('AS')) { advance(); alias = readAlias(); }
      if (node.type === 'function_call') {
        return { type: 'function', func: node.func, args: node.args, alias };
      }
      return { type: 'expression', expr: node, alias };
    }
    const colTok = advance();
    if (!colTok) throw new Error('Unexpected end of SQL: expected column or expression');
    const col = colTok.originalValue || colTok.value;
    // Check for || concatenation or arithmetic operators
    const nextTok = peek();
    const nextType = nextTok ? nextTok.type : null;
    if (nextType === 'CONCAT_OP' || nextType === 'CAST_OP' || nextType === 'PLUS' || nextType === 'MINUS' || nextType === '*' || nextType === 'SLASH' || nextType === 'MOD' || nextType === 'EQ' || nextType === 'NE' || nextType === 'LT' || nextType === 'GT' || nextType === 'LE' || nextType === 'GE') {
      let seed = colTok.type === 'STRING' || colTok.type === 'NUMBER'
        ? { type: 'literal', value: col, ...(colTok.isFloat ? { isFloat: true } : {}) }
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
          left = { type: 'function_call', func: 'CONCAT_OP', args: [left, right] };
        } else break;
      }
      // Handle :: type cast after expression
      if (peek().type === 'CAST_OP') {
        advance(); // ::
        const typeTok = advance();
        const targetType = (typeTok.originalValue || typeTok.value).toUpperCase();
        left = { type: 'cast', expr: left, targetType };
      }
      // Handle comparison operators after arithmetic: s + n > 5
      const compOps = { 'EQ': '=', 'NE': '!=', 'LT': '<', 'GT': '>', 'LE': '<=', 'GE': '>=' };
      if (compOps[peek().type]) {
        const op = peek().type;
        advance();
        const right = parsePrimaryWithConcat();
        left = { type: 'COMPARE', op, left, right };
      }
      // Handle logical AND/OR after comparisons: a > 1 AND b < 5
      while (isKeyword('AND') || isKeyword('OR')) {
        const logicOp = advance().value.toUpperCase();
        const right = parseComparison();
        left = { type: logicOp, left, right };
      }
      let alias = null;
      if (isKeyword('AS')) { advance(); alias = readAlias(); }
      return { type: 'expression', expr: left, alias };
    }
    let alias = null;
    
    // Check for :: type cast after bare column name
    if (peek().type === 'CAST_OP') {
      advance(); // ::
      const typeTok = advance();
      const targetType = (typeTok.originalValue || typeTok.value).toUpperCase();
      let expr = { type: 'cast', expr: { type: 'column_ref', name: col }, targetType };
      if (isKeyword('AS')) { advance(); alias = readAlias(); }
      return { type: 'expression', expr, alias };
    }
    
    // Check for JSON operators after column name
    if (peek() && ['JSON_ARROW', 'JSON_ARROW_TEXT', 'JSON_PATH', 'JSON_PATH_TEXT'].includes(peek().type)) {
      let left = { type: 'column_ref', name: col };
      while (peek() && ['JSON_ARROW', 'JSON_ARROW_TEXT', 'JSON_PATH', 'JSON_PATH_TEXT'].includes(peek().type)) {
        const opType = advance().type;
        const right = parsePrimary();
        const rightVal = right.type === 'literal' ? right.value : (right.name || right.value);
        const func = opType === 'JSON_ARROW_TEXT' || opType === 'JSON_PATH_TEXT' ? 'JSON_EXTRACT_TEXT' : 'JSON_EXTRACT';
        const rv = String(rightVal);
        const path = opType.includes('PATH') ? rightVal : (rv.startsWith('$') ? rightVal : `$.${rightVal}`);
        left = { type: 'function', func, args: [left, { type: 'literal', value: path }] };
      }
      if (isKeyword('AS')) { advance(); alias = readAlias(); }
      return { type: 'expression', expr: left, alias };
    }
    
    if (isKeyword('AS')) { advance(); alias = readAlias(); }
    // NUMBER and STRING tokens without operators should be literals, not column refs
    // But first check for IS NULL / IS NOT NULL after them
    if (colTok.type === 'NUMBER' || colTok.type === 'STRING') {
      const literalExpr = { type: 'literal', value: col, ...(colTok.isFloat ? { isFloat: true } : {}) };
      // Check for IS NULL / IS NOT NULL after literal
      if (!alias && isKeyword('IS')) {
        advance(); // IS
        let not = false;
        if (isKeyword('NOT')) { not = true; advance(); }
        if (isKeyword('NULL')) {
          advance();
          const expr = not
            ? { type: 'IS_NOT_NULL', left: literalExpr }
            : { type: 'IS_NULL', left: literalExpr };
          if (isKeyword('AS')) { advance(); alias = readAlias(); }
          else if (peek().type === 'IDENT' && !isKeyword('FROM') && !isKeyword('WHERE') && !isKeyword('ORDER') && !isKeyword('GROUP') && !isKeyword('HAVING') && !isKeyword('LIMIT') && !isKeyword('UNION') && !isKeyword('INTERSECT') && !isKeyword('EXCEPT')) {
            alias = readAlias();
          }
          return { type: 'expression', expr, alias };
        }
      }
      return { type: 'expression', expr: literalExpr, alias };
    }
    
    // Check for IS NULL / IS NOT NULL / comparison operators after a column name
    // These weren't consumed by earlier handlers since they don't start with arithmetic operators
    if (!alias && isKeyword('IS')) {
      advance(); // IS
      let not = false;
      if (isKeyword('NOT')) { not = true; advance(); }
      if (isKeyword('NULL')) {
        advance();
        let expr = not 
          ? { type: 'IS_NOT_NULL', left: { type: 'column_ref', name: col } }
          : { type: 'IS_NULL', left: { type: 'column_ref', name: col } };
        // Handle trailing AND/OR after IS [NOT] NULL
        while (isKeyword('AND') || isKeyword('OR')) {
          const logicOp = advance().value.toUpperCase();
          const right = parseComparison();
          expr = { type: logicOp, left: expr, right };
        }
        if (isKeyword('AS')) { advance(); alias = readAlias(); }
        else if (peek().type === 'IDENT' && !isKeyword('FROM') && !isKeyword('WHERE') && !isKeyword('ORDER') && !isKeyword('GROUP') && !isKeyword('HAVING') && !isKeyword('LIMIT') && !isKeyword('UNION') && !isKeyword('INTERSECT') && !isKeyword('EXCEPT')) {
          alias = readAlias();
        }
        return { type: 'expression', expr, alias };
      } else if (isKeyword('TRUE')) {
        advance();
        const expr = not ? { type: 'IS_NOT_TRUE', expr: { type: 'column_ref', name: col } } : { type: 'IS_TRUE', expr: { type: 'column_ref', name: col } };
        if (isKeyword('AS')) { advance(); alias = readAlias(); }
        return { type: 'expression', expr, alias };
      } else if (isKeyword('FALSE')) {
        advance();
        const expr = not ? { type: 'IS_NOT_FALSE', expr: { type: 'column_ref', name: col } } : { type: 'IS_FALSE', expr: { type: 'column_ref', name: col } };
        if (isKeyword('AS')) { advance(); alias = readAlias(); }
        return { type: 'expression', expr, alias };
      } else if (isKeyword('DISTINCT')) {
        advance(); // DISTINCT
        expect('KEYWORD', 'FROM');
        const right = parseExpr();
        const expr = not 
          ? { type: 'IS_NOT_DISTINCT_FROM', left: { type: 'column_ref', name: col }, right }
          : { type: 'IS_DISTINCT_FROM', left: { type: 'column_ref', name: col }, right };
        if (isKeyword('AS')) { advance(); alias = readAlias(); }
        return { type: 'expression', expr, alias };
      }
    }
    
    // BETWEEN in SELECT list: `col BETWEEN x AND y` — the AND is ambiguous with alias
    // but we can parse it since BETWEEN is always followed by expr AND expr
    if (!alias && isKeyword('BETWEEN')) {
      advance(); // BETWEEN
      const low = parsePrimary();
      expect('KEYWORD', 'AND');
      const high = parsePrimary();
      const expr = { type: 'BETWEEN', left: { type: 'column_ref', name: col }, low, high };
      if (isKeyword('AS')) { advance(); alias = readAlias(); }
      else if (peek()?.type === 'IDENT' && !isKeyword('FROM') && !isKeyword('WHERE') && !isKeyword('ORDER') && !isKeyword('GROUP') && !isKeyword('HAVING') && !isKeyword('LIMIT') && !isKeyword('UNION') && !isKeyword('INTERSECT') && !isKeyword('EXCEPT')) {
        alias = readAlias();
      }
      return { type: 'expression', expr, alias };
    }
    if (!alias && isKeyword('NOT') && tokens[pos+1]?.type === 'KEYWORD' && tokens[pos+1]?.value === 'BETWEEN') {
      advance(); // NOT
      advance(); // BETWEEN
      const low = parsePrimary();
      expect('KEYWORD', 'AND');
      const high = parsePrimary();
      const expr = { type: 'NOT_BETWEEN', left: { type: 'column_ref', name: col }, low, high };
      if (isKeyword('AS')) { advance(); alias = readAlias(); }
      else if (peek()?.type === 'IDENT' && !isKeyword('FROM') && !isKeyword('WHERE') && !isKeyword('ORDER') && !isKeyword('GROUP') && !isKeyword('HAVING') && !isKeyword('LIMIT') && !isKeyword('UNION') && !isKeyword('INTERSECT') && !isKeyword('EXCEPT')) {
        alias = readAlias();
      }
      return { type: 'expression', expr, alias };
    }
    
    // Check for comparison operators (=, <>, <, >, <=, >=) after column
    if (!alias && peek() && ['EQ', 'NE', 'LT', 'GT', 'LE', 'GE'].includes(peek().type)) {
      const opMap = { EQ: 'EQ', NE: 'NEQ', LT: 'LT', GT: 'GT', LE: 'LTE', GE: 'GTE' };
      const op = opMap[advance().type];
      const right = parseExpr();
      const expr = { type: 'COMPARE', op, left: { type: 'column_ref', name: col }, right };
      if (isKeyword('AS')) { advance(); alias = readAlias(); }
      return { type: 'expression', expr, alias };
    }
    
    // Check for LIKE/ILIKE after column
    if (!alias && (isKeyword('LIKE') || isKeyword('ILIKE'))) {
      const func = advance().value;
      const pattern = parseExpr();
      const expr = { type: func, left: { type: 'column_ref', name: col }, pattern };
      if (isKeyword('AS')) { advance(); alias = readAlias(); }
      return { type: 'expression', expr, alias };
    }
    
    // Check for IN after column
    if (!alias && isKeyword('IN')) {
      advance();
      expect('(');
      // Could be subquery or value list
      if (isKeyword('SELECT')) {
        const subquery = parseSelect();
        expect(')');
        const expr = { type: 'IN_SUBQUERY', left: { type: 'column_ref', name: col }, subquery };
        if (isKeyword('AS')) { advance(); alias = readAlias(); }
        return { type: 'expression', expr, alias };
      }
      const values = [parseExpr()];
      while (match(',')) values.push(parseExpr());
      expect(')');
      const expr = { type: 'IN_LIST', left: { type: 'column_ref', name: col }, values };
      if (isKeyword('AS')) { advance(); alias = readAlias(); }
      return { type: 'expression', expr, alias };
    }
    
    // Check for :: type cast after column
    if (!alias && peek().type === 'CAST_OP') {
      advance(); // ::
      const typeTok = advance();
      const targetType = (typeTok.originalValue || typeTok.value).toUpperCase();
      let expr = { type: 'cast', expr: { type: 'column_ref', name: col }, targetType };
      if (isKeyword('AS')) { advance(); alias = readAlias(); }
      return { type: 'expression', expr, alias };
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
      let columnAliases = null;
      if (isKeyword('AS')) { advance(); alias = readAlias(); }
      else if (peek().type === 'IDENT') alias = advance().value;
      if (match('(')) {
        columnAliases = [];
        columnAliases.push(advance().value);
        while (match(',')) columnAliases.push(advance().value);
        expect(')');
      }
      return { table: '__generate_series', alias, start, stop, step, columnAliases };
    }
    // UNNEST(array_expr)
    if (isKeyword('UNNEST')) {
      advance();
      expect('(');
      const arrayExpr = parseExpr();
      expect(')');
      let alias = null;
      let columnAlias = null;
      if (isKeyword('AS')) {
        advance();
        alias = readAlias();
        // Optional column alias: AS alias(col)
        if (match('(')) {
          columnAlias = advance().value;
          expect(')');
        }
      } else if (peek() && peek().type === 'IDENT') {
        alias = advance().value;
        if (match('(')) {
          columnAlias = advance().value;
          expect(')');
        }
      }
      return { table: '__unnest', alias, arrayExpr, columnAlias };
    }
    // Subquery or VALUES in FROM
    if (peek().type === '(') {
      advance(); // (
      // Check for VALUES clause inside parens
      if (isKeyword('VALUES')) {
        advance(); // VALUES
        const tuples = [];
        do {
          expect('(');
          const values = [];
          values.push(parseExpr());
          while (match(',')) values.push(parseExpr());
          expect(')');
          tuples.push(values);
        } while (match(','));
        expect(')'); // closing paren of FROM (VALUES ...)
        let alias = null;
        let columnAliases = null;
        if (isKeyword('AS')) { advance(); alias = readAlias(); }
        else if (peek().type === 'IDENT') alias = advance().value;
        // Optional column aliases: AS t(col1, col2, ...)
        if (match('(')) {
          columnAliases = [];
          columnAliases.push(advance().value);
          while (match(',')) columnAliases.push(advance().value);
          expect(')');
        }
        return { table: '__values', alias, tuples, columnAliases };
      }
      const subquery = parseSelect();
      expect(')');
      let alias = null;
      if (isKeyword('AS')) { advance(); alias = readAlias(); }
      else if (peek().type === 'IDENT') alias = advance().value;
      return { table: '__subquery', alias, subquery };
    }
    // Function call in FROM: func_name(args) [AS alias]
    if (peek().type === 'IDENT' && tokens[pos + 1]?.type === '(') {
      const func = advance().value;
      expect('(');
      const args = [];
      if (!match(')')) {
        args.push(parseExpr());
        while (match(',')) args.push(parseExpr());
        expect(')');
      }
      let alias = null;
      if (isKeyword('AS')) { advance(); alias = readAlias(); }
      else if (peek().type === 'IDENT') alias = advance().value;
      return { table: '__func_call', func: func.toLowerCase(), args, alias };
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
    
    // CROSS APPLY / OUTER APPLY (SQL Server syntax for LATERAL)
    if (isKeyword('CROSS') && tokens[pos + 1]?.value?.toUpperCase() === 'APPLY') {
      advance(); // CROSS
      advance(); // APPLY
      expect('(');
      const subquery = parseSelect();
      expect(')');
      let alias = null;
      if (isKeyword('AS')) { advance(); alias = advance().value; }
      else if (peek() && peek().type === 'IDENT' && !isKeyword('ON') && !isKeyword('WHERE') && !isKeyword('ORDER') && !isKeyword('GROUP')) {
        alias = advance().value;
      }
      return { joinType: 'CROSS', lateral: true, subquery, alias, on: null };
    }
    if (isKeyword('OUTER') && tokens[pos + 1]?.value?.toUpperCase() === 'APPLY') {
      advance(); // OUTER
      advance(); // APPLY
      expect('(');
      const subquery = parseSelect();
      expect(')');
      let alias = null;
      if (isKeyword('AS')) { advance(); alias = advance().value; }
      else if (peek() && peek().type === 'IDENT' && !isKeyword('ON') && !isKeyword('WHERE') && !isKeyword('ORDER') && !isKeyword('GROUP')) {
        alias = advance().value;
      }
      return { joinType: 'LEFT', lateral: true, subquery, alias, on: { type: 'LITERAL_BOOL', value: true } };
    }
    
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
      return { type: 'JOIN', joinType, table: null, alias, on, lateral, subquery, natural: isNatural };
    }
    const joinTok = advance();
    const table = joinTok.originalValue || joinTok.value;
    let alias = null;
    if (peek().type === 'IDENT' && !isKeyword('ON')) alias = advance().value;
    let on = null;
    let usingColumns = null;
    if (isKeyword('ON')) {
      advance();
      on = parseExpr();
    } else if (isKeyword('USING')) {
      advance();
      expect('(');
      usingColumns = [];
      do { usingColumns.push(advance().value); } while (match(','));
      expect(')');
    }
    return { type: 'JOIN', joinType, table, alias, on, usingColumns, natural: isNatural };
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
        const subNode = { type: 'SUBQUERY', subquery };
        // Check for comparison operator after subquery (e.g., (SELECT COUNT(*) ...) > 1)
        const compOps = new Set(['EQ', 'NE', 'LT', 'GT', 'LE', 'GE']);
        const nxt = peek();
        if (nxt && compOps.has(nxt.type)) {
          const op = advance().type;
          const right = parsePrimaryWithConcat();
          return { type: 'COMPARE', op, left: subNode, right };
        }
        return subNode;
      }
      // Not a subquery — put the '(' back and let parsePrimary handle it
      // (parsePrimary correctly handles nested parens with operator precedence)
      pos--;
    }

    let left = parsePrimaryWithConcat();

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
      let escape = null;
      if (isKeyword('ESCAPE')) {
        advance();
        escape = parsePrimaryWithConcat();
      }
      return { type: 'NOT', expr: { type: 'LIKE', left, pattern, escape } };
    }

    // NOT REGEXP
    if (isKeyword('NOT') && tokens[pos + 1]?.type === 'KEYWORD' && (tokens[pos + 1]?.value === 'REGEXP' || tokens[pos + 1]?.value === 'RLIKE')) {
      advance(); // NOT
      advance(); // REGEXP/RLIKE
      const pattern = parsePrimaryWithConcat();
      return { type: 'NOT', expr: { type: 'REGEXP', left, pattern } };
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
      let escape = null;
      if (isKeyword('ESCAPE')) {
        advance();
        escape = parsePrimaryWithConcat();
      }
      return { type: 'LIKE', left, pattern, escape };
    }

    if (isKeyword('ILIKE')) {
      advance();
      const pattern = parsePrimaryWithConcat();
      let escape = null;
      if (isKeyword('ESCAPE')) {
        advance();
        escape = parsePrimaryWithConcat();
      }
      return { type: 'ILIKE', left, pattern, escape };
    }

    // SIMILAR TO
    if (isKeyword('SIMILAR')) {
      advance(); // SIMILAR
      if (isKeyword('TO')) advance(); // TO
      const pattern = parsePrimaryWithConcat();
      return { type: 'SIMILAR_TO', left, pattern };
    }

    // REGEXP / RLIKE
    if (isKeyword('REGEXP') || isKeyword('RLIKE')) {
      advance();
      const pattern = parsePrimaryWithConcat();
      return { type: 'REGEXP', left, pattern };
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

    // JSON operators: ->  ->>  #>  #>>
    while (peek() && ['JSON_ARROW', 'JSON_ARROW_TEXT', 'JSON_PATH', 'JSON_PATH_TEXT'].includes(peek().type)) {
      const opType = advance().type;
      const right = parsePrimary();
      const rightVal = right.type === 'literal' ? right.value : (right.name || right.value);
      const func = opType === 'JSON_ARROW_TEXT' || opType === 'JSON_PATH_TEXT' ? 'JSON_EXTRACT_TEXT' : 'JSON_EXTRACT';
      const rv2 = String(rightVal);
      const path = opType.includes('PATH') ? rightVal : (rv2.startsWith('$') ? rightVal : `$.${rightVal}`);
      left = { type: 'function', func, args: [left, { type: 'literal', value: path }] };
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
      // Check for ANY/ALL/SOME subquery operators
      if (isKeyword('ANY') || isKeyword('SOME') || isKeyword('ALL')) {
        const quantifier = advance().value.toUpperCase(); // ANY, SOME, or ALL
        expect('(');
        const subquery = parseSelect();
        expect(')');
        return { type: 'QUANTIFIED_COMPARE', op, left, quantifier: quantifier === 'SOME' ? 'ANY' : quantifier, subquery };
      }
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
    // Text search match: expr @@ expr
    if (op === 'TS_MATCH') {
      advance();
      const right = parsePrimaryWithConcat();
      return { type: 'TS_MATCH', left, right };
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
        left = { type: 'function_call', func: 'CONCAT_OP', args: [left, right] };
      } else if (t === 'CAST_OP') {
        advance(); // ::
        const typeTok = advance();
        const targetType = typeTok.originalValue || typeTok.value;
        left = { type: 'cast', expr: left, targetType: targetType.toUpperCase() };
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
        return { type: 'literal', value: -operand.value, ...(operand.isFloat ? { isFloat: true } : {}) };      }
      return { type: 'unary_minus', operand };
    }
    // Unary plus: +expr (no-op, just parse the operand)
    if (t.type === 'PLUS') {
      advance();
      return parsePrimary();
    }
    if (t.type === 'NUMBER') { advance(); return { type: 'literal', value: t.value, isFloat: t.isFloat || false }; }
    if (t.type === 'STRING') { advance(); return { type: 'literal', value: t.value }; }
    if (t.type === 'PARAM') { advance(); return { type: 'PARAM', index: t.index }; }
    // ARRAY[...] literal
    if (t.type === 'IDENT' && t.value.toUpperCase() === 'ARRAY') {
      advance(); // ARRAY
      if (peek().type === '[') {
        advance(); // [
        const elements = [];
        if (peek().type !== ']') {
          elements.push(parseExpr());
          while (match(',')) elements.push(parseExpr());
        }
        expect(']');
        return { type: 'array_literal', elements };
      }
      // Not followed by [ — treat as identifier
      return { type: 'column_ref', name: t.value };
    }
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
    // Parenthesized expression or scalar subquery
    if (t.type === '(') {
      // Check for scalar subquery: (SELECT ...)
      if (tokens[pos + 1]?.type === 'KEYWORD' && tokens[pos + 1]?.value === 'SELECT') {
        advance(); // consume '('
        const subquery = parseSelect();
        expect(')');
        return { type: 'scalar_subquery', subquery };
      }
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
    if (t.type === 'KEYWORD' && t.value === 'CURRENT_TIME') { advance(); return { type: 'function_call', func: 'CURRENT_TIME', args: [] }; }

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
    if (t.type === 'KEYWORD' && ['UPPER', 'LOWER', 'INITCAP', 'LENGTH', 'CHAR_LENGTH', 'CONCAT', 'COALESCE', 'NULLIF', 'SUBSTRING', 'SUBSTR', 'REPLACE', 'TRIM', 'INSTR', 'ABS', 'ROUND', 'CEIL', 'FLOOR', 'IFNULL', 'ISNULL', 'NVL', 'IIF', 'TYPEOF',
      'JSON_EXTRACT', 'JSON_SET', 'JSON_ARRAY_LENGTH', 'JSON_TYPE', 'JSON_OBJECT', 'JSON_ARRAY', 'JSON_VALID', 'JSON_VALUE', 'LEFT', 'RIGHT', 'LPAD', 'RPAD', 'REVERSE', 'REPEAT', 'POWER', 'SQRT', 'LOG', 'EXP', 'RANDOM', 'STRFTIME', 'NOW', 'GREATEST', 'LEAST', 'MOD', 'LTRIM', 'RTRIM',
      'JSON_BUILD_OBJECT', 'JSON_BUILD_ARRAY', 'ROW_TO_JSON', 'TO_JSON', 'JSON_OBJECT_KEYS', 'DATE_ADD', 'DATE_DIFF', 'DATE_TRUNC', 'NEXTVAL', 'CURRVAL', 'SETVAL', 'REGEXP_MATCHES', 'REGEXP_REPLACE', 'REGEXP_COUNT',
      'SPLIT_PART', 'TRANSLATE', 'CHR', 'ASCII', 'MD5', 'DATE', 'AGE', 'TO_CHAR', 'DATE_FORMAT', 'MAKE_DATE', 'MAKE_TIMESTAMP', 'EPOCH', 'TO_TIMESTAMP', 'DATE_PART',
      'LN', 'LOG2', 'LOG10', 'SIGN', 'PI', 'DEGREES', 'RADIANS', 'SIN', 'COS', 'TAN', 'ASIN', 'ACOS', 'ATAN', 'ATAN2', 'GEN_RANDOM_UUID', 'UUID'].includes(t.value)) {
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
    if (t.type === 'KEYWORD' && AGGREGATE_FUNCS.includes(t.value) && tokens[pos + 1]?.type === '(') {
      const func = advance().value;
      expect('(');
      let distinct = false;
      if (isKeyword('DISTINCT')) { distinct = true; advance(); }
      let arg;
      if (peek().type === '*') { advance(); arg = '*'; } else { arg = parseExpr(); }
      expect(')');
      return { type: 'aggregate_expr', func, arg, distinct };
    }

    // Window functions in expressions: ROW_NUMBER(), RANK(), DENSE_RANK(), LAG(), LEAD(), etc.
    if (t.type === 'KEYWORD' && ZERO_ARG_WINDOW_FUNCS.includes(t.value) && tokens[pos + 1]?.type === '(') {
      return parseWindowCall();
    }
    if (t.type === 'KEYWORD' && ARG_WINDOW_FUNCS.includes(t.value) && tokens[pos + 1]?.type === '(') {
      return parseWindowCall();
    }
    // Aggregate OVER (...) — window aggregate in expression context
    if (t.type === 'KEYWORD' && AGGREGATE_FUNCS.includes(t.value) && tokens[pos + 1]?.type === '(') {
      // Peek ahead to see if there's an OVER after the closing paren
      let lookahead = pos + 2;
      let depth = 1;
      while (lookahead < tokens.length && depth > 0) {
        if (tokens[lookahead].type === '(') depth++;
        else if (tokens[lookahead].type === ')') depth--;
        lookahead++;
      }
      if (lookahead < tokens.length && tokens[lookahead]?.type === 'KEYWORD' && tokens[lookahead]?.value === 'OVER') {
        const func = advance().value;
        expect('(');
        let distinct = false;
        if (isKeyword('DISTINCT')) { distinct = true; advance(); }
        let arg;
        if (peek().type === '*') { advance(); arg = '*'; } else { arg = parseExpr(); }
        expect(')');
        const over = parseOverClause();
        return { type: 'window', func, arg, distinct, over };
      }
    }

    if (t.type === 'IDENT') {
      // Check if this is a function call: IDENT followed by (
      if (tokens[pos + 1]?.type === '(') {
        const func = advance().value;
        expect('(');
        const args = [];
        if (!match(')')) {
          args.push(parseExpr());
          while (match(',')) args.push(parseExpr());
          expect(')');
        }
        return { type: 'function_call', func: func.toUpperCase(), args };
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

  // Shared helper: parse a window function call (ROW_NUMBER, RANK, LAG, LEAD, etc.)
  // Assumes we've already confirmed peek() is a window function keyword.
  // Returns a {type:'window', func, arg, over, ...} node.
  
  function parseWindowCall() {
    const func = advance().value;
    expect('(');
    let arg = null, offset = null, defaultValue = null;
    if (ZERO_ARG_WINDOW_FUNCS.includes(func)) {
      expect(')');
    } else {
      if (!match(')')) {
        arg = parseExpr();
        if (match(',')) { 
          const offsetExpr = parseExpr();
          offset = offsetExpr.type === 'literal' ? offsetExpr.value : offsetExpr;
        }
        if (match(',')) { 
          const defaultExpr = parseExpr();
          defaultValue = defaultExpr.type === 'literal' ? defaultExpr.value : defaultExpr;
        }
        expect(')');
      }
    }
    const over = parseOverClause();
    return { type: 'window', func, arg, offset, defaultValue, over };
  }

  // Shared helper: parse trailing arithmetic after a node (window func, aggregate, etc.)
  // Returns null if no trailing arithmetic, otherwise wraps the node in arith expressions.
  function parseTrailingArithmetic(node) {
    if (!peek() || !['PLUS', 'MINUS', '*', 'SLASH', 'MOD'].includes(peek().type)) return node;
    let result = node;
    while (peek() && ['PLUS', 'MINUS', '*', 'SLASH', 'MOD'].includes(peek().type)) {
      const opType = peek().type;
      const op = opType === 'PLUS' ? '+' : opType === 'MINUS' ? '-' : opType === '*' ? '*' : opType === 'SLASH' ? '/' : '%';
      advance();
      const right = parsePrimary();
      result = { type: 'arith', op, left: result, right };
    }
    return result;
  }

  function parseOverClause() {
    expect('KEYWORD', 'OVER');
    // OVER w (named window reference) or OVER (...)
    if (peek() && peek().type !== '(') {
      // Named window reference
      const windowName = advance().value;
      return { windowRef: windowName };
    }
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
    // Optional frame clause: ROWS|RANGE|GROUPS BETWEEN ... AND ...
    if (isKeyword('ROWS') || isKeyword('RANGE') || isKeyword('GROUPS')) {
      const frameType = advance().value; // ROWS, RANGE, or GROUPS
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
      // Optional EXCLUDE clause
      if (isKeyword('EXCLUDE')) {
        advance(); // EXCLUDE
        if (isKeyword('CURRENT')) {
          advance(); // CURRENT
          expect('KEYWORD', 'ROW');
          frame.exclude = 'CURRENT ROW';
        } else if (isKeyword('GROUP')) {
          advance();
          frame.exclude = 'GROUP';
        } else if (isKeyword('TIES')) {
          advance();
          frame.exclude = 'TIES';
        } else if (isKeyword('NO')) {
          advance(); // NO
          expect('KEYWORD', 'OTHERS');
          frame.exclude = 'NO OTHERS';
        } else {
          throw new Error('Expected CURRENT ROW, GROUP, TIES, or NO OTHERS after EXCLUDE');
        }
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

  function parseReplace() {
    advance(); // REPLACE
    // REPLACE INTO is equivalent to INSERT OR REPLACE INTO
    // Don't call expect for INTO — parseInsert will handle it
    // We'll manually set up the AST
    expect('KEYWORD', 'INTO');
    const tableTok = advance();
    const table = tableTok.originalValue || tableTok.value;

    let columns = null;
    if (match('(')) {
      columns = [];
      do { const tok = advance(); columns.push(tok.originalValue || tok.value); } while (match(','));
      expect(')');
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

    let returning = null;
    if (isKeyword('RETURNING')) {
      returning = parseReturningClause();
    }

    return { type: 'INSERT', table, columns, rows, onConflict: null, returning, conflictAction: 'REPLACE' };
  }

  function parseInsert() {
    advance(); // INSERT
    // Handle INSERT OR REPLACE/IGNORE/ABORT/ROLLBACK/FAIL
    let conflictAction = null;
    if (isKeyword('OR')) {
      advance(); // OR
      const actionTok = advance();
      const action = (actionTok.originalValue || actionTok.value).toUpperCase();
      if (['REPLACE', 'IGNORE', 'ABORT', 'ROLLBACK', 'FAIL'].includes(action)) {
        conflictAction = action;
      } else {
        throw new Error(`Expected REPLACE, IGNORE, ABORT, ROLLBACK, or FAIL after INSERT OR, got ${action}`);
      }
    }
    expect('KEYWORD', 'INTO');
    const tableTok = advance();
    const table = tableTok.originalValue || tableTok.value;

    let columns = null;
    if (match('(')) {
      columns = [];
      do { const tok = advance(); columns.push(tok.originalValue || tok.value); } while (match(','));
      expect(')');
    }

    // INSERT INTO ... SELECT
    if (isKeyword('SELECT') || isKeyword('WITH')) {
      const selectStmt = isKeyword('WITH') ? parseWith() : parseSelect();
      let returning = null;
      if (isKeyword('RETURNING')) {
        returning = parseReturningClause();
      }
      return { type: 'INSERT_SELECT', table, columns, query: selectStmt, returning };
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

    return { type: 'INSERT', table, columns, rows, onConflict, returning, conflictAction };
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
      let notNull = false;
      // Parse column constraints: NOT NULL, DEFAULT, UNIQUE, PRIMARY KEY
      while (pos < tokens.length) {
        if (isKeyword('NOT') && tokens[pos + 1]?.value === 'NULL') {
          advance(); advance(); notNull = true;
        } else if (isKeyword('NULL')) {
          advance(); // explicit NULL (nullable)
        } else if (isKeyword('DEFAULT')) {
          advance();
          const defExpr = parseExpr();
          defaultVal = defExpr.type === 'literal' ? defExpr.value : defExpr;
        } else if (isKeyword('UNIQUE') || isKeyword('PRIMARY')) {
          advance(); // skip constraint keywords
          if (isKeyword('KEY')) advance();
        } else {
          break;
        }
      }
      return { type: 'ALTER_TABLE', table, action: 'ADD_COLUMN', column: colName, dataType: colType, defaultValue: defaultVal, notNull };
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
    if (isKeyword('EXTENSION')) {
      advance(); // EXTENSION
      let ifNotExists = false;
      if (isKeyword('IF')) { advance(); expect('KEYWORD', 'NOT'); expect('KEYWORD', 'EXISTS'); ifNotExists = true; }
      const name = advance().originalValue || advance().value;
      // Skip optional WITH SCHEMA, VERSION, CASCADE etc.
      while (peek() && peek().type !== ';' && peek().type !== 'EOF') advance();
      return { type: 'CREATE_EXTENSION', name: name || 'unknown', ifNotExists };
    }
    if (isKeyword('SCHEMA')) {
      advance(); // SCHEMA
      let ifNotExists = false;
      if (isKeyword('IF')) { advance(); expect('KEYWORD', 'NOT'); expect('KEYWORD', 'EXISTS'); ifNotExists = true; }
      const name = advance().originalValue || advance().value;
      // Skip optional AUTHORIZATION etc.
      while (peek() && peek().type !== ';' && peek().type !== 'EOF') advance();
      return { type: 'CREATE_SCHEMA', name: name || 'public', ifNotExists };
    }
    if (isKeyword('FUNCTION') || isKeyword('PROCEDURE')) {
      return parseCreateFunction(orReplace);
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
    if (isKeyword('SEQUENCE')) {
      advance(); // SEQUENCE
      const name = advance().value;
      let start = 1, increment = 1, minValue = 1, maxValue = Infinity;
      while (peek().type === 'KEYWORD' || peek().type === 'IDENT') {
        if (isKeyword('START')) { advance(); if (isKeyword('WITH')) advance(); start = Number(advance().value); }
        else if (isKeyword('INCREMENT')) { advance(); if (isKeyword('BY')) advance(); increment = Number(advance().value); }
        else if (isKeyword('MINVALUE')) { advance(); minValue = Number(advance().value); }
        else if (isKeyword('MAXVALUE')) { advance(); maxValue = Number(advance().value); }
        else break;
      }
      return { type: 'CREATE_SEQUENCE', name, start, increment, minValue, maxValue };
    }
    if (isKeyword('MATERIALIZED')) {
      advance(); // MATERIALIZED
      expect('KEYWORD', 'VIEW');
      const name = advance().value;
      expect('KEYWORD', 'AS');
      const query = parseSelect();
      return { type: 'CREATE_MATVIEW', name, query };
    }
    let temporary = false;
    if (isKeyword('TEMPORARY') || isKeyword('TEMP')) { advance(); temporary = true; }
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
        const tableConstraints = [];
    do {
      // Check for table-level constraints (PRIMARY KEY, UNIQUE, FOREIGN KEY, CHECK)
      if (isKeyword('PRIMARY')) {
        advance(); // PRIMARY
        expect('KEYWORD', 'KEY');
        expect('(');
        const pkCols = [];
        do {
          const col = advance();
          pkCols.push(col.originalValue || col.value);
        } while (match(','));
        expect(')');
        tableConstraints.push({ type: 'PRIMARY_KEY', columns: pkCols });
        continue;
      }
      if (isKeyword('UNIQUE')) {
        advance(); // UNIQUE
        if (tokens[pos] && tokens[pos].type === '(') {
          expect('(');
          const uqCols = [];
          do {
            const col = advance();
            uqCols.push(col.originalValue || col.value);
          } while (match(','));
          expect(')');
          tableConstraints.push({ type: 'UNIQUE', columns: uqCols });
          continue;
        }
        // Otherwise it's a column-level unique that got here oddly
      }
      if (isKeyword('FOREIGN')) {
        advance(); // FOREIGN
        expect('KEYWORD', 'KEY');
        expect('(');
        const fkCols = [];
        do {
          const col = advance();
          fkCols.push(col.originalValue || col.value);
        } while (match(','));
        expect(')');
        expect('KEYWORD', 'REFERENCES');
        const refTable = advance().value;
        expect('(');
        const refCols = [];
        do {
          const col = advance();
          refCols.push(col.originalValue || col.value);
        } while (match(','));
        expect(')');
        let onDelete = null, onUpdate = null;
        while (isKeyword('ON')) {
          advance();
          if (isKeyword('DELETE')) { advance(); onDelete = advance().value; if (isKeyword('NULL')) { onDelete += ' NULL'; advance(); } }
          else if (isKeyword('UPDATE')) { advance(); onUpdate = advance().value; if (isKeyword('NULL')) { onUpdate += ' NULL'; advance(); } }
        }
        tableConstraints.push({ type: 'FOREIGN_KEY', columns: fkCols, refTable, refColumns: refCols, onDelete, onUpdate });
        continue;
      }
      
      // Table-level CHECK constraint: CHECK (expr)
      if (isKeyword('CHECK')) {
        advance(); // CHECK
        expect('(');
        const checkExpr = parseExpr();
        expect(')');
        tableConstraints.push({ type: 'CHECK', expr: checkExpr });
        continue;
      }
      
      // Named constraint: CONSTRAINT name ...
      if (isKeyword('CONSTRAINT')) {
        advance(); // CONSTRAINT
        const constraintName = advance().value; // constraint name
        // The actual constraint follows (CHECK, UNIQUE, etc.)
        if (isKeyword('CHECK')) {
          advance();
          expect('(');
          const checkExpr = parseExpr();
          expect(')');
          tableConstraints.push({ type: 'CHECK', name: constraintName, expr: checkExpr });
          continue;
        }
        // Could be other named constraints...
      }

      const tok = advance();
      const name = tok.originalValue || tok.value;
      let dataType = advance().value;
      let isSerial = false;
      if (dataType && (dataType.toUpperCase() === 'SERIAL' || dataType.toUpperCase() === 'BIGSERIAL')) {
        isSerial = true;
        dataType = dataType.toUpperCase() === 'BIGSERIAL' ? 'BIGINT' : 'INT';
      }
      // Consume optional type length spec: VARCHAR(100), CHAR(10), NUMERIC(10,2)
      if (peek().type === '(') {
        advance(); // (
        while (peek().type !== ')') advance(); // consume length/precision
        advance(); // )
      }
      let primaryKey = false;
      let notNull = false;
      let unique = false;
      let check = null;
      let defaultVal = null;
      let references = null;
      // Parse column constraints
      while (true) {
        if (isKeyword('PRIMARY')) { advance(); expect('KEYWORD', 'KEY'); primaryKey = true; }
        else if (peek().type === 'IDENT' && peek().value.toUpperCase() === 'AUTOINCREMENT') { advance(); isSerial = true; }
        else if (peek().type === 'IDENT' && peek().value.toUpperCase() === 'AUTO_INCREMENT') { advance(); isSerial = true; }
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
      columns.push({ name, type: dataType, primaryKey, notNull: notNull || isSerial, unique, check, defaultValue: defaultVal, references, generated, serial: isSerial });
    } while (match(','));
    expect(')');
    
    // Apply table-level constraints to columns
    for (const tc of tableConstraints) {
      if (tc.type === 'PRIMARY_KEY') {
        for (const colName of tc.columns) {
          const col = columns.find(c => c.name.toLowerCase() === colName.toLowerCase());
          if (col) { col.notNull = true; }
        }
        if (tc.columns.length === 1) {
          const col = columns.find(c => c.name.toLowerCase() === tc.columns[0].toLowerCase());
          if (col) col.primaryKey = true;
        } else {
          // Multi-column PK: don't mark individual columns as PK to avoid
          // per-column uniqueness checks. Create composite unique index instead.
          if (!tableConstraints._compositeUniques) tableConstraints._compositeUniques = [];
          tableConstraints._compositeUniques.push(tc.columns);
        }
      } else if (tc.type === 'UNIQUE') {
        if (tc.columns.length === 1) {
          // Single-column UNIQUE: mark on column
          const col = columns.find(c => c.name.toLowerCase() === tc.columns[0].toLowerCase());
          if (col) col.unique = true;
        } else {
          // Multi-column UNIQUE: store as composite constraint
          // Will be handled as a composite unique index at CREATE TABLE time
          if (!tableConstraints._compositeUniques) tableConstraints._compositeUniques = [];
          tableConstraints._compositeUniques.push(tc.columns);
        }
      } else if (tc.type === 'FOREIGN_KEY') {
        for (let i = 0; i < tc.columns.length; i++) {
          const col = columns.find(c => c.name.toLowerCase() === tc.columns[i].toLowerCase());
          if (col) col.references = { table: tc.refTable, column: tc.refColumns[i], onDelete: tc.onDelete, onUpdate: tc.onUpdate };
        }
      }
    }
    
    // Optional: USING BTREE | USING HEAP (default: HEAP)
    let engine = null;
    if (isKeyword('USING')) {
      advance(); // USING
      const engineTok = advance();
      engine = (engineTok.originalValue || engineTok.value).toUpperCase();
    }
    return { type: 'CREATE_TABLE', table, columns, ifNotExists, engine, temporary, compositeUniques: tableConstraints._compositeUniques || [], tableConstraints: tableConstraints.filter(c => c.type === 'CHECK') };
  }

  function parseCreateIndex(unique) {
    advance(); // INDEX
    let concurrently = false;
    if (isKeyword('CONCURRENTLY')) {
      advance();
      concurrently = true;
    }
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
    do {
      // Check if this is an expression (function call like LOWER(name)) or a simple column
      if (peek().type === 'KEYWORD' || peek().type === 'IDENT') {
        const saved = pos;
        const name = advance();
        if (peek().type === '(') {
          // It's a function call — backtrack and parse as expression
          pos = saved;
          const exprStart = pos;
          const expr = parseExpr();
          // Build text from consumed tokens
          const exprText = tokens.slice(exprStart, pos).map(t => t.originalValue || t.value).join(' ').replace(/ \( /g, '(').replace(/ \) /g, ')').replace(/ ,/g, ',');
          columns.push({ expression: expr, text: exprText });
        } else {
          // Simple column name
          columns.push(name.originalValue || name.value);
        }
      } else {
        // Fallback: parse as expression
        const exprStart = pos;
        const expr = parseExpr();
        const exprText = tokens.slice(exprStart, pos).map(t => t.originalValue || t.value).join(' ');
        columns.push({ expression: expr, text: exprText });
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
    return { type: 'CREATE_INDEX', name, table, columns, unique, include, where, ifNotExists, indexType, concurrently };
  }

  function parseCreateView(orReplace = false) {
    advance(); // VIEW
    const name = advance().value;
    if (isKeyword('AS')) advance(); // optional AS
    const query = isKeyword('WITH') ? parseWith() : parseSelect();
    return { type: 'CREATE_VIEW', name, query, orReplace };
  }

  function parseCreateFunction(orReplace = false) {
    const kind = peek().value; // FUNCTION or PROCEDURE
    advance();
    const name = advance().value;
    
    // Parse parameters: (param_name type, ...)
    const params = [];
    expect('(');
    while (peek().type !== ')') {
      const paramName = advance().value;
      const paramType = advance().value;
      params.push({ name: paramName.toLowerCase(), type: paramType.toUpperCase() });
      if (peek().type === ',') advance();
    }
    expect(')');
    
    // RETURNS type or RETURNS TABLE(col type, ...) (optional for PROCEDURE)
    let returnType = null;
    let returnColumns = null;
    if (isKeyword('RETURNS')) {
      advance();
      if (isKeyword('TABLE')) {
        advance();
        returnType = 'TABLE';
        returnColumns = [];
        expect('(');
        while (peek().type !== ')') {
          const colName = advance().value;
          const colType = advance().value;
          returnColumns.push({ name: colName.toLowerCase(), type: colType.toUpperCase() });
          if (peek().type === ',') advance();
        }
        expect(')');
      } else {
        returnType = advance().value.toUpperCase();
      }
    }
    
    // Optional: LANGUAGE js|sql
    let language = 'sql'; // default
    if (isKeyword('LANGUAGE')) {
      advance();
      language = advance().value.toLowerCase();
    }
    
    // Optional: IMMUTABLE | VOLATILE | STABLE
    let volatility = 'volatile';
    if (isKeyword('IMMUTABLE')) { volatility = 'immutable'; advance(); }
    else if (isKeyword('VOLATILE')) { volatility = 'volatile'; advance(); }
    else if (isKeyword('STABLE')) { volatility = 'stable'; advance(); }
    
    // AS $$ body $$ or AS 'body'
    expect('KEYWORD', 'AS');
    let body;
    if (peek().type === 'DOLLAR_STRING') {
      body = advance().value;
    } else if (peek().type === 'STRING') {
      body = advance().value;
    } else {
      throw new Error('Expected function body as dollar-quoted or single-quoted string');
    }
    
    // Optional trailing LANGUAGE (PostgreSQL allows before or after body)
    if (isKeyword('LANGUAGE')) {
      advance();
      language = advance().value.toLowerCase();
    }
    
    // Optional trailing volatility
    if (isKeyword('IMMUTABLE')) { volatility = 'immutable'; advance(); }
    else if (isKeyword('VOLATILE')) { volatility = 'volatile'; advance(); }
    else if (isKeyword('STABLE')) { volatility = 'stable'; advance(); }

    return {
      type: 'CREATE_FUNCTION',
      name: name.toLowerCase(),
      params,
      returnType,
      returnColumns,
      language,
      volatility,
      body,
      orReplace,
      isProcedure: kind === 'PROCEDURE',
    };
  }

  function parseDrop() {
    advance(); // DROP
    if (isKeyword('EXTENSION')) {
      advance(); // EXTENSION
      let ifExists = false;
      if (isKeyword('IF')) { advance(); expect('KEYWORD', 'EXISTS'); ifExists = true; }
      const name = advance().originalValue || advance().value;
      // Skip CASCADE/RESTRICT
      while (peek() && peek().type !== ';' && peek().type !== 'EOF') advance();
      return { type: 'DROP_EXTENSION', name: name || 'unknown', ifExists };
    }
    if (isKeyword('SCHEMA')) {
      advance(); // SCHEMA
      let ifExists = false;
      if (isKeyword('IF')) { advance(); expect('KEYWORD', 'EXISTS'); ifExists = true; }
      const name = advance().originalValue || advance().value;
      while (peek() && peek().type !== ';' && peek().type !== 'EOF') advance();
      return { type: 'DROP_SCHEMA', name: name || 'public', ifExists };
    }
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
      let ifExists = false;
      if (isKeyword('IF')) { advance(); expect('KEYWORD', 'EXISTS'); ifExists = true; }
      const name = advance().value;
      return { type: 'DROP_VIEW', name, ifExists };
    }
    if (isKeyword('FUNCTION') || isKeyword('PROCEDURE')) {
      advance();
      let ifExists = false;
      if (isKeyword('IF')) { advance(); expect('KEYWORD', 'EXISTS'); ifExists = true; }
      const name = advance().value;
      return { type: 'DROP_FUNCTION', name: name.toLowerCase(), ifExists };
    }
    expect('KEYWORD', 'TABLE');
    let ifExists = false;
    if (isKeyword('IF')) { advance(); expect('KEYWORD', 'EXISTS'); ifExists = true; }
    const _dropTok = advance();
    const table = _dropTok.originalValue || _dropTok.value;
    let cascade = false;
    let restrict = false;
    if (isKeyword('CASCADE')) { advance(); cascade = true; }
    else if (isKeyword('RESTRICT')) { advance(); restrict = true; }
    return { type: 'DROP_TABLE', table, ifExists, cascade, restrict };
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
      let notNull = false;
      // Parse column constraints: NOT NULL, DEFAULT, UNIQUE, PRIMARY KEY
      while (pos < tokens.length) {
        if (isKeyword('NOT') && tokens[pos + 1]?.value === 'NULL') {
          advance(); advance(); notNull = true;
        } else if (isKeyword('NULL')) {
          advance();
        } else if (isKeyword('DEFAULT')) {
          advance();
          defaultValue = parseExpr();
        } else if (isKeyword('UNIQUE') || isKeyword('PRIMARY')) {
          advance();
          if (isKeyword('KEY')) advance();
        } else {
          break;
        }
      }
      return { type: 'ALTER_TABLE', table, action: 'ADD_COLUMN', column: { name, type: dataType, default: defaultValue, notNull } };
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
