// plsql.js — PL/HenryDB procedural language interpreter
// Implements a PL/pgSQL-like language for stored procedures and functions.
// Supports: DECLARE, IF/ELSIF/ELSE, WHILE, FOR, RETURN, RAISE, variable scoping.

/**
 * PLParser — parses PL/HenryDB procedure body into an AST.
 * 
 * Grammar:
 *   block        := DECLARE? declarations BEGIN statements END
 *   declaration  := name type [:= expression] ;
 *   statement    := assignment | if | while | for | return | raise | execute | null
 *   assignment   := name := expression ;
 *   if           := IF condition THEN statements [ELSIF condition THEN statements]* [ELSE statements] END IF ;
 *   while        := WHILE condition LOOP statements END LOOP ;
 *   for          := FOR name IN [REVERSE] expr..expr LOOP statements END LOOP ;
 *   return       := RETURN [expression] ;
 *   raise        := RAISE level 'format' [, args] ;
 *   execute      := EXECUTE sql_string ;
 *   null         := NULL ;
 */
export class PLParser {
  constructor(source) {
    this.source = source;
    this.tokens = this._tokenize(source);
    this.pos = 0;
  }

  parse() {
    return this._parseBlock();
  }

  _parseBlock() {
    const declarations = [];
    const statements = [];

    // Optional DECLARE section
    if (this._peek() === 'DECLARE') {
      this._advance(); // skip DECLARE
      while (this._peek() !== 'BEGIN' && this._peek() !== null) {
        declarations.push(this._parseDeclaration());
      }
    }

    this._expect('BEGIN');

    while (this._peek() !== 'END' && this._peek() !== null) {
      // Check for EXCEPTION block
      if (this._peek() === 'EXCEPTION') {
        break;
      }
      statements.push(this._parseStatement());
    }

    // Optional EXCEPTION handler
    let exceptionHandlers = null;
    if (this._peek() === 'EXCEPTION') {
      exceptionHandlers = this._parseExceptionBlock();
    }

    this._expect('END');
    // Consume optional trailing semicolon
    if (this._peek() === ';') this._advance();

    return { type: 'block', declarations, statements, exceptionHandlers };
  }

  _parseDeclaration() {
    const name = this._advance(); // variable name
    const dataType = this._advance(); // type
    let defaultValue = null;
    let cursorQuery = null;

    // Check for CURSOR FOR <query>
    if (dataType === 'CURSOR' && this._peek() === 'FOR') {
      this._advance(); // skip FOR
      const queryParts = [];
      while (this._peek() !== ';' && this._peek() !== null) {
        queryParts.push(this._advance());
      }
      this._expect(';');
      return { type: 'declaration', name, dataType: 'CURSOR', cursorQuery: queryParts.join(' ') };
    }

    if (this._peek() === ':=') {
      this._advance(); // skip :=
      defaultValue = this._parseExpression();
    }

    this._expect(';');
    return { type: 'declaration', name, dataType, defaultValue };
  }

  _parseStatement() {
    const token = this._peek();

    if (token === 'IF') return this._parseIf();
    if (token === 'WHILE') return this._parseWhile();
    if (token === 'FOR') return this._parseFor();
    if (token === 'CASE') return this._parseCase();
    if (token === 'RETURN') return this._parseReturn();
    if (token === 'RAISE') return this._parseRaise();
    if (token === 'EXECUTE') return this._parseExecute();
    if (token === 'PERFORM') return this._parsePerform();
    if (token === 'OPEN') return this._parseOpen();
    if (token === 'FETCH') return this._parseFetch();
    if (token === 'CLOSE') return this._parseClose();
    if (token === 'EXIT') return this._parseExit();
    if (token === 'LOOP') return this._parseLoop();
    if (token === 'NULL') { this._advance(); this._expect(';'); return { type: 'null_stmt' }; }

    // Check for SELECT ... INTO
    if (token === 'SELECT') return this._parseSelectInto();

    // DML statements: pass directly to DB engine
    if (token === 'INSERT' || token === 'UPDATE' || token === 'DELETE') return this._parseDML();

    // Assignment: name := expr;
    return this._parseAssignment();
  }

  _parseIf() {
    this._expect('IF');
    const condition = this._parseCondition();
    this._expect('THEN');

    const thenStatements = [];
    while (!['ELSIF', 'ELSE', 'END'].includes(this._peek())) {
      thenStatements.push(this._parseStatement());
    }

    const elsifClauses = [];
    while (this._peek() === 'ELSIF') {
      this._advance();
      const elsifCondition = this._parseCondition();
      this._expect('THEN');
      const elsifStatements = [];
      while (!['ELSIF', 'ELSE', 'END'].includes(this._peek())) {
        elsifStatements.push(this._parseStatement());
      }
      elsifClauses.push({ condition: elsifCondition, statements: elsifStatements });
    }

    let elseStatements = null;
    if (this._peek() === 'ELSE') {
      this._advance();
      elseStatements = [];
      while (this._peek() !== 'END') {
        elseStatements.push(this._parseStatement());
      }
    }

    this._expect('END');
    this._expect('IF');
    this._expect(';');

    return { type: 'if', condition, thenStatements, elsifClauses, elseStatements };
  }

  _parseWhile() {
    this._expect('WHILE');
    const condition = this._parseCondition();
    this._expect('LOOP');

    const statements = [];
    while (this._peek() !== 'END') {
      statements.push(this._parseStatement());
    }

    this._expect('END');
    this._expect('LOOP');
    this._expect(';');

    return { type: 'while', condition, statements };
  }

  _parseCase() {
    this._advance(); // consume CASE
    
    // Determine if simple CASE (CASE expr WHEN ...) or searched CASE (CASE WHEN ...)
    let subject = null;
    if (this._peek() !== 'WHEN') {
      // Simple CASE: parse the subject expression
      subject = this._parseExpression();
    }
    
    const branches = [];
    while (this._peek() === 'WHEN') {
      this._advance(); // consume WHEN
      const condition = this._parseExpression();
      this._expect('THEN');
      const statements = [];
      while (!['WHEN', 'ELSE', 'END'].includes(this._peek())) {
        statements.push(this._parseStatement());
      }
      branches.push({ condition, statements });
    }
    
    let elseStatements = null;
    if (this._peek() === 'ELSE') {
      this._advance(); // consume ELSE
      elseStatements = [];
      while (this._peek() !== 'END') {
        elseStatements.push(this._parseStatement());
      }
    }
    
    this._expect('END');
    this._expect('CASE');
    this._expect(';');
    
    return { type: 'case', subject, branches, elseStatements };
  }

  _parseOpen() {
    this._advance(); // OPEN
    const cursorName = this._advance();
    this._expect(';');
    return { type: 'open_cursor', cursor: cursorName };
  }

  _parseFetch() {
    this._advance(); // FETCH
    const cursorName = this._advance();
    this._expect('INTO');
    const targetName = this._advance();
    this._expect(';');
    return { type: 'fetch_cursor', cursor: cursorName, target: targetName };
  }

  _parseClose() {
    this._advance(); // CLOSE
    const cursorName = this._advance();
    this._expect(';');
    return { type: 'close_cursor', cursor: cursorName };
  }

  _parseExit() {
    this._advance(); // EXIT
    if (this._peek() === 'WHEN') {
      this._advance(); // WHEN
      const condParts = [];
      while (this._peek() !== ';' && this._peek() !== null) {
        condParts.push(this._advance());
      }
      this._expect(';');
      return { type: 'exit_when', condition: condParts.join(' ') };
    }
    this._expect(';');
    return { type: 'exit' };
  }

  _parseLoop() {
    this._advance(); // LOOP
    const statements = [];
    while (this._peek() !== 'END' && this._peek() !== null) {
      statements.push(this._parseStatement());
    }
    this._expect('END');
    this._expect('LOOP');
    this._expect(';');
    return { type: 'loop', statements };
  }

  _parseFor() {
    this._expect('FOR');
    const varName = this._advance();
    this._expect('IN');

    // Check for query FOR loop: FOR rec IN SELECT ... LOOP
    if (this._peek() === 'SELECT') {
      const queryParts = [];
      while (this._peek() !== 'LOOP' && this._peek() !== null) {
        queryParts.push(this._advance());
      }
      this._expect('LOOP');
      const statements = [];
      while (this._peek() !== 'END') {
        statements.push(this._parseStatement());
      }
      this._expect('END');
      this._expect('LOOP');
      this._expect(';');
      return { type: 'for_query', varName, query: queryParts.join(' '), statements };
    }

    // Range FOR loop: FOR i IN [REVERSE] start..end LOOP
    let reverse = false;
    if (this._peek() === 'REVERSE') {
      this._advance();
      reverse = true;
    }

    const start = this._parseExpression();
    this._expect('..');
    const end = this._parseExpression();

    this._expect('LOOP');

    const statements = [];
    while (this._peek() !== 'END') {
      statements.push(this._parseStatement());
    }

    this._expect('END');
    this._expect('LOOP');
    this._expect(';');

    return { type: 'for', varName, start, end, reverse, statements };
  }

  _parseReturn() {
    this._expect('RETURN');
    let value = null;
    if (this._peek() !== ';') {
      value = this._parseExpression();
    }
    this._expect(';');
    return { type: 'return', value };
  }

  _parseRaise() {
    this._expect('RAISE');
    const level = this._advance(); // NOTICE, WARNING, EXCEPTION, INFO, DEBUG
    let format = null;
    const args = [];

    if (this._peek() && this._peek().startsWith("'")) {
      format = this._parseStringLiteral();
      while (this._peek() === ',') {
        this._advance(); // skip comma
        args.push(this._parseExpression());
      }
    }

    this._expect(';');
    return { type: 'raise', level, format, args };
  }

  _parseExecute() {
    this._expect('EXECUTE');
    const sqlExpr = this._parseExpression();
    let into = null;
    if (this._peek() === 'INTO') {
      this._advance();
      into = this._advance();
    }
    this._expect(';');
    return { type: 'execute', sqlExpr, into };
  }

  _parsePerform() {
    this._expect('PERFORM');
    // Collect everything until semicolon as SQL
    const parts = [];
    while (this._peek() !== ';' && this._peek() !== null) {
      parts.push(this._advance());
    }
    this._expect(';');
    return { type: 'perform', sql: parts.join(' ') };
  }

  _parseDML() {
    // Collect all tokens until ; to form the SQL statement
    // Variable interpolation: $1, $name, etc. will be handled by the interpreter
    const parts = [];
    while (this._peek() !== ';' && this._peek() !== null) {
      parts.push(this._advance());
    }
    this._expect(';');
    return { type: 'dml', sql: parts.join(' ') };
  }

  _parseSelectInto() {
    // SELECT expr INTO var FROM ...;
    const parts = [];
    let intoVar = null;
    let foundInto = false;

    while (this._peek() !== ';' && this._peek() !== null) {
      const token = this._peek();
      if (token === 'INTO' && !foundInto) {
        this._advance();
        intoVar = this._advance();
        foundInto = true;
        continue;
      }
      parts.push(this._advance());
    }
    this._expect(';');

    return { type: 'select_into', sql: parts.join(' '), intoVar };
  }

  _parseAssignment() {
    const name = this._advance();
    this._expect(':=');
    const value = this._parseExpression();
    this._expect(';');
    return { type: 'assignment', name, value };
  }

  _parseCondition() {
    // Parse until THEN, LOOP, or similar keywords
    const parts = [];
    while (!['THEN', 'LOOP'].includes(this._peek()) && this._peek() !== null) {
      parts.push(this._advance());
    }
    return { type: 'condition', expr: parts.join(' ') };
  }

  _parseExpression() {
    // Simple expression parser: collect tokens until ; , THEN LOOP INTO END ELSIF ELSE ..
    const stopTokens = [';', ',', 'THEN', 'LOOP', 'INTO', 'END', 'ELSIF', 'ELSE', '..', 'WHEN'];
    const parts = [];
    let parenDepth = 0;

    while (this._peek() !== null) {
      if (parenDepth === 0 && stopTokens.includes(this._peek())) break;
      const token = this._advance();
      if (token === '(') parenDepth++;
      if (token === ')') parenDepth--;
      parts.push(token);
    }

    const expr = parts.join(' ');
    // Try to parse as a number
    if (/^-?\d+(\.\d+)?$/.test(expr)) return { type: 'literal', value: parseFloat(expr) };
    if (expr.startsWith("'") && expr.endsWith("'")) return { type: 'literal', value: expr.slice(1, -1) };
    if (expr === 'NULL' || expr === 'null') return { type: 'literal', value: null };
    if (expr === 'TRUE' || expr === 'true') return { type: 'literal', value: true };
    if (expr === 'FALSE' || expr === 'false') return { type: 'literal', value: false };
    return { type: 'expression', expr };
  }

  _parseStringLiteral() {
    let token = this._advance();
    // Handle quoted strings: 'hello world'
    if (token.startsWith("'")) {
      return token.slice(1, -1);
    }
    return token;
  }

  _parseExceptionBlock() {
    this._expect('EXCEPTION');
    const handlers = [];
    while (this._peek() === 'WHEN') {
      this._advance(); // WHEN
      const condParts = [];
      while (this._peek() !== 'THEN') {
        condParts.push(this._advance());
      }
      this._expect('THEN');
      const statements = [];
      while (!['WHEN', 'END'].includes(this._peek())) {
        statements.push(this._parseStatement());
      }
      handlers.push({ condition: condParts.join(' '), statements });
    }
    return handlers;
  }

  _peek() {
    return this.pos < this.tokens.length ? this.tokens[this.pos] : null;
  }

  _advance() {
    return this.tokens[this.pos++];
  }

  _expect(expected) {
    const token = this._advance();
    if (token !== expected) {
      throw new Error(`PL/HenryDB: expected '${expected}', got '${token}' at position ${this.pos - 1}`);
    }
    return token;
  }

  _tokenize(source) {
    const tokens = [];
    let i = 0;
    const src = source.trim();

    while (i < src.length) {
      // Skip whitespace
      if (/\s/.test(src[i])) { i++; continue; }

      // Single-line comment
      if (src[i] === '-' && src[i + 1] === '-') {
        while (i < src.length && src[i] !== '\n') i++;
        continue;
      }

      // Multi-char operators: :=, ..
      if (src[i] === ':' && src[i + 1] === '=') {
        tokens.push(':=');
        i += 2;
        continue;
      }
      if (src[i] === '.' && src[i + 1] === '.') {
        tokens.push('..');
        i += 2;
        continue;
      }

      // String literal
      if (src[i] === "'") {
        let str = "'";
        i++;
        while (i < src.length && src[i] !== "'") {
          if (src[i] === "'" && src[i + 1] === "'") {
            str += "'";
            i += 2;
          } else {
            str += src[i++];
          }
        }
        str += "'";
        i++; // skip closing quote
        tokens.push(str);
        continue;
      }

      // Dollar-quoted string
      if (src[i] === '$') {
        let tag = '$';
        i++;
        while (i < src.length && src[i] !== '$') tag += src[i++];
        tag += '$';
        i++;
        let body = '';
        const endTag = tag;
        while (i < src.length) {
          if (src.substring(i, i + endTag.length) === endTag) {
            i += endTag.length;
            break;
          }
          body += src[i++];
        }
        tokens.push("'" + body + "'");
        continue;
      }

      // Punctuation
      if ('();,.'.includes(src[i])) {
        tokens.push(src[i++]);
        continue;
      }

      // Concat operator ||
      if (src[i] === '|' && src[i + 1] === '|') {
        tokens.push('||');
        i += 2;
        continue;
      }

      // Comparison operators
      if (src[i] === '<' || src[i] === '>' || src[i] === '!' || src[i] === '=') {
        let op = src[i++];
        if (i < src.length && src[i] === '=') op += src[i++];
        tokens.push(op);
        continue;
      }

      // Arithmetic operators
      if ('+-*/%'.includes(src[i])) {
        tokens.push(src[i++]);
        continue;
      }

      // Numbers
      if (/\d/.test(src[i])) {
        let num = '';
        while (i < src.length && /\d/.test(src[i])) num += src[i++];
        // Check for .. (range operator) — don't consume the dots
        if (src[i] === '.' && src[i + 1] === '.') {
          tokens.push(num);
          continue;
        }
        // Decimal point
        if (src[i] === '.' && /\d/.test(src[i + 1])) {
          num += src[i++];
          while (i < src.length && /\d/.test(src[i])) num += src[i++];
        }
        tokens.push(num);
        continue;
      }

      // Words (identifiers/keywords)
      if (/[a-zA-Z_]/.test(src[i])) {
        let word = '';
        while (i < src.length && /[a-zA-Z0-9_]/.test(src[i])) word += src[i++];
        const upper = word.toUpperCase();
        // Keywords we care about
        const KEYWORDS = [
          'DECLARE', 'BEGIN', 'END', 'IF', 'THEN', 'ELSIF', 'ELSE', 'WHILE', 'LOOP',
          'FOR', 'IN', 'REVERSE', 'RETURN', 'RAISE', 'EXECUTE', 'PERFORM', 'INTO',
          'SELECT', 'FROM', 'WHERE', 'NULL', 'TRUE', 'FALSE', 'EXCEPTION', 'WHEN',
          'AND', 'OR', 'NOT', 'IS', 'NOTICE', 'WARNING', 'INFO', 'DEBUG',
          'INTEGER', 'TEXT', 'BOOLEAN', 'FLOAT', 'NUMERIC', 'RECORD',
          'FOUND', 'OTHERS',
        ];
        tokens.push(KEYWORDS.includes(upper) ? upper : word);
        continue;
      }

      // Unknown character
      i++;
    }

    return tokens;
  }
}

/**
 * PLInterpreter — executes a PL/HenryDB AST.
 */
export class PLInterpreter {
  constructor(db) {
    this.db = db;
    this.notices = [];
    this._maxIterations = 10000;
    this._cursors = new Map(); // cursor name → {query, rows, position}
    this._lastFetchEmpty = false;
  }

  /**
   * Execute a PL/HenryDB block.
   * @param {object} ast - Parsed block from PLParser
   * @param {object} params - Input parameters { name → value }
   * @returns {*} Return value, or undefined
   */
  execute(ast, params = {}) {
    const scope = new Map();

    // Initialize parameters
    for (const [name, value] of Object.entries(params)) {
      scope.set(name.toLowerCase(), value);
    }

    // Process declarations
    for (const decl of ast.declarations) {
      const name = decl.name.toLowerCase();
      if (decl.dataType === 'CURSOR') {
        // Register cursor query but don't execute yet
        this._cursors.set(name, { query: decl.cursorQuery, rows: null, position: -1 });
        scope.set(name, null);
      } else if (decl.dataType === 'RECORD') {
        scope.set(name, {}); // RECORD = empty object
      } else {
        const value = decl.defaultValue ? this._evalExpr(decl.defaultValue, scope) : null;
        scope.set(name, value);
      }
    }

    // Execute statements
    try {
      return this._executeStatements(ast.statements, scope);
    } catch (e) {
      if (e instanceof PLReturn) return e.value;
      if (e instanceof PLRaise && ast.exceptionHandlers) {
        return this._handleException(e, ast.exceptionHandlers, scope);
      }
      throw e;
    }
  }

  _executeStatements(statements, scope) {
    for (const stmt of statements) {
      const result = this._executeStatement(stmt, scope);
      if (result instanceof PLReturn) throw result;
    }
    return undefined;
  }

  _executeStatement(stmt, scope) {
    switch (stmt.type) {
      case 'assignment':
        scope.set(stmt.name.toLowerCase(), this._evalExpr(stmt.value, scope));
        return;

      case 'if':
        return this._executeIf(stmt, scope);

      case 'while':
        return this._executeWhile(stmt, scope);

      case 'for':
        return this._executeFor(stmt, scope);

      case 'for_query':
        return this._executeForQuery(stmt, scope);

      case 'case':
        return this._executeCase(stmt, scope);

      case 'dml':
        return this._executeDML(stmt, scope);

      case 'open_cursor':
        return this._executeOpenCursor(stmt, scope);

      case 'fetch_cursor':
        return this._executeFetchCursor(stmt, scope);

      case 'close_cursor':
        return this._executeCloseCursor(stmt, scope);

      case 'exit_when': {
        const cond = stmt.condition.trim().toUpperCase();
        if (cond === 'NOT FOUND') {
          if (this._lastFetchEmpty) throw new PLExit();
        } else {
          // Evaluate condition as SQL expression
          const condExpr = this._substituteVars(stmt.condition, scope);
          const result = this.db.execute(`SELECT (${condExpr}) as _v`);
          const val = result.rows?.[0]?._v;
          if (val) throw new PLExit();
        }
        return;
      }

      case 'exit':
        throw new PLExit();

      case 'loop':
        return this._executeLoop(stmt, scope);

      case 'return':
        throw new PLReturn(stmt.value ? this._evalExpr(stmt.value, scope) : undefined);

      case 'raise':
        return this._executeRaise(stmt, scope);

      case 'execute':
        return this._executeExecute(stmt, scope);

      case 'perform': {
        this.db.execute(stmt.sql);
        return;
      }

      case 'select_into':
        return this._executeSelectInto(stmt, scope);

      case 'null_stmt':
        return;

      default:
        throw new Error(`PL/HenryDB: unknown statement type '${stmt.type}'`);
    }
  }

  _executeIf(stmt, scope) {
    if (this._evalCondition(stmt.condition, scope)) {
      return this._executeStatements(stmt.thenStatements, scope);
    }

    for (const elsif of stmt.elsifClauses) {
      if (this._evalCondition(elsif.condition, scope)) {
        return this._executeStatements(elsif.statements, scope);
      }
    }

    if (stmt.elseStatements) {
      return this._executeStatements(stmt.elseStatements, scope);
    }
  }

  _executeWhile(stmt, scope) {
    let iterations = 0;
    while (this._evalCondition(stmt.condition, scope)) {
      if (++iterations > this._maxIterations) {
        throw new Error('PL/HenryDB: infinite loop detected (exceeded 10000 iterations)');
      }
      try {
        this._executeStatements(stmt.statements, scope);
      } catch (e) {
        if (e instanceof PLReturn) throw e;
        throw e;
      }
    }
  }

  _executeDML(stmt, scope) {
    // Substitute PL variables in the SQL string
    let sql = stmt.sql;
    // Replace variable references (simple name match against scope)
    for (const [name, value] of scope.entries()) {
      // Replace occurrences of the variable name with its value
      // Only replace whole words
      const regex = new RegExp(`\\b${name}\\b`, 'gi');
      if (regex.test(sql)) {
        const val = value;
        if (typeof val === 'string') {
          sql = sql.replace(regex, `'${val.replace(/'/g, "''")}'`);
        } else if (val === null) {
          sql = sql.replace(regex, 'NULL');
        } else {
          sql = sql.replace(regex, String(val));
        }
      }
    }
    this.db.execute(sql);
  }

  _executeOpenCursor(stmt, scope) {
    const name = stmt.cursor.toLowerCase();
    const cursor = this._cursors.get(name);
    if (!cursor) throw new PLRaise('INVALID_CURSOR', `cursor ${name} not found`);
    // Execute the query and store results
    const result = this.db.execute(cursor.query);
    cursor.rows = result.rows || [];
    cursor.position = 0;
    this._lastFetchEmpty = false;
  }

  _executeFetchCursor(stmt, scope) {
    const name = stmt.cursor.toLowerCase();
    const cursor = this._cursors.get(name);
    if (!cursor) throw new PLRaise('INVALID_CURSOR', `cursor ${name} not found`);
    if (cursor.rows === null) throw new PLRaise('INVALID_CURSOR', `cursor ${name} not open`);
    
    const targetName = stmt.target.toLowerCase();
    
    if (cursor.position >= cursor.rows.length) {
      this._lastFetchEmpty = true;
      // Set target to null/empty
      scope.set(targetName, null);
    } else {
      this._lastFetchEmpty = false;
      const row = cursor.rows[cursor.position];
      cursor.position++;
      // Store row as a record (object with lowercase keys)
      const record = {};
      for (const [k, v] of Object.entries(row)) {
        record[k.toLowerCase()] = v;
      }
      scope.set(targetName, record);
    }
  }

  _executeCloseCursor(stmt, scope) {
    const name = stmt.cursor.toLowerCase();
    const cursor = this._cursors.get(name);
    if (!cursor) throw new PLRaise('INVALID_CURSOR', `cursor ${name} not found`);
    cursor.rows = null;
    cursor.position = -1;
  }

  _executeLoop(stmt, scope) {
    let iterations = 0;
    while (iterations < this._maxIterations) {
      try {
        this._executeStatements(stmt.statements, scope);
      } catch (e) {
        if (e instanceof PLExit) break;
        throw e;
      }
      iterations++;
    }
  }

  _executeCase(stmt, scope) {
    if (stmt.subject !== null) {
      // Simple CASE: compare subject against each WHEN value
      const subjectVal = this._evalExpr(stmt.subject, scope);
      for (const branch of stmt.branches) {
        const whenVal = this._evalExpr(branch.condition, scope);
        if (subjectVal === whenVal || (subjectVal !== null && String(subjectVal) === String(whenVal))) {
          return this._executeStatements(branch.statements, scope);
        }
      }
    } else {
      // Searched CASE: evaluate each WHEN condition as boolean
      for (const branch of stmt.branches) {
        const condVal = this._evalExpr(branch.condition, scope);
        if (condVal) {
          return this._executeStatements(branch.statements, scope);
        }
      }
    }
    
    // ELSE branch
    if (stmt.elseStatements) {
      return this._executeStatements(stmt.elseStatements, scope);
    }
    // PL/pgSQL raises CASE_NOT_FOUND if no branch matched and no ELSE
    throw new PLRaise('CASE_NOT_FOUND', 'case not found');
  }

  _executeFor(stmt, scope) {
    const startVal = this._evalExpr(stmt.start, scope);
    const endVal = this._evalExpr(stmt.end, scope);
    const varName = stmt.varName.toLowerCase();

    if (stmt.reverse) {
      for (let i = endVal; i >= startVal; i--) {
        scope.set(varName, i);
        try {
          this._executeStatements(stmt.statements, scope);
        } catch (e) {
          if (e instanceof PLReturn) throw e;
          throw e;
        }
      }
    } else {
      for (let i = startVal; i <= endVal; i++) {
        scope.set(varName, i);
        try {
          this._executeStatements(stmt.statements, scope);
        } catch (e) {
          if (e instanceof PLReturn) throw e;
          throw e;
        }
      }
    }
  }

  _executeForQuery(stmt, scope) {
    const varName = stmt.varName.toLowerCase();
    // Substitute variables in the query
    let sql = stmt.query;
    for (const [name, value] of scope.entries()) {
      const regex = new RegExp(`\\b${name}\\b`, 'gi');
      if (regex.test(sql)) {
        if (typeof value === 'string') {
          sql = sql.replace(regex, `'${value.replace(/'/g, "''")}'`);
        } else if (value === null) {
          sql = sql.replace(regex, 'NULL');
        } else {
          sql = sql.replace(regex, String(value));
        }
      }
    }
    
    const result = this.db.execute(sql);
    const rows = result.rows || [];
    
    for (const row of rows) {
      // Store row as a record
      const record = {};
      for (const [k, v] of Object.entries(row)) {
        record[k.toLowerCase()] = v;
      }
      scope.set(varName, record);
      
      try {
        this._executeStatements(stmt.statements, scope);
      } catch (e) {
        if (e instanceof PLReturn) throw e;
        if (e instanceof PLExit) break;
        throw e;
      }
    }
  }

  _executeRaise(stmt, scope) {
    let message = stmt.format || '';
    const args = stmt.args.map(a => this._evalExpr(a, scope));

    // Replace % placeholders
    let argIdx = 0;
    message = message.replace(/%/g, () => {
      return argIdx < args.length ? String(args[argIdx++]) : '%';
    });

    if (stmt.level === 'EXCEPTION') {
      throw new PLRaise(message, stmt.level);
    }

    this.notices.push({ level: stmt.level, message });
  }

  _executeExecute(stmt, scope) {
    const sql = String(this._evalExpr(stmt.sqlExpr, scope));
    const result = this.db.execute(sql);

    // Set FOUND
    scope.set('found', result && result.rows && result.rows.length > 0);

    if (stmt.into && result && result.rows && result.rows.length > 0) {
      const firstRow = result.rows[0];
      const values = Object.values(firstRow);
      scope.set(stmt.into.toLowerCase(), values.length === 1 ? values[0] : firstRow);
    }

    return result;
  }

  _executeSelectInto(stmt, scope) {
    // Substitute variables in the SQL before executing
    const sql = this._substituteVars(stmt.sql, scope);
    const result = this.db.execute(sql);

    scope.set('found', result && result.rows && result.rows.length > 0);

    if (stmt.intoVar && result && result.rows && result.rows.length > 0) {
      const firstRow = result.rows[0];
      const values = Object.values(firstRow);
      scope.set(stmt.intoVar.toLowerCase(), values.length === 1 ? values[0] : firstRow);
    }

    return result;
  }

  _evalCondition(condition, scope) {
    const expr = this._substituteVars(condition.expr, scope);
    // Simple evaluation using Function constructor
    try {
      return this._evalSimpleExpr(expr, scope);
    } catch {
      return false;
    }
  }

  _evalExpr(expr, scope) {
    if (expr.type === 'literal') return expr.value;
    if (expr.type === 'expression') {
      const substituted = this._substituteVars(expr.expr, scope);
      return this._evalSimpleExpr(substituted, scope);
    }
    return null;
  }

  _substituteVars(expr, scope) {
    // Replace variable references with their values, but NOT inside string literals
    let result = '';
    let i = 0;
    while (i < expr.length) {
      // Skip string literals
      if (expr[i] === "'") {
        let end = i + 1;
        while (end < expr.length) {
          if (expr[end] === "'" && expr[end + 1] === "'") { end += 2; continue; }
          if (expr[end] === "'") { end++; break; }
          end++;
        }
        result += expr.substring(i, end);
        i = end;
        continue;
      }
      // Check for variable name at this position
      if (/[a-zA-Z_]/.test(expr[i])) {
        let word = '';
        let start = i;
        while (i < expr.length && /[a-zA-Z0-9_]/.test(expr[i])) word += expr[i++];
        const lower = word.toLowerCase();
        if (scope.has(lower)) {
          let value = scope.get(lower);
          // Handle record field access: rec.field or rec . field
          let lookAhead = i;
          while (lookAhead < expr.length && expr[lookAhead] === ' ') lookAhead++;
          if (typeof value === 'object' && value !== null && expr[lookAhead] === '.') {
            lookAhead++; // skip dot
            while (lookAhead < expr.length && expr[lookAhead] === ' ') lookAhead++;
            let field = '';
            while (lookAhead < expr.length && /[a-zA-Z0-9_]/.test(expr[lookAhead])) field += expr[lookAhead++];
            value = value[field.toLowerCase()] ?? null;
            i = lookAhead; // advance past the field access
          }
          if (value === null) result += 'null';
          else if (typeof value === 'string') result += `'${value}'`;
          else result += String(value);
        } else {
          result += word;
        }
        continue;
      }
      result += expr[i++];
    }
    return result;
  }

  _evalSimpleExpr(expr, scope) {
    // Pure literals — only if the entire expression IS a single literal
    if (expr === 'null' || expr === 'NULL') return null;
    if (expr === 'true' || expr === 'TRUE') return true;
    if (expr === 'false' || expr === 'FALSE') return false;
    if (/^-?\d+(\.\d+)?$/.test(expr)) return parseFloat(expr);
    // String literal — must be a complete single-quoted string (no operators outside)
    if (expr.startsWith("'") && expr.endsWith("'") && !expr.includes('||')) {
      // Verify it's a single string (count unescaped quotes)
      const inner = expr.slice(1, -1);
      if (!inner.includes("'") || inner.replace(/''/g, '').indexOf("'") === -1) {
        return inner.replace(/''/g, "'");
      }
    }

    // Handle string concatenation (||)
    if (expr.includes('||')) {
      const parts = expr.split('||').map(p => {
        const trimmed = p.trim();
        if (trimmed.startsWith("'") && trimmed.endsWith("'")) {
          return trimmed.slice(1, -1);
        }
        return this._evalSimpleExpr(trimmed, scope);
      });
      return parts.join('');
    }

    // Handle IS NULL / IS NOT NULL
    if (/\bIS\s+NOT\s+NULL\b/i.test(expr)) {
      const varExpr = expr.replace(/\s+IS\s+NOT\s+NULL\b/i, '').trim();
      return this._evalSimpleExpr(varExpr, scope) !== null;
    }
    if (/\bIS\s+NULL\b/i.test(expr)) {
      const varExpr = expr.replace(/\s+IS\s+NULL\b/i, '').trim();
      return this._evalSimpleExpr(varExpr, scope) === null;
    }

    // Handle comparison operators
    const compMatch = expr.match(/^(.+?)\s*(>=|<=|!=|<>|>|<|=)\s*(.+)$/);
    if (compMatch) {
      const left = this._evalSimpleExpr(compMatch[1].trim(), scope);
      const right = this._evalSimpleExpr(compMatch[3].trim(), scope);
      switch (compMatch[2]) {
        case '=': return left == right;
        case '!=': case '<>': return left != right;
        case '>': return left > right;
        case '<': return left < right;
        case '>=': return left >= right;
        case '<=': return left <= right;
      }
    }

    // Handle AND/OR
    if (/\bAND\b/i.test(expr)) {
      const parts = expr.split(/\bAND\b/i);
      return parts.every(p => this._evalSimpleExpr(p.trim(), scope));
    }
    if (/\bOR\b/i.test(expr)) {
      const parts = expr.split(/\bOR\b/i);
      return parts.some(p => this._evalSimpleExpr(p.trim(), scope));
    }

    // Handle NOT
    if (/^\s*NOT\s+/i.test(expr)) {
      return !this._evalSimpleExpr(expr.replace(/^\s*NOT\s+/i, ''), scope);
    }

    // Handle arithmetic
    const addMatch = expr.match(/^(.+)\s*([+\-])\s*([^+\-]+)$/);
    if (addMatch) {
      const left = this._evalSimpleExpr(addMatch[1].trim(), scope);
      const right = this._evalSimpleExpr(addMatch[3].trim(), scope);
      return addMatch[2] === '+' ? left + right : left - right;
    }

    const mulMatch = expr.match(/^(.+)\s*([*/%])\s*([^*/%]+)$/);
    if (mulMatch) {
      const left = this._evalSimpleExpr(mulMatch[1].trim(), scope);
      const right = this._evalSimpleExpr(mulMatch[3].trim(), scope);
      if (mulMatch[2] === '*') return left * right;
      if (mulMatch[2] === '/') return left / right;
      return left % right;
    }

    // Variable lookup
    const varName = expr.toLowerCase().trim();
    if (scope.has(varName)) return scope.get(varName);

    // Fallback
    return expr;
  }

  _handleException(error, handlers, scope) {
    for (const handler of handlers) {
      if (handler.condition === 'OTHERS' || handler.condition.toLowerCase().includes(error.level.toLowerCase())) {
        scope.set('sqlerrm', error.message);
        try {
          return this._executeStatements(handler.statements, scope);
        } catch (e) {
          if (e instanceof PLReturn) return e.value;
          throw e;
        }
      }
    }
    throw error;
  }
}

/**
 * Control flow exceptions for PL/HenryDB.
 */
class PLReturn {
  constructor(value) { this.value = value; }
}

class PLExit {
  // Thrown to break out of LOOP
}

export class PLRaise extends Error {
  constructor(message, level) {
    super(message);
    this.level = level;
    this.name = 'PLRaise';
  }
}
