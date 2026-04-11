// compiler.js — Simple compiler: tiny language → VM bytecode
// Language features: let, if/else, while, functions, arithmetic, comparisons
//
// Syntax:
//   let x = expr;
//   x = expr;
//   if (expr) { ... } else { ... }
//   while (expr) { ... }
//   fn name(params) { ... return expr; }
//   print(expr);
//   expr (arithmetic: + - * / %, comparison: == != < > <= >=)

import { OP, VM } from './vm.js';

// ---- Tokenizer ----

const TOKEN_TYPES = {
  NUMBER: 'NUMBER', IDENT: 'IDENT', STRING: 'STRING',
  PLUS: '+', MINUS: '-', STAR: '*', SLASH: '/', PERCENT: '%',
  EQ: '==', NEQ: '!=', LT: '<', GT: '>', LTE: '<=', GTE: '>=',
  ASSIGN: '=', LPAREN: '(', RPAREN: ')', LBRACE: '{', RBRACE: '}',
  SEMI: ';', COMMA: ',',
  LET: 'let', IF: 'if', ELSE: 'else', WHILE: 'while',
  FN: 'fn', RETURN: 'return', PRINT: 'print',
  TRUE: 'true', FALSE: 'false',
  EOF: 'EOF',
};

function tokenize(source) {
  const tokens = [];
  let pos = 0;
  
  while (pos < source.length) {
    // Skip whitespace and comments
    if (' \t\n\r'.includes(source[pos])) { pos++; continue; }
    if (source[pos] === '/' && source[pos + 1] === '/') {
      while (pos < source.length && source[pos] !== '\n') pos++;
      continue;
    }
    
    // Numbers
    if (source[pos] >= '0' && source[pos] <= '9') {
      let num = '';
      while (pos < source.length && source[pos] >= '0' && source[pos] <= '9') num += source[pos++];
      tokens.push({ type: TOKEN_TYPES.NUMBER, value: parseInt(num) });
      continue;
    }
    
    // Identifiers and keywords
    if ((source[pos] >= 'a' && source[pos] <= 'z') || (source[pos] >= 'A' && source[pos] <= 'Z') || source[pos] === '_') {
      let id = '';
      while (pos < source.length && /[a-zA-Z0-9_]/.test(source[pos])) id += source[pos++];
      const keywords = { let: TOKEN_TYPES.LET, if: TOKEN_TYPES.IF, else: TOKEN_TYPES.ELSE,
        while: TOKEN_TYPES.WHILE, fn: TOKEN_TYPES.FN, return: TOKEN_TYPES.RETURN,
        print: TOKEN_TYPES.PRINT, true: TOKEN_TYPES.TRUE, false: TOKEN_TYPES.FALSE };
      tokens.push({ type: keywords[id] || TOKEN_TYPES.IDENT, value: id });
      continue;
    }
    
    // Two-character operators
    const twoChar = source.slice(pos, pos + 2);
    if (['==', '!=', '<=', '>='].includes(twoChar)) {
      tokens.push({ type: twoChar }); pos += 2; continue;
    }
    
    // Single-character tokens
    const singleMap = { '+': '+', '-': '-', '*': '*', '/': '/', '%': '%',
      '=': '=', '<': '<', '>': '>', '(': '(', ')': ')', '{': '{', '}': '}',
      ';': ';', ',': ',' };
    if (singleMap[source[pos]]) {
      tokens.push({ type: singleMap[source[pos]] }); pos++; continue;
    }
    
    throw new SyntaxError(`Unexpected character: '${source[pos]}' at position ${pos}`);
  }
  
  tokens.push({ type: TOKEN_TYPES.EOF });
  return tokens;
}

// ---- Parser → AST ----

class Parser {
  constructor(tokens) {
    this.tokens = tokens;
    this.pos = 0;
  }
  
  peek() { return this.tokens[this.pos]; }
  advance() { return this.tokens[this.pos++]; }
  expect(type) {
    const tok = this.advance();
    if (tok.type !== type) throw new SyntaxError(`Expected ${type}, got ${tok.type}`);
    return tok;
  }
  
  parseProgram() {
    const body = [];
    while (this.peek().type !== TOKEN_TYPES.EOF) {
      body.push(this.parseStatement());
    }
    return { type: 'Program', body };
  }
  
  parseStatement() {
    const tok = this.peek();
    if (tok.type === TOKEN_TYPES.LET) return this.parseLet();
    if (tok.type === TOKEN_TYPES.IF) return this.parseIf();
    if (tok.type === TOKEN_TYPES.WHILE) return this.parseWhile();
    if (tok.type === TOKEN_TYPES.FN) return this.parseFn();
    if (tok.type === TOKEN_TYPES.RETURN) return this.parseReturn();
    if (tok.type === TOKEN_TYPES.PRINT) return this.parsePrint();
    if (tok.type === TOKEN_TYPES.LBRACE) return this.parseBlock();
    // Assignment or expression statement
    return this.parseExprStatement();
  }
  
  parseLet() {
    this.expect(TOKEN_TYPES.LET);
    const name = this.expect(TOKEN_TYPES.IDENT).value;
    this.expect('=');
    const value = this.parseExpr();
    this.expect(';');
    return { type: 'Let', name, value };
  }
  
  parseIf() {
    this.expect(TOKEN_TYPES.IF);
    this.expect('(');
    const condition = this.parseExpr();
    this.expect(')');
    const then = this.parseStatement();
    let elseBody = null;
    if (this.peek().type === TOKEN_TYPES.ELSE) {
      this.advance();
      elseBody = this.parseStatement();
    }
    return { type: 'If', condition, then, else: elseBody };
  }
  
  parseWhile() {
    this.expect(TOKEN_TYPES.WHILE);
    this.expect('(');
    const condition = this.parseExpr();
    this.expect(')');
    const body = this.parseStatement();
    return { type: 'While', condition, body };
  }
  
  parseFn() {
    this.expect(TOKEN_TYPES.FN);
    const name = this.expect(TOKEN_TYPES.IDENT).value;
    this.expect('(');
    const params = [];
    if (this.peek().type !== ')') {
      params.push(this.expect(TOKEN_TYPES.IDENT).value);
      while (this.peek().type === ',') { this.advance(); params.push(this.expect(TOKEN_TYPES.IDENT).value); }
    }
    this.expect(')');
    const body = this.parseBlock();
    return { type: 'Function', name, params, body };
  }
  
  parseReturn() {
    this.expect(TOKEN_TYPES.RETURN);
    const value = this.parseExpr();
    this.expect(';');
    return { type: 'Return', value };
  }
  
  parsePrint() {
    this.expect(TOKEN_TYPES.PRINT);
    this.expect('(');
    const value = this.parseExpr();
    this.expect(')');
    this.expect(';');
    return { type: 'Print', value };
  }
  
  parseBlock() {
    this.expect('{');
    const body = [];
    while (this.peek().type !== '}') body.push(this.parseStatement());
    this.expect('}');
    return { type: 'Block', body };
  }
  
  parseExprStatement() {
    const expr = this.parseExpr();
    if (this.peek().type === '=' && expr.type === 'Identifier') {
      this.advance();
      const value = this.parseExpr();
      this.expect(';');
      return { type: 'Assign', name: expr.name, value };
    }
    this.expect(';');
    return { type: 'ExprStatement', expr };
  }
  
  parseExpr() { return this.parseComparison(); }
  
  parseComparison() {
    let left = this.parseAddSub();
    while (['==', '!=', '<', '>', '<=', '>='].includes(this.peek().type)) {
      const op = this.advance().type;
      const right = this.parseAddSub();
      left = { type: 'BinaryOp', op, left, right };
    }
    return left;
  }
  
  parseAddSub() {
    let left = this.parseMulDiv();
    while (['+', '-'].includes(this.peek().type)) {
      const op = this.advance().type;
      const right = this.parseMulDiv();
      left = { type: 'BinaryOp', op, left, right };
    }
    return left;
  }
  
  parseMulDiv() {
    let left = this.parseUnary();
    while (['*', '/', '%'].includes(this.peek().type)) {
      const op = this.advance().type;
      const right = this.parseUnary();
      left = { type: 'BinaryOp', op, left, right };
    }
    return left;
  }
  
  parseUnary() {
    if (this.peek().type === '-') {
      this.advance();
      return { type: 'UnaryOp', op: '-', operand: this.parseUnary() };
    }
    return this.parsePrimary();
  }
  
  parsePrimary() {
    const tok = this.peek();
    if (tok.type === TOKEN_TYPES.NUMBER) { this.advance(); return { type: 'Number', value: tok.value }; }
    if (tok.type === TOKEN_TYPES.TRUE) { this.advance(); return { type: 'Number', value: 1 }; }
    if (tok.type === TOKEN_TYPES.FALSE) { this.advance(); return { type: 'Number', value: 0 }; }
    if (tok.type === TOKEN_TYPES.IDENT) {
      this.advance();
      // Function call?
      if (this.peek().type === '(') {
        this.advance();
        const args = [];
        if (this.peek().type !== ')') {
          args.push(this.parseExpr());
          while (this.peek().type === ',') { this.advance(); args.push(this.parseExpr()); }
        }
        this.expect(')');
        return { type: 'Call', name: tok.value, args };
      }
      return { type: 'Identifier', name: tok.value };
    }
    if (tok.type === '(') {
      this.advance();
      const expr = this.parseExpr();
      this.expect(')');
      return expr;
    }
    throw new SyntaxError(`Unexpected token: ${tok.type}`);
  }
}

// ---- Compiler → Bytecode ----

class Compiler {
  constructor() {
    this._code = [];
    this._locals = new Map(); // name → slot index
    this._nextSlot = 0;
    this._functions = new Map(); // name → { addr, params }
    this._patches = []; // { addr, label } for forward jumps
  }
  
  compile(ast) {
    // First pass: collect function declarations
    for (const node of ast.body) {
      if (node.type === 'Function') {
        this._functions.set(node.name, { params: node.params, addr: -1 });
      }
    }
    
    // Compile main code (skip function declarations)
    for (const node of ast.body) {
      if (node.type !== 'Function') this.compileNode(node);
    }
    this._emit(OP.HALT);
    
    // Compile functions
    for (const node of ast.body) {
      if (node.type === 'Function') {
        const fn = this._functions.get(node.name);
        fn.addr = this._code.length;
        
        // Set up parameters as locals
        const savedLocals = new Map(this._locals);
        const savedSlot = this._nextSlot;
        this._locals = new Map();
        this._nextSlot = 0;
        
        for (const param of node.params) {
          this._locals.set(param, this._nextSlot++);
        }
        
        // Store arguments from stack into locals (reverse order)
        for (let i = node.params.length - 1; i >= 0; i--) {
          this._emit(OP.STORE, i);
        }
        
        this.compileNode(node.body);
        this._emit(OP.PUSH, 0); // default return
        this._emit(OP.RET);
        
        this._locals = savedLocals;
        this._nextSlot = savedSlot;
      }
    }
    
    // Patch function call addresses
    for (const patch of this._patches) {
      const fn = this._functions.get(patch.label);
      if (!fn) throw new Error(`Undefined function: ${patch.label}`);
      this._code[patch.addr] = fn.addr;
    }
    
    return this._code;
  }
  
  compileNode(node) {
    switch (node.type) {
      case 'Block':
        for (const stmt of node.body) this.compileNode(stmt);
        break;
        
      case 'Let':
        this.compileNode(node.value);
        if (!this._locals.has(node.name)) this._locals.set(node.name, this._nextSlot++);
        this._emit(OP.STORE, this._locals.get(node.name));
        break;
        
      case 'Assign':
        this.compileNode(node.value);
        if (!this._locals.has(node.name)) throw new Error(`Undefined variable: ${node.name}`);
        this._emit(OP.STORE, this._locals.get(node.name));
        break;
        
      case 'If': {
        this.compileNode(node.condition);
        const jzAddr = this._code.length;
        this._emit(OP.JZ, 0); // placeholder
        this.compileNode(node.then);
        if (node.else) {
          const jmpAddr = this._code.length;
          this._emit(OP.JMP, 0); // skip else
          this._code[jzAddr + 1] = this._code.length; // patch JZ
          this.compileNode(node.else);
          this._code[jmpAddr + 1] = this._code.length; // patch JMP
        } else {
          this._code[jzAddr + 1] = this._code.length; // patch JZ
        }
        break;
      }
        
      case 'While': {
        const loopStart = this._code.length;
        this.compileNode(node.condition);
        const jzAddr = this._code.length;
        this._emit(OP.JZ, 0); // placeholder
        this.compileNode(node.body);
        this._emit(OP.JMP, loopStart);
        this._code[jzAddr + 1] = this._code.length; // patch JZ
        break;
      }
        
      case 'Return':
        this.compileNode(node.value);
        this._emit(OP.RET);
        break;
        
      case 'Print':
        this.compileNode(node.value);
        this._emit(OP.PRINT);
        this._emit(OP.POP);
        break;
        
      case 'ExprStatement':
        this.compileNode(node.expr);
        this._emit(OP.POP);
        break;
        
      case 'Number':
        this._emit(OP.PUSH, node.value);
        break;
        
      case 'Identifier': {
        const slot = this._locals.get(node.name);
        if (slot === undefined) throw new Error(`Undefined variable: ${node.name}`);
        this._emit(OP.LOAD, slot);
        break;
      }
        
      case 'BinaryOp':
        this.compileNode(node.left);
        this.compileNode(node.right);
        const opMap = { '+': OP.ADD, '-': OP.SUB, '*': OP.MUL, '/': OP.DIV, '%': OP.MOD,
          '==': OP.EQ, '!=': OP.NEQ, '<': OP.LT, '>': OP.GT, '<=': OP.LTE, '>=': OP.GTE };
        this._emit(opMap[node.op]);
        break;
        
      case 'UnaryOp':
        this.compileNode(node.operand);
        if (node.op === '-') this._emit(OP.NEG);
        break;
        
      case 'Call': {
        // Push arguments
        for (const arg of node.args) this.compileNode(arg);
        // Call function (address patched later)
        this._emit(OP.CALL, 0);
        this._patches.push({ addr: this._code.length - 1, label: node.name });
        break;
      }
        
      default:
        throw new Error(`Unknown AST node: ${node.type}`);
    }
  }
  
  _emit(...opcodes) {
    for (const op of opcodes) this._code.push(op);
  }
}

/**
 * Compile and execute a program.
 * @param {string} source — source code
 * @param {Object} [options] — VM options
 * @returns {{ result: *, output: number[], bytecode: number[] }}
 */
export function run(source, options = {}) {
  const tokens = tokenize(source);
  const parser = new Parser(tokens);
  const ast = parser.parseProgram();
  const compiler = new Compiler();
  const bytecode = compiler.compile(ast);
  const vm = new VM(options);
  const result = vm.execute(bytecode);
  return { result, output: vm.output, bytecode, steps: vm.stepCount };
}

export { tokenize, Parser, Compiler };
