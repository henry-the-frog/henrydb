/**
 * Lambda Calculus Parser: String → AST
 * 
 * Syntax: λx.body | (fn arg) | x | 123
 * Also accepts: \x.body, fun x => body
 */

class Var { constructor(name) { this.tag = 'Var'; this.name = name; } toString() { return this.name; } }
class Lam { constructor(v, body) { this.tag = 'Lam'; this.var = v; this.body = body; } toString() { return `(λ${this.var}.${this.body})`; } }
class App { constructor(fn, arg) { this.tag = 'App'; this.fn = fn; this.arg = arg; } toString() { return `(${this.fn} ${this.arg})`; } }
class Num { constructor(n) { this.tag = 'Num'; this.n = n; } toString() { return `${this.n}`; } }

class Parser {
  constructor(input) { this.input = input; this.pos = 0; }
  
  peek() { this.skipWS(); return this.input[this.pos]; }
  advance() { return this.input[this.pos++]; }
  skipWS() { while (this.pos < this.input.length && /\s/.test(this.input[this.pos])) this.pos++; }
  
  expect(ch) {
    this.skipWS();
    if (this.input[this.pos] !== ch) throw new Error(`Expected '${ch}' at ${this.pos}, got '${this.input[this.pos]}'`);
    this.pos++;
  }
  
  parseExpr() {
    this.skipWS();
    let expr = this.parseAtom();
    // Application is juxtaposition
    while (this.pos < this.input.length) {
      this.skipWS();
      if (this.pos >= this.input.length || this.input[this.pos] === ')') break;
      if (this.input[this.pos] === '.' || this.input[this.pos] === '=' && this.input[this.pos + 1] === '>') break;
      const arg = this.parseAtom();
      expr = new App(expr, arg);
    }
    return expr;
  }
  
  parseAtom() {
    this.skipWS();
    const ch = this.peek();
    
    if (ch === '(') {
      this.advance();
      const expr = this.parseExpr();
      this.expect(')');
      return expr;
    }
    
    if (ch === 'λ' || ch === '\\') {
      this.advance();
      this.skipWS();
      const v = this.parseIdent();
      this.expect('.');
      const body = this.parseExpr();
      return new Lam(v, body);
    }
    
    if (/[0-9]/.test(ch)) return this.parseNumber();
    if (/[a-zA-Z_]/.test(ch)) return new Var(this.parseIdent());
    
    throw new Error(`Unexpected '${ch}' at ${this.pos}`);
  }
  
  parseIdent() {
    this.skipWS();
    let name = '';
    while (this.pos < this.input.length && /[a-zA-Z0-9_']/.test(this.input[this.pos])) {
      name += this.advance();
    }
    if (!name) throw new Error(`Expected identifier at ${this.pos}`);
    return name;
  }
  
  parseNumber() {
    let num = '';
    while (this.pos < this.input.length && /[0-9]/.test(this.input[this.pos])) {
      num += this.advance();
    }
    return new Num(parseInt(num));
  }
}

function parse(input) {
  const parser = new Parser(input);
  const result = parser.parseExpr();
  return result;
}

function prettyPrint(expr) { return expr.toString(); }

export { Var, Lam, App, Num, Parser, parse, prettyPrint };
