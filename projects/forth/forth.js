// forth/forth.js — A Forth interpreter from scratch
class Forth {
  constructor() {
    this.stack = [];          // Data stack
    this.rstack = [];         // Return stack
    this.dictionary = {};     // Word definitions: name → { immediate, fn } or { immediate, body }
    this.memory = new Array(65536).fill(0);  // Linear memory
    this.here = 0;            // Next free memory cell
    this.output = [];         // Captured output
    this.compiling = false;   // Compilation mode
    this.currentDef = null;   // Current word being compiled
    this.currentBody = null;  // Body being compiled
    this._initBuiltins();
  }

  // ─── Stack Operations ───
  push(n) { this.stack.push(n); }
  pop() {
    if (this.stack.length === 0) throw new Error('Stack underflow');
    return this.stack.pop();
  }
  peek() {
    if (this.stack.length === 0) throw new Error('Stack underflow');
    return this.stack[this.stack.length - 1];
  }

  rpush(n) { this.rstack.push(n); }
  rpop() {
    if (this.rstack.length === 0) throw new Error('Return stack underflow');
    return this.rstack.pop();
  }

  // ─── Builtins ───
  _initBuiltins() {
    const d = this.dictionary;
    const self = this;

    // Arithmetic
    d['+'] = { immediate: false, fn() { const b = self.pop(), a = self.pop(); self.push(a + b); } };
    d['-'] = { immediate: false, fn() { const b = self.pop(), a = self.pop(); self.push(a - b); } };
    d['*'] = { immediate: false, fn() { const b = self.pop(), a = self.pop(); self.push(a * b); } };
    d['/'] = { immediate: false, fn() { const b = self.pop(), a = self.pop(); self.push(Math.trunc(a / b)); } };
    d['mod'] = { immediate: false, fn() { const b = self.pop(), a = self.pop(); self.push(a % b); } };
    d['negate'] = { immediate: false, fn() { self.push(-self.pop()); } };
    d['abs'] = { immediate: false, fn() { self.push(Math.abs(self.pop())); } };
    d['min'] = { immediate: false, fn() { const b = self.pop(), a = self.pop(); self.push(Math.min(a, b)); } };
    d['max'] = { immediate: false, fn() { const b = self.pop(), a = self.pop(); self.push(Math.max(a, b)); } };
    d['/mod'] = { immediate: false, fn() { const b = self.pop(), a = self.pop(); self.push(a % b); self.push(Math.trunc(a / b)); } };

    // Stack manipulation
    d['dup'] = { immediate: false, fn() { self.push(self.peek()); } };
    d['drop'] = { immediate: false, fn() { self.pop(); } };
    d['swap'] = { immediate: false, fn() { const b = self.pop(), a = self.pop(); self.push(b); self.push(a); } };
    d['over'] = { immediate: false, fn() { const b = self.pop(), a = self.pop(); self.push(a); self.push(b); self.push(a); } };
    d['rot'] = { immediate: false, fn() { const c = self.pop(), b = self.pop(), a = self.pop(); self.push(b); self.push(c); self.push(a); } };
    d['2dup'] = { immediate: false, fn() { const b = self.pop(), a = self.pop(); self.push(a); self.push(b); self.push(a); self.push(b); } };
    d['2drop'] = { immediate: false, fn() { self.pop(); self.pop(); } };
    d['2swap'] = { immediate: false, fn() { const d = self.pop(), c = self.pop(), b = self.pop(), a = self.pop(); self.push(c); self.push(d); self.push(a); self.push(b); } };
    d['?dup'] = { immediate: false, fn() { const n = self.peek(); if (n !== 0) self.push(n); } };
    d['nip'] = { immediate: false, fn() { const b = self.pop(); self.pop(); self.push(b); } };
    d['tuck'] = { immediate: false, fn() { const b = self.pop(), a = self.pop(); self.push(b); self.push(a); self.push(b); } };

    // Comparison
    d['='] = { immediate: false, fn() { const b = self.pop(), a = self.pop(); self.push(a === b ? -1 : 0); } };
    d['<>'] = { immediate: false, fn() { const b = self.pop(), a = self.pop(); self.push(a !== b ? -1 : 0); } };
    d['<'] = { immediate: false, fn() { const b = self.pop(), a = self.pop(); self.push(a < b ? -1 : 0); } };
    d['>'] = { immediate: false, fn() { const b = self.pop(), a = self.pop(); self.push(a > b ? -1 : 0); } };
    d['<='] = { immediate: false, fn() { const b = self.pop(), a = self.pop(); self.push(a <= b ? -1 : 0); } };
    d['>='] = { immediate: false, fn() { const b = self.pop(), a = self.pop(); self.push(a >= b ? -1 : 0); } };
    d['0='] = { immediate: false, fn() { self.push(self.pop() === 0 ? -1 : 0); } };
    d['0<'] = { immediate: false, fn() { self.push(self.pop() < 0 ? -1 : 0); } };
    d['0>'] = { immediate: false, fn() { self.push(self.pop() > 0 ? -1 : 0); } };
    d['0<>'] = { immediate: false, fn() { self.push(self.pop() !== 0 ? -1 : 0); } };

    // Boolean
    d['and'] = { immediate: false, fn() { const b = self.pop(), a = self.pop(); self.push(a & b); } };
    d['or'] = { immediate: false, fn() { const b = self.pop(), a = self.pop(); self.push(a | b); } };
    d['xor'] = { immediate: false, fn() { const b = self.pop(), a = self.pop(); self.push(a ^ b); } };
    d['invert'] = { immediate: false, fn() { self.push(~self.pop()); } };
    d['true'] = { immediate: false, fn() { self.push(-1); } };
    d['false'] = { immediate: false, fn() { self.push(0); } };

    // I/O
    d['.'] = { immediate: false, fn() { self.output.push(String(self.pop())); } };
    d['cr'] = { immediate: false, fn() { self.output.push('\n'); } };
    d['emit'] = { immediate: false, fn() { self.output.push(String.fromCharCode(self.pop())); } };
    d['.s'] = { immediate: false, fn() { self.output.push(`<${self.stack.length}> ${self.stack.join(' ')}`); } };
    d['space'] = { immediate: false, fn() { self.output.push(' '); } };
    d['spaces'] = { immediate: false, fn() { const n = self.pop(); self.output.push(' '.repeat(n)); } };

    // Memory
    d['!'] = { immediate: false, fn() { const addr = self.pop(), val = self.pop(); self.memory[addr] = val; } };
    d['@'] = { immediate: false, fn() { self.push(self.memory[self.pop()]); } };
    d['+!'] = { immediate: false, fn() { const addr = self.pop(), val = self.pop(); self.memory[addr] += val; } };
    d['here'] = { immediate: false, fn() { self.push(self.here); } };
    d['allot'] = { immediate: false, fn() { self.here += self.pop(); } };
    d[','] = { immediate: false, fn() { self.memory[self.here++] = self.pop(); } };

    // Return stack
    d['>r'] = { immediate: false, fn() { self.rpush(self.pop()); } };
    d['r>'] = { immediate: false, fn() { self.push(self.rpop()); } };
    d['r@'] = { immediate: false, fn() { self.push(self.rstack[self.rstack.length - 1]); } };

    // Stack inspection
    d['depth'] = { immediate: false, fn() { self.push(self.stack.length); } };

    // Control flow (immediate — recognized during compilation)
    d['if'] = { immediate: true };
    d['else'] = { immediate: true };
    d['then'] = { immediate: true };
    d['do'] = { immediate: true };
    d['loop'] = { immediate: true };
    d['+loop'] = { immediate: true };
    d['begin'] = { immediate: true };
    d['until'] = { immediate: true };
    d['while'] = { immediate: true };
    d['repeat'] = { immediate: true };
    d['recurse'] = { immediate: true };
  }

  // ─── Tokenizer ───
  tokenize(src) {
    const tokens = [];
    let i = 0;
    while (i < src.length) {
      // Skip whitespace
      if (/\s/.test(src[i])) { i++; continue; }
      // Comments: \ to end of line
      if (src[i] === '\\' && (i === 0 || /\s/.test(src[i - 1]))) {
        while (i < src.length && src[i] !== '\n') i++;
        continue;
      }
      // Paren comments: ( ... )
      if (src[i] === '(' && (i + 1 >= src.length || /\s/.test(src[i + 1]))) {
        i++;
        while (i < src.length && src[i] !== ')') i++;
        i++; // skip )
        continue;
      }
      // String literal: ." ... "
      if (src[i] === '.' && src[i + 1] === '"') {
        i += 2;
        if (src[i] === ' ') i++; // skip leading space
        let str = '';
        while (i < src.length && src[i] !== '"') str += src[i++];
        i++; // skip closing "
        tokens.push({ type: 'string', value: str });
        continue;
      }
      // S" string
      if (src[i] === 's' && src[i + 1] === '"') {
        i += 2;
        if (src[i] === ' ') i++;
        let str = '';
        while (i < src.length && src[i] !== '"') str += src[i++];
        i++;
        tokens.push({ type: 'sstring', value: str });
        continue;
      }
      // Word (anything separated by whitespace)
      let word = '';
      while (i < src.length && !/\s/.test(src[i])) word += src[i++];
      // Check if number
      if (/^-?\d+$/.test(word)) {
        tokens.push({ type: 'number', value: parseInt(word, 10) });
      } else {
        tokens.push({ type: 'word', value: word.toLowerCase() });
      }
    }
    return tokens;
  }

  // ─── Interpreter ───
  eval(src) {
    const tokens = this.tokenize(src);
    this._execTokens(tokens, 0);
    return this.output.join('');
  }

  _execTokens(tokens, start) {
    let i = start;
    while (i < tokens.length) {
      const tok = tokens[i];

      // Compilation mode
      if (this.compiling) {
        if (tok.type === 'word' && tok.value === ';') {
          // End definition
          this.dictionary[this.currentDef] = {
            immediate: false,
            body: [...this.currentBody],
          };
          this.compiling = false;
          this.currentDef = null;
          this.currentBody = null;
          i++;
          continue;
        }
        // Check for immediate words
        if (tok.type === 'word') {
          const entry = this.dictionary[tok.value];
          if (entry && entry.immediate) {
            // Execute immediately even during compilation
            if (tok.value === 'if') {
              this.currentBody.push({ type: 'if' });
            } else if (tok.value === 'else') {
              this.currentBody.push({ type: 'else' });
            } else if (tok.value === 'then') {
              this.currentBody.push({ type: 'then' });
            } else if (tok.value === 'do') {
              this.currentBody.push({ type: 'do' });
            } else if (tok.value === 'loop') {
              this.currentBody.push({ type: 'loop' });
            } else if (tok.value === '+loop') {
              this.currentBody.push({ type: '+loop' });
            } else if (tok.value === 'begin') {
              this.currentBody.push({ type: 'begin' });
            } else if (tok.value === 'until') {
              this.currentBody.push({ type: 'until' });
            } else if (tok.value === 'while') {
              this.currentBody.push({ type: 'while' });
            } else if (tok.value === 'repeat') {
              this.currentBody.push({ type: 'repeat' });
            } else if (tok.value === 'recurse') {
              this.currentBody.push({ type: 'recurse', name: this.currentDef });
            } else {
              entry.fn.call(this);
            }
            i++;
            continue;
          }
        }
        // Compile the token
        this.currentBody.push(tok);
        i++;
        continue;
      }

      // Interpretation mode
      if (tok.type === 'number') {
        this.push(tok.value);
        i++;
      } else if (tok.type === 'string') {
        this.output.push(tok.value);
        i++;
      } else if (tok.type === 'sstring') {
        // Push string address and length (simplified: just push the string)
        this.push(tok.value);
        i++;
      } else if (tok.type === 'word') {
        const word = tok.value;

        if (word === ':') {
          // Start compilation
          i++;
          if (i >= tokens.length) throw new Error('Expected word name after :');
          this.currentDef = tokens[i].value;
          this.currentBody = [];
          this.compiling = true;
          i++;
          continue;
        }

        if (word === 'variable') {
          i++;
          if (i >= tokens.length) throw new Error('Expected variable name');
          const name = tokens[i].value;
          const addr = this.here++;
          this.dictionary[name] = { immediate: false, fn: () => { this.push(addr); } };
          i++;
          continue;
        }

        if (word === 'constant') {
          i++;
          if (i >= tokens.length) throw new Error('Expected constant name');
          const name = tokens[i].value;
          const val = this.pop();
          this.dictionary[name] = { immediate: false, fn: () => { this.push(val); } };
          i++;
          continue;
        }

        // Control flow in interpretation mode
        if (word === 'if') {
          i = this._execIf(tokens, i + 1);
          continue;
        }
        if (word === 'do') {
          i = this._execDo(tokens, i + 1);
          continue;
        }
        if (word === 'begin') {
          i = this._execBegin(tokens, i + 1);
          continue;
        }

        const entry = this.dictionary[word];
        if (!entry) throw new Error(`Undefined word: ${word}`);

        if (entry.fn) {
          entry.fn.call(this);
        } else if (entry.body) {
          this._execBody(entry.body);
        }
        i++;
      } else {
        i++;
      }
    }
  }

  // Execute a compiled body
  _execBody(body) {
    let i = 0;
    while (i < body.length) {
      const tok = body[i];

      if (tok.type === 'number') {
        this.push(tok.value);
        i++;
      } else if (tok.type === 'string') {
        this.output.push(tok.value);
        i++;
      } else if (tok.type === 'sstring') {
        this.push(tok.value);
        i++;
      } else if (tok.type === 'if') {
        i = this._execCompiledIf(body, i + 1);
      } else if (tok.type === 'do') {
        i = this._execCompiledDo(body, i + 1);
      } else if (tok.type === 'begin') {
        i = this._execCompiledBegin(body, i + 1);
      } else if (tok.type === 'recurse') {
        const entry = this.dictionary[tok.name];
        if (entry && entry.body) this._execBody(entry.body);
        i++;
      } else if (tok.type === 'word') {
        const entry = this.dictionary[tok.value];
        if (!entry) throw new Error(`Undefined word: ${tok.value}`);
        if (entry.fn) entry.fn.call(this);
        else if (entry.body) this._execBody(entry.body);
        i++;
      } else {
        i++;
      }
    }
  }

  // ─── Control Flow Execution ───

  // IF ... THEN or IF ... ELSE ... THEN
  _execCompiledIf(body, start) {
    const cond = this.pop();
    // Find matching ELSE and THEN
    let depth = 0;
    let elseIdx = -1;
    let thenIdx = -1;
    for (let j = start; j < body.length; j++) {
      if (body[j].type === 'if') depth++;
      if (body[j].type === 'then' && depth === 0) { thenIdx = j; break; }
      if (body[j].type === 'else' && depth === 0) elseIdx = j;
      if (body[j].type === 'then' && depth > 0) depth--;
    }
    if (thenIdx === -1) throw new Error('IF without THEN');

    if (cond !== 0) {
      // True branch
      const end = elseIdx !== -1 ? elseIdx : thenIdx;
      this._execBody(body.slice(start, end));
    } else if (elseIdx !== -1) {
      // False branch
      this._execBody(body.slice(elseIdx + 1, thenIdx));
    }
    return thenIdx + 1;
  }

  // DO ... LOOP
  _execCompiledDo(body, start) {
    let index = this.pop();
    const limit = this.pop();
    // Find matching LOOP or +LOOP
    let depth = 0;
    let loopIdx = -1;
    let isPlusLoop = false;
    for (let j = start; j < body.length; j++) {
      if (body[j].type === 'do') depth++;
      if ((body[j].type === 'loop' || body[j].type === '+loop') && depth === 0) {
        loopIdx = j;
        isPlusLoop = body[j].type === '+loop';
        break;
      }
      if ((body[j].type === 'loop' || body[j].type === '+loop') && depth > 0) depth--;
    }
    if (loopIdx === -1) throw new Error('DO without LOOP');

    const loopBody = body.slice(start, loopIdx);
    // Add I word temporarily
    const savedI = this.dictionary['i'];
    while (index < limit) {
      this.dictionary['i'] = { immediate: false, fn: () => { this.push(index); } };
      this._execBody(loopBody);
      if (isPlusLoop) {
        index += this.pop();
      } else {
        index++;
      }
    }
    if (savedI) this.dictionary['i'] = savedI;
    else delete this.dictionary['i'];
    return loopIdx + 1;
  }

  // BEGIN ... UNTIL or BEGIN ... WHILE ... REPEAT
  _execCompiledBegin(body, start) {
    // Find UNTIL or WHILE/REPEAT
    let depth = 0;
    let untilIdx = -1;
    let whileIdx = -1;
    let repeatIdx = -1;
    for (let j = start; j < body.length; j++) {
      if (body[j].type === 'begin') depth++;
      if (body[j].type === 'until' && depth === 0) { untilIdx = j; break; }
      if (body[j].type === 'while' && depth === 0) whileIdx = j;
      if (body[j].type === 'repeat' && depth === 0) { repeatIdx = j; break; }
      if ((body[j].type === 'until' || body[j].type === 'repeat') && depth > 0) depth--;
    }

    if (untilIdx !== -1) {
      // BEGIN ... UNTIL
      const loopBody = body.slice(start, untilIdx);
      let maxIter = 100000;
      do {
        this._execBody(loopBody);
        if (--maxIter <= 0) throw new Error('Infinite loop');
      } while (this.pop() === 0);
      return untilIdx + 1;
    }

    if (whileIdx !== -1 && repeatIdx !== -1) {
      // BEGIN ... WHILE ... REPEAT
      const condBody = body.slice(start, whileIdx);
      const actionBody = body.slice(whileIdx + 1, repeatIdx);
      let maxIter = 100000;
      while (true) {
        this._execBody(condBody);
        if (this.pop() === 0) break;
        this._execBody(actionBody);
        if (--maxIter <= 0) throw new Error('Infinite loop');
      }
      return repeatIdx + 1;
    }

    throw new Error('BEGIN without UNTIL or WHILE/REPEAT');
  }

  // Interpretation mode control flow
  _execIf(tokens, start) {
    const cond = this.pop();
    let depth = 0;
    let elseIdx = -1;
    let thenIdx = -1;
    for (let j = start; j < tokens.length; j++) {
      if (tokens[j].type === 'word' && tokens[j].value === 'if') depth++;
      if (tokens[j].type === 'word' && tokens[j].value === 'then' && depth === 0) { thenIdx = j; break; }
      if (tokens[j].type === 'word' && tokens[j].value === 'else' && depth === 0) elseIdx = j;
      if (tokens[j].type === 'word' && tokens[j].value === 'then' && depth > 0) depth--;
    }
    if (thenIdx === -1) throw new Error('IF without THEN');
    if (cond !== 0) {
      const end = elseIdx !== -1 ? elseIdx : thenIdx;
      this._execTokens(tokens.slice(start, end), 0);
    } else if (elseIdx !== -1) {
      this._execTokens(tokens.slice(elseIdx + 1, thenIdx), 0);
    }
    return thenIdx + 1;
  }

  _execDo(tokens, start) {
    let index = this.pop();
    const limit = this.pop();
    let depth = 0;
    let loopIdx = -1;
    for (let j = start; j < tokens.length; j++) {
      if (tokens[j].type === 'word' && tokens[j].value === 'do') depth++;
      if (tokens[j].type === 'word' && tokens[j].value === 'loop' && depth === 0) { loopIdx = j; break; }
      if (tokens[j].type === 'word' && tokens[j].value === 'loop' && depth > 0) depth--;
    }
    if (loopIdx === -1) throw new Error('DO without LOOP');
    const loopTokens = tokens.slice(start, loopIdx);
    const savedI = this.dictionary['i'];
    while (index < limit) {
      this.dictionary['i'] = { immediate: false, fn: () => { this.push(index); } };
      this._execTokens(loopTokens, 0);
      index++;
    }
    if (savedI) this.dictionary['i'] = savedI;
    else delete this.dictionary['i'];
    return loopIdx + 1;
  }

  _execBegin(tokens, start) {
    let depth = 0;
    let untilIdx = -1;
    for (let j = start; j < tokens.length; j++) {
      if (tokens[j].type === 'word' && tokens[j].value === 'begin') depth++;
      if (tokens[j].type === 'word' && tokens[j].value === 'until' && depth === 0) { untilIdx = j; break; }
      if (tokens[j].type === 'word' && tokens[j].value === 'until' && depth > 0) depth--;
    }
    if (untilIdx === -1) throw new Error('BEGIN without UNTIL');
    const loopTokens = tokens.slice(start, untilIdx);
    let maxIter = 100000;
    do {
      this._execTokens(loopTokens, 0);
      if (--maxIter <= 0) throw new Error('Infinite loop');
    } while (this.pop() === 0);
    return untilIdx + 1;
  }

  getStack() { return [...this.stack]; }
  getOutput() { return this.output.join(''); }
  reset() { this.stack = []; this.rstack = []; this.output = []; }
}

export { Forth };
