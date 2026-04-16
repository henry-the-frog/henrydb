/**
 * Abstract Machines Zoo: SECD, Krivine, ZAM
 * 
 * Three classic abstract machines for lambda calculus evaluation:
 * 1. SECD (Landin 1964): Stack, Environment, Control, Dump — call-by-value
 * 2. Krivine (1985): call-by-name, environment-based
 * 3. ZAM (Zinc, Leroy 1990): optimized for ML, tail calls
 */

// Shared term representation
class Var { constructor(idx) { this.tag = 'Var'; this.idx = idx; } }
class Lam { constructor(body) { this.tag = 'Lam'; this.body = body; } }
class App { constructor(fn, arg) { this.tag = 'App'; this.fn = fn; this.arg = arg; } }
class Num { constructor(n) { this.tag = 'Num'; this.n = n; } }
class Add { constructor(l, r) { this.tag = 'Add'; this.left = l; this.right = r; } }

// ============================================================
// SECD Machine (call-by-value)
// ============================================================

class SECD {
  run(term) {
    let S = [];                // Stack
    let E = [];                // Environment
    let C = [term];            // Control
    let D = [];                // Dump
    let steps = 0;

    while (C.length > 0 || D.length > 0) {
      if (++steps > 10000) throw new Error('SECD: step limit');
      
      if (C.length === 0) {
        const [oldS, oldE, oldC] = D.pop();
        C = oldC; E = oldE; S = [...oldS, S[S.length - 1]];
        continue;
      }
      
      const instr = C.pop();
      
      if (typeof instr === 'string') {
        if (instr === 'AP') {
          const arg = S.pop();
          const clos = S.pop();
          D.push([S, E, C]);
          S = [];
          E = [arg, ...clos.env];
          C = [clos.body];
        } else if (instr === 'ADD') {
          const b = S.pop(), a = S.pop();
          S.push(a + b);
        }
        continue;
      }
      
      switch (instr.tag) {
        case 'Num': S.push(instr.n); break;
        case 'Var': S.push(E[instr.idx]); break;
        case 'Lam': S.push({ tag: 'Closure', body: instr.body, env: [...E] }); break;
        case 'App': C.push('AP'); C.push(instr.arg); C.push(instr.fn); break;
        case 'Add': C.push('ADD', instr.left); C.push(instr.right); break;
      }
    }
    
    return S[S.length - 1];
  }
}

// ============================================================
// Krivine Machine (call-by-name)
// ============================================================

class Krivine {
  run(term) {
    let code = term;           // Current term
    let env = [];              // Environment (closures)
    let stack = [];            // Argument stack
    let steps = 0;

    while (true) {
      if (++steps > 10000) throw new Error('Krivine: step limit');
      
      switch (code.tag) {
        case 'App':
          stack.push({ code: code.arg, env: [...env] });
          code = code.fn;
          break;
        case 'Lam':
          if (stack.length === 0) return { tag: 'Closure', body: code.body, env };
          const arg = stack.pop();
          env = [arg, ...env];
          code = code.body;
          break;
        case 'Var': {
          const closure = env[code.idx];
          if (!closure) throw new Error(`Unbound var ${code.idx}`);
          code = closure.code;
          env = closure.env;
          break;
        }
        case 'Num':
          return code.n;
        case 'Add': {
          const l = new Krivine().run(code.left);
          const r = new Krivine().run(code.right);
          return l + r;
        }
        default:
          return code;
      }
    }
  }
}

// ============================================================
// ZAM (simplified Zinc Abstract Machine)
// ============================================================

class ZAM {
  compile(term) {
    switch (term.tag) {
      case 'Num': return [{ op: 'CONST', n: term.n }];
      case 'Var': return [{ op: 'ACCESS', idx: term.idx }];
      case 'Lam': return [{ op: 'CLOSURE', body: this.compile(term.body) }];
      case 'App': return [...this.compile(term.arg), { op: 'PUSH' }, ...this.compile(term.fn), { op: 'APPLY' }];
      case 'Add': return [...this.compile(term.left), { op: 'PUSH' }, ...this.compile(term.right), { op: 'ADD' }];
    }
  }

  execute(code) {
    let pc = 0, stack = [], env = [], accu = null;
    let steps = 0;

    while (pc < code.length) {
      if (++steps > 10000) throw new Error('ZAM: step limit');
      const instr = code[pc++];

      switch (instr.op) {
        case 'CONST': accu = instr.n; break;
        case 'ACCESS': accu = env[instr.idx]; break;
        case 'CLOSURE': accu = { tag: 'Closure', code: instr.body, env: [...env] }; break;
        case 'PUSH': stack.push(accu); break;
        case 'APPLY': {
          const closure = accu;
          const arg = stack.pop();
          stack.push({ pc, code, env });
          env = [arg, ...closure.env];
          code = closure.code;
          pc = 0;
          break;
        }
        case 'ADD': {
          const b = accu;
          const a = stack.pop();
          accu = a + b;
          break;
        }
        case 'RETURN': {
          const frame = stack.pop();
          pc = frame.pc; code = frame.code; env = frame.env;
          break;
        }
      }
    }
    return accu;
  }

  run(term) {
    const code = this.compile(term);
    return this.execute(code);
  }
}

export { Var, Lam, App, Num, Add, SECD, Krivine, ZAM };
