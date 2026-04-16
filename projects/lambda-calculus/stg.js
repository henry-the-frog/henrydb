/**
 * Graph Reduction (STG-style)
 * 
 * The Spineless Tagless G-machine (STG) is GHC's evaluation model.
 * Key ideas:
 * - Heap-allocated closures (thunks + lambdas)
 * - Stack-based evaluation with update frames
 * - Push/enter vs eval/apply
 * - Thunk update: after evaluation, overwrite thunk with result (sharing)
 */

// Heap node types
class HLam { constructor(vars, body, env) { this.tag = 'HLam'; this.vars = vars; this.body = body; this.env = env; } }
class HCon { constructor(tag, args) { this.tag = 'HCon'; this.ctag = tag; this.args = args; } }
class HThunk { constructor(body, env) { this.tag = 'HThunk'; this.body = body; this.env = env; this.blackhole = false; } }
class HPrim { constructor(value) { this.tag = 'HPrim'; this.value = value; } }
class HIndirection { constructor(target) { this.tag = 'HIndirection'; this.target = target; } }

class Heap {
  constructor() { this.store = new Map(); this.nextAddr = 0; }
  
  alloc(node) {
    const addr = this.nextAddr++;
    this.store.set(addr, node);
    return addr;
  }
  
  read(addr) {
    let node = this.store.get(addr);
    // Follow indirections
    while (node && node.tag === 'HIndirection') {
      addr = node.target;
      node = this.store.get(addr);
    }
    return node;
  }
  
  update(addr, node) { this.store.set(addr, node); }
  
  get size() { return this.store.size; }
}

// Expressions
class EVar { constructor(name) { this.tag = 'EVar'; this.name = name; } }
class ELam { constructor(vars, body) { this.tag = 'ELam'; this.vars = vars; this.body = body; } }
class EApp { constructor(fn, args) { this.tag = 'EApp'; this.fn = fn; this.args = args; } }
class ECon { constructor(tag, args) { this.tag = 'ECon'; this.ctag = tag; this.args = args; } }
class ECase { constructor(scrut, alts) { this.tag = 'ECase'; this.scrut = scrut; this.alts = alts; } }
class ELet { constructor(binds, body) { this.tag = 'ELet'; this.binds = binds; this.body = body; } }
class ENum { constructor(n) { this.tag = 'ENum'; this.n = n; } }
class EPrim { constructor(op, args) { this.tag = 'EPrim'; this.op = op; this.args = args; } }

// ============================================================
// STG Machine
// ============================================================

class STGMachine {
  constructor() {
    this.heap = new Heap();
    this.steps = 0;
    this.maxSteps = 10000;
    this.updates = 0;
  }

  eval(expr, env = new Map()) {
    this.steps++;
    if (this.steps > this.maxSteps) throw new Error('Step limit exceeded');
    
    switch (expr.tag) {
      case 'ENum': return this.heap.alloc(new HPrim(expr.n));
      
      case 'EVar': {
        const addr = env.get(expr.name);
        if (addr === undefined) throw new Error(`Unbound: ${expr.name}`);
        // Force thunks
        const node = this.heap.read(addr);
        if (node.tag === 'HThunk') {
          if (node.blackhole) throw new Error('Black hole!');
          node.blackhole = true;
          const result = this.eval(node.body, new Map([...node.env]));
          this.heap.update(addr, new HIndirection(result));
          this.updates++;
          return result;
        }
        return addr;
      }
      
      case 'ELam': return this.heap.alloc(new HLam(expr.vars, expr.body, new Map(env)));
      
      case 'EApp': {
        const fnAddr = this.eval(expr.fn, env);
        const fn = this.heap.read(fnAddr);
        if (fn.tag !== 'HLam') throw new Error('Not a function');
        const newEnv = new Map([...fn.env]);
        for (let i = 0; i < fn.vars.length; i++) {
          const argAddr = this.eval(expr.args[i], env);
          newEnv.set(fn.vars[i], argAddr);
        }
        return this.eval(fn.body, newEnv);
      }
      
      case 'ECon': {
        const argAddrs = expr.args.map(a => this.eval(a, env));
        return this.heap.alloc(new HCon(expr.ctag, argAddrs));
      }
      
      case 'ELet': {
        const newEnv = new Map(env);
        for (const [name, rhs] of expr.binds) {
          const addr = this.heap.alloc(new HThunk(rhs, newEnv));
          newEnv.set(name, addr);
        }
        return this.eval(expr.body, newEnv);
      }
      
      case 'ECase': {
        const scrutAddr = this.eval(expr.scrut, env);
        const scrutNode = this.heap.read(scrutAddr);
        for (const [pattern, body] of expr.alts) {
          if (pattern === '_') return this.eval(body, env);
          if (scrutNode.tag === 'HCon' && scrutNode.ctag === pattern) {
            return this.eval(body, env);
          }
          if (scrutNode.tag === 'HPrim' && scrutNode.value === pattern) {
            return this.eval(body, env);
          }
        }
        throw new Error(`No matching case for ${scrutNode.tag}`);
      }
      
      case 'EPrim': {
        const vals = expr.args.map(a => {
          const addr = this.eval(a, env);
          return this.heap.read(addr).value;
        });
        let result;
        switch (expr.op) {
          case '+': result = vals[0] + vals[1]; break;
          case '-': result = vals[0] - vals[1]; break;
          case '*': result = vals[0] * vals[1]; break;
          default: throw new Error(`Unknown primop: ${expr.op}`);
        }
        return this.heap.alloc(new HPrim(result));
      }
    }
  }

  run(expr) {
    const addr = this.eval(expr);
    return this.heap.read(addr);
  }
}

export {
  HLam, HCon, HThunk, HPrim, HIndirection, Heap,
  EVar, ELam, EApp, ECon, ECase, ELet, ENum, EPrim,
  STGMachine
};
