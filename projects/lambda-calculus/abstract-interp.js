/**
 * Abstract Interpretation
 * 
 * Approximates program behavior by computing over abstract domains
 * instead of concrete values. Sound: if abstract analysis says "safe",
 * concrete execution is guaranteed safe.
 * 
 * Abstract domains:
 * 1. Sign domain: {Neg, Zero, Pos, ⊤ (any), ⊥ (none)}
 * 2. Interval domain: [lo, hi] (range of possible values)
 * 3. Constant domain: {const(n), ⊤ (unknown), ⊥ (unreachable)}
 * 
 * Based on Cousot & Cousot (1977).
 */

// ============================================================
// SIGN DOMAIN
// ============================================================

const Sign = {
  BOT: 'bot',   // ⊥ — unreachable
  NEG: 'neg',   // Negative
  ZERO: 'zero', // Zero
  POS: 'pos',   // Positive
  TOP: 'top',   // Any (unknown sign)
};

const SignDomain = {
  name: 'Sign',
  
  // Abstract a concrete value
  abstract: (n) => {
    if (n < 0) return Sign.NEG;
    if (n === 0) return Sign.ZERO;
    return Sign.POS;
  },
  
  // Join (least upper bound)
  join: (a, b) => {
    if (a === Sign.BOT) return b;
    if (b === Sign.BOT) return a;
    if (a === b) return a;
    return Sign.TOP;
  },
  
  // Meet (greatest lower bound)
  meet: (a, b) => {
    if (a === Sign.TOP) return b;
    if (b === Sign.TOP) return a;
    if (a === b) return a;
    return Sign.BOT;
  },
  
  // Abstract operations
  add: (a, b) => {
    if (a === Sign.BOT || b === Sign.BOT) return Sign.BOT;
    if (a === Sign.TOP || b === Sign.TOP) return Sign.TOP;
    if (a === Sign.ZERO) return b;
    if (b === Sign.ZERO) return a;
    if (a === Sign.POS && b === Sign.POS) return Sign.POS;
    if (a === Sign.NEG && b === Sign.NEG) return Sign.NEG;
    return Sign.TOP; // pos + neg = unknown
  },
  
  sub: (a, b) => {
    if (a === Sign.BOT || b === Sign.BOT) return Sign.BOT;
    if (a === Sign.TOP || b === Sign.TOP) return Sign.TOP;
    if (b === Sign.ZERO) return a;
    if (a === Sign.ZERO) {
      if (b === Sign.POS) return Sign.NEG;
      if (b === Sign.NEG) return Sign.POS;
    }
    if (a === Sign.POS && b === Sign.NEG) return Sign.POS;
    if (a === Sign.NEG && b === Sign.POS) return Sign.NEG;
    return Sign.TOP; // same sign subtraction = unknown
  },
  
  mul: (a, b) => {
    if (a === Sign.BOT || b === Sign.BOT) return Sign.BOT;
    if (a === Sign.ZERO || b === Sign.ZERO) return Sign.ZERO;
    if (a === Sign.TOP || b === Sign.TOP) return Sign.TOP;
    if ((a === Sign.POS && b === Sign.POS) || (a === Sign.NEG && b === Sign.NEG)) return Sign.POS;
    return Sign.NEG; // different signs
  },
  
  div: (a, b) => {
    if (a === Sign.BOT || b === Sign.BOT) return Sign.BOT;
    if (b === Sign.ZERO) return Sign.BOT; // Division by zero
    if (a === Sign.ZERO) return Sign.ZERO;
    if (a === Sign.TOP || b === Sign.TOP) return Sign.TOP;
    if ((a === Sign.POS && b === Sign.POS) || (a === Sign.NEG && b === Sign.NEG)) return Sign.POS;
    return Sign.NEG;
  },
};

// ============================================================
// INTERVAL DOMAIN
// ============================================================

class Interval {
  constructor(lo, hi) { this.lo = lo; this.hi = hi; }
  toString() { return `[${this.lo}, ${this.hi}]`; }
  static bot() { return new Interval(Infinity, -Infinity); }
  static top() { return new Interval(-Infinity, Infinity); }
  static exact(n) { return new Interval(n, n); }
  isBot() { return this.lo > this.hi; }
  contains(n) { return n >= this.lo && n <= this.hi; }
}

const IntervalDomain = {
  name: 'Interval',
  
  abstract: (n) => Interval.exact(n),
  
  join: (a, b) => {
    if (a.isBot()) return b;
    if (b.isBot()) return a;
    return new Interval(Math.min(a.lo, b.lo), Math.max(a.hi, b.hi));
  },
  
  meet: (a, b) => {
    return new Interval(Math.max(a.lo, b.lo), Math.min(a.hi, b.hi));
  },
  
  add: (a, b) => {
    if (a.isBot() || b.isBot()) return Interval.bot();
    return new Interval(a.lo + b.lo, a.hi + b.hi);
  },
  
  sub: (a, b) => {
    if (a.isBot() || b.isBot()) return Interval.bot();
    return new Interval(a.lo - b.hi, a.hi - b.lo);
  },
  
  mul: (a, b) => {
    if (a.isBot() || b.isBot()) return Interval.bot();
    const products = [a.lo*b.lo, a.lo*b.hi, a.hi*b.lo, a.hi*b.hi];
    return new Interval(Math.min(...products), Math.max(...products));
  },
  
  widen: (old, new_) => {
    // Widening: accelerate convergence
    const lo = new_.lo < old.lo ? -Infinity : old.lo;
    const hi = new_.hi > old.hi ? Infinity : old.hi;
    return new Interval(lo, hi);
  },
};

// ============================================================
// CONSTANT DOMAIN
// ============================================================

const Const = {
  BOT: { tag: 'bot' },
  TOP: { tag: 'top' },
  val: (n) => ({ tag: 'const', value: n }),
};

const ConstDomain = {
  name: 'Constant',
  
  abstract: (n) => Const.val(n),
  
  join: (a, b) => {
    if (a.tag === 'bot') return b;
    if (b.tag === 'bot') return a;
    if (a.tag === 'const' && b.tag === 'const' && a.value === b.value) return a;
    return Const.TOP;
  },
  
  meet: (a, b) => {
    if (a.tag === 'top') return b;
    if (b.tag === 'top') return a;
    if (a.tag === 'const' && b.tag === 'const' && a.value === b.value) return a;
    return Const.BOT;
  },
  
  add: (a, b) => {
    if (a.tag === 'bot' || b.tag === 'bot') return Const.BOT;
    if (a.tag === 'const' && b.tag === 'const') return Const.val(a.value + b.value);
    return Const.TOP;
  },
  
  sub: (a, b) => {
    if (a.tag === 'bot' || b.tag === 'bot') return Const.BOT;
    if (a.tag === 'const' && b.tag === 'const') return Const.val(a.value - b.value);
    return Const.TOP;
  },
  
  mul: (a, b) => {
    if (a.tag === 'bot' || b.tag === 'bot') return Const.BOT;
    if (a.tag === 'const' && b.tag === 'const') return Const.val(a.value * b.value);
    // Special: 0 * anything = 0
    if ((a.tag === 'const' && a.value === 0) || (b.tag === 'const' && b.value === 0)) return Const.val(0);
    return Const.TOP;
  },
};

// ============================================================
// ABSTRACT INTERPRETER
// ============================================================

class AbstractInterpreter {
  constructor(domain) {
    this.domain = domain;
    this.env = new Map(); // var → abstract value
  }

  /**
   * Evaluate a simple expression language abstractly
   * expr = num(n) | var(x) | add(l,r) | sub(l,r) | mul(l,r) | let(x,v,body)
   */
  eval(expr) {
    switch (expr.tag) {
      case 'num': return this.domain.abstract(expr.n);
      case 'var': return this.env.get(expr.name) || (this.domain.name === 'Sign' ? Sign.TOP : this.domain.name === 'Interval' ? Interval.top() : Const.TOP);
      case 'add': return this.domain.add(this.eval(expr.left), this.eval(expr.right));
      case 'sub': return this.domain.sub(this.eval(expr.left), this.eval(expr.right));
      case 'mul': return this.domain.mul(this.eval(expr.left), this.eval(expr.right));
      case 'div': return this.domain.div?.(this.eval(expr.left), this.eval(expr.right)) || Const.TOP;
      case 'let': {
        const val = this.eval(expr.value);
        const saved = this.env.get(expr.name);
        this.env.set(expr.name, val);
        const result = this.eval(expr.body);
        if (saved !== undefined) this.env.set(expr.name, saved);
        else this.env.delete(expr.name);
        return result;
      }
      default: throw new Error(`Abstract: unknown expr tag ${expr.tag}`);
    }
  }
}

// Convenience constructors
const num = n => ({ tag: 'num', n });
const vr = name => ({ tag: 'var', name });
const add = (l, r) => ({ tag: 'add', left: l, right: r });
const sub = (l, r) => ({ tag: 'sub', left: l, right: r });
const mul = (l, r) => ({ tag: 'mul', left: l, right: r });
const div = (l, r) => ({ tag: 'div', left: l, right: r });
const let_ = (name, value, body) => ({ tag: 'let', name, value, body });

export {
  Sign, SignDomain, Interval, IntervalDomain, Const, ConstDomain,
  AbstractInterpreter,
  num, vr, add, sub, mul, div, let_
};
