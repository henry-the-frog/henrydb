/**
 * Lazy Evaluation: Thunks, WHNF, and Sharing
 * 
 * Call-by-need: evaluate only when forced, memoize results.
 * 
 * Three-state thunks:
 * - Unevaluated: (env, expr) — closure waiting to be evaluated
 * - In progress: being evaluated (detect infinite loops)
 * - Evaluated: cached result (sharing — same thunk returns same value)
 */

class Thunk {
  constructor(computation) {
    this.computation = computation;
    this.state = 'unevaluated'; // 'unevaluated' | 'in-progress' | 'evaluated'
    this.value = null;
    this.forceCount = 0;
  }

  force() {
    this.forceCount++;
    switch (this.state) {
      case 'evaluated': return this.value;
      case 'in-progress': throw new Error('Infinite loop: thunk forced during own evaluation');
      case 'unevaluated':
        this.state = 'in-progress';
        try {
          this.value = this.computation();
          this.state = 'evaluated';
          this.computation = null; // GC the closure
          return this.value;
        } catch (e) {
          this.state = 'unevaluated'; // Reset on error
          throw e;
        }
    }
  }

  get isEvaluated() { return this.state === 'evaluated'; }
}

function thunk(computation) { return new Thunk(computation); }
function force(t) { return t instanceof Thunk ? t.force() : t; }

// ============================================================
// Lazy list (stream with sharing)
// ============================================================

class LazyList {
  constructor(headThunk, tailThunk) {
    this.headThunk = headThunk;
    this.tailThunk = tailThunk;
  }

  get head() { return force(this.headThunk); }
  get tail() { return force(this.tailThunk); }

  take(n) {
    if (n <= 0) return [];
    const result = [this.head];
    let current = this.tail;
    for (let i = 1; i < n && current; i++) {
      result.push(current.head);
      current = current.tail;
    }
    return result;
  }

  map(f) {
    return new LazyList(
      thunk(() => f(this.head)),
      thunk(() => this.tail ? this.tail.map(f) : null)
    );
  }

  filter(pred) {
    const h = this.head;
    if (pred(h)) {
      return new LazyList(
        thunk(() => h),
        thunk(() => this.tail ? this.tail.filter(pred) : null)
      );
    }
    return this.tail ? this.tail.filter(pred) : null;
  }
}

function lazyRange(start, step = 1) {
  return new LazyList(
    thunk(() => start),
    thunk(() => lazyRange(start + step, step))
  );
}

function lazyFrom(arr) {
  if (arr.length === 0) return null;
  return new LazyList(
    thunk(() => arr[0]),
    thunk(() => lazyFrom(arr.slice(1)))
  );
}

// ============================================================
// WHNF (Weak Head Normal Form)
// ============================================================

// A value is in WHNF if its outermost constructor is known
function isWHNF(value) {
  if (value instanceof Thunk) return value.isEvaluated;
  if (value instanceof LazyList) return true; // Constructor visible
  return true; // Primitives are always in WHNF
}

function toWHNF(value) {
  if (value instanceof Thunk) return value.force();
  return value;
}

// ============================================================
// Call-by-need evaluator
// ============================================================

function lazyEval(expr, env = new Map()) {
  switch (expr.tag) {
    case 'Num': return expr.n;
    case 'Var': {
      const val = env.get(expr.name);
      if (val === undefined) throw new Error(`Unbound: ${expr.name}`);
      return force(val);
    }
    case 'Lam': return { tag: 'Closure', var: expr.var, body: expr.body, env };
    case 'App': {
      const fn = lazyEval(expr.fn, env);
      if (fn.tag !== 'Closure') throw new Error('Not a function');
      const argThunk = thunk(() => lazyEval(expr.arg, env));
      return lazyEval(fn.body, new Map([...fn.env, [fn.var, argThunk]]));
    }
    case 'Let': {
      const valThunk = thunk(() => lazyEval(expr.init, new Map([...env, [expr.var, valThunk]])));
      return lazyEval(expr.body, new Map([...env, [expr.var, valThunk]]));
    }
    case 'Add': return lazyEval(expr.left, env) + lazyEval(expr.right, env);
    case 'If0': return lazyEval(expr.cond, env) === 0 ? lazyEval(expr.then, env) : lazyEval(expr.else, env);
    default: throw new Error(`Unknown: ${expr.tag}`);
  }
}

export {
  Thunk, thunk, force,
  LazyList, lazyRange, lazyFrom,
  isWHNF, toWHNF, lazyEval
};
