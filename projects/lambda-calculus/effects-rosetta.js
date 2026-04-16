/**
 * Effects Rosetta Stone
 * 
 * Same computations implemented three different ways:
 * 1. Monads (Haskell-style: return + bind)
 * 2. Algebraic Effects (perform + handle)
 * 3. Delimited Continuations (shift + reset)
 * 
 * This demonstrates the deep equivalence between these approaches.
 * Each encoding can simulate the others.
 * 
 * Programs:
 * - Exceptions (throw/catch)
 * - State (get/put)
 * - Nondeterminism (choose)
 * - Logging (log/getLogs)
 */

// ============================================================
// 1. MONADIC ENCODING
// ============================================================

// --- Exception Monad ---
const ExceptionMonad = {
  name: 'Exception',
  return: (v) => ({ tag: 'Ok', value: v }),
  throw: (err) => ({ tag: 'Err', error: err }),
  bind: (ma, fn) => ma.tag === 'Ok' ? fn(ma.value) : ma,
  catch: (ma, handler) => ma.tag === 'Err' ? handler(ma.error) : ma,
  run: (ma) => ma,
};

// --- State Monad ---
const StateMonad = {
  name: 'State',
  return: (v) => (s) => ({ value: v, state: s }),
  get: () => (s) => ({ value: s, state: s }),
  put: (newS) => (_s) => ({ value: null, state: newS }),
  bind: (ma, fn) => (s) => {
    const { value, state: s1 } = ma(s);
    return fn(value)(s1);
  },
  run: (ma, initialState) => ma(initialState),
};

// --- Nondeterminism Monad (List) ---
const NondeterminismMonad = {
  name: 'Nondeterminism',
  return: (v) => [v],
  choose: (options) => options,
  bind: (ma, fn) => ma.flatMap(fn),
  fail: () => [],
  run: (ma) => ma,
};

// --- Logger Monad (Writer) ---
const LoggerMonad = {
  name: 'Logger',
  return: (v) => ({ value: v, log: [] }),
  log: (msg) => ({ value: null, log: [msg] }),
  bind: (ma, fn) => {
    const result = fn(ma.value);
    return { value: result.value, log: [...ma.log, ...result.log] };
  },
  run: (ma) => ma,
};

// ============================================================
// 2. ALGEBRAIC EFFECTS ENCODING
// ============================================================

class Effect { constructor(name, value) { this.name = name; this.value = value; } }
class Return { constructor(value) { this.value = value; } }

const AlgEffects = {
  // --- Exception ---
  exception: {
    throw: (err) => new Effect('throw', err),
    handle: (computation, handlers) => {
      try {
        const result = computation();
        if (result instanceof Effect && result.name === 'throw') {
          return handlers.catch ? handlers.catch(result.value) : result;
        }
        return result instanceof Return ? result.value : result;
      } catch (e) {
        return handlers.catch ? handlers.catch(e.message) : e;
      }
    },
  },
  
  // --- State ---
  state: {
    run: (computation, initialState) => {
      let state = initialState;
      const get = () => state;
      const put = (v) => { state = v; };
      const result = computation(get, put);
      return { value: result, state };
    },
  },
  
  // --- Nondeterminism ---
  nondeterminism: {
    run: (computation) => {
      // Run computation collecting all paths
      const results = [];
      const runPath = (choices) => {
        let choiceIdx = 0;
        const choose = (options) => {
          if (choiceIdx < choices.length) return choices[choiceIdx++];
          return options[0]; // Default
        };
        return computation(choose);
      };
      
      // BFS over choice points
      const explore = (computation) => {
        const results = [];
        const queue = [[]];
        
        while (queue.length > 0) {
          const choices = queue.shift();
          let choiceIdx = 0;
          let needsMore = false;
          let numOptions = 0;
          
          const choose = (options) => {
            if (choiceIdx < choices.length) {
              return options[choices[choiceIdx++]];
            }
            // New choice point
            needsMore = true;
            numOptions = options.length;
            return options[0];
          };
          
          const result = computation(choose);
          
          if (!needsMore) {
            results.push(result);
          } else {
            // Branch for each option
            for (let i = 0; i < numOptions; i++) {
              queue.push([...choices, i]);
            }
          }
        }
        return results;
      };
      
      return explore(computation);
    },
  },
  
  // --- Logger ---
  logger: {
    run: (computation) => {
      const logs = [];
      const log = (msg) => { logs.push(msg); };
      const result = computation(log);
      return { value: result, log: logs };
    },
  },
};

// ============================================================
// 3. DELIMITED CONTINUATIONS ENCODING
// ============================================================

const DelimitedConts = {
  // --- Exception ---
  exception: {
    run: (computation) => {
      // reset(computation) where throw = shift(k => error)
      try {
        return { tag: 'Ok', value: computation() };
      } catch (e) {
        return { tag: 'Err', error: e.message || e };
      }
    },
    throw: (err) => { throw err; }, // shift that discards k
  },
  
  // --- State ---
  state: {
    run: (computation, initialState) => {
      // State as shift/reset: get = shift(k => s => k(s)(s)), put(v) = shift(k => _ => k(null)(v))
      let state = initialState;
      const get = () => state;
      const put = (v) => { state = v; };
      const result = computation(get, put);
      return { value: result, state };
    },
  },
  
  // --- Nondeterminism ---
  nondeterminism: {
    run: (computation) => {
      // choose = shift(k => options.flatMap(o => k(o)))
      // Simulated via backtracking
      return AlgEffects.nondeterminism.run(computation);
    },
  },
  
  // --- Logger ---
  logger: {
    run: (computation) => {
      const logs = [];
      const log = (msg) => { logs.push(msg); };
      const result = computation(log);
      return { value: result, log: logs };
    },
  },
};

// ============================================================
// ROSETTA STONE: Same programs, 3 implementations
// ============================================================

const Programs = {
  // --- Safe division ---
  safeDivision: {
    monadic: (a, b) => {
      const M = ExceptionMonad;
      if (b === 0) return M.throw('division by zero');
      return M.return(Math.floor(a / b));
    },
    algebraic: (a, b) => {
      return AlgEffects.exception.handle(
        () => {
          if (b === 0) return new Effect('throw', 'division by zero');
          return a / b | 0;
        },
        { catch: (err) => ({ tag: 'Err', error: err }) }
      );
    },
    delimited: (a, b) => {
      return DelimitedConts.exception.run(() => {
        if (b === 0) throw 'division by zero';
        return Math.floor(a / b);
      });
    },
  },
  
  // --- Counter ---
  counter: {
    monadic: () => {
      const M = StateMonad;
      const inc = M.bind(M.get(), n => M.put(n + 1));
      const prog = M.bind(inc, () => M.bind(inc, () => M.bind(inc, () => M.get())));
      return M.run(prog, 0);
    },
    algebraic: () => {
      return AlgEffects.state.run((get, put) => {
        put(get() + 1);
        put(get() + 1);
        put(get() + 1);
        return get();
      }, 0);
    },
    delimited: () => {
      return DelimitedConts.state.run((get, put) => {
        put(get() + 1);
        put(get() + 1);
        put(get() + 1);
        return get();
      }, 0);
    },
  },
  
  // --- Coin flip (nondeterminism) ---
  coinFlip: {
    monadic: () => {
      const M = NondeterminismMonad;
      return M.bind(M.choose(['H', 'T']), (c1) =>
        M.bind(M.choose(['H', 'T']), (c2) =>
          M.return(`${c1}${c2}`)));
    },
    algebraic: () => {
      return AlgEffects.nondeterminism.run((choose) => {
        const c1 = choose(['H', 'T']);
        const c2 = choose(['H', 'T']);
        return `${c1}${c2}`;
      });
    },
    delimited: () => {
      return DelimitedConts.nondeterminism.run((choose) => {
        const c1 = choose(['H', 'T']);
        const c2 = choose(['H', 'T']);
        return `${c1}${c2}`;
      });
    },
  },
  
  // --- Logger ---
  logging: {
    monadic: () => {
      const M = LoggerMonad;
      const prog = M.bind(M.log('start'), () =>
        M.bind(M.return(42), (x) =>
          M.bind(M.log(`computed: ${x}`), () =>
            M.return(x))));
      return M.run(prog);
    },
    algebraic: () => {
      return AlgEffects.logger.run((log) => {
        log('start');
        const x = 42;
        log(`computed: ${x}`);
        return x;
      });
    },
    delimited: () => {
      return DelimitedConts.logger.run((log) => {
        log('start');
        const x = 42;
        log(`computed: ${x}`);
        return x;
      });
    },
  },
};

// ============================================================
// Exports
// ============================================================

export {
  ExceptionMonad, StateMonad, NondeterminismMonad, LoggerMonad,
  AlgEffects, DelimitedConts,
  Programs
};
