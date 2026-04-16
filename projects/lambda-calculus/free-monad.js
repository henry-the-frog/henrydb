/**
 * Free Monad
 * 
 * Build computation descriptions (ASTs), then interpret them later.
 * Separates "what to do" from "how to do it".
 * 
 * Free f a = Pure a | Free (f (Free f a))
 * 
 * Used for: DSLs, testing (mock interpreter), effects.
 */

class Pure { constructor(value) { this.tag = 'Pure'; this.value = value; } }
class Free { constructor(functor) { this.tag = 'Free'; this.functor = functor; } }

// return :: a → Free f a
function freturn(value) { return new Pure(value); }

// bind :: Free f a → (a → Free f b) → Free f b
function fbind(ma, fn) {
  if (ma.tag === 'Pure') return fn(ma.value);
  // Need to map fn over the functor
  return new Free({ ...ma.functor, next: v => fbind(ma.functor.next(v), fn) });
}

// liftF :: f a → Free f a
function liftF(command) {
  return new Free(command);
}

// ============================================================
// Example DSL: Key-Value Store
// ============================================================

// Commands
function Get(key, next) { return { tag: 'Get', key, next }; }
function Put(key, value, next) { return { tag: 'Put', key, value, next }; }
function Delete(key, next) { return { tag: 'Delete', key, next }; }

// Smart constructors (lift into Free)
function get(key) { return liftF(Get(key, v => new Pure(v))); }
function put(key, value) { return liftF(Put(key, value, () => new Pure(null))); }
function del(key) { return liftF(Delete(key, () => new Pure(null))); }

// ============================================================
// Interpreters
// ============================================================

// In-memory interpreter
function runInMemory(program, store = new Map()) {
  let current = program;
  
  while (current.tag === 'Free') {
    const cmd = current.functor;
    switch (cmd.tag) {
      case 'Get': {
        const value = store.get(cmd.key) ?? null;
        current = cmd.next(value);
        break;
      }
      case 'Put': {
        store.set(cmd.key, cmd.value);
        current = cmd.next();
        break;
      }
      case 'Delete': {
        store.delete(cmd.key);
        current = cmd.next();
        break;
      }
      default:
        throw new Error(`Unknown command: ${cmd.tag}`);
    }
  }
  
  return { value: current.value, store };
}

// Logging interpreter (records operations)
function runWithLogging(program) {
  const log = [];
  let current = program;
  const store = new Map();
  
  while (current.tag === 'Free') {
    const cmd = current.functor;
    switch (cmd.tag) {
      case 'Get': {
        const value = store.get(cmd.key) ?? null;
        log.push(`GET ${cmd.key} → ${value}`);
        current = cmd.next(value);
        break;
      }
      case 'Put': {
        log.push(`PUT ${cmd.key} = ${cmd.value}`);
        store.set(cmd.key, cmd.value);
        current = cmd.next();
        break;
      }
      case 'Delete': {
        log.push(`DELETE ${cmd.key}`);
        store.delete(cmd.key);
        current = cmd.next();
        break;
      }
    }
  }
  
  return { value: current.value, log, store };
}

export { Pure, Free, freturn, fbind, liftF, get, put, del, runInMemory, runWithLogging };
