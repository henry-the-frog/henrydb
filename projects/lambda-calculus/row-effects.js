/**
 * Row-Level Effects (Extensible Effect Rows)
 * 
 * Effects form a row: {State, IO, Exception | ε}
 * Functions declare which effects they need in their type.
 * Handlers peel off one effect at a time.
 * 
 * This is the Koka/Frank/Eff approach to algebraic effects.
 */

class EffectRow {
  constructor(effects = [], rest = null) {
    this.effects = effects; // [{name, ops}]
    this.rest = rest;       // EffectRow variable (for polymorphism)
  }
  
  has(name) { return this.effects.some(e => e.name === name); }
  
  get(name) { return this.effects.find(e => e.name === name); }
  
  add(effect) {
    if (this.has(effect.name)) return this; // Already present
    return new EffectRow([...this.effects, effect], this.rest);
  }
  
  remove(name) {
    return new EffectRow(this.effects.filter(e => e.name !== name), this.rest);
  }
  
  isEmpty() { return this.effects.length === 0 && !this.rest; }
  
  toString() {
    const effs = this.effects.map(e => e.name).join(', ');
    return this.rest ? `{${effs} | ${this.rest}}` : `{${effs}}`;
  }
}

// Effect definitions
function effect(name, ops) { return { name, ops }; }

const stateEffect = effect('State', ['get', 'put']);
const ioEffect = effect('IO', ['print', 'read']);
const exceptionEffect = effect('Exception', ['throw']);
const asyncEffect = effect('Async', ['await', 'spawn']);
const logEffect = effect('Log', ['log', 'debug']);

// ============================================================
// Effect handler
// ============================================================

class Handler {
  constructor(effectName, handlers, returnHandler = x => x) {
    this.effectName = effectName;
    this.handlers = handlers;       // Map<opName, (args, resume) => result>
    this.returnHandler = returnHandler;
  }
}

class EffectSystem {
  constructor() {
    this.handlers = [];
  }

  handle(effectName, handlers, returnHandler = x => x) {
    this.handlers.push(new Handler(effectName, handlers, returnHandler));
    return this;
  }

  /**
   * Perform an operation
   */
  perform(effectName, opName, ...args) {
    const handler = this.handlers.findLast(h => h.effectName === effectName);
    if (!handler) throw new Error(`Unhandled effect: ${effectName}.${opName}`);
    
    const opHandler = handler.handlers.get(opName);
    if (!opHandler) throw new Error(`Unhandled operation: ${effectName}.${opName}`);
    
    return opHandler(args);
  }

  /**
   * Run a computation with handlers
   */
  run(computation) {
    return computation(this);
  }
}

// ============================================================
// Row subtyping: {A, B} <: {A, B, C}
// ============================================================

function isSubRow(row1, row2) {
  return row1.effects.every(e => row2.has(e.name));
}

function rowUnion(row1, row2) {
  let result = new EffectRow([...row1.effects], row1.rest || row2.rest);
  for (const eff of row2.effects) {
    if (!result.has(eff.name)) result = result.add(eff);
  }
  return result;
}

function rowDifference(row1, row2) {
  return new EffectRow(row1.effects.filter(e => !row2.has(e.name)), row1.rest);
}

export {
  EffectRow, effect, Handler, EffectSystem,
  stateEffect, ioEffect, exceptionEffect, asyncEffect, logEffect,
  isSubRow, rowUnion, rowDifference
};
