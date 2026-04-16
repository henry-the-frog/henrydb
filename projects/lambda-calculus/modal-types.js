/**
 * Modal Types: □A (necessarily/box) and ◇A (possibly/diamond)
 * 
 * From modal logic applied to type theory:
 * - □A: "A is true at all possible worlds" (compile-time, staged computation)
 * - ◇A: "A is true at some possible world" (runtime, effectful)
 * 
 * Applications:
 * 1. Staged computation (MetaML): □A = code that produces A at next stage
 * 2. Distributed computing: □A = available everywhere, ◇A = available somewhere
 * 3. Information flow: □A = public, ◇A = secret
 */

// Types
class TBox { constructor(inner) { this.tag = 'TBox'; this.inner = inner; } toString() { return `□${this.inner}`; } }
class TDia { constructor(inner) { this.tag = 'TDia'; this.inner = inner; } toString() { return `◇${this.inner}`; } }
class TFun { constructor(p, r) { this.tag = 'TFun'; this.param = p; this.ret = r; } toString() { return `(${this.param} → ${this.ret})`; } }
class TBase { constructor(name) { this.tag = 'TBase'; this.name = name; } toString() { return this.name; } }

const tInt = new TBase('Int');
const tStr = new TBase('Str');
const tBool = new TBase('Bool');

// Values
class VBox { constructor(value) { this.tag = 'VBox'; this.value = value; } }
class VDia { constructor(value, world) { this.tag = 'VDia'; this.value = value; this.world = world; } }

// ============================================================
// Staged Computation (□ as code generation)
// ============================================================

class Code {
  constructor(generate) { this.generate = generate; } // () → string (code)
}

function quote(value) { return new Code(() => JSON.stringify(value)); }
function splice(code) { return JSON.parse(code.generate()); }
function liftCode(fn, ...codes) {
  return new Code(() => {
    const args = codes.map(c => c.generate());
    return `(${fn})(${args.join(', ')})`;
  });
}

// ============================================================
// World-indexed types (Kripke semantics)
// ============================================================

class World {
  constructor(name, values = new Map()) {
    this.name = name;
    this.values = values;
  }

  get(key) { return this.values.get(key); }
  set(key, value) { return new World(this.name, new Map([...this.values, [key, value]])); }
}

class WorldSystem {
  constructor() {
    this.worlds = new Map();
    this.accessibility = new Map(); // world → Set<world>
  }

  addWorld(name) {
    const w = new World(name);
    this.worlds.set(name, w);
    return w;
  }

  addEdge(from, to) {
    if (!this.accessibility.has(from)) this.accessibility.set(from, new Set());
    this.accessibility.get(from).add(to);
  }

  accessibleFrom(worldName) {
    return this.accessibility.get(worldName) || new Set();
  }

  /**
   * □A: A holds at ALL accessible worlds
   */
  checkBox(worldName, predicate) {
    const accessible = this.accessibleFrom(worldName);
    if (accessible.size === 0) return true; // Vacuously true
    return [...accessible].every(w => predicate(this.worlds.get(w)));
  }

  /**
   * ◇A: A holds at SOME accessible world
   */
  checkDiamond(worldName, predicate) {
    const accessible = this.accessibleFrom(worldName);
    return [...accessible].some(w => predicate(this.worlds.get(w)));
  }
}

// ============================================================
// Information flow (□ = public, ◇ = secret)
// ============================================================

class Label {
  constructor(level) { this.level = level; } // 'public' | 'secret'
}

const PUBLIC = new Label('public');
const SECRET = new Label('secret');

class Labeled {
  constructor(value, label) { this.value = value; this.label = label; }

  map(f) { return new Labeled(f(this.value), this.label); }

  // Can only bind to same or higher security
  bind(f) {
    const result = f(this.value);
    if (this.label.level === 'secret' && result.label.level === 'public') {
      throw new Error('Information flow violation: secret → public');
    }
    return result;
  }
}

function publicVal(v) { return new Labeled(v, PUBLIC); }
function secretVal(v) { return new Labeled(v, SECRET); }

export {
  TBox, TDia, TFun, TBase, tInt, tStr, tBool,
  VBox, VDia,
  Code, quote, splice, liftCode,
  World, WorldSystem,
  Label, PUBLIC, SECRET, Labeled, publicVal, secretVal
};
