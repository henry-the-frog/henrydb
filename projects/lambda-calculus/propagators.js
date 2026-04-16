/**
 * Propagators: Bidirectional constraint propagation
 * Cells hold values, propagators spread information between cells.
 */

class Cell {
  constructor(name, value = null) { this.name = name; this.value = value; this.propagators = []; }
  addPropagator(p) { this.propagators.push(p); }
  update(newValue) {
    if (this.value !== null && this.value !== newValue) throw new Error(`Contradiction in ${this.name}: ${this.value} vs ${newValue}`);
    if (this.value === newValue) return false;
    this.value = newValue;
    return true;
  }
}

class PropNet {
  constructor() { this.cells = new Map(); this.propagators = []; }
  
  cell(name, value = null) {
    const c = new Cell(name, value);
    this.cells.set(name, c);
    return c;
  }
  
  propagator(inputs, output, fn) {
    const p = { inputs, output, fn };
    this.propagators.push(p);
    for (const i of inputs) i.addPropagator(p);
  }
  
  run(maxIter = 100) {
    for (let i = 0; i < maxIter; i++) {
      let changed = false;
      for (const p of this.propagators) {
        const vals = p.inputs.map(c => c.value);
        if (vals.some(v => v === null)) continue;
        const result = p.fn(...vals);
        if (result !== null && p.output.update(result)) changed = true;
      }
      if (!changed) break;
    }
  }
  
  get(name) { return this.cells.get(name)?.value; }
}

// Adder propagator (bidirectional)
function addConstraint(net, a, b, sum) {
  net.propagator([a, b], sum, (x, y) => x + y);
  net.propagator([sum, b], a, (s, y) => s - y);
  net.propagator([sum, a], b, (s, x) => s - x);
}

function mulConstraint(net, a, b, prod) {
  net.propagator([a, b], prod, (x, y) => x * y);
  net.propagator([prod, b], a, (p, y) => y !== 0 ? p / y : null);
  net.propagator([prod, a], b, (p, x) => x !== 0 ? p / x : null);
}

export { Cell, PropNet, addConstraint, mulConstraint };
