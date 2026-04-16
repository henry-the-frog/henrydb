/**
 * Region-Based Memory Management (Tofte-Talpin 1997)
 * 
 * Allocate values into lexically-scoped regions. When a region goes out of scope,
 * ALL its allocations are freed at once — no garbage collector needed.
 * 
 * Key ideas:
 * - letregion ρ in e: creates region ρ, evaluates e, deallocates ρ
 * - Every allocation targets a specific region
 * - Region inference: determine which region each allocation goes to
 * - Regions are stack-allocated: LIFO discipline
 */

// ============================================================
// Region runtime
// ============================================================

let regionIdCounter = 0;

class Region {
  constructor(name) {
    this.id = regionIdCounter++;
    this.name = name;
    this.allocations = [];
    this.alive = true;
  }

  alloc(value) {
    if (!this.alive) throw new Error(`Allocation in dead region: ${this.name}`);
    const ref = { value, region: this, idx: this.allocations.length };
    this.allocations.push(ref);
    return ref;
  }

  dealloc() {
    this.alive = false;
    const count = this.allocations.length;
    this.allocations = [];
    return count; // Number of freed allocations
  }

  get size() { return this.allocations.length; }
}

function deref(ref) {
  if (!ref.region.alive) throw new Error(`Dangling reference: region ${ref.region.name} is dead`);
  return ref.value;
}

function assign(ref, value) {
  if (!ref.region.alive) throw new Error(`Write to dead region: ${ref.region.name}`);
  ref.value = value;
}

/**
 * letregion: create a region, run body, deallocate
 */
function letregion(name, body) {
  const region = new Region(name);
  try {
    const result = body(region);
    return { result, freed: region.dealloc() };
  } catch (e) {
    region.dealloc();
    throw e;
  }
}

// ============================================================
// Region inference (simplified)
// ============================================================

class RVar { constructor(name) { this.tag = 'RVar'; this.name = name; } }
class RAlloc { constructor(value, region) { this.tag = 'RAlloc'; this.value = value; this.region = region; } }
class RLet { constructor(name, init, body) { this.tag = 'RLet'; this.name = name; this.init = init; this.body = body; } }
class RLetRegion { constructor(region, body) { this.tag = 'RLetRegion'; this.region = region; this.body = body; } }
class RDeref { constructor(ref) { this.tag = 'RDeref'; this.ref = ref; } }
class RNum { constructor(n) { this.tag = 'RNum'; this.n = n; } }

/**
 * Annotate allocations with regions
 * Simple strategy: each let binding allocates in the nearest enclosing region
 */
function annotateRegions(expr, currentRegion = 'ρ0') {
  switch (expr.tag) {
    case 'RNum': return { ...expr, region: currentRegion };
    case 'RVar': return expr;
    case 'RAlloc': return { ...expr, region: currentRegion };
    case 'RLet': {
      const init = annotateRegions(expr.init, currentRegion);
      const body = annotateRegions(expr.body, currentRegion);
      return { tag: 'RLet', name: expr.name, init, body, region: currentRegion };
    }
    case 'RLetRegion': {
      const body = annotateRegions(expr.body, expr.region);
      return { tag: 'RLetRegion', region: expr.region, body };
    }
    default: return expr;
  }
}

/**
 * Collect all regions used by an expression
 */
function usedRegions(expr) {
  const regions = new Set();
  function walk(e) {
    if (!e) return;
    if (e.region) regions.add(e.region);
    if (e.init) walk(e.init);
    if (e.body) walk(e.body);
    if (e.ref) walk(e.ref);
  }
  walk(expr);
  return regions;
}

/**
 * Region stack simulator: tracks active regions
 */
class RegionStack {
  constructor() {
    this.stack = [];
    this.totalAllocated = 0;
    this.totalFreed = 0;
  }

  push(name) {
    const region = new Region(name);
    this.stack.push(region);
    return region;
  }

  pop() {
    const region = this.stack.pop();
    if (!region) throw new Error('Region stack underflow');
    this.totalFreed += region.dealloc();
    return region;
  }

  current() {
    return this.stack[this.stack.length - 1];
  }

  alloc(value) {
    const region = this.current();
    if (!region) throw new Error('No active region');
    this.totalAllocated++;
    return region.alloc(value);
  }

  get depth() { return this.stack.length; }
}

export {
  Region, deref, assign, letregion,
  RVar, RAlloc, RLet, RLetRegion, RDeref, RNum,
  annotateRegions, usedRegions, RegionStack
};
