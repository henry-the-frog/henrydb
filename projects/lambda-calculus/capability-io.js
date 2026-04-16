/**
 * Capability-Safe IO
 * 
 * Track what side effects a computation can perform through its type.
 * Inspired by: Wasm capabilities, capability-based security, object-capability model.
 * 
 * A function gets only the capabilities it needs:
 *   readFile : (FileRead cap) → Path → String
 *   No FileRead cap? Can't read files. Period.
 */

// Capabilities
const CAP_FILE_READ = Symbol('FileRead');
const CAP_FILE_WRITE = Symbol('FileWrite');
const CAP_NETWORK = Symbol('Network');
const CAP_CONSOLE = Symbol('Console');
const CAP_CLOCK = Symbol('Clock');
const CAP_RANDOM = Symbol('Random');

class CapabilitySet {
  constructor(caps = []) {
    this.caps = new Set(caps);
  }
  
  has(cap) { return this.caps.has(cap); }
  add(cap) { return new CapabilitySet([...this.caps, cap]); }
  remove(cap) {
    const newCaps = new Set(this.caps);
    newCaps.delete(cap);
    return new CapabilitySet([...newCaps]);
  }
  
  isSubsetOf(other) { return [...this.caps].every(c => other.has(c)); }
  union(other) { return new CapabilitySet([...this.caps, ...other.caps]); }
  intersect(other) { return new CapabilitySet([...this.caps].filter(c => other.has(c))); }
  
  get size() { return this.caps.size; }
  toString() {
    const names = [...this.caps].map(c => c.description);
    return `{${names.join(', ')}}`;
  }
}

// ============================================================
// Capability-tracked computations
// ============================================================

class CapIO {
  constructor(requiredCaps, action) {
    this.requiredCaps = requiredCaps; // CapabilitySet
    this.action = action;             // (runtime) → result
  }

  /**
   * Run the computation, checking capabilities
   */
  run(availableCaps) {
    if (!this.requiredCaps.isSubsetOf(availableCaps)) {
      const missing = [...this.requiredCaps.caps].filter(c => !availableCaps.has(c));
      throw new Error(`Missing capabilities: ${missing.map(c => c.description).join(', ')}`);
    }
    return this.action(availableCaps);
  }

  /**
   * Chain computations (monad bind)
   */
  then(f) {
    return new CapIO(this.requiredCaps, caps => {
      const result = this.action(caps);
      const next = f(result);
      // Combine capabilities
      if (!next.requiredCaps.isSubsetOf(caps)) {
        const missing = [...next.requiredCaps.caps].filter(c => !caps.has(c));
        throw new Error(`Missing capabilities in chain: ${missing.map(c => c.description).join(', ')}`);
      }
      return next.action(caps);
    });
  }

  /**
   * Map over result (functor)
   */
  map(f) {
    return new CapIO(this.requiredCaps, caps => f(this.action(caps)));
  }
}

// Smart constructors
function pure(value) { return new CapIO(new CapabilitySet(), () => value); }

function readFile(path) {
  return new CapIO(new CapabilitySet([CAP_FILE_READ]), () => `contents of ${path}`);
}

function writeFile(path, content) {
  return new CapIO(new CapabilitySet([CAP_FILE_WRITE]), () => { return `wrote ${content.length} bytes to ${path}`; });
}

function httpGet(url) {
  return new CapIO(new CapabilitySet([CAP_NETWORK]), () => `response from ${url}`);
}

function log(msg) {
  return new CapIO(new CapabilitySet([CAP_CONSOLE]), () => { return msg; });
}

function now() {
  return new CapIO(new CapabilitySet([CAP_CLOCK]), () => Date.now());
}

function random() {
  return new CapIO(new CapabilitySet([CAP_RANDOM]), () => Math.random());
}

// ============================================================
// Capability attenuation (principle of least privilege)
// ============================================================

function attenuate(computation, allowedCaps) {
  if (!computation.requiredCaps.isSubsetOf(allowedCaps)) {
    throw new Error('Cannot attenuate: computation requires capabilities not in allowed set');
  }
  return computation;
}

// Pre-built capability sets
const fullCaps = new CapabilitySet([CAP_FILE_READ, CAP_FILE_WRITE, CAP_NETWORK, CAP_CONSOLE, CAP_CLOCK, CAP_RANDOM]);
const readOnlyCaps = new CapabilitySet([CAP_FILE_READ, CAP_CONSOLE]);
const sandboxCaps = new CapabilitySet([CAP_CONSOLE, CAP_CLOCK]);

export {
  CAP_FILE_READ, CAP_FILE_WRITE, CAP_NETWORK, CAP_CONSOLE, CAP_CLOCK, CAP_RANDOM,
  CapabilitySet, CapIO,
  pure, readFile, writeFile, httpGet, log, now, random,
  attenuate, fullCaps, readOnlyCaps, sandboxCaps
};
