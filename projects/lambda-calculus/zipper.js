/**
 * Zipper: Navigate and Edit Lambda Terms
 * 
 * A zipper represents a "focused" position in a tree:
 * - The subtree at the focus
 * - The path (context) from root to focus
 * 
 * Enables O(1) navigation and editing without copying the whole tree.
 * (Huet 1997)
 */

class Var { constructor(name) { this.tag = 'Var'; this.name = name; } toString() { return this.name; } }
class Lam { constructor(v, body) { this.tag = 'Lam'; this.var = v; this.body = body; } toString() { return `(λ${this.var}.${this.body})`; } }
class App { constructor(fn, arg) { this.tag = 'App'; this.fn = fn; this.arg = arg; } toString() { return `(${this.fn} ${this.arg})`; } }

// Context frames
class LamCtx { constructor(v) { this.tag = 'LamCtx'; this.var = v; } }
class AppFnCtx { constructor(arg) { this.tag = 'AppFnCtx'; this.arg = arg; } }     // Focus is fn, arg is saved
class AppArgCtx { constructor(fn) { this.tag = 'AppArgCtx'; this.fn = fn; } }      // Focus is arg, fn is saved

class Zipper {
  constructor(focus, path = []) { this.focus = focus; this.path = path; }
  
  // Navigation
  down() {
    if (this.focus.tag === 'Lam') return new Zipper(this.focus.body, [...this.path, new LamCtx(this.focus.var)]);
    if (this.focus.tag === 'App') return new Zipper(this.focus.fn, [...this.path, new AppFnCtx(this.focus.arg)]);
    return null; // Can't go down from Var
  }
  
  downRight() {
    if (this.focus.tag === 'App') return new Zipper(this.focus.arg, [...this.path, new AppArgCtx(this.focus.fn)]);
    return null;
  }
  
  up() {
    if (this.path.length === 0) return null;
    const frame = this.path[this.path.length - 1];
    const newPath = this.path.slice(0, -1);
    switch (frame.tag) {
      case 'LamCtx': return new Zipper(new Lam(frame.var, this.focus), newPath);
      case 'AppFnCtx': return new Zipper(new App(this.focus, frame.arg), newPath);
      case 'AppArgCtx': return new Zipper(new App(frame.fn, this.focus), newPath);
    }
  }
  
  // Editing
  replace(newFocus) { return new Zipper(newFocus, this.path); }
  
  modify(fn) { return new Zipper(fn(this.focus), this.path); }
  
  // Reconstruct whole term
  toTerm() {
    let z = this;
    while (z.path.length > 0) z = z.up();
    return z.focus;
  }
  
  isTop() { return this.path.length === 0; }
  depth() { return this.path.length; }
}

function zipper(term) { return new Zipper(term); }

// Navigate to leftmost leaf
function leftmost(z) {
  while (true) {
    const d = z.down();
    if (!d) return z;
    z = d;
  }
}

// Navigate to rightmost leaf
function rightmost(z) {
  while (true) {
    const d = z.focus.tag === 'App' ? z.downRight() : z.down();
    if (!d) return z;
    z = d;
  }
}

export { Var, Lam, App, LamCtx, AppFnCtx, AppArgCtx, Zipper, zipper, leftmost, rightmost };
