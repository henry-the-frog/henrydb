/**
 * Focusing: Structural proof search via polarity
 * 
 * Types are classified as positive (built by intro) or negative (used by elim).
 * Focusing restricts search to eliminate non-determinism.
 * 
 * Positive: ∧, ∨, ∃ (right rules are invertible)
 * Negative: →, ∀ (left rules are invertible)
 */

class Pos { constructor(conn, args) { this.tag = 'Pos'; this.conn = conn; this.args = args; } toString() { return `${this.conn}(${this.args.join(', ')})`; } }
class Neg { constructor(conn, args) { this.tag = 'Neg'; this.conn = conn; this.args = args; } toString() { return `${this.conn}(${this.args.join(', ')})`; } }
class Atom { constructor(name) { this.tag = 'Atom'; this.name = name; } toString() { return this.name; } }

function polarity(prop) {
  if (prop.tag === 'Atom') return 'neutral';
  if (prop.tag === 'Pos') return 'positive';
  if (prop.tag === 'Neg') return 'negative';
  return 'unknown';
}

// Inversion: decompose without choice
function invert(prop) {
  if (prop.tag === 'Neg' && prop.conn === '→') {
    return { invertible: true, subgoals: [prop.args[0], prop.args[1]], rule: 'imp-right' };
  }
  if (prop.tag === 'Pos' && prop.conn === '∧') {
    return { invertible: true, subgoals: prop.args, rule: 'and-right' };
  }
  return { invertible: false };
}

// Focus: decompose with choice
function focus(prop) {
  if (prop.tag === 'Pos' && prop.conn === '∨') {
    return { choices: prop.args.map((a, i) => ({ subgoal: a, rule: `or-right-${i+1}` })) };
  }
  return { choices: [{ subgoal: prop, rule: 'identity' }] };
}

// Focused proof search
function search(target, hyps, depth = 5) {
  if (depth <= 0) return null;
  
  // Check if target is in hypotheses
  for (const h of hyps) {
    if (h.toString() === target.toString()) return { proof: 'axiom', from: h };
  }
  
  // Try inversion (no choice, always safe)
  const inv = invert(target);
  if (inv.invertible) {
    const subproofs = inv.subgoals.map(sg => {
      const newHyps = inv.rule === 'imp-right' ? [...hyps, inv.subgoals[0]] : hyps;
      return search(sg, newHyps, depth - 1);
    });
    if (subproofs.every(p => p !== null)) return { proof: inv.rule, children: subproofs };
  }
  
  // Try focusing (choice required)
  const foc = focus(target);
  for (const choice of foc.choices) {
    const result = search(choice.subgoal, hyps, depth - 1);
    if (result) return { proof: choice.rule, child: result };
  }
  
  return null;
}

export { Pos, Neg, Atom, polarity, invert, focus, search };
