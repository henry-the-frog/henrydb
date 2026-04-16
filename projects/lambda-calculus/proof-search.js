/**
 * Proof Search: Automated theorem proving for propositional logic
 */
function provable(goal, hyps = []) {
  if (hyps.some(h => equal(h, goal))) return { proved: true, by: 'axiom' };
  if (goal.tag === 'Imp') {
    const r = provable(goal.right, [...hyps, goal.left]);
    return r.proved ? { proved: true, by: 'imp-intro', child: r } : r;
  }
  if (goal.tag === 'And') {
    const l = provable(goal.left, hyps), r = provable(goal.right, hyps);
    return l.proved && r.proved ? { proved: true, by: 'and-intro', left: l, right: r } : { proved: false };
  }
  if (goal.tag === 'Or') {
    const l = provable(goal.left, hyps);
    if (l.proved) return { proved: true, by: 'or-intro-left', child: l };
    const r = provable(goal.right, hyps);
    if (r.proved) return { proved: true, by: 'or-intro-right', child: r };
  }
  // Try eliminations from hypotheses
  for (const h of hyps) {
    if (h.tag === 'Imp' && hyps.some(h2 => equal(h2, h.left)) && equal(h.right, goal))
      return { proved: true, by: 'modus-ponens', major: h, minor: h.left };
    if (h.tag === 'And') {
      const newHyps = [...hyps, h.left, h.right];
      const r = provable(goal, newHyps);
      if (r.proved) return { proved: true, by: 'and-elim', child: r };
    }
  }
  return { proved: false };
}

function equal(a, b) {
  if (a.tag !== b.tag) return false;
  if (a.tag === 'Atom') return a.name === b.name;
  if (a.tag === 'Imp' || a.tag === 'And' || a.tag === 'Or') return equal(a.left, b.left) && equal(a.right, b.right);
  if (a.tag === 'Not') return equal(a.inner, b.inner);
  return false;
}

const Atom = n => ({ tag:'Atom', name:n }); const Imp = (l,r) => ({ tag:'Imp', left:l, right:r });
const And = (l,r) => ({ tag:'And', left:l, right:r }); const Or = (l,r) => ({ tag:'Or', left:l, right:r });
const Not = i => ({ tag:'Not', inner:i });

export { provable, Atom, Imp, And, Or, Not };
