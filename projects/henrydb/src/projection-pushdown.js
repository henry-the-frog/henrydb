// projection-pushdown.js — Query optimization: push projections down the tree
// Reduces memory by only passing needed columns through operators.
export function pushdownProjection(plan, needed) {
  if (plan.type === 'scan') {
    return { ...plan, columns: plan.columns.filter(c => needed.has(c)) };
  }
  if (plan.type === 'filter') {
    // Filter needs its predicate columns too
    const filterNeeded = new Set([...needed, ...(plan.predicateColumns || [])]);
    return { ...plan, child: pushdownProjection(plan.child, filterNeeded) };
  }
  if (plan.type === 'project') {
    const childNeeded = new Set(plan.columns.filter(c => needed.has(c)));
    return { ...plan, columns: [...childNeeded], child: pushdownProjection(plan.child, childNeeded) };
  }
  return plan;
}
