// comparator.js — Typed comparators for database sorting
export const Comparators = {
  numeric: (a, b) => a - b,
  string: (a, b) => String(a).localeCompare(String(b)),
  reverseNumeric: (a, b) => b - a,
  reverseString: (a, b) => String(b).localeCompare(String(a)),
  nullsFirst: (cmp) => (a, b) => {
    if (a == null && b == null) return 0;
    if (a == null) return -1;
    if (b == null) return 1;
    return cmp(a, b);
  },
  nullsLast: (cmp) => (a, b) => {
    if (a == null && b == null) return 0;
    if (a == null) return 1;
    if (b == null) return -1;
    return cmp(a, b);
  },
  multiColumn: (specs) => (a, b) => {
    for (const { col, order, cmp } of specs) {
      const result = (cmp || Comparators.numeric)(a[col], b[col]);
      if (result !== 0) return order === 'DESC' ? -result : result;
    }
    return 0;
  },
};
