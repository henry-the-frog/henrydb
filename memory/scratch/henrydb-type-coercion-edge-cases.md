# HenryDB Type Coercion Edge Cases (2026-04-25)

## Hypothesis Results
1. ❌ NULL + 1 → null (CORRECT! Standard SQL behavior)
2. ❌ 'abc' = 0 → false (different from SQLite which returns true, but false is more intuitive)
3. ❌ TRUE + 1 → 2 (WORKS correctly, boolean is treated as 0/1)

## Confirmed Correct
- NULL + 1 → null ✅
- NULL * 5 → null ✅
- NULL || 'hello' → null ✅ (SQLite standard: should be 'hello' in some dbs)
- '5' = 5 → true ✅ (type coercion on comparison)
- TRUE + 1 → 2 ✅
- FALSE + 1 → 1 ✅
- '10' > '9' → false ✅ (string comparison, '1' < '9')
- 10 > '9' → true ✅ (numeric comparison when one side is number)

## Issues Found
1. **1 / 0 → null** — Should either error or return Infinity. SQLite returns null.
   Decision: null is fine (matches SQLite behavior)
   
2. **SUM(text) concatenates** — `SUM('10', '20', 'abc')` → '01020abc'
   Should convert to numbers: SUM should be 30 (ignore non-numeric 'abc')
   
3. **AVG(text) → NaN** — Should convert text to numbers, NaN is bad output

## Not Bugs (Different from SQLite but OK)
- 'abc' = 0 → false (SQLite: true because 'abc' converts to 0)
  Our behavior is more intuitive — string 'abc' doesn't equal integer 0
