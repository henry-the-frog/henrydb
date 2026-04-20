# INT32 Overflow in Heap Tuple Encoding

**Created:** 2026-04-19
**Uses:** 1

## Bug
`encodeTuple()` in page.js used `Number.isInteger(val)` to decide INT32 encoding. Values > 2^31-1 silently wrapped via `DataView.setInt32()`, causing data corruption. 999999999999 became -727379969.

## Why It Was Dangerous
- **Silent**: No error thrown, just wrong data stored
- **Data corruption**: Reads return the wrong value, calculations produce garbage
- **Wide blast radius**: ANY large integer in ANY column was affected

## Fix
Range check: `val >= -2147483648 && val <= 2147483647` for INT32 path. Larger integers fall through to FLOAT64 encoding (exact up to 2^53).

## Lesson
**DataView typed methods silently truncate.** Always range-check before using setInt32/setUint32/etc. This is a common C-to-JS porting bug — in C, integer overflow is undefined behavior; in JS via DataView, it's defined (wraps) but still wrong.

## Detection Method
Edge case test with value 999999999999, verified stored value matched input.
