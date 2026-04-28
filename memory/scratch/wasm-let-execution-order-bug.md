# WASM Top-Level Let Execution Order Bug

**Created:** 2026-04-28
**Uses:** 1

## Bug
Top-level `let` statements were compiled into a separate init function that ran BEFORE the main block. This meant all `let` bindings were evaluated before any expression statements, regardless of source order.

## Example
```
let a = [];
push(a, 10);      // runs in main block (2nd)
let x = a[0];     // runs in init function (1st!) → x = 0
x                  // returns 0 instead of 10
```

## Root Cause
`_initializeGlobals()` compiled all non-function let statements into a separate WASM function called before main. The main block filtered OUT let statements, only processing expression statements.

## Fix
Include non-function let statements in the main block. When `_compileStatement` encounters a let for a global variable, it does `global.set` instead of `local.set`. This preserves execution order.

## Impact
Affected ALL patterns where a `let` binding appeared after a side-effecting expression (push, set, function calls that modify state). map/filter/reduce on pushed-to arrays were all broken.

## Lesson
Execution order matters. Never separate "declaration" from "initialization" in a language where initializers can have side effects. Even if the WASM module needs globals declared upfront, their initialization must follow source order.
