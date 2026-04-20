# Method Naming Mismatch Pattern (2026-04-20)

## Bug
`_tryVectorizedExecution` was called in `_select()` but the method was actually named `_selectInnerCore`.
Similarly, `_executeAst` was used in 5 places but the real method is `execute_ast`.

## Root Cause
Methods were renamed during refactoring but not all call sites were updated.
JavaScript doesn't catch `this.undefinedMethod()` at parse time — only at runtime.

## Why It Wasn't Caught
- `_tryVectorizedExecution` was only called for tables with 500+ rows via auto-vectorization
- `_executeAst` was only called from prepared statements, cursors, and MERGE — all new features
- The main test suite uses `Database` directly, not `TransactionalDatabase`, so the paths diverged
- No TypeScript or static analysis to catch the mismatch

## Lesson
When renaming methods in db.js, ALWAYS grep for all call sites: `grep -n "methodName" src/db.js`
Consider adding a `_methodCheck()` to the constructor that validates all expected methods exist.

## Systemic Pattern
This is the SAME class of bug as the MVCC interception issue — fragile references to methods
that may not exist. The codebase lacks static type checking, so method name changes are
silent failures that only manifest at runtime on specific code paths.
