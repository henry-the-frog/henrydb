# Monkey-Lang Complete Feature List

## AST Node Types (41)
### Statements (7)
LetStatement, ReturnStatement, ImportStatement, ExpressionStatement, BlockStatement, BreakStatement, ContinueStatement, SetStatement, EnumStatement, DestructureLetStatement, DestructureHashLetStatement

### Expressions (30+)
IntegerLiteral, FloatLiteral, StringLiteral, BooleanLiteral, NullLiteral, ArrayLiteral, HashLiteral, TemplateLiteral, FStringExpression, RangeExpression, SpreadExpression, PrefixExpression, InfixExpression, IfExpression, FunctionLiteral, CallExpression, IndexExpression, OptionalChainExpression, WhileExpression, DoWhileExpression, ForExpression, ForInExpression, AssignExpression, IndexAssignExpression, SliceExpression, TernaryExpression, MatchExpression, SwitchExpression, TryCatchExpression, ThrowExpression

## VM Builtins (55)
len, first, last, rest, push, puts, print, type, str, int, bool, format, range, split, join, trim, upper, lower, contains, indexOf, replace, reverse, abs, min, max, startsWith, endsWith, char, ord, repeat, enumerate, zip, slice, sum, count, compact, unique, isEmpty, flatten, keys, values, sort, padStart, padEnd, float, floor, ceil, sqrt, pow, chars, sin, cos, merge, product, import, __range_inclusive

## Prelude HOFs (17)
map, filter, reduce, any, all, find, flat_map, take, drop, take_while, scan, chunk, zip_with, tap, partition, group_by, each

## Language Features
### Control Flow
- if/else with expression return
- while loop
- do-while loop
- for loop (C-style)
- for-in loop (iterable)
- break/continue
- match expression (pattern matching)
- switch expression
- ternary operator

### Types & Literals
- Integers, floats, strings, booleans, null
- Arrays, hashes
- Template literals (`` `${expr}` ``)
- F-strings
- Range literals (1..10)

### Functions
- First-class functions (fn)
- Closures (mutable via set)
- IIFE
- Recursive functions
- Currying (nested functions)
- Spread arguments (...args)
- Rest parameters (...rest)
- Pipe operator (|>)

### Pattern Matching
- Integer patterns
- String patterns
- Boolean patterns
- Wildcard (_)
- Nested match
- Expression match
- (NOT: array destructuring in match)

### Data
- Array destructuring: let [a, b, c] = arr
- Hash destructuring: let {name, age} = obj
- Optional chaining: obj?.field
- Index assignment: arr[0] = val
- Slice syntax: arr[1:3]
- List comprehension: [x * 2 for x in arr]

### Error Handling
- try/catch expressions
- throw expressions

### Modules
- import/export statements
- import() builtin for dynamic imports

### Other
- Comments (// and /* */)
- Enums
- Set statement (mutation)

## Optimization Pipeline
1. Constant substitution (AST)
2. Constant folding (AST)
3. Escape analysis (AST)
4. Per-function SSA (AST → CFG → SSA)
5. Tail call optimization (bytecode)
6. Escape annotation (bytecode)
7. Peephole optimizer (bytecode)

## Runtime
- Generational GC (young/old, write barriers, weak refs)
- Integer cache (-128 to 255)
- String interning
- Inline caching (hidden shapes)
- 1149 tests, 0 failures

## Stats
- 22K LOC
- 78 source files
- 41 AST node types
- 55 VM builtins
- 17 prelude HOFs
- 68 type checker tests
- 7 optimization passes
- VM 3.6x faster than evaluator (fib), 2.8x (counter)
