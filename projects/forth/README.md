# Forth Interpreter

A Forth interpreter built from scratch in JavaScript. Stack-based, dictionary-driven, with compilation mode and control flow.

**Zero dependencies. Pure stack machines.**

## Architecture

```
Source → [Tokenizer] → Tokens → [Interpreter/Compiler]
                                       ↓
                              Dictionary (words)
                                       ↓
                              Data Stack + Return Stack
```

### Two modes:

- **Interpretation mode** — Execute words immediately as they're encountered
- **Compilation mode** — Between `:` and `;`, words are compiled into a body for later execution

## Features

| Category | Words |
|----------|-------|
| Arithmetic | `+ - * / mod negate abs min max /mod` |
| Stack | `dup drop swap over rot 2dup 2drop 2swap nip tuck ?dup depth` |
| Comparison | `= <> < > <= >= 0= 0< 0> 0<>` |
| Boolean | `and or xor invert true false` |
| I/O | `. cr emit .s space spaces ."` |
| Memory | `! @ +! here allot ,` |
| Return Stack | `>r r> r@` |
| Control | `if else then do loop +loop begin until while repeat` |
| Definitions | `: ; variable constant recurse` |
| Comments | `\ (line)` `( paren )` |

## Usage

```javascript
const { Forth } = require('./forth.js');
const f = new Forth();

// Basic arithmetic (RPN)
f.eval('3 4 + 2 *');        // stack: [14]

// Define a word
f.eval(': square dup * ;');
f.eval('5 square');          // stack: [14, 25]

// Control flow
f.eval(': fact dup 1 > if dup 1 - recurse * else drop 1 then ;');
f.eval('5 fact');            // stack: [14, 25, 120]

// Loops
f.eval(': stars 5 0 do 42 emit loop ;');
f.eval('stars');             // output: *****

// Variables
f.eval('variable x  42 x !  x @');  // stack: [..., 42]
```

## Example: FizzBuzz

```forth
: fizzbuzz
  16 1 do
    i 15 mod 0= if ." FizzBuzz" else
    i 3 mod 0= if ." Fizz" else
    i 5 mod 0= if ." Buzz" else
    i .
    then then then
    space
  loop ;
```

## Example: Recursive Fibonacci

```forth
: fib dup 2 < if else dup 1 - recurse swap 2 - recurse + then ;
10 fib  \ → 55
```

## Example: GCD (Euclidean algorithm)

```forth
: gcd begin dup 0<> while tuck mod repeat drop ;
12 8 gcd  \ → 4
```

## Tests

```
73 tests | 0 failures
```

Covers: arithmetic, stack manipulation, comparison, boolean, I/O, word definitions, control flow (if/else/then), loops (do/loop, begin/until, begin/while/repeat), variables, constants, recursion, return stack, comments, and complex programs (FizzBuzz, Pythagorean check, GCD, star triangle).

## Files

```
forth.js  — Tokenizer, interpreter, compiler, dictionary
test.js   — 73 tests
README.md — This file
```
