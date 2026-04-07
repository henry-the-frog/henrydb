# Forth Compilation Mode

uses: 1
created: 2026-04-07
tags: forth, stack-machine, compilation, interpreters

## Two Modes
Forth has two execution modes:
- **Interpret mode**: Read word → execute immediately
- **Compile mode**: Read word → append to current definition

## How It Works
```forth
: square ( n -- n ) dup * ;
5 square .   \ prints 25
```

1. `:` switches to compile mode, creates new dictionary entry "square"
2. `dup` → in compile mode, appends DUP opcode to definition
3. `*` → appends MUL opcode
4. `;` switches back to interpret mode, finalizes definition
5. `5` → pushes 5 (interpret mode)
6. `square` → executes the compiled definition

## IMMEDIATE Words
Some words execute EVEN in compile mode:
- `IF/THEN/ELSE` — compile conditional jumps
- `DO/LOOP` — compile loop structure
- `[` — switch to interpret mode temporarily
- `]` — switch back to compile mode
- `LITERAL` — compile a push instruction for TOS value

## Key Implementation Detail
The compiler is trivially simple: just a flag (`STATE`).
- STATE = 0 → interpret (execute words)
- STATE = 1 → compile (append words to current definition)

## Threaded Code Variants
- **Direct threading**: Each cell = machine code address
- **Indirect threading**: Each cell = pointer to code field
- **Subroutine threading**: Each cell = CALL instruction
- **Token threading**: Each cell = small index into dispatch table

## Meta-Circular Beauty
Forth's compiler is written in Forth. `:` and `;` are just Forth words.
The entire language bootstraps from ~30 primitive words in assembly.
