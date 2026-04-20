# Query Compilation Research Notes

uses: 0
created: 2026-04-20
tags: henrydb, query-compilation, codegen, research

## Current HenryDB Approach
- **Push-based pipeline compilation** (pipeline-compiler.js, 342 lines)
- Identifies pipeline segments (non-breaking operator chains)
- Generates JS functions that process rows without virtual dispatch
- `compilePipelineJIT` generates JS strings, eval'd into functions
- Falls back to Volcano iterators for complex queries

## Copy-and-Patch (Haas et al., VLDB 2023)
- Pre-compiles template functions ("stencils") for each operator
- At query compile time: copies stencil code and patches constants/offsets
- Much faster compilation than traditional codegen (no optimizer warmup)
- **Not directly applicable to JS**: requires native code manipulation
- Relevant concept: pre-compiled operator templates with parameter injection

## What HenryDB Could Borrow
Instead of literal copy-and-patch (native code), apply the concept:
1. **Pre-compiled template functions**: Write tight JS for each operator type ahead of time
2. **Closure-based specialization**: Instead of eval'd strings, use closures with captured constants
3. **Template instantiation**: For each pipeline, instantiate templates with query-specific params

## More Impactful for JS: Vectorized Execution
- Process batches (e.g., 1024 rows) through each operator
- Column-oriented batch processing with typed arrays
- Eliminates per-row function call overhead
- Compatible with V8's optimization (tight loops over arrays)
- **This is the real performance opportunity for HenryDB**

## Conclusion
Copy-and-patch is elegant for C/C++ databases (DuckDB adopted it). For JavaScript,
the equivalent win comes from vectorized batch processing. The pipeline compiler already
eliminates virtual dispatch; the next step is columnar batches.
