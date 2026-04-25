# Escape Analysis → Allocation Sinking Research (2026-04-25)

## Current State
- `escape.js`: Full escape analyzer exists, correctly classifies variables as stack/heap
- `pipeline.js`: Runs escape analysis, records stats (stackAllocatable count, heapRequired count)
- **Gap**: Results are never used by compiler or VM to optimize allocation

## Opportunity: Stack-Allocated Closures
Every closure creates a `Closure` object + `free` array (heap-allocated).
For non-escaping closures (used in loops like `map`, `filter`, inner functions that don't escape):
- These could be allocated on the call frame's stack
- Automatic reclaim on frame return (no GC needed)
- Significant reduction in GC pressure for functional-style code

## Implementation Plan
### Phase 1: Wire escape results into compiler
1. Pass escape analysis results from pipeline to compiler
2. During `OpClosure` emission, check if the closure is `stackAllocatable`
3. Emit `OpStackClosure` for non-escaping closures

### Phase 2: VM support for stack closures
1. Add `OpStackClosure` handler in VM
2. Allocate closure data on the current frame's stack (or a dedicated arena)
3. On frame return, reclaim all stack closures for that frame
4. Skip GC tracking for stack closures (`_track` vs no-track)

### Phase 3: Validation
1. Run benchmarks comparing heap vs stack allocation
2. Test that escaping closures are still correctly heap-allocated
3. Verify closure semantics preserved (captured variables, mutation)

## Key Risks
- Closure escaping through returned values: must fall back to heap
- Closures stored in data structures: escape analysis must be conservative
- Recursive closures: may need special handling

## References
- V8's TurboFan: "scalar replacement" for non-escaping allocations
- HotSpot's escape analysis: C2 compiler does this for Java objects
- Graal: partial escape analysis (allocate lazily, only on escape path)

## Hypothesis Results
1. ✅ Escape info is collected but not applied — confirmed
2. ✅ Closure allocation is the main hotspot — confirmed (every `OpClosure` allocates)
3. ✅ Allocation sinking could reduce GC pressure — plausible, needs benchmarking
4. NEW: The escape analyzer's `stackAllocatable` list is already available in pipeline results
