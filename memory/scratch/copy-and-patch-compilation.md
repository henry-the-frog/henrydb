# Copy-and-Patch Compilation for Database Queries

## Core Technique (Haas & Roth, OOPSLA 2021)

Instead of generating machine code from scratch (like LLVM) or interpreting bytecodes (like Volcano), copy-and-patch:

1. **Pre-compile "stencils"** — small code templates for each operation (filter, hash lookup, projection, etc.)
2. **At query time**, copy the relevant stencils into a buffer and patch in the runtime values (column offsets, hash table addresses, predicate constants)
3. The result is native code without requiring a full compiler at runtime

### Why it's fast to compile:
- No IR optimization passes
- No register allocation
- No instruction selection
- Just memcpy + fixup — O(n) where n is plan size

### Why the generated code is fast:
- No interpreter dispatch
- Function calls are inlined (the stencils ARE the inlined code)
- Constants are baked in (no indirection through closure variables)

## Relevance to HenryDB

Our current approach (compiled-query.js) generates JS closures. This is actually similar in spirit to copy-and-patch:

| Aspect | Copy-and-Patch | HenryDB Closures |
|--------|---------------|-------------------|
| Templates | Pre-compiled x86 stencils | Hand-written JS functions |
| Runtime | memcpy + patch constants | Closure capture + V8 JIT |
| Dispatch | Direct jumps (no dispatch) | V8 inlines the closures |
| Constants | Patched into instruction stream | Captured in closure scope |

### Key difference: V8 does our "patching" for us

When we write:
```javascript
const filterFn = (row) => row.region === 'US';
```

V8's TurboFan compiler:
1. Sees the closure captures 'US' as a constant
2. Inlines the property access `row.region`
3. Emits a direct string comparison
4. Eliminates the function call overhead

This is effectively copy-and-patch at the JIT level. We get the benefit without managing machine code ourselves.

### What we COULD do better:

1. **`new Function()` with constant folding** — bake column indices directly into generated code:
   ```javascript
   const fn = new Function('row', `return row[3] === 'US'`);
   // Instead of: (row) => row[schema.indexOf('region')] === 'US'
   ```

2. **Batch compilation** — generate one big function for the entire query instead of composing closures:
   ```javascript
   const queryFn = new Function('tables', `
     const hashTable = new Map();
     for (const row of tables.orders.scan()) {
       const key = row[1]; // customer_id at index 1
       if (!hashTable.has(key)) hashTable.set(key, []);
       hashTable.get(key).push(row);
     }
     // ...
   `);
   ```

3. **ArrayBuffer-based rows** — instead of JS objects, use typed arrays for rows. Eliminates GC pressure from object spread in joins.

## Sea-of-Nodes IR (Cliff Click, 1995)

An alternative approach: represent the query plan as a sea-of-nodes graph where:
- Data dependencies determine order (not a fixed tree)
- The scheduler can reorder operations freely
- Dead code is automatically eliminated (unreferenced nodes)

V8's TurboFan uses sea-of-nodes internally. In theory, we could build a sea-of-nodes IR for queries:
- Filter nodes reference column nodes
- Join nodes reference both inputs
- Projection nodes select outputs
- The "scheduler" orders them for maximum cache locality

### Verdict: Overkill for our purposes

Sea-of-nodes is a compiler optimization technique. It helps when you need to optimize complex code with many possible orderings. For database queries, the planner already handles strategy selection, and the execution is straightforward (scan → filter → join → aggregate → project). The overhead of building and scheduling a graph IR would exceed the benefit.

**Better investment: focus on the data representation (columnar/ArrayBuffer) and the join compilation, not the IR.**

## Action Items for HenryDB

1. **Near-term**: Investigate `new Function()` batch compilation (one function per query, not composed closures)
2. **Medium-term**: ArrayBuffer-based row storage for joins (eliminate object spread GC pressure)
3. **Skip**: Sea-of-nodes IR (overkill for SQL query compilation in JS)
4. **Blog opportunity**: Write about copy-and-patch in the context of database JIT vs CPython JIT — the same technique appears in both domains
