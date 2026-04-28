# WASM GC Exploration — Apr 28, 2026

## Status: Feasible in Node.js v22

### Verified Operations
All work correctly with proper binary encoding:
- `struct.new` (0xfb 0x00) — create struct with field values from stack
- `struct.get` (0xfb 0x02) — read struct field
- `array.new` (0xfb 0x06) — create array with fill value + length
- `array.get` (0xfb 0x0b) — read array element by index
- `array.set` (0xfb 0x0e) — write array element by index
- Struct and array type definitions compile without special flags

### Binary Encoding Notes
- Type section: struct = 0x5f, array = 0x5e
- Field encoding: [valtype][mutability] (NOT [mutability][valtype])
- Local types for GC refs: `(ref TYPE_IDX)` = 0x64 TYPE_IDX, `(ref null TYPE_IDX)` = 0x63 TYPE_IDX
- i32.const values > 63 need multi-byte signed LEB128 (bit 6 trap)

### What's Needed for monkey-lang GC Backend
1. Add GC type definitions to WasmModuleBuilder (struct/array type opcodes)
2. Add GC instruction emit methods (struct.new/get/set, array.new/get/set/len)
3. Add ref type support for locals (0x64/0x63 prefix)
4. Closure representation: struct { funcref, envref }
5. Array representation: GC array of i32 (replaces linear memory arrays)
6. Keep strings in linear memory (interned, immutable)

### Estimated Effort
Medium — the binary encoding is straightforward but requires adding 10-15 new opcodes
to the module builder and creating a parallel compilation path for GC-managed objects.
The existing 335 tests should still pass (GC backend = new target, not replacement).

### Not Explored Yet
- funcref in GC structs (needed for closures)
- array.len opcode verification
- Performance comparison (GC arrays vs linear memory)
- Interaction with existing GC module (mark-sweep)
