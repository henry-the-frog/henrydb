# RISC-V Emulator Architecture Notes

## Key Insights
- `uses: 1` | `created: 2026-04-07`

### Instruction Encoding
- RV32I uses 6 formats (R/I/S/B/U/J) but opcode, rd, rs1, funct3 are always in the same bit positions
- The `li` pseudo-instruction is tricky: LUI+ADDI, but if low 12 bits have sign bit set, ADDI sign-extends negatively and you need to bump the upper bits
- Size estimation for pseudo-instructions must match actual expansion or labels will be wrong

### Pipeline Hazards
- Load-use hazard: can't forward from EX stage of a LOAD (data not available until MEM)
- Forwarding eliminates most RAW hazards for ALU→ALU
- Branches resolved in EX stage → 2-cycle penalty when taken

### Branch Prediction
- 2-bit saturating counter is the sweet spot: >99% on loops, resistant to occasional mispredictions
- GShare (global history XOR PC) handles correlated branches better than local predictors
- 1-bit predictor is terrible on alternating patterns (always predicts opposite of what happens)

### Cache
- Row-major vs column-major matrix access shows dramatic hit rate difference
- Direct-mapped caches suffer from conflict misses; even 2-way helps significantly
- Sequential scan with 64-byte cache lines → 93.8% hit rate (spatial locality)

### Virtual Memory (Sv32)
- Two-level page table: 4KB pages, 1024 entries per level, covers 4GB address space
- TLB critical for performance — page walk costs 2 memory accesses
- Accessed/Dirty flags must be set by hardware (MMU) on access

### Tomasulo's Algorithm
- Key insight: rename registers to ROB entries to eliminate WAW and WAR hazards
- CDB broadcast wakes up all dependent reservation stations simultaneously
- In-order commit through ROB preserves precise exceptions
- Critical bug: RS.clear() must reset cycleExecuteStart or reused RS entries execute immediately
