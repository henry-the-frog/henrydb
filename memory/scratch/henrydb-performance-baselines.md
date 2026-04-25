# HenryDB Performance Baselines (2026-04-25)

## Data Structure Benchmark (5K elements)

| Structure  | Insert | Lookup | Insert/op | Lookup/op |
|-----------|--------|--------|-----------|-----------|
| B+Tree    | 5.1ms  | 2.1ms  | 1.03µs    | 0.41µs    |
| SkipList  | 6.3ms  | 3.0ms  | 1.27µs    | 0.60µs    |
| ART       | 4.8ms  | 4.1ms  | 0.96µs    | 0.82µs    |
| Trie      | 3.9ms  | 2.9ms  | 0.78µs    | 0.58µs    |
| ExtHash   | 9.3ms  | 2.2ms  | 1.86µs    | 0.45µs    |
| RobinHood | 4.5ms  | 1.1ms  | 0.89µs    | 0.22µs    |
| SortedArr | 2.3ms  | 1.8ms  | 0.46µs    | 0.36µs    |
| LSM-Tree  | 8.7ms  | 2.7ms  | 1.75µs    | 0.55µs    |
| Bε-Tree   | 3.5ms  | 2.2ms  | 0.70µs    | 0.44µs    |

**Winners**: RobinHood for lookups (0.22µs), SortedArr for inserts (0.46µs)

## Notes
- All benchmarks on Apple Silicon (M-series) running Node v22
- Disk PAGE_SIZE changed from 4KB to 32KB in this session — may affect persistence benchmarks
- Wire protocol benchmark: ~98s (includes TCP overhead)
