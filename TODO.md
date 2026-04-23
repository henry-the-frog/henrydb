# Tomorrow's Action Items — Apr 24, 2026

## Neural-net: Cleanup Day (Priority 1)

### Quick Fixes (~1 hour total)
1. **Fix rope.js import bug** — Add `applyRoPEToSequence` wrapper or fix gqa-attention.js to use `applyRoPE`. Unblocks 5 test files.
2. **Fix MultiHeadFlashAttention NaN** — Change `flashAttention(Q,K,V, {blockSize, causal})` → `flashAttention(Q,K,V, blockSize, causal)`. 1 line.
3. **Fix MoE serialization** — Update network.js to use `expert.up`/`expert.down` instead of `expert.W1`/`expert.b1`. ~30 LOC.
4. **Fix AdamW step counter** — Don't increment `this.step` in `update()`. Add `step()` method like optimizer.js's Adam.
5. **Fix reward model bias** — Add `db1[j] += dReward`, `db2[j] += dReward` in trainStep.
6. **Fix MSE gradient** — Add `/n` factor to match forward's `/(2n)`.
7. **Fix pruning consistency** — Use `>` (not `>=`) for threshold in Matrix path too.

### Moderate Fixes (~30 min)
8. **Update README.md** — 168 modules not 71, 26K LOC not 15.6K, 150 test files not 100.
9. **Delete or fix grouped-query-attention.js** — Duplicate of working gqa.js. Either delete + redirect tests, or fix the 3/8 failures.

### After fixes: run full test suite targeting 0 failures

## HenryDB: Polish Day (Priority 2)

### Bug Fixes
1. **AFTER DELETE trigger** — Add 6th parameter to `_fireTriggers` wrapper: `_fireTriggers(timing, event, table, row, schema, oldRow)`.
2. **Optimizer-quality test failure** — Investigate, likely cost model estimation issue.

### Volcano Gaps
3. **SELECT * + window** — At final projection, expand `*` to base-table columns only, then append window result columns. ~1-2 BUILD tasks.
4. **NOT NOT NOT parser** — Parser bug, low priority.

### Nice-to-have
5. **Connect stats collector to cost model** — MCV + histograms exist but aren't used.
6. **Query cache: upgrade FIFO → LRU** — Simple improvement.

## Key Metrics from Session B
- 120+ modules audited across both projects
- 10/10 numerical gradient checks pass
- 5 bugs found (3 neural-net, 2 HenryDB)
- Combined: 536 source modules, 1018 test files, 104K LOC
- HenryDB true test pass rate: ~99% (not 66% as morning sweep suggested)
- All backward passes verified correct
