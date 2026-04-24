# Cross-Project: Neural Query Optimization (Apr 24, 2026)

## Concept
Use neural-net's ML capabilities to improve HenryDB's query optimizer.

## Specific Applications

### 1. Learned Cardinality Estimation
- **Current**: HenryDB uses basic histogram/sampling
- **ML approach**: Train a small NN on (query features) → (actual cardinality)
- **Features**: column names, predicates, table sizes, index availability
- **Architecture**: Simple feedforward (neural-net's Dense layers)
- **Training data**: EXPLAIN ANALYZE results from query workloads

### 2. Join Order Selection via RL
- **Current**: HenryDB uses dynamic programming
- **ML approach**: DQN agent selects join order actions
- **State**: Current join plan, remaining tables
- **Actions**: Which table to join next
- **Reward**: -1 * actual execution time
- **Architecture**: neural-net's DQN class

### 3. Index Recommendation
- **Problem**: Which indexes to create for a workload?
- **ML approach**: Score each candidate index with a NN
- **Input**: Query workload stats, table sizes, existing indexes
- **Output**: Probability that an index will help

## Feasibility Assessment
- **Cardinality estimation**: HIGH — simple to implement, high impact
- **Join order**: MEDIUM — needs more infrastructure (workload replay)
- **Index recommendation**: LOW — complex, multi-objective optimization

## Implementation Plan
1. Add workload statistics collection to HenryDB (query log with actual cardinalities)
2. Create feature extraction for queries (parse → features)
3. Train a simple NN on (features → cardinality) using neural-net
4. Integrate the model into the query planner's cost model
5. Benchmark: compare estimated vs actual cardinality

## Key Insight
The two projects complement each other perfectly:
- HenryDB generates training data (query workloads with ground truth)
- Neural-net provides the learning infrastructure (NN training, inference)
- The result improves HenryDB's query performance
