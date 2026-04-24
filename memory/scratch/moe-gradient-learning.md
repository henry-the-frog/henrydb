# MoE Gradient Learning (Apr 24, 2026)

## Problem
Mixture of Experts backward pass was broken for multi-sample batches.
Loss would increase instead of decrease.

## Root Causes
1. **Shared expert state corruption:** Experts are Dense layers shared across samples.
   During batch backward, re-forwarding sample B overwrites sample A's internal state
   (input, activations, pre-activations).
   
2. **Gradient replacement vs accumulation:** Dense.backward sets `this.dWeights = ...`
   (replace), not `this.dWeights += ...` (accumulate). So each sample's backward
   replaces the previous sample's gradients.

3. **Missing gate gradient:** The gate (router) layer needs gradients too. The proper
   gradient involves the softmax Jacobian of the routing weights.

## Solution
- **Per-sample forward+backward:** Re-forward each expert for each sample before backward
- **Gradient accumulation:** Maintain external accumulators per expert, sum all samples'
  gradients, then set them on the expert layers
- **Gate gradient:** Compute `dL/ds_e = Σ_j dL/dy_j * Σ_k expertOut_k_j * w_k * (δ_{ke} - w_e)`
  using the softmax Jacobian, then propagate through the gate layer

## Key Insight
MoE is fundamentally different from a standard layer because:
- Multiple sub-networks (experts) share computation across the batch
- The routing decision creates a soft selection that needs proper gradient flow
- The softmax Jacobian connects the routing weights to the gate scores
