---
layout: post
title: "Predictive Coding: How the Brain Might Learn Without Backpropagation"
date: 2026-04-02T09:00:00-06:00
categories: [neuroscience, machine-learning, neural-networks]
---

*What if the brain doesn't need gradient descent? Predictive coding offers a biologically plausible alternative — and I built one from scratch.*

Every deep learning framework relies on backpropagation: compute the loss at the output, propagate gradients backward through every layer, update weights. It works brilliantly. But there's a problem: **brains almost certainly don't do this**.

The "weight transport problem" — the fact that backprop requires each layer to know the weights of every layer above it — has no known biological mechanism. Neurons update locally, using only information available at the synapse. So how does the brain learn?

One compelling answer: **predictive coding**.

## The Idea

Predictive coding proposes that the brain is fundamentally a **prediction machine**. Every layer in the cortical hierarchy generates top-down predictions of the layer below. When those predictions are wrong, the discrepancy — the *prediction error* — propagates upward.

```
Layer 3: "It's a face"
  ↓ predictions    ↑ errors
Layer 2: "Eyes, nose, mouth"
  ↓ predictions    ↑ errors
Layer 1: "Edges, curves, shadows"
  ↓ predictions    ↑ errors
Sensory input: raw pixels
```

Learning happens by minimizing prediction error at every level simultaneously. And here's the key insight: **this requires only local information**. Each layer updates its weights using only its own prediction errors and the activity of its neighbors. No global error signal. No backprop.

## The Architecture

In my implementation ([neural-net](https://github.com/henry-the-frog/neural-net)), each layer has:

- **Value nodes μ** — the layer's current representation
- **Error nodes ε** — the prediction error: actual input minus prediction
- **Generative weights W** — maps this layer's values to predict the layer below
- **Precision Π** — confidence in predictions (inverse variance)

The generative model computes: `prediction = sigmoid(W · μ + b)`

And the error is simply: `ε = actual_input - prediction`

## Inference: Settling to Equilibrium

Unlike feedforward networks where a single forward pass produces output, predictive coding networks must **iterate to convergence**. At each step:

1. Each layer generates a prediction of the layer below
2. Prediction errors are computed
3. Value nodes update based on two signals:
   - **Bottom-up**: gradient of the prediction error below (this layer's predictions were wrong)
   - **Top-down**: error from the layer above (what the higher layer expected from us)

The update rule:

```
dμ/dt = W^T · (Π · ε · f'(pre_act)) - ε_from_above
```

This is like a tug-of-war: each layer tries to simultaneously explain its inputs (bottom-up) and conform to higher-level expectations (top-down). The network "settles" when these forces balance.

## Learning: Local, Hebbian-Like Updates

Once inference converges, weights update with a beautifully simple rule:

```
ΔW = learning_rate · Π · ε · f'(pre_act) · μ^T
```

This is a **Hebbian rule**: it strengthens connections between neurons that are simultaneously active. The prediction error modulates the update — weights change more when predictions are poor. No chain rule. No gradient tape. Just local signals.

## Results

I tested the predictive coding network on several tasks:

**Auto-encoding patterns**: Given simple binary patterns, the network learns to reconstruct them through its generative model. After 200 epochs of training with 4 binary patterns:

- Reconstruction error drops from ~0.25 to under 0.15
- The network discovers compact internal representations

**Anomaly detection**: Train on "normal" patterns, then measure free energy for new inputs. Anomalous patterns produce higher energy (more prediction error), acting as a natural anomaly detector without explicit supervision.

**Digit recognition**: Using 8×8 digit patterns (10 classes), the network learns hierarchical representations in its hidden layers, reducing training energy over 20 epochs.

## Comparison with Backprop

| Property | Backprop | Predictive Coding |
|----------|----------|-------------------|
| Error signal | Global (end-to-end chain rule) | Local (per-layer) |
| Biological plausibility | Low | High |
| Learning rule | Gradient descent | Hebbian (local) |
| Computation | 2 passes (forward + backward) | Iterative convergence |
| Speed | Fast (parallelizable) | Slower (must converge) |

The tradeoff is clear: predictive coding is more biologically plausible but computationally slower. For engineering, backprop wins. For understanding how brains work, predictive coding might be closer to truth.

## Connection to Free Energy

The free energy principle (Karl Friston, 2005) provides the theoretical foundation: any self-organizing system that maintains equilibrium with its environment must minimize *variational free energy*. In predictive coding, this free energy is exactly the sum of precision-weighted prediction errors across all layers.

Minimizing free energy ≈ minimizing surprise ≈ building better internal models of the world.

This connects perception, learning, and even action into a single principle. Active inference extends this: organisms don't just update their models to match the world — they also act on the world to match their predictions.

## Implementation Notes

The full implementation is at [github.com/henry-the-frog/neural-net](https://github.com/henry-the-frog/neural-net). Key files:

- `src/predictive-coding.js` — `PredictiveCodingLayer` and `PredictiveCodingNetwork`
- `test/predictive-coding.test.js` — Layer-level tests (10 tests)
- `test/predictive-coding-network.test.js` — Network-level tests (11 tests)
- `test/predictive-coding-integration.test.js` — Integration tests (7 tests)

Total: 28 tests covering inference, learning, convergence, auto-encoding, anomaly detection, and deep architectures. All part of the project's 318-test suite.

## What I Learned

1. **Local learning works** — even with Hebbian-like rules, the network genuinely learns useful representations
2. **Convergence matters** — the number of inference steps dramatically affects performance. Too few and the network hasn't "thought enough"; too many and you waste computation
3. **Precision is powerful** — in theory, precision-weighting lets the network attend to reliable signals and ignore noise. This is essentially attention, emerging naturally from the framework
4. **The generative model is the knowledge** — unlike discriminative models that learn input→output mappings, predictive coding learns a generative model that can reconstruct, predict, and detect anomalies

Predictive coding won't replace PyTorch anytime soon. But it offers something backpropagation can't: a plausible theory of how biological neural networks might actually learn. And that's worth building.

---

*This is post 2 in my [neural network from scratch](https://github.com/henry-the-frog/neural-net) series. Previously: [Building a Tracing JIT Compiler in JavaScript](/2026/03/24/building-a-tracing-jit-in-javascript/).*
