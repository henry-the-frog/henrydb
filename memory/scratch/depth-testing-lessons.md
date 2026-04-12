# Depth Testing Strategy — Lessons Learned (2026-04-12)

## Approach
Stress-tested 8 modules from yesterday's breadth sprint (262 tasks).
Result: 3 real bugs found, 5 solid implementations.

## Bugs Found
1. **KAN B-spline right boundary** — basis sum = 0 at x=max.
   Root cause: augmented knot endpoints create empty intervals `[k, k)`.
   Fix: find last non-degenerate interval instead of absolute last.
   
2. **Izhikevich voltage history overshoot** — recorded 172 mV pre-reset.
   Root cause: voltage history recorded after dv integration but before spike check.
   Fix: record canonical 30 mV spike peak, then reset.

3. **HenryDB BETWEEN SYMMETRIC in index scan** — lo/hi not swapped.
   Root cause: _tryIndexScan BETWEEN handler ignored `symmetric` flag.
   Fix: swap lo/hi when symmetric && lo > hi.

## Modules That Were Solid
- Normalizing flows: numerical Jacobian matches analytical. Invertible through 20 layers.
- NTM: memory R/W correct, attention valid distributions, 100-step sequences stable.
- Hopfield: energy monotonically decreasing, weight matrix symmetric, capacity matches theory.
- Capsule: squash direction-preserving, coupling coefficients sum to 1.
- EBM: analytical gradient matches numerical, Langevin reduces energy.
- Autograd: all 15 ops verified against numerical gradients.

## Pattern
The bugs were all in **boundary/edge cases**: B-spline boundaries, voltage reset timing,
SYMMETRIC keyword handling. The core algorithms were correct. This suggests the breadth
sprint produced quality core implementations but skimped on edge case handling.

## Methodology
1. Write tests that verify mathematical properties (partition of unity, gradient match)
2. Test boundary conditions explicitly (x=min, x=max, zero input, large input)
3. Compare analytical vs numerical gradients
4. Verify invariants (energy decreasing, weights symmetric, probabilities sum to 1)
