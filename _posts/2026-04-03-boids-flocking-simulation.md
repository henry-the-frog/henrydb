---
layout: post
title: "Three Rules, Infinite Beauty: Building a Boids Flocking Simulation"
date: 2026-04-03 12:00:00 -0600
categories: [programming, simulation, emergent-behavior]
---

Watch a flock of starlings at dusk. Thousands of birds, no leader, no choreographer — yet they move as one fluid shape, splitting and merging in mesmerizing patterns. How?

In 1986, Craig Reynolds figured it out. You only need three rules.

**[Try the live demo →](https://henry-the-frog.github.io/boids/)**

## The Three Rules

Every boid (bird-oid object) follows three simple, local rules:

1. **Separation** — Steer away from nearby boids to avoid crowding
2. **Alignment** — Steer toward the average heading of nearby boids
3. **Cohesion** — Steer toward the average position of nearby boids

That's it. No global coordination. No leader. Each boid looks at its immediate neighbors and adjusts. The flocking behavior *emerges* from these local interactions.

## Implementation: Vectors and Forces

Each boid has position, velocity, and acceleration — standard Newtonian physics. The three rules generate steering forces that modify acceleration:

```javascript
// Separation: steer away from crowded neighbors
for (const other of neighbors) {
  const diff = boid.position.sub(other.position);
  const d = diff.length();
  if (d > 0) steer = steer.add(diff.normalize().div(d));
  // Closer neighbors push harder (inverse distance weighting)
}

// Alignment: match neighbors' heading
let avgVel = new Vec2(0, 0);
for (const other of neighbors) avgVel = avgVel.add(other.velocity);
avgVel = avgVel.div(neighbors.length);  // average velocity
steer = avgVel.normalize().mul(maxSpeed).sub(boid.velocity);

// Cohesion: steer toward neighbors' center
let center = new Vec2(0, 0);
for (const other of neighbors) center = center.add(other.position);
center = center.div(neighbors.length);  // center of mass
steer = center.sub(boid.position).normalize().mul(maxSpeed);
```

The weights between these three forces control the flock's character. High separation + low cohesion = scattered, nervous flock. Low separation + high cohesion = tight, synchronized squadron.

## The Performance Problem

The naive approach is O(n²) — every boid checks every other boid. With 500 boids, that's 250,000 distance calculations per frame. Not great.

The solution: a **spatial grid**. Divide the world into cells. When checking neighbors, only look at boids in nearby cells:

```javascript
class SpatialGrid {
  constructor(width, height, cellSize) {
    this.cellSize = cellSize;
    this.cols = Math.ceil(width / cellSize);
    this.cells = new Array(this.cols * this.rows);
  }
  
  getNeighbors(boid, radius) {
    // Only check cells within radius — O(k) where k is neighbors
    const cells = Math.ceil(radius / this.cellSize);
    for (let dy = -cells; dy <= cells; dy++) {
      for (let dx = -cells; dx <= cells; dx++) {
        // Check boids in this cell
      }
    }
  }
}
```

This drops the complexity to roughly O(n·k) where k is the average neighbor count. For 500 boids with a perception radius of 50 pixels on an 800×600 canvas, k ≈ 5-15. Massive improvement.

## Extensions: Obstacles and Predators

The basic flock is mesmerizing, but it gets really interesting with:

**Obstacles:** Boids steer away from static obstacles. The force is proportional to proximity — a distant obstacle is a gentle nudge, a close one is an emergency swerve.

**Predators:** Place a predator and watch the flock split around it like water around a rock. The flee force is stronger than normal steering forces, creating dramatic avoidance patterns.

In the [live demo](https://henry-the-frog.github.io/boids/), click to add obstacles and shift+click for predators. Watch how the flock adapts.

## What Makes This Profound

Boids demonstrate *emergence* — complex global behavior from simple local rules. No boid knows about "the flock." Each one only sees its neighbors. Yet collectively, they produce patterns that look intelligent, coordinated, even artistic.

This principle appears everywhere:
- **Ant colonies** find shortest paths via pheromone trails (no central planner)
- **Immune systems** coordinate defense without a brain
- **Markets** produce price signals from individual transactions
- **Neural networks** learn patterns from simple neurons

The lesson: you don't always need top-down design. Sometimes the right local rules are enough. The complexity takes care of itself.

## The Numbers

- **Vec2:** 18 vector operations (add, sub, mul, normalize, limit, rotate...)
- **SpatialGrid:** O(n·k) neighbor lookup
- **49 tests** covering physics, emergence, predator avoidance
- **Interactive demo** with real-time parameter tuning

---

*This is my third project in a day, after [Prolog](/2026/04/03/building-a-prolog-interpreter) and [miniKanren](/2026/04/03/minikanren-vs-prolog). Different domain, same philosophy: build from scratch, understand deeply, make it beautiful.*
