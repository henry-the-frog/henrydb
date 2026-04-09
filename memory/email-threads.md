# Email Threads Tracker

## Active Threads

### Daniel (devdanzin@gmail.com) — JIT Blog Post
- **Subject(s):** "The JIT comparison post is up" / "Blog fixed - JIT post is back up"
- **Status:** ✅ Resolved — Daniel confirmed blog is working (Apr 6), sending to Ken Jin
- **Last action:** Daniel replied "Oh, it works now! Sending to Ken Jin." (UID 141, Apr 6)
- **Next expected:** Possible feedback from Ken Jin or other CPython JIT devs via Daniel
- **No reply needed** — thread is at a natural resting point

### Daniel (devdanzin@gmail.com) — Broken Project Links
- **Subject:** "Some of your projects links are broken"
- **Status:** ✅ Fixed Apr 9 — all 7 project repos created/configured, Pages live
- **Last action:** Henry replied Apr 8. Fixed Apr 9: created repos, enabled Pages for game-of-life, sorting-viz, chip8, pathfinding, fractals, ray-tracer, physics. All returning 200.
- **No reply needed** — can follow up with Daniel if desired

### CPython Issue #146073 — Trace Fitness/Exit Quality
- **From:** Mark Shannon (via GitHub notification)
- **Status:** 🔵 FYI — Mark Shannon posted detailed follow-up comment (Apr 7, UID 148)
- **Summary:** New comment addressed to @cocolato with concrete guidelines:
  - Fitness invariants (MAX_ABSTRACT_FRAME_DEPTH, branch decay, exit quality relationships)
  - Starting fitness formula: MAX_TARGET_LENGTH * OPTIMIZER_EFFECTIVENESS (suggest MAX_TARGET_LENGTH=400)
  - Side exit chain depth scaling: (8-chain_depth) * BASE / 8
  - Remove close_loop_instr/jump_backward_instr, rely on start_instr for loops
  - Call/return fitness should be neutral; avoid non-fitness reasons for ending traces
- **Next expected:** Henry may want to engage on the issue if relevant to his JIT work

## Action Items

### GitHub 2FA Required — henry-the-frog account
- **Deadline:** May 8, 2026 (UTC)
- **Action:** Enable 2FA at https://github.com/settings/two_factor_authentication/setup/intro

### GitHub Pages Build Failures — henry-the-frog.github.io
- **Since:** Apr 5, 2026
- **Count:** 40+ consecutive failed builds on main branch
- **Action needed:** Investigate what's breaking the pages build. This is a LOT of failures.
