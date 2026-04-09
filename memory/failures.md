# Failures & Patterns

## 2026-04-08
- **Dashboard API routes 404** — Server runs on port 3000, responds to requests, but archive-day and regenerate endpoints return {"error":"Not found"}. Server was rebuilt from scratch this morning — likely route naming mismatch between generate.cjs expectations and new server.js routes.
- **Knowledge system underutilized** — 468 BUILD tasks today but only 1 reference to lessons/failures in daily log. THINK/PLAN tasks didn't consult failures.md. Pattern: high-velocity build sessions skip knowledge feedback loops.

## 2026-04-07
- **Dashboard server down** — port 3000 unreachable during both MAINTAIN tasks (T4 and evening review). Archive-day and regenerate both failed. Cause unknown — server may not have been restarted after last reboot. This is 2nd occurrence (also failed during Session C part 2 MAINTAIN). Pattern: dashboard server doesn't auto-start.
