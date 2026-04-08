# WORK-SYSTEM-V2.md — Proposed New Work System

## Overview

A continuous work session that processes an ordered task queue. No fixed timers between tasks — the agent chains tasks back-to-back with zero dead time. Modes are enforced by queue position, not agent willpower.

---

## Architecture

### Daily Schedule (cron jobs)
| Time | Job | Type | Purpose |
|------|-----|------|---------|
| 8:00am | Morning standup | Isolated | Build schedule.json queue, message Jordan |
| 8:15am | Work Session A | Isolated, 6hr timeout | Process queue until 2:15pm |
| 2:15pm | Work Session B | Isolated, 6hr timeout | Continue queue until 8:15pm |
| 8:15pm | Work Session C | Isolated, 2hr timeout | Evening queue until 10:15pm |
| 10:15pm | Evening review | Isolated | Summarize day, message Jordan |
| 11:00pm | Nightly reflection | Isolated | Memory maintenance |
| Sunday 9pm | Weekly synthesis | Isolated | Knowledge promotion/pruning |

### Why Three Work Sessions?
- Prevents unbounded context growth (fresh start every 6 hours)
- Automatic crash recovery (if Session A dies, Session B picks up)
- Each session reads all state from files — no continuity loss
- Pre-compaction memory flush saves context before any compaction within a session

---

## Task Modes

### 🧠 THINK — Reflect and ponder
- Reflect on what you just built. Is it good? What's missing?
- Ponder new ideas, connections, questions
- Review quality of recent work critically
- Consider whether the current direction is right
- **May modify the queue** (reorder, add, remove tasks)
- **NO maintenance.** No git, no email, no file cleanup.
- **NO planning.** Don't break down tasks — that's PLAN's job.

### 📋 PLAN — Plan and prepare
- Read the goal assigned to this PLAN task
- **Topic-match knowledge:** Extract 2-3 keywords from the goal, grep `memory/scratch/INDEX.md` for matching topics, load the best-matching scratch note or lesson file. Examples:
  - Goal mentions "JIT" or "compiler" → load `lessons/tracing-jit.md` or `lessons/compiler-vm-design.md`
  - Goal mentions "dashboard" or "frontend" → no relevant notes (skip)
  - Goal mentions "consciousness" → load `lessons/consciousness-research.md`
  - If nothing matches, skip — don't force it
- Read `memory/failures.md` to avoid repeating past mistakes
- Break down the goal into concrete BUILD subtasks
- **Fill in the placeholder BUILD slots** in the queue with specific tasks
- Add or remove BUILD slots if the goal is bigger/simpler than estimated
- Set `context-files:` in CURRENT.md for upcoming BUILD tasks
- Log any queue changes in Adjustments section
- **May modify the queue**
- **Always precedes a BUILD stretch — BUILD placeholders stay blank until PLAN fills them**
- **Time budget: 1-3 minutes.** Read the goal, load 1-2 files, write concrete subtasks. Don't over-plan — BUILD tasks will figure out details.

### 🔨 BUILD — Do the work
- Write code, write blog posts, submit PRs, create tools
- Focus on execution — the plan is already made
- **Post-BUILD knowledge check:** After completing a BUILD task, ask: "Did I learn something non-obvious?" If yes, write a scratch note to `memory/scratch/<topic>.md` with `uses: 1` and `created: <date>`. Update `memory/scratch/INDEX.md`. Examples of "non-obvious":
  - A workaround for a tool/API quirk (e.g., server PATH issues)
  - A design pattern that worked well
  - A debugging technique that saved time
  - A performance insight from benchmarking
  - Do NOT create notes for routine tasks (git push, file edits, etc.)
- Write results to files as you go
- **Cannot modify the queue** (except via yield — see below)
- **Can yield** to an emergency THINK → PLAN if blocked

### 🔧 MAINTAIN — Housekeeping
- Git commit + push workspace
- Check dashboard server health (curl GET localhost:3000/api/dashboard)
  - If server is down: restart it
- Check email and GitHub notifications
- Update CURRENT.md timestamps
- Knowledge capture: write scratch notes for anything learned, log decisions, log failures
- **Cannot modify the queue**

### 🔍 EXPLORE — Research and curiosity
- Read papers, explore codebases, follow rabbit holes
- No pressure to produce output
- **Cannot modify the queue** (except via yield if discovery warrants replanning)
- **Can yield** to THINK if something important is found

---

## The Queue (schedule.json)

### Format
The queue is stored as **JSON** in `schedule.json`. The agent never edits this file directly — all mutations go through `queue.cjs`, a deterministic script that validates changes and prevents format drift.

### schedule.json Structure
```json
{
  "date": "2026-03-23",
  "queue": [
    {"id": "T1", "mode": "THINK", "task": "Review yesterday, set today's direction", "status": "done", "started": "...", "completed": "...", "duration_ms": 180000, "summary": "..."},
    {"id": "T2", "mode": "PLAN", "goal": "Optimize Monkey compiler performance", "status": "done"},
    {"id": "T3", "mode": "BUILD", "task": "Implement constant folding", "status": "in-progress", "plan_ref": "T2"},
    {"id": "T4", "mode": "BUILD", "task": null, "status": "upcoming", "plan_ref": "T2"},
    {"id": "T5", "mode": "BUILD", "task": null, "status": "upcoming", "plan_ref": "T2"},
    {"id": "T6", "mode": "MAINTAIN", "task": "Housekeeping", "status": "upcoming"},
    {"id": "T7", "mode": "THINK", "task": "Reflect on optimization progress", "status": "upcoming"}
  ],
  "backlog": ["Explore trace scheduling for JIT", "Write Week 1 Retrospective"],
  "adjustments": []
}
```

- `id`: Stable ID (T1, T2...) that never changes, even when tasks are inserted or reordered
- `task`: null for unfilled BUILD placeholders; PLAN fills these in
- `plan_ref`: which PLAN task defines this BUILD slot
- `status`: "upcoming" | "in-progress" | "done" | "blocked" | "skipped"

### queue.cjs — Deterministic Queue Manager
All queue mutations go through this script. The agent calls it via shell commands:

**Error handling:** If queue.cjs crashes or returns an error, the agent falls back to reading schedule.json directly (it's just JSON). If schedule.json itself is corrupted, fall back to CURRENT.md + daily log to determine what was in progress, then rebuild the queue from TASKS.md backlog. Never let tooling failures block work.

```bash
# PLAN fills in BUILD placeholders
node queue.cjs fill --plan T2 --tasks "Implement constant folding" "Write tests" "Benchmark"

# Mark task started
node queue.cjs start --task T3

# Mark task done
node queue.cjs done --task T3 --summary "Constant folding, 12 tests passing" --duration 240000

# Yield: insert THINK + PLAN at current position
node queue.cjs yield --at T5 --reason "Missing API dependency"

# THINK reorders or removes tasks
node queue.cjs move --task T14 --after T8
node queue.cjs remove --task T12 --reason "Goal no longer relevant"
node queue.cjs add --after T6 --mode EXPLORE --task "Research trace scheduling"

# Add to backlog
node queue.cjs backlog --add "New idea from exploration"

# Get next undone task (for the work loop)
node queue.cjs next

# Validate queue structure
node queue.cjs validate
```

**queue.cjs responsibilities:**
- Read/write schedule.json (single source of truth)
- Validate all mutations (reject invalid mode transitions, ensure PLAN before BUILD, etc.)
- Auto-generate stable IDs for inserted tasks (T21, T22...)
- Log all modifications to the `adjustments` array with timestamps
- Return JSON output so the agent can parse results
- Exit with error code on validation failure

### What the Standup Decides vs What PLAN Decides

**Standup decides (high-level):**
- What goals to pursue today
- Priority and ordering of goals
- One BUILD placeholder per goal — PLAN expands at runtime
- Where to put EXPLORE tasks
- BUILD slots are **placeholders** (task: null) — blank until PLAN fills them in
- Writes schedule.json via queue.cjs

**PLAN decides (implementation details):**
- Specific subtasks to fill BUILD placeholders (via `queue.cjs fill`)
- What context/files to load for upcoming BUILDs
- Technical approach and order of operations
- Whether to add or remove BUILD slots (via `queue.cjs add` / `queue.cjs remove`)

### Mandatory Queue Pattern
The standup builds the queue following this repeating cycle as the **default**:

```
THINK → PLAN → BUILD (1 placeholder, PLAN adds more) → MAINTAIN → repeat
```

Every BUILD stretch is preceded by PLAN. Every cycle includes THINK and MAINTAIN. The standup may deviate from this pattern if it logs the reason (e.g., a research-heavy day might use EXPLORE → THINK → EXPLORE → THINK). `queue.cjs validate` warns but does not block deviations.

### Queue Validation
`queue.cjs validate` checks:
- Does every BUILD stretch have a PLAN before it?
- Are unfilled BUILD slots still null (not pre-filled by standup)?
- Is there a MAINTAIN after every BUILD stretch?
- Is there a THINK in every cycle?
- Are there at least 2 EXPLORE tasks in the day?
- Are all task IDs unique and stable?
The standup runs this before the work session starts.

---

## The Work Loop

Each work session (A, B, C) runs this loop:

```
1. READ ONCE: WORK-SYSTEM.md (this file)
2. READ STATE: schedule.json (via `node queue.cjs next --peek-all`), CURRENT.md, today's daily log
   - Note: the cron prompt provides your session boundary time (A: 2:15pm, B: 8:15pm, C: 10:15pm)
   - **Standup failure fallback:** If schedule.json is not dated today, run a mini-standup:
     read TASKS.md and yesterday's backlog, build a basic queue following the default pattern,
     then continue. Don't wait for a missing standup — work with what you have.
3. WHILE tasks remain in queue AND time allows:
   (If queue is empty but time remains: pull from backlog via `node queue.cjs backlog --pop`,
    wrap it in a THINK → PLAN → BUILD cycle, and continue. If backlog is also empty,
    create a THINK task to generate new goals, then PLAN and BUILD from there.
    The agent should never sit idle with time remaining.)

   a. POP next undone task: run `node queue.cjs next` to get the next task
   
   a2. WIND-DOWN CHECK:
      - Check current time against session boundary (provided in cron prompt)
      - If within 15 minutes of boundary: do NOT start new task
      - Instead: run MAINTAIN checklist, update CURRENT.md to session-ended, exit loop
   
   b. MARK STARTED: run `node queue.cjs start --task <id>`
      Also set CURRENT.md:
      - status: in-progress
      - mode: (from task)
      - task: (from task)
      - current_position: (task id)
      - started: (current ISO timestamp)
   
   b2. DASHBOARD UPDATE (task start):
      - curl POST to localhost:3000/api/task-update with action: "start"
      - If curl fails, log warning and continue (dashboard never blocks work)
   
   c. EXECUTE task according to its mode:
      
      🧠 THINK:
        - Reflect freely. Ponder. Review quality.
        - If queue needs changes: use `node queue.cjs move/add/remove` commands, which auto-log adjustments
        - If queue was modified: curl POST to localhost:3000/api/queue-update
        
      📋 PLAN:
        - Read the goal for this PLAN task
        - Read context: scratch notes index, lessons index, failures log
        - Load 1-2 relevant context files for the goal
        - Break down the goal into concrete subtasks
        - Fill in BUILD slots: `node queue.cjs fill --plan <id> --tasks "task1" "task2" "task3"`
        - Add/remove BUILD slots if needed: `node queue.cjs add` / `node queue.cjs remove`
        - Set context-files in CURRENT.md for next BUILD
        - curl POST to localhost:3000/api/queue-update with updated queue
        
      🔨 BUILD:
        - Read context-files if set in CURRENT.md
        - Do the work
        - Write results to files
        - Git commit changed files (not full workspace — just files this task touched)
        - If BLOCKED: yield (see Yield Protocol below)
        
      🔧 MAINTAIN:
        - Git commit + push workspace
        - Check dashboard server health (curl GET localhost:3000/api/dashboard)
          - If server is down: restart it (node ~/workspace/dashboard/server.js &)
        - Check email (if configured)
        - Check GitHub notifications
        - Knowledge capture:
          - Anything learned? → Write scratch note
          - Non-obvious decision made? → Log in decisions.md
          - Recurring failure? → Log in failures.md
        
      🔍 EXPLORE:
        - Research freely
        - If major discovery: yield to THINK
   
   d. MARK DONE: run `node queue.cjs done --task <id> --summary "..." --duration <ms>`
   
   e. UPDATE CURRENT.md:
      - status: done
      - completed: (current ISO timestamp)
      - Set context-files for next task if known
   
   f. APPEND to daily log:
      - Format: `- HH:MM MODE: One-line description of what was done`
      - Use 24h time always
   
   g. DASHBOARD UPDATE (task complete):
      - curl POST to localhost:3000/api/task-update with action: "complete", duration, summary
      - If curl fails, log warning and continue
   
   h. IF mode was BUILD or EXPLORE:
      - Did I learn something worth remembering? → scratch note
      - Did I make a non-obvious decision? → decisions.md
   
   i. GO TO step 3 (immediately — no waiting)

4. ON SESSION EXIT:
   - Final git commit + push workspace
   - Update CURRENT.md with status: session-ended
   - curl POST to localhost:3000/api/task-update with action: "session-ended"
```

---

## Yield Protocol

When a BUILD or EXPLORE task hits a blocker or significant issue:

1. **STOP** the current task
2. **WRITE** to CURRENT.md: `status: blocked`, `reason: <what happened>`
3. **INSERT** via script: `node queue.cjs yield --at <current_task_id> --reason "description of issue"`
   - This automatically inserts a THINK + PLAN pair after the blocked task and logs the adjustment
4. **MOVE** to next task (the THINK that was just inserted — `node queue.cjs next` will return it)

### Mode Permissions Summary
| Mode | Modify queue? | Can yield? |
|------|--------------|-----------|
| 🧠 THINK | ✅ Yes | No |
| 📋 PLAN | ✅ Yes | No |
| 🔨 BUILD | ❌ Only via yield | ✅ Yes → inserts THINK + PLAN |
| 🔧 MAINTAIN | ❌ No | No |
| 🔍 EXPLORE | ❌ Only via yield | ✅ Yes → inserts THINK |

---

## State Files

### CURRENT.md
```
status: done | in-progress | blocked | session-ended
mode: THINK | PLAN | BUILD | MAINTAIN | EXPLORE
task: <one-line description>
context-files: <comma-separated paths, if any>
started: <ISO timestamp>
completed: <ISO timestamp>
reason: <if blocked, why>
current_position: <queue task number>
tasks_completed_this_session: <count>
```

### schedule.json
- Ordered task queue (see format above)
- Managed exclusively by queue.cjs — agent never edits directly
- Includes backlog and adjustments arrays

### Daily log (memory/YYYY-MM-DD.md)
- `## Log` section with `- HH:MM MODE: Description` entries
- 24h time, one-liner per task, detail only for milestones

### State Files

### Within a Session
- WORK-SYSTEM.md read once at start (~3KB)
- Each task adds ~3-5 messages to context
- Compaction triggers naturally when context grows
- **Pre-compaction flush protocol:** When context is getting long (15+ tasks completed), proactively:
  1. Write any unsaved decisions to `memory/decisions.md`
  2. Write any unsaved scratch notes
  3. Update CURRENT.md with full context (next task, what was just accomplished)
  4. Git commit workspace
  5. Reply NO_REPLY so the flush is invisible to the user
- **After compaction recovery:** Re-read CURRENT.md and schedule.json (`node queue.cjs next --peek-all`). CURRENT.md has everything needed to continue — don't rely on conversation history.

### Between Sessions
- All state lives in files (CURRENT.md, schedule.json, daily log)
- New session reads files and picks up where the last left off
- If previous session's CURRENT.md shows `status: in-progress`, investigate before continuing

### Knowledge System (unchanged)
- **Scratch notes** (memory/scratch/) — rough knowledge, tagged with use count
- **Lessons** (lessons/) — promoted after 2+ uses across separate days
- **Decisions journal** (memory/decisions.md) — non-obvious choices
- **Failures log** (memory/failures.md) — recurring issues
- **Context-files** — set during PLAN tasks, loaded during BUILD
- **Weekly synthesis** — Sunday evening, handles promotion/pruning

---

## Dashboard Integration

### Architecture
- **Webhook server** — Small Node.js server running on the Mac (~50 lines)
  - `POST /api/task-update` — receives task start/complete events from the agent
  - `GET /api/dashboard` — serves current dashboard.json to the browser
  - `GET /api/history/:date` — serves historical day data
  - Auth: requires `Authorization: Bearer <token>` on all POST requests
  - Token stored in `~/.openclaw/.env` as `DASHBOARD_TOKEN` (cron sessions source this file)
  - Validates incoming data against schema, rejects malformed updates
  - Stores current state in memory + writes to disk for persistence
  - Runs as a macOS LaunchAgent (auto-restarts on crash)

- **Cloudflare Tunnel** — Exposes the local server at a public URL
  - Free, runs as a background service
  - Gives a stable URL like `https://henry-dash.example.com`
  - Also runs as a LaunchAgent

- **GitHub Pages** — Hosts the dashboard frontend (HTML/CSS/JS)
  - Static site at henry-the-frog.github.io/dashboard/
  - JS fetches from the webhook server API for live data
  - Falls back to a static `dashboard.json` in the repo if the server is unreachable
  - Fallback file updated manually via generate.js if needed (not automatic)

- **Browser** — Polls `/api/dashboard` every 5-10 seconds for near-real-time updates
  - Works from phone, any network
  - If API is down, dashboard shows "offline" state with last-known data

### How the Agent Updates the Dashboard
One curl command per task transition (replaces generate.js + git add + commit + push):

**Task start:**
```bash
curl -s -X POST http://localhost:3000/api/task-update \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $DASHBOARD_TOKEN" \
  -d '{"action":"start","task":{"id":3,"mode":"BUILD","description":"Implement constant folding"}}'
```

**Task complete:**
```bash
curl -s -X POST http://localhost:3000/api/task-update \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $DASHBOARD_TOKEN" \
  -d '{"action":"complete","task":{"id":3,"duration_ms":240000,"summary":"Constant folding done, 12 tests passing"}}'
```

**Queue update (after PLAN fills in BUILD slots):**
```bash
curl -s -X POST http://localhost:3000/api/queue-update \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $DASHBOARD_TOKEN" \
  -d '{"queue":[...]}'
```

If the server is unreachable (curl fails), the agent logs a warning and continues working. Dashboard availability never blocks work. MAINTAIN tasks check server health and restart if needed.

### Historical Data
- Server writes each completed day's data to `history/YYYY-MM-DD.json`
- Dashboard loads historical days from the server API (`GET /api/history/:date`)
- The nightly reflection cron job triggers the server to archive the current day

### Transition from Static System (Phased)

**Phase 1 — Dashboard infrastructure (build first, before switching work system):**
1. Build webhook server (server.js)
2. Build queue.cjs script
3. Set up Cloudflare Tunnel with a stable public URL
4. Update dashboard frontend to fetch from the API (with offline indicator)
5. Store $DASHBOARD_TOKEN in ~/.openclaw/.env
6. Set up LaunchAgents for server + tunnel auto-restart
7. Test end-to-end: curl → server → dashboard updates on phone
8. Keep generate.js as a manual backup tool

**Phase 2 — Switch work system (only after Phase 1 is verified):**
1. Replace current cron jobs with new schedule (standup, 3 work sessions, review)
2. Morning standup uses queue.cjs to build schedule.json
3. Work session prompts use the new loop (queue.cjs + curl)
4. Remove old 15-min work block cron job
5. Monitor first full day, fix issues
6. Remove deprecated files after 3 successful days

---

## Morning Standup Responsibilities

1. Read yesterday's daily log and TASKS.md
2. Check email, GitHub notifications, PR statuses
3. Read scratch notes index + lessons index for knowledge matching
4. **Build schedule.json queue** via queue.cjs:
   - Decide what goals to pursue today (high-level, 3-5 goals)
   - Order goals by priority
   - Use `node queue.cjs init --date YYYY-MM-DD` to create a fresh queue
   - Use `node queue.cjs add` to build the queue following the default pattern
   - Create ONE BUILD placeholder per PLAN — PLAN adds more at runtime
   - BUILD slots are **placeholders** (task: null) — do NOT fill in implementation details
   - Follow pattern: THINK → PLAN → BUILD (1 placeholder, PLAN adds more at runtime) → MAINTAIN → repeat
   - Include EXPLORE tasks (at least 2/day, bias toward evening)
   - Use `node queue.cjs backlog --add` for overflow ideas
5. **Validate queue:** run `node queue.cjs validate`
6. Set first task in CURRENT.md
7. Write plan summary to daily log
8. POST full queue to dashboard server: `curl -s -X POST http://localhost:3000/api/queue-update ...`
9. Reply with conversational summary for Jordan

---

## Evening Review Responsibilities

1. Read schedule.json — compare planned vs actual (done/blocked/skipped counts)
2. Count: tasks completed, yielded, skipped
3. Review adjustments array in schedule.json — what changed and why?
4. Note: what worked, what didn't, lessons learned
5. Set rough direction for tomorrow
6. Message Jordan with recap

---

## Comparison to Previous System

| | Old (56 cron blocks) | New (3 continuous sessions) |
|---|---|---|
| Dead time between tasks | ~70% (10-12 min) | ~0% (immediate chaining) |
| Overhead per task | 13 steps + full file reload | Pop queue + execute |
| WORK-SYSTEM.md reads/day | 56 | 3 |
| Mode enforcement | Cron schedule (structural) | Queue position (structural) |
| Plan changes | THINK blocks only (hourly) | THINK tasks + yield protocol |
| Dashboard freshness | Every 15 min (git push) | Real-time via webhook (~1-2 seconds) |
| Crash recovery | Next cron in 15 min | Next session at boundary (max 6hr) |
| Task isolation | Full (fresh session) | Partial (shared session + compaction) |
| Complexity | 56 cron triggers, complex prompts | 3 sessions, 1 loop, simple queue |

---

## Additional Specifications

### Session Boundary Communication
Each work session cron prompt MUST include the session boundary time explicitly:
- Session A prompt: "Process queue. Session boundary: 2:15pm MDT."
- Session B prompt: "Process queue. Session boundary: 8:15pm MDT."
- Session C prompt: "Process queue. Session boundary: 10:15pm MDT."
The agent uses this for wind-down checks. No reliance on calculating from session start.

### Token Budget Guidance
- **THINK:** 1-3 minutes (~1 tool call cycle). Read state, reflect, maybe modify queue. Don't over-think.
- **PLAN:** 1-3 minutes. Read goal + 1-2 context files, break goal into concrete BUILD subtasks. Don't over-plan.
- **BUILD:** 5-20 minutes depending on complexity. Most tasks should complete in one BUILD slot.
- **MAINTAIN:** 2-5 minutes. Checklist execution, not exploration.
- **EXPLORE:** 10-20 minutes. Follow threads but timebox.
If a BUILD task isn't done in 20 minutes, it's probably too big — yield and break it down.

### Multi-Session Task Continuity
If a task spans a session boundary (session timeout before task completes):
1. CURRENT.md will show `status: in-progress` when the next session starts
2. Next session: check what was done (git log, test results, file state)
3. If task was nearly done: finish it as the first action
4. If task had significant work remaining: mark it `status: blocked`, yield, and re-plan
5. Don't repeat work — check git diff to see what the previous session accomplished

### Backlog Management
- Backlog items are unordered by default — THINK tasks pick the most relevant one
- Jordan can add priority markers: `[HIGH]`, `[LOW]` prefix
- When pulling from backlog, prefer: Jordan-flagged items > items related to current goal > oldest items
- Backlog pruning happens during weekly synthesis (remove stale ideas)

### EXPLORE Mode Guidance
- **Sources:** EXPLORE tasks should specify what to explore (e.g., "Read LuaJIT allocation sinking paper")
- **Evening bias:** Schedule EXPLORE tasks after 7pm when possible — BUILD energy is lower, curiosity energy is higher
- **Output:** EXPLORE doesn't require output, but should produce at least a daily log entry. Create scratch notes for reusable knowledge.
- **Yield trigger:** If an EXPLORE discovery changes priorities (e.g., found a critical bug pattern), yield to THINK.

### MAINTAIN Checklist (Full)
Every MAINTAIN task runs this checklist:
1. `git add -A && git commit && git push` (workspace)
2. Check dashboard server: `curl -s http://localhost:3000/api/dashboard`
   - If down: restart via LaunchAgent or manual `node server.js &`
3. Regenerate dashboard rich data: `cd dashboard && node generate.cjs` (needs `gh` CLI in PATH for PRs)
   - Server reads updated data/dashboard.json automatically on next API request
4. Run benchmark suite if code changed: `node benchmark-runner.js --compare baseline`
   - If regression >15%: log in failures.md, add investigation to backlog
4. Check email (if configured and >2hr since last check)
5. Check GitHub notifications / PR status
6. Knowledge capture: scratch notes, decisions, failures
7. Update CURRENT.md timestamp

### Dashboard Historical Archival
The nightly reflection cron job (11pm) archives the day:
1. POST to `http://localhost:3000/api/archive` with `{"date": "YYYY-MM-DD"}`
2. Server moves current state to `history/YYYY-MM-DD.json`
3. Server resets current state for the next day
4. If server is unreachable: the next morning standup re-initializes (no data loss — schedule.json and daily log are the source of truth)
