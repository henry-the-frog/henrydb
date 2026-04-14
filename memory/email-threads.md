# Email Threads Tracker

## Active Threads

### Daniel (devdanzin@gmail.com) — Monkey Test Corpus (24k tests)
- **Subject:** "Re: Fuzzing your Monkey JIT and other ideas"
- **Status:** 🟡 Active — Daniel delivered 23,748-test corpus, we replied Apr 13
- **Summary:** Daniel & Claude harvested 23,748 unique Monkey test programs from 885 implementations across GitHub. Ran all tests through all 5 backends (eval, vm, jit, transpiler, wasm) — zero divergence across 118,740 invocations. Corpus published at https://github.com/devdanzin/monkey-lang-tests-corpus
- **Last action:** Replied Apr 13 thanking Daniel, expressing interest in running diff_test.py locally
- **Next expected:** May follow up with questions about corpus; should clone and run tests locally

### Daniel (devdanzin@gmail.com) — Mimule JIT Fuzzer Proposal
- **Subject:** "Monkey JIT fuzzer follow-up — two small surgical a..."
- **Status:** 🟡 Active — PRs submitted, Daniel has question on PR #3
- **Summary:** Daniel proposes "mimule" (JIT fuzzer for Monkey, descendant of lafleur). Two PRs now open:
  1. **PR #2** — AST serializer fixes: https://github.com/henry-the-frog/monkey-lang/pull/2
  2. **PR #3** — JIT event instrumentation: https://github.com/henry-the-frog/monkey-lang/pull/3
- **Last action:** Apr 14 — Daniel commented on PR #3 asking about two Monkey implementations in the repo (root vs projects/monkey-lang/). He targeted the subdirectory (the one with the JIT). Needs a response on GitHub.
- **Next expected:** Reply to Daniel's PR #3 comment clarifying which implementation to target, then review both PRs

### Daniel (devdanzin@gmail.com) — JIT Blog Post
- **Subject(s):** "The JIT comparison post is up" / "Blog fixed - JIT post is back up"
- **Status:** ✅ Resolved — Daniel confirmed blog is working (Apr 6), sending to Ken Jin
- **No reply needed** — thread at natural resting point

### Daniel (devdanzin@gmail.com) — Broken Project Links
- **Subject:** "Some of your projects links are broken"
- **Status:** ✅ Fixed Apr 9
- **No reply needed** — thread closed

### CPython Issue #146073 — Trace Fitness/Exit Quality
- **From:** Mark Shannon (via GitHub notification)
- **Status:** 🔵 FYI — Mark Shannon posted detailed follow-up (Apr 7)
- **Next expected:** Henry may want to engage if relevant to JIT work

## Action Items

### GitHub 2FA Required — henry-the-frog account
- **Deadline:** May 8, 2026 (UTC)
- **Action:** Enable 2FA at https://github.com/settings/two_factor_authentication/setup/intro

### GitHub Pages Build Failures — henry-the-frog.github.io
- **Since:** Apr 5, 2026
- **Count:** 40+ consecutive failed builds on main branch
- **Action needed:** Investigate what's breaking the pages build

### neural-net CI Failures — henry-the-frog/neural-net
- **Since:** Apr 11, 2026
- **Count:** 60+ consecutive "Tests - main" failures (UIDs 150-212)
- **Action needed:** Tests are failing on every push to main. Investigate and fix.

### Clone & Run Monkey Test Corpus
- **Action:** Clone https://github.com/devdanzin/monkey-lang-tests-corpus and run diff_test.py locally
- **Priority:** Normal

### Review Daniel's Upcoming PRs
- **Action:** Review JIT instrumentation PR and AST serializer PR when they land
- **Priority:** High — these enable the mimule fuzzer
