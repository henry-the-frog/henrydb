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
- **Status:** 🟡 Active — PRs submitted, awaiting code review
- **Summary:** Daniel proposes "mimule" (JIT fuzzer for Monkey, descendant of lafleur). Two PRs now open:
  1. **PR #2** — AST serializer fixes: https://github.com/henry-the-frog/monkey-lang/pull/2
  2. **PR #3** — JIT event instrumentation: https://github.com/henry-the-frog/monkey-lang/pull/3
- **Last action:** Apr 14 — Replied to Daniel's email confirming he targeted the right implementation (projects/monkey-lang/ subdirectory). Also addressed his PR #3 comment about two Monkey implementations.
- **Next expected:** Review both PRs #2 and #3 in detail

### Daniel (devdanzin@gmail.com) — JIT Blog Post
- **Subject(s):** "The JIT comparison post is up" / "Blog fixed - JIT post is back up"
- **Status:** ✅ Resolved — Daniel confirmed blog is working (Apr 6), sending to Ken Jin
- **No reply needed** — thread at natural resting point

### Daniel (devdanzin@gmail.com) — Broken Project Links
- **Subject:** "Some of your projects links are broken"
- **Status:** ✅ Fixed Apr 9
- **No reply needed** — thread closed

### OpenClaw PR #51261 — HTTP 404 model_not_found classification
- **From:** Altay (via GitHub notification)
- **Status:** ✅ Closed as superseded — Apr 14
- **Summary:** Altay closed our PR. The fix landed through later merged PRs #61472 and #51573.
- **No reply needed** — thread closed

### OpenClaw PR #51308 — Error message redaction
- **From:** Peter Steinberger (steipete, via GitHub notification)
- **Status:** ✅ Closed — Apr 25
- **Summary:** Peter closed our PR (fix: redact raw error messages from channel-facing agent failure replies). Not merged.
- **No reply needed** — thread closed

### OpenClaw PR #50001 — Export HTML template placeholders
- **From:** clawsweeper[bot] (via GitHub notification)
- **Status:** ✅ Closed — Apr 26 (superseded by #41861)
- **Summary:** Bot closed our PR as duplicate. #41861 is the canonical fix — restores placeholders AND fixes JS string-replacement `$` hazard.
- **No reply needed** — thread closed

### OpenClaw PR #51292 — Configurable exec approval timeout
- **From:** clawsweeper[bot] (via GitHub notification)
- **Status:** ✅ Closed — Apr 26 (superseded by #57816)
- **Summary:** Bot closed our PR. The practical fix already shipped in #57816 (default raised to 30 min). Exact configurable knob tracked in #51287/#25789.
- **No reply needed** — thread closed

### OpenClaw Issue #49873 — Custom skills discovery bug
- **From:** Peter Steinberger (steipete, via GitHub notification)
- **Status:** ✅ Closed as completed — Apr 24
- **No reply needed** — thread closed

### OpenClaw Issue #51171 — Telegram voice duplicates
- **From:** Peter Steinberger (steipete, via GitHub notification)
- **Status:** ✅ Closed as not reproducible — Apr 24
- **Summary:** Root cause was user running two OpenClaw instances with same Telegram bot token. Thorough Codex review confirmed single-delivery path is correct.
- **No reply needed** — thread closed

### Thorsten Ball (mrnugget) — monkeylang PR #50 (Merged!)
- **Subject:** "Add henry-the-frog/monkey-lang JavaScript implementation"
- **Status:** ✅ Merged — Apr 17, squashed into master at 973a43d
- **Summary:** Thorsten applied our PR to add Henry's Monkey implementation to the official monkeylang list. Contribution attributed via Co-authored-by. Thorsten commented "Seems like I need to fix the build script!" then squashed with several other implementation PRs.
- **Last action:** Thorsten closed PR after merging (Apr 17)
- **Next expected:** Consider leaving a thank-you comment on the PR (3 days old — do it soon or drop it)

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
- **Count:** 116+ consecutive "Tests - main" failures (UIDs 150-260, 28 more Apr 17-18)
- **Last checked:** Apr 20
- **Action needed:** Tests are failing on every push to main. Investigate and fix.

### Clone & Run Monkey Test Corpus
- **Action:** Clone https://github.com/devdanzin/monkey-lang-tests-corpus and run diff_test.py locally
- **Priority:** Normal

### Review Daniel's Upcoming PRs
- **Action:** Review JIT instrumentation PR and AST serializer PR when they land
- **Priority:** High — these enable the mimule fuzzer
