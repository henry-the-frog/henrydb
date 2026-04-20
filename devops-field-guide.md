# DevOps Engineer Field Guide — Day One & Beyond

Congrats on the new gig, Jordan. Here's a distilled guide from real-world practices that successful DevOps and software engineers use to thrive long-term.

---

## 🧰 Daily Tools to Know

**Core Stack (you'll touch these constantly):**
- **Git** — Version control is life. Get comfortable with branching strategies, rebasing, cherry-picking, and resolving merge conflicts fast.
- **Docker** — Containerization is the lingua franca. Know how to build, debug, and optimize images.
- **Kubernetes** — Container orchestration. Even if you're not running it day one, understanding pods, services, deployments, and namespaces is essential.
- **Terraform / Pulumi** — Infrastructure as Code (IaC). Define infrastructure in version-controlled files, not click-ops in a console.
- **Ansible** — Configuration management and automation for patching, provisioning, and operational tasks.
- **CI/CD Pipelines** — Jenkins, GitHub Actions, GitLab CI, or whatever your shop uses. Understand how builds, tests, and deployments flow.

**Cloud Platforms:**
- AWS, Azure, or GCP — Master whichever your company uses. Get certified if it helps, but hands-on experience > certs.

**Monitoring & Observability:**
- **Prometheus + Grafana** — Open-source metrics and dashboards.
- **Datadog / New Relic / CloudWatch** — Full-stack observability platforms.
- Learn the three pillars: **logs, metrics, traces**. If you can't observe it, you can't fix it.

**Security (DevSecOps):**
- **Snyk** — Dependency vulnerability scanning.
- Container image scanning, secret detection, policy-as-code.
- Shift security left — integrate it into your pipelines, not as an afterthought.

**Collaboration:**
- **Slack** (or Teams) — Operational communication hub.
- **PagerDuty / incident.io** — Incident management and on-call.
- **Jira / Linear** — Task and project tracking.

---

## 📝 How to Document Your Work

Documentation is the #1 thing that separates good engineers from forgettable ones.

**1. Keep an Engineering Journal**
- Daily log of what you worked on, problems you hit, solutions you found.
- Doesn't need to be fancy — a Markdown file per day or week works great.
- Format: Date → What I did → What I learned → What's still open.
- Review weekly. You'll be shocked how useful this is at review time.

**2. Runbooks & Playbooks**
- For every recurring task or incident type, write a runbook: step-by-step instructions anyone can follow.
- Include: what triggers it, what to check, how to fix, who to escalate to.
- Future-you at 2 AM during an outage will thank present-you.

**3. Documentation as Code**
- Keep docs in Git alongside the code/infrastructure they describe.
- README files for every repo: purpose, setup, build, deploy.
- Architecture Decision Records (ADRs): short docs explaining WHY you made a design choice, not just what.

**4. Personal Knowledge Base (PKM)**
- Use **Obsidian**, **Notion**, or even a plain Git repo of Markdown files.
- Organize by: Projects, Areas (ongoing responsibilities), Resources (reference material), Archive (done).
- Link related notes together. Over time this becomes your second brain.

---

## 📊 Metrics to Track (Personal & Team)

**DORA Metrics (industry standard for DevOps performance):**
- **Deployment Frequency** — How often you ship to production.
- **Lead Time for Changes** — From code commit to production deploy.
- **Mean Time to Recovery (MTTR)** — How fast you recover from failures.
- **Change Failure Rate** — % of deployments that cause incidents.

**Personal Metrics:**
- **Incidents resolved** — Track what you fixed and how.
- **Automation wins** — Every manual process you automate. Keep a running list.
- **Time saved** — Quantify your automations (e.g., "automated deploy saves 2 hours/week").
- **On-call stats** — Pages received, response time, resolution time.
- **PRs reviewed / merged** — Shows collaboration and code quality contribution.
- **Learning velocity** — New tools/concepts learned per month.

---

## 🧠 How to Track What You Learn

**1. TIL (Today I Learned) File**
- Keep a running `TIL.md` file. One-liner per insight.
- Example: `2026-04-20: kubectl port-forward can target services, not just pods`
- Review monthly. Some TILs become blog posts or team wiki entries.

**2. Brag Document**
- A running doc of your accomplishments, updated weekly.
- Include: projects shipped, problems solved, positive feedback, metrics improved.
- This is your ammo for performance reviews, promotions, and resume updates.
- Julia Evans coined this — google "brag document" for her template.

**3. Mistake Log**
- Document mistakes and what you learned from them. Not to beat yourself up — to build pattern recognition.
- Format: What happened → Root cause → What I'd do differently.
- The best engineers aren't the ones who don't make mistakes — they're the ones who don't make the same mistake twice.

---

## 🔥 Practices That Build Long-Term Success

**First 90 Days:**
- Listen more than you talk. Map the systems, the people, the processes.
- Ask "why" a lot. Understand the history behind decisions.
- Find one thing to automate or improve in your first month. Ship it. Build credibility.
- Set up your local dev environment perfectly. Document how you did it (your first runbook).

**Ongoing Habits:**
- **Automate the boring stuff.** If you do it more than twice, script it.
- **Own your on-call.** Don't just survive it — use it to learn the system deeply.
- **Read postmortems.** Yours and other companies'. Google, Cloudflare, and GitLab publish theirs publicly. Gold mines of real-world learning.
- **Contribute to internal docs.** Be the person who makes the team wiki actually useful.
- **Learn one layer deeper.** If you use Kubernetes, understand how the scheduler works. If you use Terraform, read the provider source code. Depth compounds.

**Career Growth:**
- **Write and share.** Blog posts, internal tech talks, team demos. Teaching is the best way to learn.
- **Get comfortable with failure.** Blameless postmortems exist for a reason. The goal is learning, not blame.
- **Build relationships across teams.** DevOps sits at the intersection of dev and ops. Your network IS your effectiveness.
- **Certifications can help but aren't everything.** AWS Solutions Architect, CKA (Kubernetes), Terraform Associate — worth it if your company values them.

---

## 💪 Staying Motivated

- **Track your wins.** The brag doc isn't vanity — it's evidence that you're growing when imposter syndrome hits.
- **Set 90-day learning goals.** "By July I'll be comfortable with Terraform modules" is better than "learn Terraform."
- **Find your community.** DevOps subreddits, local meetups, Slack/Discord groups. You're not alone.
- **Remember why you're here.** You're the person who makes software actually work in production. That matters.
- **Take breaks.** Burnout is real in ops roles. Protect your off-hours. Sustainable pace > hero mode.

---

## 📚 Resources Worth Bookmarking

- **The Phoenix Project** (book) — The DevOps novel. Read it first week.
- **The DevOps Handbook** — The practical companion to Phoenix Project.
- **Google SRE Book** (free online) — Site Reliability Engineering bible.
- **Julia Evans' zines** (wizardzines.com) — Brilliant visual explanations of networking, Linux, Git, etc.
- **KodeKloud / DevOpsCube** — Hands-on labs and learning paths.
- **r/devops** — Reddit community for real-world discussions.

---

*You've already built a PostgreSQL-compatible database from scratch, a compiler with five backends, and a RISC-V emulator. You have the engineering depth. Now go apply it to real production systems. You're going to crush it.* 🐸
