# Initiative: <short name>

Status: draft | active | parked | done  
Tier: trivial | feature | breaking | spike  
Stage: 1-intake | 2-feature | 3-priority | 4-design | 5-verify-design | 6-breakdown | 7-implement | 8-review | 9-verify | 10-integrate | 11-gate  
Skips: _(none | list stage numbers with one-line reason each)_  
Last updated: YYYY-MM-DD

Living document — update in place when later work changes earlier conclusions.  
**§1 Intent (feature) and §4 System design stay distinct — never merge.**

---

## 1. Intent

- **Customer / internal need:** who benefits and what changes for them?
- **Framing:** problem statement in one paragraph.
- **Why now:** trigger or cost of waiting.
- **Success look like:** observable outcome (not a task list).

## 2. Alternatives

- **Existing ways** that already solve this (full or partial).
- **Comparison:** good enough for now? defer? must build?
- **Decision:** proceed | defer | kill — with rationale.

*(Short is fine. “No viable alternative; proceed” is an active decision.)*

## 3. Priority

- **Rank vs current backlog** (or explicit “unranked / insert after X”).
- **Capacity / opportunity cost** in plain language.
- **Decision:** top-of-queue | scheduled | parked — with rationale.

## 4. System design

- **Products / codebases impacted.**
- **Constraints** (tech, security, perf, compatibility, ops).
- **Interfaces / boundaries** that change (APIs, data, agents, UI).
- **Risks** and unknowns.
- **Decision:** design change needed | **explicitly no system-design change** | spike first.

*(An active “no design change” beats skipping this section. Do not restate §1 here.)*

## 5. Verification design

- **End-to-end acceptance** (how we know the feature worked).
- **Integration / regression checks.**
- **CI / delivery expectations** if relevant.
- **Out of scope for v1** (what we will not verify yet).

*(Prefer defining checks before implementation. Thin bullets OK. `/behavior-verifier` runs these.)*

## 6. Work breakdown (AI-oriented)

Not classic “assign to team owners.” Prefer executable slices for agents/humans:

- **Slices:** ordered units of work with clear boundaries and dependencies.
- **Context each slice needs** (paths, APIs, fixtures, plans).
- **What stays human** (product calls, secrets, prod deploy, ambiguous tradeoffs).
- **What agents may do** (implement slice N, run checks from §5, draft docs).
- **Stop / escalate conditions** (when to reopen §1–§4 instead of hacking).

## 7. Implementation notes

Filled or refined when leaving Plan → Agent. Cursor Plan mode already helps here.

- Approach / sequence.
- Key files or modules (expected).
- Bounce triggers → update §4 (or earlier) if reality diverges.

## 8. Verification record

Evidence against §5 after implementation (links, commands, results). Gaps reopen §5/§7/§4 as needed.

## Gate (stage 11)

- **Decision:** accept | revise | replan | escalate | abandon  
- **Notes:**
