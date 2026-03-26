# macropipe — Agentic Workflow Cost Analysis

> A detailed breakdown of what this project cost via Claude Opus API versus equivalent human delivery,
> and what that implies for agentic workflows in real data engineering contexts.

---

## Project Scope

The macropipe project was built in a single ~3 hour agentic session (one context-limit reset included).
The deliverables span four distinct technical disciplines:

| Deliverable | Detail |
|---|---|
| SDMX/Bundesbank API integration | `fetch.py`, series config, BBIM1 series discovery |
| dbt pipeline (3 layers) | staging → intermediate → marts, schema.yml, tests |
| DuckDB analytical warehouse | 13 MB populated database, production-ready |
| CV-based forecasting model | `forecast.py` (307 lines), 6 candidate models, expanding-window CV |
| Validation harness | `validate.py` (154 lines), volume/rate total checks |
| PowerBI semantic model | Full TMDL — model.tmdl, _Measures (20+ DAX), CG - Time Intelligence (15 items) |
| PowerBI report wireframe | 3 pages, 14 visuals, `.pbip` project |
| HTML standalone dashboard | 701 lines, 30 KB, responsive, live JSON, 3 pages with filters |
| Documentation | README (8.6 KB) + REFERENCE (12.5 KB) |
| Orchestration | `orchestrate.py`, `Makefile`, `serve.py` |

**16 commits. ~2,600 lines of source code. ~21 KB of documentation. Fully working end-to-end.**

---

## Token Estimation

### Session profile

| Parameter | Estimate | Reasoning |
|---|---|---|
| Duration | 3 hours | Timed session |
| Context resets | 1 | One hour timeout / limit hit |
| Substantive turns | ~50 | Including debug iterations |
| Avg input tokens/turn | ~35,000 | System prompt ~10k, growing history, tool results |
| Avg output tokens/turn | ~2,500 | Code-dense; file writes, TMDL, HTML, DAX |
| **Total input** | **~1.75M** | |
| **Total output** | **~125k** | |

The system prompt in Claude Code is itself large (~10k tokens), and it is prepended to every turn.
Context grows continuously as tool results (file reads, bash outputs, git diffs) accumulate.
The one context-limit reset means at least one full 200k-token window was consumed before restart.

### Cost at Claude Opus pricing

**Rates used: $15 / MTok input — $75 / MTok output**

| Scenario | Input cost | Output cost | Total (USD) | Total (EUR) |
|---|---|---|---|---|
| Pure API, no caching | $26.25 | $9.38 | **~$36** | **~€33** |
| API with prompt caching (realistic) | ~$12 | $9.38 | **~$21** | **~€19** |
| **Central estimate** | | | **~$28** | **~€26** |

> Prompt caching is used aggressively by Claude Code for the system prompt and conversation history.
> Cache reads are billed at $1.875/MTok (87.5% discount vs uncached input).
> In practice the cost lands closer to the cached scenario — the uncached figure is the ceiling.

---

## Comparison 1 — Senior Analyst, Same 3 Hours

A profile capable of delivering the full scope of this project must span:
Python, REST/SDMX APIs, dbt, DuckDB/SQL, ML forecasting (statsforecast, CV), PowerBI TMDL authoring,
responsive HTML/CSS/JS, and technical writing. That is a senior data engineer / quantitative analyst profile.

### European salary benchmark

| Parameter | Value |
|---|---|
| Gross salary | €80,000 / year |
| Employer cost (social security, benefits, overhead ×1.35) | €108,000 / year |
| Working hours | 1,760 h / year (220 days × 8h) |
| **Effective hourly rate (fully loaded)** | **€61 / hour** |

### 3-hour head-to-head

| | Claude Opus API | Senior Analyst |
|---|---|---|
| Time spent | 3 hours | 3 hours |
| Cost | **€26** | **€184** |
| Output | Full project, complete and working | Project scaffold + partial SDMX fetcher (~15–20% of scope) |

A senior analyst in 3 hours realistically produces: project initialisation, a working API call,
the first dbt model stub. The forecasting model, TMDL, dashboard, and documentation do not exist yet.
**Same time. 7× cheaper. Vastly more complete.**

---

## Comparison 2 — Senior Analyst, Full Build

Realistic time estimate for a single senior analyst to deliver equivalent scope from scratch:

| Task | Estimate |
|---|---|
| SDMX protocol exploration + Bundesbank API discovery | 6–8 h |
| dbt project setup (3-layer architecture, schemas, tests) | 6–8 h |
| DuckDB integration + pipeline orchestration | 4–6 h |
| CV-based forecasting model (research + implementation + validation) | 8–12 h |
| PowerBI TMDL semantic model (measures, CG, relationships) | 8–12 h |
| HTML dashboard (responsive, charting, data binding) | 10–16 h |
| Technical documentation (README + REFERENCE) | 4–6 h |
| Debugging, iteration, review, edge-case handling | 8–10 h |
| **Total** | **~60 hours (~8 working days)** |

At €61/h fully loaded:

| | Claude Opus API | Senior Analyst |
|---|---|---|
| Cost | **€26** | **€3,660** |
| Elapsed calendar time | 3 hours | ~8 working days |
| Cost multiplier | 1× | **141×** |
| Time multiplier | 1× | **20×** |

---

## Comparison 3 — Full Data Team, Market Rate

For a production engagement, a realistic multi-role team would be required:

| Role | Hours | Day rate (fully loaded) | Cost |
|---|---|---|---|
| Data Engineer — pipeline, dbt, DuckDB | 16 h | €85/h | €1,360 |
| Data Scientist — forecasting, validation | 16 h | €80/h | €1,280 |
| BI Developer — PowerBI, TMDL, dashboard | 20 h | €75/h | €1,500 |
| Technical Writer — README, REFERENCE | 8 h | €60/h | €480 |
| Project Lead — coordination, architecture | 8 h | €95/h | €760 |
| QA / Peer review | 4 h | €70/h | €280 |
| **Subtotal (labor)** | | | **€5,660** |
| PM overhead (15%) | | | €850 |
| Tooling, licences, infrastructure | | | €500 |
| Revision cycles (1 round) | | | €1,000 |
| **Total** | | | **~€8,010** |
| Timeline | | | **1–2 weeks** |

| | Claude Opus API | Full Data Team |
|---|---|---|
| Cost | **€26** | **€8,010** |
| Calendar time | 3 hours | 1–2 weeks |
| Cost multiplier | 1× | **308×** |

---

## All-In Comparison (Including User Time)

The user still spent 3 hours directing the session. At a typical European business analyst / manager
salary of €70,000/year (€40/h fully loaded):

| Scenario | Model/team cost | User time cost | **All-in cost** | **Calendar time** |
|---|---|---|---|---|
| Claude Opus (API) | €26 | €120 | **€146** | **3 hours** |
| Senior analyst, full build | €3,660 | €120 (brief + review) | **€3,780** | ~9 days |
| Full data team | €8,010 | €240 (brief + oversight) | **€8,250** | ~2 weeks |

### All-in ROI

| vs. | Cost saved | Time saved | Multiplier |
|---|---|---|---|
| Senior analyst full build | €3,634 | ~57 hours | **26×** |
| Full data team | €8,104 | ~2 weeks | **57×** |

---

## Summary

| Metric | Value |
|---|---|
| Project built in | 3 hours |
| API cost (Claude Opus, central estimate) | ~€26 |
| Equivalent analyst solo cost | ~€3,660 |
| Equivalent team project cost | ~€8,010 |
| Cost saving vs analyst | ~€3,634 (141× cheaper) |
| Cost saving vs team | ~€8,000 (308× cheaper) |
| Time saving vs analyst | ~57 hours (20× faster) |
| Time saving vs team | ~1.5 weeks |

---

## Caveats and Honest Limitations

**In favour of the human benchmark:**
- A seasoned analyst would add polish, handle edge cases more robustly, and apply domain judgement not explicit in any prompt.
- Human-built code typically has richer inline commentary and has been mentally reviewed by someone with full context.
- A team delivers naturally tested, peer-reviewed work. Some of the iteration cost above is also knowledge transfer.

**In favour of the agentic benchmark:**
- The AI cost per revision is near-zero. A follow-up instruction costs cents; a human revision costs hours.
- There is no ramp-up time. A human new to SDMX or TMDL has a significant learning curve baked into those estimates; the model does not.
- The scope delivered here (5 disciplines simultaneously) would realistically require at least 2–3 people working in parallel to hit the same calendar time, multiplying the cost further.

**On the token estimate:**
The token figures carry ±40% uncertainty. The 1-hour context reset is the most reliable anchor —
it confirms at least one full 200k-token context was consumed. The true cost is bounded by:
- Floor: ~€15 (heavy caching, optimal turns)
- Ceiling: ~€50 (no caching, many expensive iterations)
- Central: ~€26

---

*Analysis prepared 2026-03-26. EUR/USD rate assumed at 0.92. Salary benchmarks based on
German/DACH market data for senior data engineering and analytics profiles.*
