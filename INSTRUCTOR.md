# Instructor notes — OpenSlava 2026

**Not published.** This file lives outside `docs/`, so it never reaches the built site.
Keep anything attendees shouldn't read in here, not in `docs/`.

> MkDocs publishes `docs/` only. HTML comments in those `.md` files **do** survive into
> the rendered page source — don't hide instructor notes in `<!-- ... -->`.

## Before the session

- **Promo code.** The lab publishes Confluent's public codes (`CONFLUENTDEV1` for
  skipping the credit card, `KAFKA101` for $25 extra). If you have an event-specific
  code, add it to the table in `docs/lab/index.md`, Section 2.1.
  - `CONFLUENTDEV1` works on **first-time accounts only**. Anyone who has signed up
    before will be asked for a card — have a fallback code ready for them.
- **Dry run.** Work through Sections 2–4 once on a throwaway Confluent account,
  re-running `bash docs/lab/check-lab.sh` after each. All-green means the lab holds.
  Then `--e2e` for the full pipeline. Include Section 4.7 (action tools) and the
  dashboard's live mode — see [What still needs a real dry run](#what-still-needs-a-real-dry-run).
- **Region.** The setup guide tells attendees to put watsonx Orchestrate in Frankfurt (eu-de).
  If your Confluent cluster is outside Europe, every agent call crosses regions and
  Section 6 feels slower than it needs to.

- **Send the pre-work.** Email attendees `docs/assets/agentic-ai-live-before-you-arrive.pdf` a few days
  ahead. It needs nothing from the site or the workspace: IBMid + Bob trial, Bob IDE,
  the ADK extension, Python, uv. The watsonx Orchestrate trial and credentials are
  optional — expect some of the room to need Setup Step 1 and 12.1 at the start. Rebuild it after edits with
  `python3 src/handouts/build_pdf.py`; keep `docs/before-you-arrive.md` in step.
  In the room, people who did everything start at Setup Step 6; the rest at Step 1.

## During the session

- The promo code field is behind a **"Have a promo code?" → Click Here** link at the
  **bottom** of Confluent's payment screen. Call this out before anyone starts — it's
  the single most likely place to lose the room.
- Attendees must redeem the code **before** creating a cluster, or Confluent blocks
  cluster creation until a payment method exists.
- Section 4 is the long one (40 min: Bob prompts, plus the action tools in 4.7). If
  you're behind, the knowledge base in 4.4 indexes while you talk — start it early.
- Open Section 1 with the dashboard in replay mode on the projector (below). It shows
  the whole pipeline before anyone has built anything.

## Credentials

Never commit them. `.gitignore` covers `.env`, `.env.*`, `*.key`, `*.pem` at any depth.
Attendees get placeholders via `.env.example` inside the workspace zip.

## Pipeline dashboard

`docs/lab/pipeline_dashboard.py` is a local web page showing the pipeline as four
columns — **sales → Flink alerts → agent decisions → actions taken** (restock orders
and ServiceNow tickets). Click any alert, decision or action for the panel that explains
it. Attendees download it in Section 1 and use it through Sections 3 and 6.

### Run it

It serves on <http://localhost:8050> and only runs while its terminal is open — if the
page won't load, the process isn't running. Stop it with `Ctrl+C`.

| Where you are | Command |
| --- | --- |
| This repo, for the projector (no accounts needed) | `python3 docs/lab/pipeline_dashboard.py --replay --workspace /path/to/bobchestrate-confluent/retail-inventory-optimization` |
| Inside a workspace's `fashion-inventory-consumer/`, replay | `uv run pipeline_dashboard.py --replay` |
| Inside a workspace's `fashion-inventory-consumer/`, live | `uv run pipeline_dashboard.py` |

For the first row, unzip `docs/lab/bobchestrate-confluent.zip` anywhere and point
`--workspace` at its `retail-inventory-optimization/` folder. Add `--port 8051` if 8050
is taken.

### Modes

- **Replay** (`--replay`) needs no Kafka, wxO or `.env`. It replays the real lab files:
  the test CSV through the Flink SQL rule, `store_locations.csv`, today's Open-Meteo
  forecast, `product-history-baselines.csv`, and decisions computed from
  `decision-rules.pdf`. **No LLM is involved** — the badge says so. Orders and tickets
  are what the action tools would write. Takes about 30 seconds to fill.
- **Live** (no flag) reads all five topics with the Kafka credentials in `.env`. The
  decisions and actions are the real agent's.

Either mode adds the **Run this alert through the agent** button when the `WXO_*`
settings are in `.env`. On start-up it prints `Agent execution: enabled` or `off`. It
reads `.env` only at start-up, so attendees must restart it after Section 5 (Section 6.1
says so).

### What the panel shows

1. **What Flink sent** — the alert.
2. **Watch the agent execute** — sends the alert through the wxO **runs** API
   (`POST /v1/orchestrate/runs`, then `GET /threads/{id}/messages`), which returns the
   `step_history` that the consumer's `chat/completions` call doesn't. Every tool call
   is shown with its arguments, each labelled **from Flink alert**, **from an earlier
   tool's result**, or **agent's choice**, then each tool result and the answer.
   **This is a real run** — if the agent has action tools, it places another order and
   opens another ticket.
3. **What the agent reported using** — its `reasoning`, grouped by the `[store]`,
   `[weather]`, `[history]`, `[rules]`, `[stock]` tags required by the Section 4.5
   instructions.
4. **What it filled in** — placeholders the consumer sent vs what came back.
5. **What it decided.**
6. **What it did** — the order and ticket for this alert.

Then a **rulebook check**: the dashboard's own lookups and the points each rule in
`decision-rules.pdf` adds, next to the agent's answer. Where they differ is the best
discussion point in the session.

### Action tools (Section 4.7)

`docs/lab/stock_actions.py` holds two wxO Python tools that write through Confluent
Cloud's REST API (standard library only — they run in wxO's cloud and can't reach
laptops):

- `create_restock_order` → `fashion.logistics.orders`
- `open_servicenow_ticket` → `servicenow.incidents`, records shaped like the ServiceNow
  Table API (`POST /api/now/table/incident`). Swap `_publish` for that call to use a real
  instance.

Credentials come from a key-value connection, `confluent_rest`
(`rest_endpoint`, `cluster_id`, `api_key`, `api_secret`), configured for both `draft`
and `live`. Orders over `APPROVAL_LIMIT_USD = 20000`, or with an unknown unit price,
are written as `PENDING_APPROVAL`. The limit is in code on purpose — make that point.

### Things attendees will find (talking points)

- **The test SKU isn't in the knowledge base.** The CSV uses `JACKET-001-M-BLACK`; the
  KB has `WJ-2024-BLK-M`. The dashboard matches them by brand, category, colour and
  size; ask whether the agent's trace found it.
- **Rule 4.1 says it isn't an anomaly.** 5 units/h against the KB baseline of 1.8 is
  under 3×. Flink's fixed baseline of 2.0 made it look like one.
- **The rulebook rating zig-zags** as stock drains, because rule 1.3 drops a point once
  remaining stock is worth under $5,000. In mild weather it never reaches CRITICAL.
- **Weather is live.** Results depend on the day's New York forecast; two runs on
  different days can differ.
- **Duplicate orders.** 18 alerts for one parka become up to 18 restock orders — the
  agent has no memory between alerts. A good closing discussion, or a stretch exercise
  (a tool that checks for open orders first).
- **Same alert, different answer.** "Run it again" on one alert; the panel flags when a
  run disagrees with the published decision.
- **Flink `alertId` collisions.** The SQL builds it from `UNIX_TIMESTAMP()` (seconds),
  so alerts produced in the same second share an ID. The dashboard matches by SKU and
  stock instead; anything downstream that de-duplicates on `alertId` would not.

### What still needs a real dry run

Built and tested against mocks shaped from IBM's ADK source and a local Confluent REST
mock — not yet against a live tenant. Before the session, confirm:

- [ ] `orchestrate tools import -k python -f stock_action_tools/stock_actions.py -p stock_action_tools -a confluent_rest` succeeds and both tools appear in `orchestrate tools list`.
- [ ] The `confluent_rest` connection commands in Section 4.7 work on your ADK version.
- [ ] The REST endpoint in **Cluster settings → Endpoints** matches the `https://pkc-…:443` form in Section 2.8, and a tool call lands a record on `fashion.logistics.orders`.
- [ ] The Section 4.7 Bob prompt updates the agent (tools attached, step 9 added) without rewriting the rest of its instructions.
- [ ] **Run this alert through the agent** shows steps. The runs API wants the agent **ID** (Section 4.6); knowledge-base searches may appear under a step name the dashboard doesn't recognise — they still show, just less neatly.
- [ ] Replace the illustrative stage-3 values in Section 6.3 with real output from a run.

### Troubleshooting

| Symptom | Cause / fix |
| --- | --- |
| `localhost:8050` won't load | The process isn't running — start it again (it stops when its terminal closes) |
| `Can't find the lab files` | Not run from `fashion-inventory-consumer/`; pass `--workspace` |
| `Missing KAFKA_BOOTSTRAP_SERVERS…` | Live mode without `.env`; fill Section 3.1 or use `--replay` |
| No **Run** button | `WXO_*` missing from `.env`, or the dashboard wasn't restarted after Section 5 |
| Actions column empty in live mode | Section 4.7 not done, topics from 2.3 missing, or the agent decided not to act |
| Decisions say "Matching alert not seen yet" | The agent changed `currentStock`; the dashboard pairs cards by SKU + stock |
