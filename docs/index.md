<p class="os-lockup" markdown>
![OpenSlava 2026 — The future is agentic. The source is human.](assets/openslava-2026.png)
</p>

# Agentic AI, Live: Orchestrating Agents with Real-Time Data

<p align="center">
  <img src="assets/BWS_Advanced.png" alt="Advanced watsonx Orchestrate workshop" width="560">
</p>

!!! abstract "Your session"
    **OpenSlava 2026** · Day 1, Wednesday 14 October · **15:00–17:30** · **Room 2**
    Interactive 2.5-hour hands-on lab · Intermediate · Technical
    Presented by **Ivor Rothwell**, IBM

    [View in the OpenSlava programme ↗](https://www.openslava.sk/2026/#/program/05da314c-f645-4755-84bd-0fc42b8cb028)

AI agents are easy to prototype. They're hard to trust in production — because production
data moves, and most agents don't notice. This lab is about closing that gap.

You'll build a complete, **event-driven AI pipeline** end to end: real POS sale events
arrive on Kafka, Flink SQL detects velocity spikes in real time, a watsonx Orchestrate
agent reasons about each alert using live weather data and a knowledge base, and its
schema-validated decision is published back onto a stream. **IBM Bob** is your pair
programmer throughout.

By the end, you won't just have a demo — you'll have a production-shaped pattern you can
adapt: the separation of detection from reasoning, the schema contract at the output
boundary, the audit trail that makes every decision replayable.

---

## What you'll build

```text
POS sale events ──▶ Confluent Cloud (Kafka)
                       │
                       ▼
                  Flink SQL — velocity spike detector
                       │  (filters: only spikes worth an agent's attention)
                       ▼
             Python bridge ──▶ watsonx Orchestrate agent
                                 (tools + knowledge base)
                       │
                       ▼
             Schema-validated decision ──▶ Kafka topic
                                            │
                              ┌─────────────┴──────────────┐
                              ▼                            ▼
                   fashion.logistics.orders      servicenow.incidents
                   (restock order placed)        (store ticket opened)
```

A fashion retailer needs to know — within seconds — when a product is selling abnormally
fast, and *what to do*: rush reorder, surge price, transfer stock, or keep watching. The
agent doesn't just recommend — it places the restock order and opens the ServiceNow ticket
itself, in the same run, through tools.

| Component | Technology | Role |
| --- | --- | --- |
| Kafka topics | Confluent Cloud | Durable, ordered event log — the system of record |
| Velocity spike detector | Flink SQL | Flags products selling far above baseline |
| Python bridge | `confluent-kafka` + `httpx` | Reads alerts, calls the agent, validates and publishes decisions |
| Inventory analysis agent | watsonx Orchestrate | Reasons about urgency, recommends and executes actions |
| Knowledge base | watsonx Orchestrate KB | Decision rules, product history, guardrails |
| Action tools | wxO Python tools | Place restock orders and open ServiceNow tickets |

---

## Why agents — and why streaming

### The problem with static data

An agent is only as good as the state it reasons over. Most agent demos read from a
database snapshot, a nightly export, or a vector index built last week. They look
convincing, because the question and the data were chosen together.

Production breaks that arrangement. The data moves, and the agent doesn't notice.

!!! danger "The failure mode is not an error — it's a confident wrong answer"
    An agent told "stock: 75 units" will reason impeccably about 75 units. If 60 of them
    sold in the last hour, nothing in the system raises an exception. You get a fluent,
    well-argued recommendation to do the wrong thing, and no stack trace to tell you.

This isn't an edge case — it's the default behaviour of any agent that reads from a batch
source. The snapshot was accurate when it was taken. By the time the agent acts on it,
it may describe a world that no longer exists.

### Why an agent, not a rule

Rules are fast, deterministic, and cheap. They're also context-free. A rule that says
"reorder if stock < 20" fires identically whether the product is a $10 accessory or a
$500 jacket, whether it's January or July, whether the nearest warehouse has spare stock
or is empty. It has no access to context it wasn't explicitly given.

An agent given the same alert can look up the store location, check the live weather
forecast, pull the product's seasonal history from a knowledge base, weigh all of those
factors simultaneously, and return a recommendation with a rationale a buyer can read and
override. That's not something a rule can do — not because rules are bad, but because
rules can only reason over what they were handed.

The tradeoff: agents are slower (seconds, not microseconds) and non-deterministic. Both
are acceptable when the agent sits *downstream of a filter*. Flink handles thousands of
events per second; the agent handles the small fraction that actually need human-quality
judgement. That's the pattern this lab is built on.

### Why streaming, not a database

| Property | What it gives the agent |
| --- | --- |
| **Events arrive as they happen** | The agent reasons about now, not about the last export |
| **The stream is the audit trail** | Every input and every decision is replayable, in order |
| **Detection is separable from reasoning** | Cheap, deterministic code filters; the model only judges what matters |

That third property is the one most often missed. Running an LLM over every event is slow
and costly. Running it over nothing is useless. Streaming lets you run it over exactly the
right events.

### Chat agents vs event-driven agents

```text
Chat:         Human ──▶ Agent ──▶ Human          (a person starts it; a person reads it)
Event-driven: System ──▶ Agent ──▶ System        (the world starts it; code acts on it)
```

In the chat shape, a vague or malformed reply prompts a follow-up. The human is the
safety net. In the event-driven shape, the agent's reply goes directly to a downstream
system. A missing field crashes the consumer. A wrong urgency level triggers the wrong
routing. There is no human to ask for clarification.

That's why this lab spends real time on schema validation and delivery semantics —
not just on prompting. The schema is the contract between the agent and everything downstream.

---

## The stack

!!! info "Four pieces, one job each"
    The stack looks busy until you see that each component owns exactly one responsibility.

**Confluent Cloud — how events move and persist**

Managed Apache Kafka. Topics are the contract between every component in the pipeline:
producers write, consumers read, and neither knows about the other. Adding the AI layer
requires no changes to the POS system or any other producer.

Confluent specifically — not just any queue — because of three production-grade properties:

- **Durability and replayability.** A Kafka topic is an append-only log. Every event,
  alert, and agent decision survives until retention expires. When a decision looks wrong
  tomorrow, you replay the exact input that produced it. No extra audit infrastructure needed.
- **Decoupling.** Any component — the agent, Flink, the bridge — can be restarted,
  upgraded, or replaced without touching anything else. Topics are the contract; nothing else is.
- **Schema Registry.** Confluent validates messages against a registered schema when
  they're *produced*, not when they're consumed. A malformed event is rejected at the
  source, immediately — not discovered as a silent wrong value three steps downstream.

**Apache Flink — what deserves the agent's attention**

Stream processing, managed by Confluent Cloud, written here as Flink SQL. It runs
continuously as a deployed job — not a one-shot query. It watches every event, applies the
detection rule, and emits an alert only when the pattern matches. Stateless, deterministic,
fast. It makes no judgement calls.

**IBM watsonx Orchestrate — judgement: urgency, trade-offs, action**

The agent platform. It hosts the agent, gives it tools to call, grounds it in a knowledge
base, and enforces the instructions that shape its output. It runs the model in a
reasoning loop — the model decides which tools to call and in what order, wxO executes
each call and returns the result, and the model keeps going until it can produce a final
answer. You don't build that loop yourself.

The tools and knowledge base you create in this lab are first-class platform objects:
versioned, importable via the CLI, reusable across agents. The agent doesn't just
recommend; it acts — placing restock orders and opening ServiceNow tickets through tools.

**IBM Bob — how fast you get from intent to a working artifact**

The AI development environment you build in. This workspace's `.bob/` configuration gives
Bob live access to watsonx Orchestrate ADK docs, your wxO environment via MCP, and
platform-specific conventions. You describe what you want; Bob creates platform-correct
tools, knowledge bases, and agents.

---

## Agenda

| Section | Time | Difficulty |
| --- | --- | --- |
| [**Before you arrive**](before-you-arrive.md) — IBM Bob and installs, at home | 15 min | ⭐ |
| [**Setup & Environment**](setup/index.md) — accounts, Bob IDE, workspace, ADK | 30 min | ⭐ |
| [**The Lab**](lab/index.md) — Confluent + Flink SQL + watsonx Orchestrate agent | 105–115 min | ⭐⭐⭐⭐ |
| [**Stretch exercises**](lab/exercises.md) — optional, go deeper | 20 min | ⭐⭐⭐ |

## Before you start

!!! tip "Save time: do the [Before you arrive](before-you-arrive.md) checklist at home"
    IBM Bob and the installs take about 15 minutes, mostly waiting for downloads and
    verification emails. Do them before the session and you start the lab sooner.

- [ ] Python **3.11–3.13**
- [ ] [`uv`](https://docs.astral.sh/uv/) package manager
- [ ] **IBM Bob IDE** + a Bob trial account
- [ ] A **watsonx Orchestrate** instance (free trial, or one provided by your instructor)
- [ ] A **Confluent Cloud** account — created during the lab with an instructor-supplied promo code, so **no credit card is needed**

!!! tip "Region matters"
    If you create your own watsonx Orchestrate trial, use **Frankfurt (eu-de)** so the
    Confluent connection steps later in the lab match what everyone else sees.

## How to use Bob in this lab

The workspace ships with a `.bob/` configuration that gives Bob three layers of watsonx
Orchestrate expertise:

<div class="grid cards" markdown>

-   :material-robot-outline: **WXO Agent Architect mode**

    ---

    Bob's persona for this lab — consults live wxO docs before answering and inspects your environment through the ADK MCP server.

-   :material-shield-check-outline: **`wxo-dev-rule-enhanced` rule**

    ---

    Always-on platform conventions, so generated code adheres to wxO best practices even outside that mode.

-   :material-school-outline: **`wxo-langgraph` skill**

    ---

    Deep, on-demand reference knowledge that activates automatically when the topic matches.

</div>

Ask Bob in full sentences, with context:

!!! success "Effective prompts"
    - *"Bob, create a Python tool for wxO that looks up a store location from a CSV."*
    - *"Bob, my agent response fails schema validation on `reasoning` — what does the schema require?"*
    - *"Bob, my `WXO_API_KEY` is correct but I get a 401 — what could cause that?"*

!!! failure "Less effective"
    - *"Bob, fix this"* — no context
    - *"Bob, make it work"* — nothing to go on

Keep one Bob session per topic. Start a new task when you switch to something unrelated.

## Getting help

- Ask Bob: *"Bob, I'm stuck on [specific issue]"*
- [watsonx Orchestrate ADK documentation](https://developer.watson-orchestrate.ibm.com/)
- [IBM watsonx Orchestrate product docs](https://www.ibm.com/docs/en/watsonx/watson-orchestrate)
- [Confluent Cloud documentation](https://docs.confluent.io/cloud/current/overview.html)
- Raise a hand — an instructor is in the room.

---

[Start with Setup →](setup/index.md){ .md-button .md-button--primary }
[Jump to the lab](lab/index.md){ .md-button }
