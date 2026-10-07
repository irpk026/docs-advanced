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

    AI agents are easy to prototype and hard to run in production — the gap is usually
    integration, live data, and control. In this hands-on lab, you'll build a working
    agentic AI setup end-to-end: assembling and orchestrating agents (using IBM watsonx
    Orchestrate), wiring real-time event streams (using IBM Confluent) so agents act on
    current state instead of static snapshots, and applying governance controls.

    [View in the OpenSlava programme ↗](https://www.openslava.sk/2026/#/program/05da314c-f645-4755-84bd-0fc42b8cb028)

!!! quote "The short version"
    Build a production-shaped, **event-driven AI pipeline**: Kafka events are detected by
    Flink SQL, handed to a watsonx Orchestrate agent for reasoning, and the agent's
    schema-validated decision is published back onto a stream — all built with
    **IBM Bob** as your pair programmer.

## Why agents need real-time data

An agent is only as good as the state it reasons over. Most agent demos read from a
database snapshot, a nightly export, or a vector index built last week — and they look
convincing, because the question and the data were chosen together.

Production breaks that arrangement. The data moves, and the agent doesn't notice.

!!! danger "The failure mode is not an error — it's a confident wrong answer"
    An agent told "stock: 75 units" will reason impeccably about 75 units. If 60 of them
    sold in the last hour, nothing in the system raises an exception. You get a fluent,
    well-argued recommendation to do the wrong thing, and no stack trace to tell you.

This isn't an edge case — it's the default behaviour of any agent that reads from a batch
source. The snapshot was accurate when it was taken. By the time the agent acts on it,
it may describe a world that no longer exists.

**Why an agent specifically, rather than a rule?** Rules are fast, deterministic, and
cheap. They're also brittle: a rule that says "reorder if stock < 20" can't weigh a cold
snap in the forecast, a supplier's current lead time, and the product's price trajectory
at the same time. An agent can. The LLM brings genuine reasoning capability — the ability
to trade off multiple factors, consult unstructured knowledge, and produce a nuanced
recommendation — that no static rule set can replicate. The cost is latency and
unpredictability of output. That's why agents belong downstream of a stream processor,
not in front of every raw event.

Three properties of streaming change what an agent can be trusted to do:

| Property | What it gives the agent |
| --- | --- |
| **Events arrive as they happen** | The agent reasons about now, not about the last export |
| **The stream is the audit trail** | Every input and every decision is replayable, in order |
| **Detection is separable from reasoning** | Cheap, deterministic code finds candidates; the expensive model only judges the ones that matter |

That last point is the one most often missed. Running an LLM over every event is slow and
costly. Running it over nothing is useless. The pattern in this lab — a stream processor
deciding *what deserves attention*, an agent deciding *what to do about it* — is what
makes agentic AI affordable at event-stream volume.

### What an agent adds that rules can't

Consider the same inventory spike handled two ways:

**Rule-based:** `IF velocity_ratio > 2.5 AND hours_to_stockout < 12 THEN flag=REORDER`

This fires reliably and consistently. It also fires identically whether the product is a
$10 accessory or a $500 jacket, whether it's January or July, whether the nearest
warehouse has spare stock or is empty. The rule has no access to context it wasn't
explicitly given.

**Agent-based:** the same alert arrives, and the agent calls its tools. It discovers
the store is in Manhattan, checks the forecast (snow for three days), looks up the
product's seasonal history, notes that this SKU is typically low-margin but the winter
run is short, and recommends a rush reorder with a modest price adjustment — with a
one-paragraph rationale that a buyer can read and override.

The agent output is richer, slower, and harder to unit-test. It's also the kind of
judgement a buyer would give if they had time for every alert. That's the exchange.

### Request-response vs event-driven

```text
Chat agent:     Human ──▶ Agent ──▶ Human          (a person starts every interaction)
Event-driven:   System ──▶ Agent ──▶ System        (the world starts it; nobody is waiting)
```

The second shape is where most enterprise value sits, and it's the harder one to build:
no human in the loop to sanity-check the output, so the contract between the agent and
everything downstream has to be enforced by machinery. That's why this lab spends real
time on schema validation and delivery semantics, not just on prompting.

A chat agent can recover from a vague or malformed reply — the human asks for
clarification. An event-driven agent publishes to a topic; a downstream system consumes
it and acts. If the output is missing a field, the consumer fails. If the urgency level
is a typo, the routing logic breaks. The agent's reply is not a message to a person; it's
a message to code. Code is less forgiving than people.

---

## The products, and what each is actually for

!!! info "Four pieces, one job each"
    The stack looks busy until you see that each product owns exactly one decision.

**Confluent Cloud** — managed Apache Kafka. It is the transport and the system of record
for events: durable, ordered, replayable. Topics decouple producers from consumers, so
the agent can be added, removed, or redeployed without touching the systems generating
events. In this lab it carries raw sale events, velocity alerts, and the agent's
decisions. *Owns: how events move and persist.*

Why Confluent specifically, rather than a message queue or a database change-feed? Three
reasons matter here:

- **Durability and replayability.** A Kafka topic is an append-only log. Every event
  survives until its retention period expires, and any consumer can re-read the log from
  the beginning. When the agent produces a decision, it goes onto a topic too — so the
  full input→detection→decision chain is auditable indefinitely, not just while the
  process is running.
- **Decoupling.** The Python producer doesn't know the agent exists. The agent doesn't
  know about the POS system. Topics are the contract. Adding, removing, or redeploying
  any component doesn't require changing another. In production this means the AI layer
  can be upgraded or replaced without a coordinated rollout across every producer and
  consumer.
- **Schema Registry.** Confluent's Schema Registry enforces a JSON (or Avro) schema at
  the topic level, before a message is written. A producer that sends a malformed event
  gets an error immediately — not a silent corruption that surfaces when the consumer
  tries to parse it two hours later. This is the data-contract layer that makes an
  automated pipeline trustworthy.

**Apache Flink** — stream processing, also managed by Confluent Cloud and written here as
Flink SQL. It runs continuously as a deployed job, not as a one-shot query. It watches every event and emits an alert only when a pattern matches.
It is deterministic, fast, and cheap, and it makes no judgement calls. *Owns: what
deserves the agent's attention.*

**IBM watsonx Orchestrate** — the agent platform. It hosts the agent, gives it tools it
can call, grounds it in a knowledge base, and enforces the instructions that shape its
output. It handles the model, the reasoning loop, and the tool invocations so you don't
build that machinery yourself. *Owns: judgement — urgency, trade-offs, recommended
action.*

Why a platform rather than calling an LLM API directly? Because "agent" is more than a
single model call. An agent reasons in a loop: it reads the input, decides it needs more
information, calls a tool, reads the result, decides to search its knowledge base, reads
those passages, and finally produces a structured answer. Building that loop reliably —
handling tool errors, enforcing output structure, managing token limits, logging each step
— is the infrastructure that watsonx Orchestrate provides. The tools and knowledge base
you create in this lab are first-class platform objects: versioned, importable, inspectable
through the CLI, and reusable across agents.

**IBM Bob** — the AI development environment you build in. With this workshop's
configuration it knows the watsonx Orchestrate platform: it consults live ADK docs,
inspects your environment through MCP, and generates platform-correct tools and agents
from a plain-language description. *Owns: how fast you get from intent to a working
artifact.*

And the glue you write yourself — a small Python bridge — reads alerts, calls the agent,
**validates the response against a JSON schema**, and publishes the result. That
validation step is not ceremony: it's the boundary that stops a plausible-sounding but
malformed answer from reaching a downstream system.

---

## What you'll build

```text
POS sale events ──▶ Confluent Cloud (Kafka)
                       │
                       ▼
                  Flink SQL — velocity spike detector
                       │
                       ▼
             Python bridge ──▶ watsonx Orchestrate agent
                                 (tools + knowledge base)
                       │
                       ▼
             Schema-validated decision ──▶ Kafka topic
```

A fashion retailer needs to know — within seconds — when a product starts selling
abnormally fast, and what to do about it: rush reorder, surge price, transfer stock, or
just keep watching. By the end of the lab you will have that decision loop running
end to end.

| Component | Technology | Role |
| --- | --- | --- |
| Kafka topics | Confluent Cloud | Carry raw inventory events and velocity alerts |
| Velocity spike detector | Flink SQL | Flags products selling far above baseline |
| Python bridge | `confluent-kafka` + `httpx` | Reads alerts, calls the agent, publishes decisions |
| Inventory analysis agent | watsonx Orchestrate | Reasons about urgency, recommends actions, emits strict JSON |
| Knowledge base | watsonx Orchestrate KB | Decision rules, product history, guardrails |

## Agenda

| Section | Time | Difficulty |
| --- | --- | --- |
| [**Before you arrive**](before-you-arrive.md) — IBM Bob and installs, at home | 15 min | ⭐ |
| [**Setup & Environment**](setup/index.md) — accounts, Bob IDE, workspace, ADK | 30 min | ⭐ |
| [**The Lab**](lab/index.md) — Confluent + Flink SQL + watsonx Orchestrate agent | 105–115 min | ⭐⭐⭐⭐ |
| [**Stretch exercises**](lab/exercises.md) — optional, go deeper | 20 min | ⭐⭐⭐ |

## Before you start

You need the following. The setup guide walks you through every one of them.

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

The workspace you download ships with a `.bob/` configuration that gives Bob three
layers of watsonx Orchestrate expertise:

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
