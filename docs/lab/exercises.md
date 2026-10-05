# Stretch Exercises

<!--lab estimatedMinutes=20 difficulty=medium-->

Work through these after completing the main lab. Each one ships with a starter asset —
download it into your workspace and build from there.

---

## Exercise 1 — Detect LOW_STOCK events in Flink SQL

**Asset:** [:material-download: `low_stock_detection.sql`](exercises/low_stock_detection.sql)

The velocity detector fires on how *fast* something sells. It stays silent on a product
that sells slowly but is nearly gone. Add a second detector for low absolute stock.

Run the query in the Flink SQL workspace **alongside** the velocity detector — both are
long-running jobs writing to `fashion.velocity.anomalies`.

!!! tip "Why it needs no schema change"
    `LOW_STOCK` is already in the `anomalyType` enum next to `VELOCITY_SPIKE` and
    `SEASONAL_ANOMALY`, and the output topic's schema sets `additionalProperties: false`
    with a fixed required list. Keep the column list identical to the velocity detector
    and the existing consumer and agent pick the alerts up unchanged.

**Take it further:** the agent currently reasons as though every alert is a velocity
spike. Update its instructions so `anomalyType: LOW_STOCK` leads to a different line of
reasoning — restocking rather than surge pricing.

---

## Exercise 2 — Add a store manager contact tool

**Asset:** [:material-download: `store_managers.csv`](exercises/store_managers.csv)

Keyed on the same `storeId` values as `store_locations.csv`, with a name, email, phone
and escalation tier per store.

Build a `get_store_manager` tool the same way you built `get_store_location` in
Section 4.1, then attach it to the agent and update its instructions so escalations name
the responsible manager.

```
Create a Python tool for watsonx Orchestrate called get_store_manager.

It takes storeId (string, required) and returns managerName, managerEmail,
managerPhone and escalationTier by looking up store_managers.csv, which sits
in the same directory as the tool. Use this pattern to find the file:

  import os
  current_dir = os.path.dirname(os.path.abspath(__file__))
  csv_path = f"{current_dir}/store_managers.csv"

Return an error dictionary if the store ID is not found. Handle exceptions
gracefully.

Name the package-root directory differently from the tool function and from
any Python file inside it.
```

!!! warning "The naming constraint bites here"
    The package-root directory must not share a name with the tool function or any file
    in the package. `store_manager_tool/` containing `manager_lookup.py` is fine;
    `get_store_manager/` containing `get_store_manager.py` fails on import.

---

## Exercise 3 — Enforce decision coherence in the validator

**Assets:** [:material-download: `business_rules.py`](exercises/business_rules.py) ·
[:material-download: `test_business_rules.py`](exercises/test_business_rules.py)

Schema validation answers *"is this well-formed?"*. It cannot answer *"does this make
sense?"* — a response can be perfectly schema-valid and still call the situation
`CRITICAL` while recommending no reorder.

Drop both files next to `response_validator.py` and chain the check:

```python
from response_validator import validate_agent_response
from business_rules import enforce_business_rules

payload = enforce_business_rules(validate_agent_response(raw))
```

Order matters — schema first so the fields are guaranteed to exist and have the right
types, business rules second to reason about the values.

Run the tests:

```bash
uv run python test_business_rules.py
```

The module ships five rules. Rule 1 is the one the exercise asks for; the rest are there
to argue with:

| Rule | Catches |
| --- | --- |
| 1 | `CRITICAL` urgency with `shouldReorder: false` |
| 2 | A reorder with no quantity |
| 3 | `urgencyScore` inconsistent with `urgencyLevel` |
| 4 | `CRITICAL`/`HIGH` recommending nothing that moves stock |
| 5 | Empty `reasoning` entries |

!!! note "What a violation does"
    `enforce_business_rules` raises `ValueError`, which propagates to the main loop — so
    the Kafka offset is **not** committed and the alert is reprocessed. Same
    at-least-once behaviour the lab already relies on for schema failures. Worth deciding
    deliberately: an incoherent decision retried forever is its own failure mode.

---

## Exercise 4 — Wire the agent as a collaborator

**Asset:** [:material-download: `inventory_orchestrator.yaml`](exercises/inventory_orchestrator.yaml)

The Section 4 agent is built to be called by a machine — structured alert in, strict JSON
out. This puts a conversational front door on it.

```bash
orchestrate agents import -f inventory_orchestrator.yaml
```

Open the wxO chat UI, select **Inventory Desk**, and ask:

> A winter jacket at the Manhattan store is selling 12x faster than normal and we have
> 75 units left. What should we do?

Watch what the orchestrator does: it has to collect `storeId`, `sku`, stock and velocity
before it can delegate, then translate the JSON that comes back into something a buyer
would actually want to read.

!!! warning "The collaborator name is exact"
    `Fashion_Inventory_Alert_Processor` must match the agent name from Section 4.5
    character for character, including case. A mismatch imports cleanly and then silently
    fails to route.

---

## Exercise 5 (hard) — Resolve schemas from the registry

**Asset:** [:material-download: `schema_registry_deserializer.py`](exercises/schema_registry_deserializer.py)

The consumer currently reads the alert schema off disk. That works, but the file on your
laptop and the schema registered against the topic can drift apart silently. Resolving
from Schema Registry makes the registry the single source of truth.

Check the registry resolves before touching the consumer:

```bash
uv run python schema_registry_deserializer.py
```

Then swap the deserializer in `consume_velocity_alerts_with_agent.py`:

```python
from schema_registry_deserializer import build_registry_deserializer

value_deserializer = build_registry_deserializer(
    _schema_registry_client, "fashion.velocity.anomalies")
```

Three things make this the hard one:

- **Subject naming.** The default `TopicNameStrategy` means the subject is
  `<topic>-value`, not the topic name. Get it wrong and the registry returns a 404 that
  doesn't explain itself.
- **Startup coupling.** The consumer now fails to start if the registry is unreachable,
  where before it started and failed later. Usually what you want — but it is a real
  behavioural change.
- **Caching.** `SchemaRegistryClient` caches by schema id, so the lookup cost is paid
  once, not per message.

---

[← Back to the lab](index.md){ .md-button }
[Workshop home](../index.md){ .md-button }
