# Event-Driven AI Agents with Confluent Cloud

<p align="center">
  <img src="images/bobchestrate-confluent.png" alt="Event-driven AI agents lab" width="640">
</p>

**Duration:** 75–90 minutes · **Difficulty:** ⭐⭐⭐⭐

---

## Before you start

This lab assumes you completed [Part 0 — Setup & Environment](../setup/index.md).
You should already have the `bobchestrate-confluent/` workspace open in Bob IDE, a
Python virtual environment, the ADK installed, and `orchestrate agents list` working.

You also need a **Confluent Cloud** account — the free trial is enough.
Sign up at [confluent.cloud](https://confluent.cloud) if you don't have one.

### Install the lab dependencies

Open a terminal in Bob IDE (**Terminal** → **New Terminal**) and run:

```bash
cd retail-inventory-optimization
uv sync --locked
```

This installs `confluent-kafka`, `ibm-watsonx-orchestrate`, `python-dotenv`, `jsonschema`
and the rest, at the exact versions pinned in `uv.lock`.

### Create your `.env` file

```bash
cd fashion-inventory-consumer
cp .env.example .env
```

Leave the values as placeholders for now — you'll fill in the Confluent credentials in
[Section 2](#section-2-confluent-cloud-setup-15-min) and the watsonx Orchestrate
credentials in [Section 5](#section-5-configure-the-python-consumer-5-min).

!!! success "Ready to start when"
    - [ ] `bobchestrate-confluent/` is open in Bob IDE and **WXO Agent Architect** appears in the mode selector
    - [ ] `uv sync --locked` completed without errors
    - [ ] `.env` exists in `fashion-inventory-consumer/`
    - [ ] `orchestrate agents list` returns without error
    - [ ] You can log in to Confluent Cloud

---

## Overview

This lab teaches you to build an **event-driven AI pipeline** — a pattern where real-time streaming events automatically trigger AI agent analysis. You'll combine two cloud platforms: **Confluent Cloud** for stream processing and **IBM watsonx Orchestrate** for AI-powered decision-making.

The business scenario: a fashion retailer needs to detect when a product is selling abnormally fast and immediately get an AI-driven inventory decision — reorder, surge price, or monitor — before stock runs out.

### What You'll Build

| Component                          | Technology              | What it does                                                         |
| ---------------------------------- | ----------------------- | -------------------------------------------------------------------- |
| **Kafka topics**             | Confluent Cloud         | Carry raw inventory events and velocity alerts                       |
| **Velocity spike detector**  | Flink SQL               | Identifies products selling 3× faster than baseline                 |
| **Python consumer**          | confluent-kafka + httpx | Reads alerts, calls the wxO agent, publishes decisions               |
| **Inventory analysis agent** | watsonx Orchestrate     | Reasons about urgency, recommends actions, outputs schema-valid JSON |
| **Knowledge base**           | wxO KB                  | Gives the agent decision rules, product history, and guardrails      |

All Python code is **pre-built** in `retail-inventory-optimization/`. Your job is to wire it together, configure the platforms, and use Bob to create the agent.

### Why This Pattern Matters

| Use event-driven AI when...                | Use a chat agent when...                  |
| ------------------------------------------ | ----------------------------------------- |
| Events happen continuously and at volume   | A human initiates each request            |
| Decisions must be made in near-real-time   | Response latency of seconds is acceptable |
| AI enrichment feeds a downstream system    | Output is for a human to read             |
| You need audit trails of every AI decision | Conversation context is the primary state |

---

## Using Bob for this lab

Bob (in **WXO Agent Architect mode**) handles the watsonx Orchestrate side — creating tools, a knowledge base, and the agent. The prompts in Section 4 are written to be copy-pasted directly.

| When you're on…                               | Ask Bob…                                                                                    |
| ---------------------------------------------- | -------------------------------------------------------------------------------------------- |
| **Section 4.1 — Store Location Tool**   | Copy-paste the prompt block provided in the section                                          |
| **Section 4.2 — Weather Forecast Tool** | Copy-paste the prompt block provided in the section                                          |
| **Section 4.3 — Knowledge Base**        | Copy-paste the prompt block provided in the section                                          |
| **Section 4.4 — Agent**                 | Copy-paste the prompt block provided in the section                                          |
| **Section 5 — Consumer config**         | `"Show me which environment variables the orchestrate_client.py needs"`                    |
| **Debugging auth errors**                | `"My WXO_API_KEY is correct but I get 401 — what could cause this?"`                      |
| **Debugging schema validation**          | `"The agent response is failing validation on reasoning — what does the schema require?"` |

---

## Section 0 — What is Event-Driven AI? (5 min)

### The pattern

Traditional AI workflows are **request-response**: a human asks a question, the agent replies. Event-driven AI flips this: an **event in a stream** triggers the agent automatically, with no human in the loop.

```
Traditional:   Human → Agent → Human
Event-driven:  System event → Agent → Downstream system
```

This unlocks AI automation at scale. Inventory spikes, fraud signals, equipment anomalies, sensor readings — any event stream can be enriched with AI reasoning.

### The three roles in this pipeline

**Stream processor (Flink SQL)** — Watches the raw event stream and detects patterns. Stateless, deterministic, fast. Produces structured alerts. Does not make judgment calls.

**AI agent (watsonx Orchestrate)** — Receives a structured alert, reasons about it using knowledge and tools, and returns a structured decision. Slow relative to Flink (seconds), but capable of nuanced judgment.

**Bridge (Python consumer)** — Connects the two worlds. Reads Kafka alerts, calls the agent synchronously, validates the response against a JSON schema, and publishes the enriched result to a new Kafka topic.

### What you learned

- Event-driven AI separates fast pattern detection from slow AI reasoning
- A Python bridge connects streaming infrastructure to an AI agent
- Schema validation at the output boundary is essential for downstream reliability

---

## Section 1 — Architecture Overview (5 min)

```text
┌──────────────────────────────────────────────────────────────────┐
│                     Confluent Cloud                              │
│                                                                  │
│  Python Producer  ──▶  fashion.inventory.events  ──▶  Flink SQL  │
│  (test CSV data)        (raw POS events)              (velocity   │
│                                                        spike      │
│                                                        detector)  │
│                                                          │        │
│                                          fashion.velocity.anomalies
│                                                          │        │
└──────────────────────────────────────────────────────────┼───────┘
                                                           │
                                              Python Consumer reads
                                                           │
                                                           ▼
                                          ┌─────────────────────────┐
                                          │   watsonx Orchestrate   │
                                          │                         │
                                          │  Fashion Inventory      │
                                          │  Alert Processor        │
                                          │                         │
                                          │  Tools:                 │
                                          │  - get_store_location   │
                                          │  - get_weather_forecast │
                                          │                         │
                                          │  Knowledge Base:        │
                                          │  - inventory-alert-     │
                                          │    knowledge            │
                                          └──────────┬──────────────┘
                                                     │
                                          Structured JSON decision
                                                     │
                              ┌──────────────────────▼───────────────┐
                              │  Python Consumer validates + publishes │
                              │  ▶  fashion.agent.responses (Kafka)   │
                              └───────────────────────────────────────┘
```

### Three Kafka topics

| Topic                          | Producer             | Consumer           | Contents              |
| ------------------------------ | -------------------- | ------------------ | --------------------- |
| `fashion.inventory.events`   | Python test producer | Flink SQL          | Raw POS sale events   |
| `fashion.velocity.anomalies` | Flink SQL            | Python consumer    | Velocity spike alerts |
| `fashion.agent.responses`    | Python consumer      | Downstream systems | AI-enriched decisions |

### Key files in `retail-inventory-optimization/`

```text
retail-inventory-optimization/
├── fashion-inventory-consumer/          # Python applications (pre-built)
│   ├── .env.example                     # ← You'll fill this in Section 5
│   ├── produce_inventory_events.py      # Test data producer
│   ├── consume_velocity_alerts.py       # Basic consumer (Section 3)
│   ├── consume_velocity_alerts_with_agent.py  # Agent consumer (Section 6)
│   ├── orchestrate_client.py            # wxO HTTP client + token management
│   ├── agent_payload_builder.py         # Alert → agent request transformer
│   ├── agent_response_producer.py       # Kafka publisher for agent responses
│   └── response_validator.py           # JSON schema validator
│
├── fashion-inventory-setup/
│   ├── schemas/                         # Three JSON schemas
│   │   ├── fashion-inventory-event.schema.json
│   │   ├── velocity-anomaly-alert.schema.json
│   │   └── agent-response.schema.json
│   ├── sql/
│   │   └── velocity_anomaly_detection.sql   # Flink SQL query
│   └── data/
│       ├── test_winter_jacket_spike.csv     # Test data
│       └── store_locations.csv              # Store location data for tool
│
└── labs/part2-watsonx-orchestrate/
    └── inventory-alert-demo-knowledge/  # Knowledge base documents (6 files)
```

---

## Section 2 — Confluent Cloud Setup (15 min)

You need a Confluent Cloud environment with three topics, a Schema Registry, and a Flink compute pool.

> **Already done this?** If you completed the original Confluent setup lab, you can skip to Section 3 — just confirm your topics and Flink query are running.

### 2.1 Create your environment and cluster

1. Log in to [confluent.cloud](https://confluent.cloud)
2. Click **Add environment** → name it `retail-inventory-bootcamp`
3. Inside the environment, click **Add cluster** → choose **Basic** → select a cloud/region → name it `retail-inventory-cluster`
4. Note the **Bootstrap server URL** — you'll need it for `.env`

### 2.2 Create three Kafka topics

In your cluster, navigate to **Topics** → **Add topic**. Create all three:

| Topic name                     | Partitions |
| ------------------------------ | ---------- |
| `fashion.inventory.events`   | 3          |
| `fashion.velocity.anomalies` | 3          |
| `fashion.agent.responses`    | 3          |

### 2.3 Register JSON Schemas

For each topic, attach the corresponding schema from `retail-inventory-optimization/fashion-inventory-setup/schemas/`:

| Topic                          | Schema file                             |
| ------------------------------ | --------------------------------------- |
| `fashion.inventory.events`   | `fashion-inventory-event.schema.json` |
| `fashion.velocity.anomalies` | `velocity-anomaly-alert.schema.json`  |
| `fashion.agent.responses`    | `agent-response.schema.json`          |

**Steps for each topic:** click the topic → **Schema** tab → **Add schema** → paste the JSON content.

### 2.4 Create a Flink compute pool

1. In your environment, click **Stream Processing** (left sidebar)
2. Click **Create compute pool** → choose a region → name it `retail-inventory-flink` → **Continue**
3. Once the pool is ready, click **Open SQL workspace**

### 2.5 Deploy the velocity spike detector

In the Flink SQL workspace, paste and run the query from `retail-inventory-optimization/fashion-inventory-setup/sql/velocity_anomaly_detection.sql`.

The query monitors `fashion.inventory.events` and writes to `fashion.velocity.anomalies` whenever a SALE event removes 5 or more units:

```sql
INSERT INTO `fashion.velocity.anomalies` ( ... )
SELECT
    ...
    CASE
        WHEN ABS(quantityChange) > 20 THEN 'CRITICAL'
        WHEN ABS(quantityChange) > 10 THEN 'HIGH'
        ELSE 'MEDIUM'
    END as severity,
    CAST(ABS(quantityChange) AS DOUBLE) as currentVelocity,
    CAST(2.0 AS DOUBLE) as baselineVelocity,   -- ← see callout below
    ...
FROM `fashion.inventory.events`
WHERE eventType = 'SALE'
    AND ABS(quantityChange) >= 5;
```

> **⚠️ Demo simplification — hardcoded baseline:** The query uses a fixed baseline velocity of `2.0 units/hour`. In production you would compute a rolling 7-day average per SKU using Flink's windowing functions (`TUMBLE`, `HOP`, or `CUMULATE` windows with `ORDER BY eventTime`). The fixed value makes the demo deterministic and removes the warm-up period that a real rolling window requires. The pipeline logic and alert schema are production-grade; only the baseline calculation is simplified.

### 2.6 Generate API keys

You need **two sets** of API keys:

**Kafka API key:**

1. In your cluster → **API Keys** → **Create key** → Scope: `Global access`
2. Save the **Key** and **Secret** — these are `KAFKA_API_KEY` / `KAFKA_API_SECRET`

**Schema Registry API key:**

1. In your environment (not cluster) → **Schema Registry** → **API credentials** → **Create key**
2. Save the **Key** and **Secret** — these are `SCHEMA_REGISTRY_API_KEY` / `SCHEMA_REGISTRY_API_SECRET`

Also note your **Schema Registry URL** from the Schema Registry panel — this is `SCHEMA_REGISTRY_URL`.

### What you learned

- Confluent Cloud organises resources into environments → clusters → topics
- Schema Registry enforces data contracts at the topic level
- Flink SQL runs continuously as a deployed job — it's not a one-shot query
- A fixed baseline is a valid demo simplification; production requires windowed aggregation

---

## Section 3 — Test the Velocity Detection Pipeline (10 min)

Before adding the AI layer, verify that Flink SQL correctly detects velocity spikes.

### 3.1 Fill in your Confluent credentials

Edit `retail-inventory-optimization/fashion-inventory-consumer/.env` and fill in the
Confluent section with the values from [Section 2.6](#26-generate-api-keys). Leave the
`WXO_*` lines as placeholders for now:

```bash
KAFKA_BOOTSTRAP_SERVERS=pkc-xxxxx.us-east-1.aws.confluent.cloud:9092
KAFKA_API_KEY=your_kafka_api_key
KAFKA_API_SECRET=your_kafka_api_secret

SCHEMA_REGISTRY_URL=https://psrc-xxxxx.us-east-1.aws.confluent.cloud
SCHEMA_REGISTRY_API_KEY=your_schema_registry_api_key
SCHEMA_REGISTRY_API_SECRET=your_schema_registry_api_secret
```

### 3.2 Run the test producer

```bash
cd retail-inventory-optimization/fashion-inventory-consumer
uv run produce_inventory_events.py --csv-file ../fashion-inventory-setup/data/test_winter_jacket_spike.csv
```

This produces a series of SALE events with large `quantityChange` values — designed to trigger the Flink velocity detector.

### 3.3 Run the basic consumer to verify alerts

```bash
uv run consume_velocity_alerts.py
```

You should see velocity alert messages printed to stdout. Verify that `severity` is `CRITICAL` or `HIGH` and that `velocityRatio` is well above 1.0.

If you see alerts flowing — **the Flink pipeline is working**. Stop the consumer with `Ctrl+C` and move to Section 4.

### What you learned

- Flink SQL runs as a persistent job; events flow through as soon as they arrive on the input topic
- The test CSV provides reproducible spike data to validate detection logic
- Separating "does Flink work?" from "does the agent work?" makes debugging much easier

---

## Section 4 — Create the AI Agent Using Bob (20 min)

This is the Bob-driven section. You'll create two tools, a knowledge base, and the agent — all by pasting prompts into Bob.

**Switch to WXO Agent Architect mode before starting:**

1. In Bob's chat panel, click the mode selector
2. Select **WXO Agent Architect**

### 4.1 Create the Store Location Lookup Tool

Paste this prompt into Bob:

```
Search the watsonx Orchestrate ADK documentation to understand how to create a Python tool for watsonx Orchestrate.

Then, create a Python tool with the following specifications:

Tool Name: get_store_location

Tool Description:
Returns the geographic location (latitude, longitude) and details for a given store ID. This tool reads from a CSV file containing store location data.

Tool Parameters:
- storeId (string, required): The unique identifier for the store (e.g., "STORE-NYC-001")

Tool Returns:
Dictionary containing store location details including:
- storeId: The store identifier
- storeName: Name of the store
- city: City where store is located
- state: State abbreviation
- country: Country code
- latitude: Latitude coordinate (float)
- longitude: Longitude coordinate (float)
- timezone: Timezone string

Implementation Requirements:
1. The tool should read from a CSV file named "store_locations.csv"
2. Use the following code pattern to locate the CSV file:
   import os
   current_dir = os.path.dirname(os.path.abspath(__file__))
   csv_path = f"{current_dir}/store_locations.csv"
3. Return an error dictionary if the store ID is not found or if the CSV file doesn't exist
4. Handle exceptions gracefully

Data File:
Copy the file "retail-inventory-optimization/fashion-inventory-setup/data/store_locations.csv" to the same directory as the tool (this will be the package-root).

After creating the tool:
1. Save it to a directory that will serve as the package-root
2. Copy store_locations.csv to that same directory
3. Import the tool to watsonx Orchestrate using the package-root parameter

Important Naming Constraints:
- The package-root directory name MUST NOT match the tool function name or any Python file names in the package
- Use only alphanumeric characters and underscores (_) in directory and file names
- Example: If the tool function is named "get_store_location", name the directory something like "store_location_tool" instead of "get_store_location"
- The Python file containing the tool should be named something simple like "store_lookup_tool.py" to avoid naming conflicts
```

> Bob will search the documentation, create the Python tool file, copy the CSV, and import it to watsonx Orchestrate. Wait for confirmation before proceeding.

### 4.2 Create the Weather Forecast Tool

Paste this prompt into Bob:

```
Create another Python tool for watsonx Orchestrate with the following specifications:

Tool Name: get_weather_forecast

Tool Description:
Returns weather forecast for the next few days for a given location (latitude, longitude). Uses the free Open-Meteo API which doesn't require an API key.

Tool Parameters:
- latitude (float, required): Latitude of the location (-90 to 90)
- longitude (float, required): Longitude of the location (-180 to 180)
- days (integer, optional, default=7): Number of days to forecast (1-16)

Tool Returns:
Dictionary containing weather forecast data including:
- latitude: Location latitude
- longitude: Location longitude
- timezone: Timezone string
- elevation: Elevation in meters
- forecast_days: Number of forecast days
- forecast: Array of daily forecasts with:
  - date: Date string (YYYY-MM-DD)
  - temperature_max: Maximum temperature in Fahrenheit
  - temperature_min: Minimum temperature in Fahrenheit
  - precipitation_sum: Total precipitation in inches
  - windspeed_max: Maximum wind speed in mph
  - weather_code: WMO weather code
  - weather_description: Human-readable weather description
- summary: Brief text summary of the forecast

Implementation Requirements:
1. Use the Open-Meteo API: https://api.open-meteo.com/v1/forecast
2. No API key is required for this free service
3. Use urllib.request to make HTTP requests (no external dependencies)
4. Include weather code descriptions based on WMO Weather interpretation codes
5. Generate a helpful summary of the forecast
6. Handle network errors and invalid coordinates gracefully
7. Validate latitude (-90 to 90) and longitude (-180 to 180) ranges

Weather Code Mappings (include these in the tool):
- 0: Clear sky
- 1: Mainly clear
- 2: Partly cloudy
- 3: Overcast
- 45: Foggy
- 51-55: Drizzle (light to dense)
- 61-65: Rain (slight to heavy)
- 71-75: Snow (slight to heavy)
- 80-82: Rain showers
- 85-86: Snow showers
- 95-99: Thunderstorm

After creating the tool, import it to watsonx Orchestrate.
```

> This tool uses a free public API — no credentials needed.

### 4.3 Verify both tools are imported

Paste this prompt into Bob:

```
List all the tools in watsonx Orchestrate to verify that both "get_store_location" and "get_weather_forecast" tools have been successfully created and imported.
```

You should see both tools listed. If either is missing, review the previous step and re-run the import.

### 4.4 Create the Knowledge Base

Paste this prompt into Bob:

```
Create a knowledge base with the following specifications:

Knowledge Base Name: inventory-alert-knowledge

Knowledge Base Description:
These files give the agent the reference material it needs to:
- interpret velocity spike patterns
- apply consistent inventory decision logic
- explain why the alert is urgent or routine
- recommend appropriate reorder quantities and pricing
- stay within the response boundaries defined for the demo

Upload the following files from the directory "retail-inventory-optimization/labs/part2-watsonx-orchestrate/inventory-alert-demo-knowledge/":
- agent-guardrails.pdf
- inventory-playbook.pdf
- decision-rules.pdf
- product-history-baselines.csv
- product-history-summary.txt
- product-history-examples.txt
```

> Bob will create the knowledge base and upload all six documents. Wait for confirmation that the knowledge base is ready before creating the agent — indexing can take a minute or two.

### 4.5 Create the Agent

This is the longest prompt — it contains the complete agent instructions. Paste it entirely:

```
Create a watsonx Orchestrate agent with the following specifications:

Agent Display Name: Fashion Inventory Alert Processor
Agent Name: Fashion_Inventory_Alert_Processor
Agent Description:
The agent that receives a fashion inventory velocity spike alert, uses store location and weather data to enhance analysis, reviews compact inventory management knowledge and product history baselines, determines urgency level, recommends reorder actions and pricing adjustments in a JSON message as output

Tools:
- get_store_location
- get_weather_forecast

Knowledge Base:
- inventory-alert-knowledge

Model: groq/openai/gpt-oss-120b

Agent Instructions:
You are the Fashion Inventory Alert Response Agent in a watsonx Orchestrate demo.

Purpose
Process an incoming velocity spike alert, evaluate the product using the loaded compact knowledge sources, determine urgency level and recommended actions, construct a payload that conforms exactly to the format defined below.

Workflow

1. Receive alert
Input contains at least: alertId, anomalyType, severity, timestamp, storeId, productId, sku, currentVelocity, baselineVelocity, velocityRatio, currentStock, hoursToStockout

2. Get store location and weather data
Use get_store_location with the storeId to retrieve store geographic coordinates, city, state, and timezone.
Then use get_weather_forecast with the latitude and longitude to retrieve weather forecast for the next 5-7 days including temperature trends, precipitation predictions, and weather conditions.

3. Extract and organize
Capture all product and alert facts exactly as provided. Do not invent values. If an optional value is unavailable, omit it rather than fabricating it.

4. Build velocityAnalysis
Construct a velocityAnalysis object with: baselineVelocity, currentVelocity, velocityRatio, velocityTrend (ACCELERATING/STABLE/DECELERATING), durationHours, triggerType.

Use weather data to determine triggerType:
- Cold weather + outerwear spike = WEATHER_EVENT
- Hot weather + summer items spike = WEATHER_EVENT
- Rain/snow + related items spike = WEATHER_EVENT
- Otherwise use knowledge base patterns (SEASONAL, TRENDING, UNKNOWN)

5. Consult knowledge and weather context
Use the loaded knowledge to compare current velocity with product history baselines, identify spike patterns, calculate urgency score using decision rules, and apply demo guardrails. Incorporate weather insights: weather-driven spikes may be temporary but require quick action.

6. Determine urgency
Set agentDecision.urgencyLevel to exactly one of: CRITICAL, HIGH, MEDIUM, LOW
Calculate urgencyScore (0-10) based on velocity ratio, hours to stockout, product value, and weather context.

7. Select recommended actions
Set agentDecision.recommendedActions as an array containing one or more of:
RUSH_REORDER, STANDARD_REORDER, STOCK_TRANSFER, SURGE_PRICING, MONITOR, ESCALATE

Recommended mapping:
- CRITICAL -> ["RUSH_REORDER", "SURGE_PRICING", "STOCK_TRANSFER"]
- HIGH -> ["RUSH_REORDER", "SURGE_PRICING"]
- MEDIUM -> ["STANDARD_REORDER", "MONITOR"]
- LOW -> ["MONITOR"]

8. Build the final payload — this exact structure must be followed:
{
  "alertId": "string",
  "sku": "string",
  "productId": "string",
  "storeId": "string",
  "processedAt": "ISO-8601 date-time string",
  "productDetails": { "productName": "string", "category": "string", "brand": "string", "size": "string", "color": "string", "unitPrice": 0 },
  "velocityAnalysis": { "baselineVelocity": 0, "currentVelocity": 0, "velocityRatio": 0, "velocityTrend": "string", "durationHours": 0, "triggerType": "string" },
  "stockAnalysis": { "currentStock": 0, "hoursToStockout": 0, "estimatedValue": 0, "reorderPoint": 0, "typicalStock": 0 },
  "productHistorySummary": { "baselineVelocity": 0, "last30DaysSales": 0, "stockoutCount": 0, "peakVelocityRecorded": 0, "seasonalPattern": "string", "historyNote": "string" },
  "agentDecision": {
    "urgencyLevel": "CRITICAL | HIGH | MEDIUM | LOW",
    "urgencyScore": 0,
    "reasoning": ["short reason 1", "short reason 2"],
    "recommendedActions": ["RUSH_REORDER", ...],
    "actionRationale": "string",
    "analystSummary": "string"
  },
  "reorderRecommendation": { "shouldReorder": true, "reorderQuantity": 0, "reorderPriority": "string", "estimatedLeadTimeHours": 0, "estimatedCost": 0, "vendorNotes": "string" },
  "pricingRecommendation": { "shouldAdjustPrice": true, "priceAdjustmentPercent": 0, "adjustmentRationale": "string", "expectedDuration": "string" },
  "notifications": { "notifyTeams": ["buying_team", "store_manager"], "escalationRequired": false, "escalationReason": "string" }
}

Schema rules that must always be respected:
- Required top-level fields: alertId, sku, productId, storeId, processedAt, agentDecision
- agentDecision.reasoning MUST be an array of strings, not a single string
- agentDecision.urgencyScore MUST be an integer from 0 to 10
- agentDecision.urgencyLevel MUST be exactly one of: CRITICAL, HIGH, MEDIUM, LOW
- agentDecision.recommendedActions MUST be an array of valid action strings
- productHistorySummary, if included, MUST be an object, not a string
- Do not include fields outside the schema
- Do not publish partial payloads

Restrictions:
- Do not fabricate product or sales history facts
- Do not use placeholder strings where structured objects are required
- Do not make actual purchase orders or financial commitments
- Keep reasoning concise, operational, and demo-friendly
```

> Bob will create the agent with all tools and the knowledge base attached. Wait for confirmation before proceeding.

### 4.6 Get the Agent ID

Paste this prompt into Bob:

```
Get the agent ID for the "Fashion Inventory Alert Processor" agent. I need this ID to configure my Python consumer application.
```

Copy the returned agent ID — this is your `WXO_AGENT_ID_OR_NAME` value for the next section.

### What you learned

- Bob creates watsonx Orchestrate artifacts (tools, knowledge bases, agents) programmatically via the ADK MCP server
- The `get_store_location` tool reads a CSV — no external API needed; data lives in the package root
- The `get_weather_forecast` tool uses the free Open-Meteo API — no credentials needed
- Agent instructions are the schema contract: detailed output rules prevent the LLM from free-styling
- Knowledge bases give the agent factual grounding without baking facts into the prompt

---

## Section 5 — Configure the Python Consumer (5 min)

### 5.1 Add the watsonx Orchestrate credentials to `.env`

Edit `retail-inventory-optimization/fashion-inventory-consumer/.env` and add the wxO section:

```bash
# Watsonx Orchestrate Configuration
WXO_INSTANCE_URL=https://api.us-south.watson-orchestrate.cloud.ibm.com/instances/your-instance-id
WXO_AGENT_ID_OR_NAME=your-agent-id-from-section-4.6
WXO_INSTANCE_CLOUD=ibmcloud     # use "aws" if your instance is on AWS
WXO_API_KEY=your_wxo_api_key
WXO_TIMEOUT_SECONDS=60

# Agent Response Topic
AGENT_RESPONSE_TOPIC=fashion.agent.responses
```

**Where to find these values:**

| Variable                 | Where to get it                                                                   |
| ------------------------ | --------------------------------------------------------------------------------- |
| `WXO_INSTANCE_URL`     | wxO console → profile icon → Settings → API details → Service Instance URL    |
| `WXO_AGENT_ID_OR_NAME` | Returned by Bob in Section 4.6                                                    |
| `WXO_INSTANCE_CLOUD`   | `ibmcloud` for TechZone/IBM Cloud instances; `aws` for AWS-deployed instances |
| `WXO_API_KEY`          | wxO console → Settings → API details → Generate API key                        |

### 5.2 How the consumer uses these credentials

`orchestrate_client.py` exchanges your API key for a short-lived bearer token before each agent call (with caching to avoid unnecessary round-trips). It supports both IBM Cloud IAM (`iam.cloud.ibm.com`) and AWS IAM (`iam.platform.saas.ibm.com`) token endpoints, selected automatically based on `WXO_INSTANCE_CLOUD`.

### What you learned

- The Python consumer does not use the ADK CLI — it calls the wxO REST API directly
- `WXO_INSTANCE_CLOUD` controls which IAM endpoint the client uses for token exchange
- Bearer tokens are cached and refreshed automatically; you don't need to manage expiry manually

---

## Section 6 — Run the End-to-End Pipeline (10 min)

### 6.1 Start the agent-enabled consumer

```bash
cd retail-inventory-optimization/fashion-inventory-consumer
uv run consume_velocity_alerts_with_agent.py
```

### 6.2 In a second terminal, run the test producer

```bash
cd retail-inventory-optimization/fashion-inventory-consumer
uv run produce_inventory_events.py --csv-file ../fashion-inventory-setup/data/test_winter_jacket_spike.csv
```

### 6.3 Expected log output

The consumer produces structured log output for each alert it processes:

```
======== VELOCITY ALERT AGENT CONSUMER ========
Input topic : fashion.velocity.anomalies
Output topic: fashion.agent.responses
================================================

-------- 1. VELOCITY ALERT RECEIVED -----------
Alert         : VELOCITY_SPIKE
Product       : JACKET-NORTH-001-M-BLACK
Store         : STORE-NYC-001
Severity      : CRITICAL
Velocity      : 12.5x baseline
Stock         : 75 units (3.0 hours to stockout)
Kafka         : partition=0 offset=42
------------------------------------------------

-------- 2. AGENT REVIEWING CASE ---------------
Agent         : Fashion_Inventory_Alert_Processor
Status        : Invoking watsonx Orchestrate and waiting for response
------------------------------------------------

-------- 3. AGENT DECISION READY ---------------
Status        : Agent response received and schema validated
------------------------------------------------
Decision       : CRITICAL
Urgency        : CRITICAL (score: 9)
Actions        : RUSH_REORDER, SURGE_PRICING, STOCK_TRANSFER
Reorder        : 200 units (RUSH_EXPEDITED priority)
Pricing        : 15.0% adjustment
Summary        : Cold weather spike detected — winter jacket selling 12.5x baseline ...

-------- 4. RESULT PUBLISHED -------------------
Product       : JACKET-NORTH-001-M-BLACK
Output topic  : fashion.agent.responses
Status        : Published successfully
------------------------------------------------
```

### 6.4 Verify in Confluent Cloud

In the Confluent Cloud console, click `fashion.agent.responses` → **Messages**. You should see structured JSON messages appearing with the full agent decision payload.

### What you learned

- The consumer processes one alert at a time, synchronously — agent latency is the bottleneck
- Log output clearly separates the four pipeline stages, making debugging straightforward
- Kafka offset commit happens only after the full pipeline (agent + validate + publish) succeeds

---

## Section 7 — Understand What Happened (10 min)

Now that the pipeline is running, let's look inside the Python code to understand the design decisions.

### 7.1 Alert → Agent request transformation

`agent_payload_builder.py` transforms the raw Kafka alert into a structured dict the agent expects. It normalises timestamps (epoch ms → ISO 8601), handles missing fields with sensible defaults, and groups related fields into sub-objects (`velocityAnalysis`, `stockAnalysis`, `productDetails`).

The builder sets some fields to sentinel values like `"ACCELERATING"` or `None` — these are placeholders the agent is expected to refine using its knowledge base and tools.

### 7.2 Token management and response parsing

`orchestrate_client.py` does three important things:

1. **Token caching** — bearer tokens are expensive to fetch; the client caches them and only refreshes when close to expiry (`TOKEN_EXPIRY_SKEW_SECONDS = 30`)
2. **Markdown fence stripping** — LLMs sometimes wrap JSON in triple-backtick code blocks. `_strip_markdown_fence()` handles this silently
3. **JSON extraction fallback** — `_extract_first_json_object()` scans for the first valid `{...}` block if the response contains surrounding text

These three patterns make the integration robust to common LLM output quirks.

### 7.3 Schema validation

`response_validator.py` validates every agent response against `agent-response.schema.json` using the `jsonschema` library (Draft 7). If validation fails, the exception propagates to the main loop — the Kafka offset is **not committed**, so the message will be reprocessed on the next run.

This is intentional: a schema violation means the agent produced an unreliable decision. It's safer to retry than to publish bad data downstream.

### 7.4 Manual offset commit pattern

`consume_velocity_alerts_with_agent.py` sets `"enable.auto.commit": False`. Offsets are committed explicitly only after all four steps succeed:

```
read alert → call agent → validate response → publish to Kafka → commit offset
```

If any step throws, the offset is not committed, and the message is reprocessed. This gives the pipeline **at-least-once delivery semantics** — the same alert may be processed twice on retry, but no alert is silently dropped.

> **Trade-off:** At-least-once means the agent could receive the same alert twice. For this use case (inventory decisions), processing twice is safer than silently missing an alert. For financial transactions you would need idempotency keys or exactly-once semantics.

### 7.5 The agent response schema

The `agent-response.schema.json` defines strict rules the agent must follow:

- `agentDecision.reasoning` — **array of strings** (not a single string — this is the most common validation failure)
- `agentDecision.urgencyScore` — **integer 0–10** (not a float, not a string)
- `agentDecision.urgencyLevel` — **enum**: `CRITICAL`, `HIGH`, `MEDIUM`, `LOW`
- `additionalProperties: false` — no extra fields allowed anywhere in the response

The schema is also registered in Confluent Schema Registry for the `fashion.agent.responses` topic, so downstream consumers get the same validation guarantee.

### What you learned

- The payload builder normalises messy Kafka data into a clean agent request
- Token caching and markdown-fence stripping are essential for production reliability
- Schema validation at the output boundary protects downstream consumers
- Manual offset commit + at-least-once delivery is the right default for AI-enriched pipelines

---

## Troubleshooting

### Authentication errors (`401 Unauthorized`)

- Verify `WXO_API_KEY` is correct and not expired
- Confirm `WXO_INSTANCE_CLOUD` matches your deployment: `ibmcloud` for TechZone/IBM Cloud, `aws` for AWS
- Check `WXO_INSTANCE_URL` includes the correct region segment (e.g. `us-south`, `eu-de`)
- wxO API keys expire — regenerate from the Settings → API details page if needed

### Schema validation failures

The most common causes:

| Error                                                        | Cause                                          | Fix                                                                            |
| ------------------------------------------------------------ | ---------------------------------------------- | ------------------------------------------------------------------------------ |
| `agentDecision.reasoning: ... is not of type 'array'`      | Agent returned reasoning as a plain string     | Check agent instructions —`reasoning` must be an array                      |
| `agentDecision.urgencyScore: ... is not of type 'integer'` | Agent returned`9.0` (float) instead of `9` | Prompt constraint is correct; this may resolve on retry                        |
| `Additional properties are not allowed`                    | Agent added a field not in the schema          | Review agent instructions; confirm`additionalProperties: false` is respected |

### Confluent connection errors

- Verify `KAFKA_BOOTSTRAP_SERVERS` format: `pkc-xxxxx.region.provider.confluent.cloud:9092`
- Confirm API key has `Global access` scope (not a topic-scoped key)
- Check `SCHEMA_REGISTRY_URL` starts with `https://` (not `http://`)

### Flink SQL produces no output

- Confirm the Flink query is running (status = `Running` in the SQL workspace)
- Confirm `fashion.inventory.events` topic has messages (check Messages tab)
- Verify the Flink query uses the correct topic names including the backtick quoting

---

## Verify your watsonx Orchestrate setup

After completing Section 4, run the verification script to confirm the tools, knowledge
base and agent all exist:

1. Download [:material-download: **`import-all.sh`**](import-all.sh) into your
   `bobchestrate-confluent/` folder
2. Run it with your wxO environment active:

```bash
bash import-all.sh
```

Every line should show ✅. Anything showing ❌ points you back to the section that
creates it.

---

## Key Commands Reference

```bash
# Install dependencies
cd retail-inventory-optimization && uv sync --locked

# Run test producer
cd fashion-inventory-consumer
uv run produce_inventory_events.py --csv-file ../fashion-inventory-setup/data/test_winter_jacket_spike.csv

# Run basic consumer (verify Flink detection, no agent)
uv run consume_velocity_alerts.py

# Run agent-enabled consumer (full pipeline)
uv run consume_velocity_alerts_with_agent.py

# Verify wxO agent exists
orchestrate agents list

# Verify tools are imported
orchestrate tools list

# Verify knowledge base is ready
orchestrate knowledge-bases list
```

---

## Reference Links

- [Confluent Cloud Documentation](https://docs.confluent.io/cloud/current/overview.html)
- [Flink SQL Reference — Confluent Cloud](https://docs.confluent.io/cloud/current/flink/reference/overview.html)
- [Open-Meteo API](https://open-meteo.com/en/docs) — free weather API used by the tool
- [watsonx Orchestrate ADK Documentation](https://developer.watson-orchestrate.ibm.com/)
- [ADK Docs — Knowledge Bases](https://developer.watson-orchestrate.ibm.com/knowledge_bases/knowledge_bases_intro)

---

[Stretch exercises →](exercises.md){ .md-button .md-button--primary }
[← Workshop home](../index.md){ .md-button }