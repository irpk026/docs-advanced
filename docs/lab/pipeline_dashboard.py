#!/usr/bin/env python3
"""
Live pipeline dashboard for the event-driven AI lab.

Reads all three topics and shows each message as it moves through the pipeline:

    fashion.inventory.events  ->  fashion.velocity.anomalies  ->  fashion.agent.responses
         (raw POS sales)            (Flink velocity alerts)        (agent decisions)

For every alert it also does its own lookups -- the store, the live weather forecast, the
product history from the knowledge base -- and scores the alert with the rules in
decision-rules.pdf, so you can compare the agent's decision with what the rulebook says.

Click an alert and "Run this alert through the agent" to watch the agent execute it: the
dashboard sends it through the watsonx Orchestrate runs API, which -- unlike the consumer's
chat/completions call -- returns the step history, so you see every tool call, its
arguments, and every tool result. Needs the WXO_* settings in .env (lab Section 5).

Usage (from retail-inventory-optimization/fashion-inventory-consumer/):
    uv run pipeline_dashboard.py            # live: reads your topics using .env
    uv run pipeline_dashboard.py --replay   # no Kafka or agent: replays the lab's test CSV,
                                            # decisions come from the rulebook, not an LLM

Then open http://localhost:8050

The dashboard joins its own throwaway consumer group and never commits offsets, so it
does not affect the agent consumer -- run it alongside everything else.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import queue
import threading
import time
import urllib.parse
import urllib.request
import uuid
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, List, Optional

TOPICS = {
    "fashion.inventory.events": "event",
    "fashion.velocity.anomalies": "alert",
    # Written by the agent's action tools (stock_actions.py)
    "fashion.logistics.orders": "order",
    "servicenow.incidents": "incident",
}
APPROVAL_LIMIT_USD = 20000  # same guardrail as stock_actions.py

_history: List[Dict[str, Any]] = []
_clients: List[queue.Queue] = []
_lock = threading.Lock()


def publish(lane: str, payload: Dict[str, Any]) -> None:
    message = {"lane": lane, "payload": payload, "seenAt": time.time()}
    with _lock:
        _history.append(message)
        for client in _clients:
            client.put(message)


# ---------------------------------------------------------------------------------------
# Lab data: the same files the agent's tools and knowledge base use
# ---------------------------------------------------------------------------------------

class LabData:
    SIZE_WORDS = {"XS": "Extra Small", "S": "Small", "M": "Medium", "L": "Large", "XL": "Extra Large"}
    # Months each category sells in season. Not in the knowledge base; a dashboard assumption.
    SEASONS = {"Outerwear": {10, 11, 12, 1, 2, 3}, "Dresses": {5, 6, 7, 8}}

    def __init__(self, workspace: Path) -> None:
        self.workspace = workspace
        data = workspace / "fashion-inventory-setup" / "data"
        knowledge = workspace / "labs" / "part2-watsonx-orchestrate" / "inventory-alert-demo-knowledge"
        self.test_csv = data / "test_winter_jacket_spike.csv"
        self.stores = {row["storeId"]: row for row in self._read(data / "store_locations.csv")}
        self.products = self._read(knowledge / "product-history-baselines.csv")
        self._weather: Dict[str, Any] = {}

    @staticmethod
    def _read(path: Path) -> List[Dict[str, str]]:
        with open(path, newline="", encoding="utf-8") as handle:
            return list(csv.DictReader(handle))

    def store(self, store_id: str) -> Optional[Dict[str, str]]:
        """What get_store_location returns."""
        return self.stores.get(store_id)

    def history(self, alert: Dict[str, Any]) -> Dict[str, Any]:
        """What the agent can find in product-history-baselines.csv."""
        for row in self.products:
            if row["sku"] == alert.get("sku"):
                return {"row": row, "match": "exact SKU"}
        size_word = self.SIZE_WORDS.get(str(alert.get("size", "")).upper(), "")
        for row in self.products:
            name = row["product_name"]
            if (row["brand"] == alert.get("brand") and row["category"] == alert.get("category")
                    and str(alert.get("color", "")) in name and name.endswith(size_word) and size_word):
                return {"row": row, "match": f"SKU {alert.get('sku')} is not in the knowledge base; "
                                             f"closest product by brand, category, colour and size"}
        return {"row": None, "match": f"SKU {alert.get('sku')} is not in the knowledge base"}

    def weather(self, store: Optional[Dict[str, str]]) -> Optional[Dict[str, Any]]:
        """The same Open-Meteo forecast get_weather_forecast uses, cached for 30 minutes."""
        if not store:
            return None
        cached = self._weather.get(store["storeId"])
        if cached and time.time() - cached["fetchedAt"] < 1800:
            return cached
        query = urllib.parse.urlencode({
            "latitude": store["latitude"], "longitude": store["longitude"], "forecast_days": 3,
            "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum,snowfall_sum",
            "temperature_unit": "fahrenheit", "precipitation_unit": "inch", "timezone": "auto"})
        try:
            with urllib.request.urlopen(f"https://api.open-meteo.com/v1/forecast?{query}", timeout=10) as response:
                daily = json.load(response)["daily"]
        except Exception as exc:  # offline, rate limited, ...
            print(f"Weather lookup failed for {store['storeId']}: {exc}")
            return None
        low, high = min(daily["temperature_2m_min"]), max(daily["temperature_2m_max"])
        snow = sum(v or 0 for v in daily["snowfall_sum"])
        rain = sum(v or 0 for v in daily["precipitation_sum"])
        result = {"low": low, "high": high, "snowInches": round(snow / 2.54, 1), "precipInches": round(rain, 2),
                  "summary": f"Next 3 days in {store['city']}: {low:.0f}–{high:.0f}°F"
                             + (f", snow" if snow else ", rain" if rain >= 0.2 else ", dry"),
                  "fetchedAt": time.time()}
        self._weather[store["storeId"]] = result
        return result

    def context(self, alert: Dict[str, Any]) -> Dict[str, Any]:
        store = self.store(alert.get("storeId", ""))
        weather = self.weather(store)
        history = self.history(alert)
        return {"store": store, "weather": weather, "history": history,
                "rulebook": rulebook(alert, weather, history["row"], self.SEASONS)}


def rulebook(alert: Dict[str, Any], weather: Optional[Dict[str, Any]], product: Optional[Dict[str, str]],
             seasons: Dict[str, set]) -> Dict[str, Any]:
    """Rules 1.1-2.5 from decision-rules.pdf in the agent's knowledge base."""
    ratio = float(alert.get("velocityRatio") or 0)
    hours = float(alert.get("hoursToStockout") or 0)
    value = float(alert.get("estimatedValue") or 0)
    velocity = float(alert.get("currentVelocity") or 0)
    category = alert.get("category", "")

    season_match = datetime.now().month in seasons.get(category, set())
    weather_event = bool(weather) and (
        (category == "Outerwear" and (weather["low"] <= 35 or weather["snowInches"] > 0))
        or (category == "Dresses" and weather["high"] >= 85))

    def pick(conditions):
        return next(((pts, why) for ok, pts, why in conditions if ok), (0, None))

    steps = []
    for rule, (pts, why), otherwise in [
        ("1.1 Velocity ratio", pick([(ratio >= 10, 3, "ratio ≥ 10"), (ratio >= 5, 2, "ratio ≥ 5"),
                                     (ratio >= 3, 1, "ratio ≥ 3")]), f"ratio {ratio:g} is under 3"),
        ("1.2 Stockout timeline", pick([(hours <= 4, 3, "≤ 4 h"), (hours <= 12, 2, "≤ 12 h"),
                                        (hours <= 24, 1, "≤ 24 h")]), f"{hours:g} h is over 24 h"),
        ("1.3 Product value", pick([(value >= 10000, 2, "≥ $10,000"), (value >= 5000, 1, "≥ $5,000")]),
         f"${value:,.0f} is under $5,000"),
        ("1.4 Seasonal context", pick([(season_match and weather_event, 2, "in season and a weather event"),
                                       (season_match, 1, "in season")]),
         "out of season" if not season_match else "no weather event"),
    ]:
        steps.append({"rule": rule, "points": pts, "why": why or otherwise})
    score = sum(step["points"] for step in steps)
    level = "CRITICAL" if score >= 7 else "HIGH" if score >= 4 else "MEDIUM"

    days = {"CRITICAL": 3, "HIGH": 2}.get(level, 1)
    if level == "CRITICAL":
        priority = "RUSH_EXPEDITED" if hours <= 4 else "RUSH"
        price = 15 if ratio >= 10 else 5 if ratio >= 3 else 0
    else:
        priority = "STANDARD_PRIORITY" if level == "HIGH" else "STANDARD"
        price = 10 if level == "HIGH" and ratio >= 5 else 5 if ratio >= 3 else 0
    baseline = float(product["baseline_velocity_per_hour"]) if product else None
    return {
        "steps": steps, "score": score, "level": level,
        "seasonMatch": season_match, "weatherEvent": weather_event,
        "reorderQuantity": round(velocity * 24 * days), "reorderDays": days, "priority": priority,
        "priceAdjustment": price, "transfer": hours <= 8,
        "notify": {"CRITICAL": ["buying_team", "store_manager", "regional_manager"],
                   "HIGH": ["buying_team", "store_manager"]}.get(level, ["buying_team"]),
        # Rule 4.1: is this an anomaly against the knowledge base's own baseline?
        "anomalyConfirmed": None if baseline is None else velocity > baseline * 3,
        "kbBaseline": baseline,
    }


# ---------------------------------------------------------------------------------------
# Readers
# ---------------------------------------------------------------------------------------

def decode_value(raw: bytes) -> Dict[str, Any]:
    # Flink and the Schema Registry serializer prefix JSON with a 5-byte header
    # (magic byte 0 + 4-byte schema id). The agent consumer publishes plain JSON.
    if raw[:1] == b"\x00":
        raw = raw[5:]
    return json.loads(raw.decode("utf-8"))


def run_kafka_reader(lab: Optional[LabData]) -> None:
    from confluent_kafka import Consumer

    response_topic = os.getenv("AGENT_RESPONSE_TOPIC", "fashion.agent.responses")
    lanes = {**TOPICS, response_topic: "decision"}

    consumer = Consumer(
        {
            "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
            "security.protocol": "SASL_SSL",
            "sasl.mechanism": "PLAIN",
            "sasl.username": os.environ["KAFKA_API_KEY"],
            "sasl.password": os.environ["KAFKA_API_SECRET"],
            "group.id": f"pipeline-dashboard-{uuid.uuid4().hex[:8]}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe(list(lanes))
    print(f"Reading {', '.join(lanes)}")

    while True:
        message = consumer.poll(1.0)
        if message is None or message.value() is None:
            continue
        if message.error():
            print(f"Kafka error: {message.error()}")
            continue
        try:
            lane, payload = lanes[message.topic()], decode_value(message.value())
        except (ValueError, KeyError) as exc:
            print(f"Skipping unreadable message on {message.topic()}: {exc}")
            continue
        if lane == "alert" and lab:
            payload["_context"] = lab.context(payload)
        publish(lane, payload)


def run_replay_reader(lab: LabData) -> None:
    """Replays the lab's test CSV through the Flink rule, and decides with the rulebook."""
    for row in LabData._read(lab.test_csv):
        time.sleep(0.8)
        change, after, price = int(row["quantityChange"]), int(row["quantityAfter"]), float(row["unitPrice"])
        event = {**row, "quantityBefore": int(row["quantityBefore"]), "quantityChange": change,
                 "quantityAfter": after, "unitPrice": price}
        publish("event", event)
        if row["eventType"] != "SALE" or abs(change) < 5:
            continue

        # The SELECT in velocity_anomaly_detection.sql, line for line.
        alert = {key: row[key] for key in ("storeId", "productId", "sku", "size", "color", "category", "brand")}
        alert.update({
            "alertId": f"ALERT-{int(time.time())}-{row['sku']}", "anomalyType": "VELOCITY_SPIKE",
            "severity": "CRITICAL" if abs(change) > 20 else "HIGH" if abs(change) > 10 else "MEDIUM",
            "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "currentVelocity": float(abs(change)), "baselineVelocity": 2.0,
            "velocityRatio": abs(change) / 2.0, "currentStock": after,
            "hoursToStockout": after // abs(change), "estimatedValue": round(after * price, 2),
            "recommendation": "IMMEDIATE_ACTION_REQUIRED",
        })
        alert["_context"] = lab.context(alert)
        time.sleep(0.3)
        publish("alert", alert)
        threading.Thread(target=_rulebook_decision, args=(alert, price), daemon=True).start()


def _rulebook_decision(alert: Dict[str, Any], unit_price: float) -> None:
    """Builds the response payload the rulebook implies. No LLM is involved."""
    time.sleep(2.5)
    ctx = alert["_context"]
    rb, store, weather, product = ctx["rulebook"], ctx["store"], ctx["weather"], ctx["history"]["row"]
    actions = {"CRITICAL": ["RUSH_REORDER", "SURGE_PRICING", "STOCK_TRANSFER"],
               "HIGH": ["RUSH_REORDER", "SURGE_PRICING"]}.get(rb["level"], ["STANDARD_REORDER", "MONITOR"])
    reasoning = []
    if store:
        reasoning.append(f"[store] {store['storeName']}, {store['city']} ({store['latitude']}, {store['longitude']})")
    reasoning.append(f"[weather] {weather['summary']}" if weather else "[weather] Forecast unavailable")
    if product:
        reasoning.append(f"[history] {product['product_name']} normally sells {product['baseline_velocity_per_hour']} "
                         f"units/hour; typical stock {product['typical_stock_level']}")
    reasoning.append(f"[stock] {alert['currentStock']} units left, about {alert['hoursToStockout']} hours of cover")
    reasoning.append(f"[rules] Score {rb['score']} ({' + '.join(str(s['points']) for s in rb['steps'])}) "
                     f"maps to {rb['level']} under rule 1.5")
    publish("decision", {
        "alertId": alert["alertId"], "sku": alert["sku"], "productId": alert["productId"],
        "storeId": alert["storeId"], "processedAt": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "velocityAnalysis": {"baselineVelocity": alert["baselineVelocity"], "currentVelocity": alert["currentVelocity"],
                             "velocityRatio": alert["velocityRatio"], "velocityTrend": "STABLE", "durationHours": 1,
                             "triggerType": "WEATHER_EVENT" if rb["weatherEvent"]
                             else "SEASONAL" if rb["seasonMatch"] else "UNKNOWN"},
        "stockAnalysis": {"currentStock": alert["currentStock"], "hoursToStockout": alert["hoursToStockout"],
                          "estimatedValue": alert["estimatedValue"], "reorderPoint": None,
                          "typicalStock": int(product["typical_stock_level"]) if product else None},
        **({"productHistorySummary": {
            "baselineVelocity": float(product["baseline_velocity_per_hour"]),
            "last30DaysSales": int(product["last_30_days_sales"]), "stockoutCount": int(product["stockout_count"]),
            "peakVelocityRecorded": float(product["peak_velocity_recorded"]),
            "seasonalPattern": "IN_SEASON" if rb["seasonMatch"] else "OFF_SEASON",
            "historyNote": ctx["history"]["match"]}} if product else {}),
        "agentDecision": {
            "urgencyLevel": rb["level"], "urgencyScore": min(rb["score"], 10), "recommendedActions": actions,
            "reasoning": reasoning,
            "actionRationale": f"Rules 2.1-2.5 for {rb['level']}: {rb['reorderDays']} day(s) of supply, "
                               f"{rb['priority']} priority",
            "analystSummary": f"Rulebook replay: {rb['level']} with score {rb['score']}.",
        },
        "reorderRecommendation": {"shouldReorder": True, "reorderQuantity": rb["reorderQuantity"],
                                  "reorderPriority": rb["priority"],
                                  "estimatedLeadTimeHours": int(product["lead_time_hours"]) if product else 48,
                                  "estimatedCost": round(rb["reorderQuantity"] * unit_price, 2), "vendorNotes": None},
        "pricingRecommendation": {"shouldAdjustPrice": rb["priceAdjustment"] > 0,
                                  "priceAdjustmentPercent": rb["priceAdjustment"],
                                  "adjustmentRationale": "Rule 2.3", "expectedDuration": None},
        "notifications": {"notifyTeams": rb["notify"], "escalationRequired": rb["level"] == "CRITICAL",
                          "escalationReason": "Rule 2.5" if rb["level"] == "CRITICAL" else None},
    })

    # What the action tools would write, with the same approval guardrail.
    time.sleep(0.5)
    now = datetime.now(timezone.utc)
    cost = round(rb["reorderQuantity"] * unit_price, 2)
    approval = cost > APPROVAL_LIMIT_USD
    publish("order", {
        "orderId": f"PO-{now:%Y%m%d}-{uuid.uuid4().hex[:6].upper()}", "alertId": alert["alertId"],
        "sku": alert["sku"], "storeId": alert["storeId"], "quantity": rb["reorderQuantity"],
        "priority": rb["priority"], "currentStock": alert["currentStock"], "unitPrice": unit_price,
        "estimatedCost": cost, "status": "PENDING_APPROVAL" if approval else "SUBMITTED",
        "approvalReason": f"over the ${APPROVAL_LIMIT_USD:,} limit" if approval else None,
        "reason": f"{alert['hoursToStockout']} h of cover left", "createdAt": f"{now:%Y-%m-%dT%H:%M:%SZ}",
        "createdBy": "rulebook replay"})
    if rb["level"] in ("CRITICAL", "HIGH"):
        level = {"CRITICAL": "1", "HIGH": "2"}[rb["level"]]
        publish("incident", {
            "number": f"INC{uuid.uuid4().int % 90000 + 10000:07d}", "sys_id": uuid.uuid4().hex,
            "short_description": f"{alert['sku']} at {alert['storeId']}: {alert['currentStock']} units left",
            "description": f"{rb['level']} velocity alert. Restock order raised for {rb['reorderQuantity']} units.",
            "urgency": level, "impact": level, "priority": level, "state": "1", "category": "inventory",
            "assignment_group": "Store Operations", "caller_id": "rulebook replay",
            "opened_at": f"{now:%Y-%m-%dT%H:%M:%SZ}", "u_alert_id": alert["alertId"], "u_sku": alert["sku"],
            "u_store_id": alert["storeId"], "u_current_stock": alert["currentStock"], "u_urgency_level": rb["level"]})


# ---------------------------------------------------------------------------------------
# Agent execution: run one alert through the agent and capture every step it takes
# ---------------------------------------------------------------------------------------

WXO_VARS = ("WXO_INSTANCE_URL", "WXO_AGENT_ID_OR_NAME", "WXO_API_KEY", "WXO_INSTANCE_CLOUD")


def wxo_ready() -> bool:
    return all(os.getenv(name) for name in WXO_VARS)


def _content_text(content: Any) -> str:
    if isinstance(content, list):
        return "\n".join(item.get("text", "") for item in content if isinstance(item, dict))
    return "" if content is None else str(content)


def execute_alert(alert: Dict[str, Any]) -> Dict[str, Any]:
    """
    Sends one alert to the agent through the runs API. Unlike the consumer's
    chat/completions call, a run keeps the step history: every tool call the agent
    made, with its arguments, and everything each tool returned.
    """
    import requests
    from agent_payload_builder import build_agent_request
    from orchestrate_client import _parse_agent_content, build_headers

    base = os.environ["WXO_INSTANCE_URL"].rstrip("/") + "/v1/orchestrate"
    request = build_agent_request({k: v for k, v in alert.items() if not k.startswith("_")})
    started = time.time()

    response = requests.post(f"{base}/runs", headers=build_headers(), timeout=30, json={
        "message": {"role": "user", "content": json.dumps(request, separators=(",", ":"))},
        "agent_id": os.environ["WXO_AGENT_ID_OR_NAME"],
    })
    response.raise_for_status()
    run = response.json()

    deadline = started + float(os.getenv("WXO_TIMEOUT_SECONDS", "60")) * 2
    while True:
        status = requests.get(f"{base}/runs/{run['run_id']}", headers=build_headers(), timeout=30).json()
        if status.get("status") in ("completed", "failed", "cancelled"):
            break
        if time.time() > deadline:
            raise TimeoutError(f"Run {run['run_id']} still {status.get('status')} after {deadline - started:.0f}s")
        time.sleep(1)
    if status.get("status") != "completed":
        raise RuntimeError(f"Run {status.get('status')}: {status.get('error') or status}")

    messages = requests.get(f"{base}/threads/{run['thread_id']}/messages", headers=build_headers(), timeout=30).json()
    if isinstance(messages, dict):
        messages = messages.get("data", [])
    reply = next((m for m in reversed(messages) if isinstance(m, dict) and m.get("role") == "assistant"), {})

    steps: List[Dict[str, Any]] = []
    for step in reply.get("step_history") or []:
        for detail in step.get("step_details") or []:
            if detail.get("type") == "tool_calls":
                for call in detail.get("tool_calls") or []:
                    steps.append({"kind": "call", "name": call.get("name"), "args": call.get("args")})
            elif detail.get("type") == "tool_response":
                steps.append({"kind": "result", "name": detail.get("name"), "content": detail.get("content")})
            else:
                steps.append({"kind": detail.get("type") or "step", "detail": detail})

    answer = _content_text(reply.get("content"))
    try:
        decision = _parse_agent_content(answer)
    except Exception:
        decision = None
    return {"ok": True, "seconds": round(time.time() - started, 1), "request": request, "steps": steps,
            "answer": answer, "decision": decision, "threadId": run["thread_id"], "runId": run["run_id"]}


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args: Any) -> None:
        pass

    def do_GET(self) -> None:
        if self.path == "/stream":
            self.stream()
        elif self.path == "/":
            body = (PAGE.replace("__MODE__", "replay" if self.server.replay else "live")
                    .replace("__WXO__", "true" if wxo_ready() else "false").encode("utf-8"))
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        else:
            self.send_error(404)

    def do_POST(self) -> None:
        if self.path != "/execute":
            self.send_error(404)
            return
        alert = json.loads(self.rfile.read(int(self.headers.get("Content-Length", 0))) or b"{}")
        if not wxo_ready():
            result = {"ok": False, "error": f"Set {', '.join(WXO_VARS)} in .env (lab Section 5)"}
        else:
            print(f"Running {alert.get('alertId')} through the agent...")
            try:
                result = execute_alert(alert)
                print(f"  {len(result['steps'])} steps in {result['seconds']}s")
            except Exception as exc:
                print(f"  failed: {exc}")
                result = {"ok": False, "error": str(exc)}
        body = json.dumps(result).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def stream(self) -> None:
        client: queue.Queue = queue.Queue()
        with _lock:
            backlog = list(_history)
            _clients.append(client)
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.end_headers()
        try:
            for message in backlog:
                self.wfile.write(f"data: {json.dumps(message)}\n\n".encode("utf-8"))
            self.wfile.flush()
            while True:
                try:
                    message = client.get(timeout=15)
                    self.wfile.write(f"data: {json.dumps(message)}\n\n".encode("utf-8"))
                except queue.Empty:
                    self.wfile.write(b": keep-alive\n\n")
                self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError):
            pass
        finally:
            with _lock:
                _clients.remove(client)


PAGE = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Pipeline Dashboard</title>
<style>
  :root {
    --bg: #f6f7f9; --panel: #ffffff; --text: #1b1f24; --muted: #5d6673; --line: #dde1e6;
    --event: #2f6fde; --alert: #c7771b; --decision: #1e8a5a;
    --low: #5d6673; --medium: #b58a00; --high: #d0661b; --critical: #c9303c;
    --hl: #fff4c2; --action: #6d4bd1;
  }
  @media (prefers-color-scheme: dark) {
    :root {
      --bg: #111418; --panel: #1a1e24; --text: #e6e9ed; --muted: #98a1ad; --line: #2c323a;
      --event: #6b9bf0; --alert: #e3a04f; --decision: #4cc28a;
      --low: #98a1ad; --medium: #e0c04a; --high: #f08a4b; --critical: #f0606b;
      --hl: #3a3420; --action: #a68cf2;
    }
  }
  * { box-sizing: border-box; }
  body { margin: 0; background: var(--bg); color: var(--text);
         font: 14px/1.45 system-ui, -apple-system, "Segoe UI", sans-serif; }
  header { padding: 16px; border-bottom: 1px solid var(--line); background: var(--panel); }
  h1 { font-size: 18px; margin: 0 0 4px; }
  .sub { color: var(--muted); margin: 0; }
  .demo { display: inline-block; margin-left: 8px; padding: 1px 8px; border-radius: 999px;
          background: var(--alert); color: #fff; font-size: 12px; vertical-align: 2px; }
  .flow { display: grid; grid-template-columns: 1fr auto 1fr auto 1fr auto 1fr; gap: 8px;
          align-items: center; padding: 16px; max-width: 1440px; margin: 0 auto; }
  .stage { background: var(--panel); border: 1px solid var(--line); border-radius: 10px;
           padding: 12px; border-top: 4px solid var(--c); }
  .stage .n { font-size: 28px; font-weight: 650; font-variant-numeric: tabular-nums; }
  .stage .what { font-weight: 600; }
  .stage .note { color: var(--muted); font-size: 12px; }
  .arrow { color: var(--muted); font-size: 22px; text-align: center; }
  .arrow small { display: block; font-size: 11px; }
  .lanes { display: grid; grid-template-columns: repeat(4, 1fr); gap: 16px;
           padding: 0 16px 24px; max-width: 1440px; margin: 0 auto; }
  .status { font-size: 11px; font-weight: 650; padding: 0 7px; border-radius: 999px; border: 1px solid; }
  .status.ok { color: var(--decision); border-color: var(--decision); }
  .status.wait { color: var(--high); border-color: var(--high); }
  .did { margin-top: 4px; font-size: 12px; color: var(--action); }
  .lane h2 { font-size: 13px; text-transform: uppercase; letter-spacing: .04em;
             color: var(--muted); margin: 0 0 8px; }
  .lane h2 code { text-transform: none; letter-spacing: 0; }
  .cards { display: flex; flex-direction: column; gap: 8px; }
  .card { background: var(--panel); border: 1px solid var(--line); border-left: 4px solid var(--c);
          border-radius: 8px; padding: 10px 12px; transition: background .2s; }
  .card.hl { background: var(--hl); }
  .card.dim { opacity: .55; }
  .row { display: flex; justify-content: space-between; gap: 8px; flex-wrap: wrap; }
  .mono { font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: 12px; }
  .muted { color: var(--muted); font-size: 12px; }
  .chip { display: inline-block; padding: 0 7px; border-radius: 999px; font-size: 11px;
          font-weight: 650; color: #fff; background: var(--k); }
  .vs { margin-top: 6px; font-size: 12px; }
  .actions { margin-top: 4px; font-size: 12px; }
  details { margin-top: 6px; font-size: 12px; }
  details ul { margin: 4px 0 0; padding-left: 18px; }
  .empty { color: var(--muted); font-size: 13px; padding: 12px; border: 1px dashed var(--line);
           border-radius: 8px; }
  .card.clickable { cursor: pointer; }
  .card.clickable:hover { border-color: var(--c); }
  .how { margin-top: 6px; font-size: 12px; color: var(--decision); font-weight: 600; }
  dialog { width: min(760px, calc(100vw - 32px)); max-height: calc(100vh - 32px); padding: 0;
           border: 1px solid var(--line); border-radius: 12px; background: var(--panel); color: var(--text); }
  dialog::backdrop { background: rgb(0 0 0 / .45); }
  .p-head { position: sticky; top: 0; display: flex; justify-content: space-between; align-items: center;
            gap: 8px; padding: 14px 16px; background: var(--panel); border-bottom: 1px solid var(--line); }
  .p-head h2 { margin: 0; font-size: 16px; }
  .p-body { padding: 4px 16px 16px; }
  .p-body h3 { font-size: 12px; text-transform: uppercase; letter-spacing: .04em; color: var(--muted);
               margin: 18px 0 8px; }
  .x { border: 0; background: none; color: var(--muted); font-size: 22px; cursor: pointer; line-height: 1; }
  .src { border: 1px solid var(--line); border-left: 4px solid var(--c); border-radius: 8px;
         padding: 8px 12px; margin-bottom: 8px; }
  .src .who { display: flex; justify-content: space-between; gap: 8px; flex-wrap: wrap; font-weight: 600; }
  .src .who span { color: var(--muted); font-weight: 400; font-size: 12px; }
  .src ul { margin: 4px 0 0; padding-left: 18px; }
  .src.none { opacity: .6; }
  .kv { display: grid; grid-template-columns: max-content 1fr; gap: 2px 12px; font-size: 13px; margin-top: 4px; }
  .kv dt { color: var(--muted); }
  .kv dd { margin: 0; }
  table.diff { width: 100%; border-collapse: collapse; font-size: 13px; }
  table.diff th, table.diff td { text-align: left; padding: 4px 8px; border-bottom: 1px solid var(--line); }
  table.diff th { color: var(--muted); font-weight: 600; }
  .hint { font-size: 12px; color: var(--muted); padding: 8px 12px; border: 1px dashed var(--line);
          border-radius: 8px; }
  .btn { font: inherit; font-size: 13px; padding: 6px 12px; border-radius: 8px; cursor: pointer;
         border: 1px solid var(--line); background: var(--bg); color: var(--text); }
  .btn.primary { background: var(--decision); border-color: var(--decision); color: #fff; font-weight: 600; }
  .btn:disabled { opacity: .6; cursor: progress; }
  .timeline { list-style: none; margin: 8px 0 0; padding: 0; }
  .timeline > li { position: relative; padding: 0 0 12px 22px; border-left: 2px solid var(--line); margin-left: 6px; }
  .timeline > li:last-child { border-left-color: transparent; }
  .timeline > li::before { content: ""; position: absolute; left: -7px; top: 3px; width: 12px; height: 12px;
                           border-radius: 50%; background: var(--c, var(--muted)); }
  .timeline .t { font-weight: 600; }
  .timeline .t .kind { font-weight: 400; font-size: 11px; color: var(--muted); margin-left: 6px; }
  .timeline pre { white-space: pre-wrap; word-break: break-word; font-size: 11px; max-height: 240px; overflow: auto;
                  background: var(--bg); border: 1px solid var(--line); border-radius: 6px; padding: 8px; margin: 4px 0 0; }
  .from { display: inline-block; font-size: 11px; padding: 0 6px; border-radius: 999px; margin-left: 6px;
          background: var(--bg); border: 1px solid var(--line); color: var(--muted); }
  .from.flink { border-color: var(--alert); color: var(--alert); }
  .from.tool { border-color: var(--event); color: var(--event); }
  @media (max-width: 1100px) and (min-width: 761px) {
    .flow { grid-template-columns: 1fr auto 1fr; }
    .flow .arrow.wrap { display: none; }
    .lanes { grid-template-columns: repeat(2, 1fr); }
  }
  @media (max-width: 760px) {
    .flow { grid-template-columns: 1fr; }
    .arrow { transform: rotate(90deg); }
    .arrow small { transform: rotate(-90deg); }
    .lanes { grid-template-columns: 1fr; }
  }
</style>
</head>
<body>
<header>
  <h1>Event-driven AI pipeline <span class="demo" id="demo" hidden>REPLAY · lab data, rulebook decisions, no LLM</span></h1>
  <p class="sub">Hover any card to follow one sale from the till to the stock-up. Click an alert, decision or action to see what the agent did with it.</p>
</header>

<section class="flow">
  <div class="stage" style="--c: var(--event)">
    <div class="n" id="n-event">0</div>
    <div class="what">Raw sales</div>
    <div class="note">fashion.inventory.events</div>
  </div>
  <div class="arrow">→<small>Flink SQL</small></div>
  <div class="stage" style="--c: var(--alert)">
    <div class="n" id="n-alert">0</div>
    <div class="what">Velocity alerts</div>
    <div class="note" id="filtered">fashion.velocity.anomalies</div>
  </div>
  <div class="arrow">→<small>Python + wxO agent</small></div>
  <div class="stage" style="--c: var(--decision)">
    <div class="n" id="n-decision">0</div>
    <div class="what">Agent decisions</div>
    <div class="note" id="waiting">fashion.agent.responses</div>
  </div>
  <div class="arrow wrap">→<small>agent's action tools</small></div>
  <div class="stage" style="--c: var(--action)">
    <div class="n" id="n-action">0</div>
    <div class="what">Actions taken</div>
    <div class="note" id="action-note">logistics orders · ServiceNow tickets</div>
  </div>
</section>

<section class="lanes">
  <div class="lane"><h2>1 · Sales <code>quantityChange</code></h2><div class="cards" id="lane-event"></div></div>
  <div class="lane"><h2>2 · Flink alert <code>severity</code></h2><div class="cards" id="lane-alert"></div></div>
  <div class="lane"><h2>3 · Agent decision <code>urgencyLevel</code></h2><div class="cards" id="lane-decision"></div></div>
  <div class="lane"><h2>4 · Actions taken <code>orders · tickets</code></h2><div class="cards" id="lane-action"></div></div>
</section>

<dialog id="panel" aria-labelledby="panel-title">
  <div class="p-head"><h2 id="panel-title">How the agent decided</h2>
    <button class="x" aria-label="Close" onclick="this.closest('dialog').close()">×</button></div>
  <div class="p-body" id="panel-body"></div>
</dialog>

<script>
const MODE = "__MODE__";
if (MODE === "replay") document.getElementById("demo").hidden = false;

const data = { event: [], alert: [], decision: [], order: [], incident: [] };
const esc = s => String(s ?? "").replace(/[&<>"]/g, c => ({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;"}[c]));
const level = l => `<span class="chip" style="--k: var(--${String(l || "low").toLowerCase()})">${esc(l || "?")}</span>`;

// A sale and its alert share sku + remaining stock; the agent copies currentStock through.
const stockOf = (lane, p) => ({ event: p.quantityAfter, alert: p.currentStock, order: p.currentStock,
  incident: p.u_current_stock, decision: p.stockAnalysis?.currentStock })[lane];
const skuOf = p => p.sku ?? p.u_sku;
const actionsFor = key => [
  ...data.order.filter(o => keyOf("order", o) === key).map(o => ({ kind: "order", p: o })),
  ...data.incident.filter(i => keyOf("incident", i) === key).map(i => ({ kind: "incident", p: i })),
].sort((x, y) => x.p._seq - y.p._seq);
const orderStatus = o => `<span class="status ${o.status === "SUBMITTED" ? "ok" : "wait"}">${esc(o.status)}</span>`;
function actionCard(kind, p, key) {
  if (kind === "order") return `<div class="card clickable" style="--c: var(--action)" data-key="${esc(key)}">
      <div class="row"><span class="mono">${esc(p.orderId)}</span>${orderStatus(p)}</div>
      <div>Restock <b>${esc(p.quantity)}</b> × ${esc(p.sku)} · ${esc(p.priority)}</div>
      <div class="muted">${p.estimatedCost != null ? Number(p.estimatedCost).toLocaleString("en-US", { style: "currency", currency: "USD" }) : "cost unknown"}
        ${p.approvalReason ? ` · needs approval: ${esc(p.approvalReason)}` : " · sent to logistics"}</div></div>`;
  return `<div class="card clickable" style="--c: var(--action)" data-key="${esc(key)}">
      <div class="row"><span class="mono">${esc(p.number)}</span>
        <span class="status wait">ServiceNow · P${esc(p.priority)}</span></div>
      <div>${esc(p.short_description)}</div>
      <div class="muted">${esc(p.assignment_group)} · New</div></div>`;
}
// Replaying the producer repeats the same stock levels, so the nth match pairs with the nth.
const keyOf = (lane, p) => p._key ??= (() => {
  const base = `${skuOf(p)}|${stockOf(lane, p)}`;
  const n = data[lane].filter(q => q !== p && q._key?.startsWith(base + "#")).length;
  return `${base}#${n}`;
})();

function render() {
  const decisionsByKey = new Map(data.decision.map(d => [keyOf("decision", d), d]));
  const alertKeys = new Set(data.alert.map(a => keyOf("alert", a)));

  const filtered = data.event.filter(e => Math.abs(e.quantityChange) < 5).length;
  const waiting = data.alert.filter(a => !decisionsByKey.has(keyOf("alert", a))).length;
  document.getElementById("n-event").textContent = data.event.length;
  document.getElementById("n-alert").textContent = data.alert.length;
  document.getElementById("n-decision").textContent = data.decision.length;
  document.getElementById("filtered").textContent =
    `${filtered} sale${filtered === 1 ? "" : "s"} below the threshold, no alert`;
  document.getElementById("waiting").textContent =
    waiting ? `${waiting} alert${waiting === 1 ? "" : "s"} waiting for the agent` : "fashion.agent.responses";
  const pendingApproval = data.order.filter(o => o.status === "PENDING_APPROVAL").length;
  document.getElementById("n-action").textContent = data.order.length + data.incident.length;
  document.getElementById("action-note").textContent = data.order.length + data.incident.length
    ? `${data.order.length} orders (${pendingApproval} need approval) · ${data.incident.length} tickets`
    : "logistics orders · ServiceNow tickets";

  fill("event", data.event, e => {
    const quiet = Math.abs(e.quantityChange) < 5;
    return `<div class="card${quiet ? " dim" : ""}" style="--c: var(--event)" data-key="${esc(keyOf("event", e))}">
      <div class="row"><span class="mono">${esc(e.sku)}</span><span class="muted">${esc(e.eventId)}</span></div>
      <div>Sold <b>${Math.abs(e.quantityChange)}</b> · ${esc(e.quantityAfter)} left
        ${quiet ? '<span class="muted">· filtered by Flink</span>' : ""}</div></div>`;
  });

  fill("alert", data.alert, a => {
    const d = decisionsByKey.get(keyOf("alert", a));
    const verdict = d
      ? `<div class="vs">Flink ${level(a.severity)} → agent ${level(d.agentDecision?.urgencyLevel)}</div>`
      : `<div class="vs muted">Waiting for the agent…</div>`;
    const did = actionsFor(keyOf("alert", a)).map(x => x.kind === "order"
      ? `restock ${esc(x.p.quantity)} (${x.p.status === "SUBMITTED" ? "sent" : "needs approval"})` : `ticket ${esc(x.p.number)}`);
    return `<div class="card clickable" style="--c: var(--alert)" data-key="${esc(keyOf("alert", a))}">
      <div class="row"><span class="mono">${esc(a.sku)}</span>${level(a.severity)}</div>
      <div>${Number(a.velocityRatio).toFixed(1)}× baseline · ${esc(a.currentStock)} units ·
        ${esc(a.hoursToStockout)} h to stockout</div>${verdict}
      ${did.length ? `<div class="did">→ ${did.join(" · ")}</div>` : ""}</div>`;
  });

  fill("decision", data.decision, d => {
    const ad = d.agentDecision || {};
    const orphan = !alertKeys.has(keyOf("decision", d));
    return `<div class="card clickable" style="--c: var(--decision)" data-key="${esc(keyOf("decision", d))}">
      <div class="row"><span class="mono">${esc(d.sku)}</span>
        <span>${level(ad.urgencyLevel)} <span class="muted">score ${esc(ad.urgencyScore)}</span></span></div>
      <div class="actions">${(ad.recommendedActions || []).map(esc).join(" · ")}</div>
      <div class="muted">${esc(ad.analystSummary)}</div>
      ${orphan ? '<div class="muted">Matching alert not seen yet</div>' : ""}
      <div class="how">How did it decide? →</div>
    </div>`;
  });

  const actions = [...data.order.map(p => ["order", p]), ...data.incident.map(p => ["incident", p])]
    .sort((x, y) => y[1]._seq - x[1]._seq);
  if (actions.length) document.getElementById("lane-action").innerHTML =
    actions.map(([kind, p]) => actionCard(kind, p, keyOf(kind, p))).join("");

  const EMPTY = { event: "No sales yet — run the producer.",
    alert: "No alerts yet — is the Flink job running?",
    decision: "No decisions yet — run the agent consumer.",
    action: "No actions yet — the agent places orders and opens tickets once it has its action tools (Section 4.7)." };
  for (const lane of Object.keys(EMPTY)) {
    const count = lane === "action" ? actions.length : data[lane].length;
    if (!count) document.getElementById(`lane-${lane}`).innerHTML = `<div class="empty">${EMPTY[lane]}</div>`;
  }
}

function fill(lane, items, card) {
  if (!items.length) return;
  document.getElementById(`lane-${lane}`).innerHTML = items.slice().reverse().map(card).join("");
}

document.addEventListener("mouseover", ev => {
  const card = ev.target.closest(".card");
  const key = card?.dataset.key;
  document.querySelectorAll(".card").forEach(c => c.classList.toggle("hl", !!key && c.dataset.key === key));
});

// ---- "How the agent decided" panel -------------------------------------------
// The agent instructions (lab Section 4.5) ask it to tag each reason with its source.
const SOURCES = [
  ["store", "Store location", "tool · get_store_location", "--event"],
  ["weather", "Weather forecast", "tool · get_weather_forecast", "--event"],
  ["history", "Product history", "knowledge base", "--alert"],
  ["rules", "Decision rules", "knowledge base", "--alert"],
  ["stock", "Stock position", "from the Flink alert", "--muted"],
];
// What agent_payload_builder.py sends as placeholders for the agent to fill in.
const PLACEHOLDERS = [
  ["velocityAnalysis.triggerType", "UNKNOWN", "weather + history"],
  ["velocityAnalysis.velocityTrend", "ACCELERATING", "history"],
  ["velocityAnalysis.durationHours", 1, "alert history"],
  ["stockAnalysis.typicalStock", null, "knowledge base"],
  ["stockAnalysis.reorderPoint", null, "knowledge base"],
];
const get = (o, path) => path.split(".").reduce((v, k) => v?.[k], o);
const show = v => v === null || v === undefined || v === "" ? "—" : esc(typeof v === "object" ? JSON.stringify(v) : v);
const kv = rows => `<dl class="kv">${rows.filter(([, v]) => v !== undefined)
  .map(([k, v]) => `<dt>${esc(k)}</dt><dd>${show(v)}</dd>`).join("")}</dl>`;

function groupReasons(reasons) {
  const groups = {}; let tagged = 0;
  for (const r of reasons || []) {
    const m = /^\s*\[(\w+)\]\s*(.*)$/.exec(r);
    const k = m && SOURCES.some(([id]) => id === m[1].toLowerCase()) ? m[1].toLowerCase() : "other";
    if (k !== "other") tagged++;
    (groups[k] ??= []).push(k === "other" ? r : m[2]);
  }
  return { groups, tagged };
}

// Mirrors agent_payload_builder.build_agent_request, for pasting into the wxO preview chat.
function agentRequest(a) {
  return {
    alertId: a.alertId, anomalyType: a.anomalyType || "VELOCITY_SPIKE", severity: a.severity,
    timestamp: a.timestamp, storeId: a.storeId, productId: a.productId, sku: a.sku,
    productDetails: { productName: a.productName || `${a.brand || ""} ${a.category || ""}`.trim(),
      category: a.category || "", brand: a.brand || "", size: a.size || "", color: a.color || "",
      unitPrice: a.unitPrice || 0 },
    velocityAnalysis: { baselineVelocity: a.baselineVelocity, currentVelocity: a.currentVelocity,
      velocityRatio: a.velocityRatio, velocityTrend: "ACCELERATING", durationHours: 1, triggerType: "UNKNOWN" },
    stockAnalysis: { currentStock: a.currentStock, hoursToStockout: a.hoursToStockout,
      estimatedValue: a.estimatedValue || 0, reorderPoint: null, typicalStock: null },
  };
}

const RANK = { LOW: 0, MEDIUM: 1, HIGH: 2, CRITICAL: 3 };

// The dashboard's own lookups for the alert, scored with decision-rules.pdf.
function rulebookSection(a, d) {
  const ctx = a?._context;
  if (!ctx) return `<h3>Check against the rulebook</h3><div class="hint">No rulebook check: the dashboard
    couldn't find the lab files. Run it from fashion-inventory-consumer/.</div>`;
  const rb = ctx.rulebook, ad = d?.agentDecision || {}, row = ctx.history.row;
  const agentLevel = ad.urgencyLevel;
  const verdict = !d ? `<div class="hint">No published decision for this alert yet.</div>` : MODE === "replay"
    ? `<div class="hint">In replay mode the decision above <b>is</b> the rulebook, so they always agree. Run the
       dashboard live (Section 6) to compare the rulebook with the real agent.</div>`
    : rb.level === agentLevel
      ? `<div class="hint">The agent <b>agrees</b> with the rulebook: both say ${level(rb.level)}.</div>`
      : `<div class="hint">The agent rated this ${RANK[agentLevel] > RANK[rb.level] ? "<b>higher</b>" : "<b>lower</b>"}
         than the rulebook (${level(agentLevel)} vs ${level(rb.level)}). Look at its reasoning above for what it
         weighed differently — this is the judgement an LLM adds, or gets wrong.</div>`;
  const anomaly = rb.anomalyConfirmed === null ? "No baseline in the knowledge base"
    : `${a.currentVelocity} units/h vs knowledge-base baseline ${rb.kbBaseline} units/h — ${rb.anomalyConfirmed
      ? "confirmed (over 3× baseline)" : `<b>not</b> confirmed (needs over ${(rb.kbBaseline * 3).toFixed(1)})`}`;
  const compare = [
    ["Urgency", level(rb.level), level(agentLevel)],
    ["Score", `${rb.score}`, show(ad.urgencyScore)],
    ["Reorder", `${rb.reorderQuantity} units (${rb.reorderDays} day supply)`, show(d?.reorderRecommendation?.reorderQuantity)],
    ["Priority", esc(rb.priority), show(d?.reorderRecommendation?.reorderPriority)],
    ["Price change", `${rb.priceAdjustment}%`, show(d?.pricingRecommendation?.priceAdjustmentPercent) + "%"],
    ["Notify", esc(rb.notify.join(", ")), show((d?.notifications?.notifyTeams || []).join(", "))],
  ].map(([k, r, g]) => `<tr><td>${k}</td><td>${r}</td><td>${g}</td></tr>`).join("");
  return `<h3>Check against the rulebook</h3>
    <div class="muted" style="margin-bottom:8px">The dashboard looked up the same sources the agent can use and
      applied <span class="mono">decision-rules.pdf</span> from its knowledge base.</div>
    ${kv([["store", ctx.store ? `${ctx.store.storeName}, ${ctx.store.city}` : "Not found in store_locations.csv"],
      ["weather", ctx.weather ? ctx.weather.summary : "Forecast unavailable"],
      ["product", row ? `${row.product_name} (${row.sku})` : "—"],
      ["match", ctx.history.match]])}
    <dl class="kv"><dt>rule 4.1</dt><dd>${anomaly}</dd></dl>
    <table class="diff" style="margin-top:8px"><tr><th>Rule</th><th>Points</th><th>Because</th></tr>
      ${rb.steps.map(st => `<tr><td>${esc(st.rule)}</td><td>+${st.points}</td><td class="muted">${esc(st.why)}</td></tr>`).join("")}
      <tr><td><b>1.5 Total</b></td><td><b>${rb.score}</b></td><td>${level(rb.level)} <span class="muted">(≥ 7 critical, ≥ 4 high)</span></td></tr>
    </table>
    <table class="diff" style="margin-top:12px"><tr><th></th><th>Rulebook</th><th>Agent</th></tr>${compare}</table>
    ${verdict}`;
}

const WXO = __WXO__;
const KNOWN_TOOLS = { get_store_location: "tool", get_weather_forecast: "tool",
  create_restock_order: "action · writes fashion.logistics.orders", open_servicenow_ticket: "action · writes servicenow.incidents" };
const toolKind = name => KNOWN_TOOLS[name] || (/knowledge|search|retriev/i.test(name || "") ? "knowledge base" : "tool");
const stripPrivate = o => Object.fromEntries(Object.entries(o).filter(([k]) => !k.startsWith("_")));
const pretty = v => { try { return JSON.stringify(typeof v === "string" ? JSON.parse(v) : v, null, 2); } catch { return String(v); } };

// Where did each argument the agent passed come from? The Flink alert, or an earlier tool result?
// Strings match by value. Numbers only match a same-named alert field, or a tool-result value
// that isn't a small whole number -- otherwise "days: 5" would look like it came from "currentVelocity: 5".
function leaves(v) {
  try { v = typeof v === "string" ? JSON.parse(v) : v; } catch { return [v]; }
  return v && typeof v === "object" ? Object.values(v).flatMap(leaves) : [v];
}
function provenance(key, value, alert, earlier) {
  if (value === null || value === undefined || value === "") return "";
  const isNum = typeof value === "number";
  const field = Object.entries(stripPrivate(alert)).find(([k, v]) => isNum
    ? k.toLowerCase() === key.toLowerCase() && Number(v) === value
    : typeof v === "string" && v === String(value));
  if (field) return `<span class="from flink">from Flink alert · ${esc(field[0])}</span>`;
  if (!(isNum && Number.isInteger(value) && Math.abs(value) < 100)) {
    const source = [...earlier].reverse().find(r => leaves(r.content).some(x => x === value || String(x) === String(value)));
    if (source) return `<span class="from tool">from ${esc(source.name)}'s result</span>`;
  }
  return `<span class="from">agent's choice</span>`;
}

function renderExecution(r, a, d) {
  if (!r.ok) return `<div class="hint"><b>The run failed:</b> ${esc(r.error)}</div>`;
  const results = [];
  const items = r.steps.map(st => {
    if (st.kind === "call") {
      const args = Object.entries(st.args || {});
      return `<li style="--c: var(--${toolKind(st.name).startsWith("action") ? "action" : "event"})"><div class="t">Called ${esc(st.name)}<span class="kind">${toolKind(st.name)}</span></div>
        ${args.length ? `<dl class="kv">${args.map(([k, v]) => `<dt>${esc(k)}</dt><dd>${show(v)}${provenance(k, v, a, results)}</dd>`).join("")}</dl>`
          : `<div class="muted">No arguments</div>`}</li>`;
    }
    if (st.kind === "result") {
      results.push(st);
      const body = pretty(st.content);
      return `<li style="--c: var(--alert)"><div class="t">${esc(st.name)} returned</div>
        <details><summary class="muted">${esc(body.replace(/\s+/g, " ").slice(0, 140))}${body.length > 140 ? "…" : ""}</summary>
        <pre>${esc(body)}</pre></details></li>`;
    }
    return `<li><div class="t">${esc(st.kind)}</div><details><summary class="muted">details</summary><pre>${esc(pretty(st.detail))}</pre></details></li>`;
  }).join("");

  const nd = r.decision?.agentDecision;
  const pd = d?.agentDecision;
  const vsPublished = nd && pd ? (nd.urgencyLevel === pd.urgencyLevel
      ? `<div class="muted">Same urgency as the decision the pipeline published for this alert.</div>`
      : `<div class="hint">The pipeline published ${level(pd.urgencyLevel)} for this same alert; this run says
         ${level(nd.urgencyLevel)}. Same input, different run — an LLM agent isn't deterministic.</div>`) : "";

  return `<div class="muted">${r.steps.filter(x => x.kind === "call").length} tool calls in ${r.seconds}s ·
      run <span class="mono">${esc(r.runId)}</span></div>
    <ol class="timeline">
      <li style="--c: var(--alert)"><div class="t">Received the Flink alert</div>
        <details><summary class="muted">as the request agent_payload_builder.py builds from it</summary>
        <pre>${esc(pretty(r.request))}</pre></details></li>
      ${items || `<li><div class="hint">The agent answered without calling any tools or the knowledge base.
        Check they're attached to the agent (lab Sections 4.3–4.7).</div></li>`}
      <li style="--c: var(--decision)"><div class="t">Answered</div>
        ${nd ? `<div class="row" style="justify-content:flex-start;gap:8px">${level(nd.urgencyLevel)}
          <span class="muted">score ${show(nd.urgencyScore)} · ${esc((nd.recommendedActions || []).join(", "))}</span></div>
          ${nd.reasoning ? `<ul>${nd.reasoning.map(x => `<li>${esc(x)}</li>`).join("")}</ul>` : ""}${vsPublished}`
          : `<div class="hint">The answer wasn't valid JSON, so the consumer would reject it.</div>`}
        <details><summary class="muted">raw answer</summary><pre>${esc(r.answer)}</pre></details></li>
    </ol>`;
}

function executionSection(a, d) {
  if (!a) return "";
  const copy = `<button class="btn" id="copy-req">Copy as agent request</button>`;
  if (!WXO) return `<div class="hint">To watch the agent execute this alert, add the WXO_* settings to .env
    (lab Section 5) and restart the dashboard. Or copy the alert and paste it into the agent's preview chat
    (lab Section 4.8).<br><br>${copy}</div>`;
  return `<div id="exec"><div class="row" style="justify-content:flex-start;gap:8px">
      <button class="btn primary" id="run-agent">▶ Run this alert through the agent</button>${copy}</div>
    <div class="muted" style="margin-top:6px">Sends the same request the consumer sends as a new run, and shows
      every step: which tools the agent called, with what, and what came back. Usually 10–30 seconds.
      <b>This is a real run</b>: if the agent decides to act, it places another order and opens another ticket.</div></div>`;
}

function openPanel(key) {
  const a = data.alert.find(x => keyOf("alert", x) === key);
  const d = data.decision.find(x => keyOf("decision", x) === key);
  if (!a && !d && !actionsFor(key).length) return;
  const ad = d?.agentDecision || {};
  const ph = d?.productHistorySummary || {};
  const ro = d?.reorderRecommendation || {};
  const pr = d?.pricingRecommendation || {};
  const nt = d?.notifications || {};
  const { groups, tagged } = groupReasons(ad.reasoning);

  const extra = {
    weather: get(d, "velocityAnalysis.triggerType") && kv([["triggerType", get(d, "velocityAnalysis.triggerType")]]),
    history: Object.keys(ph).length && kv([
      ["normal rate", ph.baselineVelocity !== undefined ? `${ph.baselineVelocity} units/hour` : undefined],
      ["last 30 days", ph.last30DaysSales], ["past stockouts", ph.stockoutCount],
      ["peak rate", ph.peakVelocityRecorded], ["season", ph.seasonalPattern], ["note", ph.historyNote],
      ["typical stock", get(d, "stockAnalysis.typicalStock")], ["reorder point", get(d, "stockAnalysis.reorderPoint")]]),
    rules: d && kv([["urgency score", `${show(ad.urgencyScore)} / 10`]]),
  };
  const evidence = SOURCES.map(([id, title, via, color]) => {
    const lines = groups[id] || [];
    const more = extra[id] || "";
    if (!lines.length && !more) return `<div class="src none" style="--c: var(${color})">
      <div class="who">${title}<span>${via}</span></div><div class="muted">Not cited in this decision</div></div>`;
    return `<div class="src" style="--c: var(${color})"><div class="who">${title}<span>${via}</span></div>
      ${lines.length ? `<ul>${lines.map(l => `<li>${esc(l)}</li>`).join("")}</ul>` : ""}${more}</div>`;
  }).join("") + (groups.other ? `<div class="src" style="--c: var(--line)"><div class="who">Other reasoning</div>
      <ul>${groups.other.map(l => `<li>${esc(l)}</li>`).join("")}</ul></div>` : "");
  const tagHint = (ad.reasoning || []).length && !tagged
    ? `<div class="hint">None of the reasons say which source they came from. Add the source-tag rule from
       lab Section 4.5 to the agent's instructions to sort them by tool and knowledge base.</div>` : "";
  const changed = PLACEHOLDERS.map(([path, sent, from]) => {
    const got = get(d, path);
    const same = JSON.stringify(got ?? null) === JSON.stringify(sent);
    return `<tr><td class="mono">${esc(path.split(".").pop())}</td><td>${show(sent)}</td>
      <td>${same ? `<span class="muted">${show(got)} (unchanged)</span>` : `<b>${show(got)}</b>`}</td><td class="muted">${from}</td></tr>`;
  }).join("");

  const who = MODE === "replay" ? "rulebook" : "agent";
  const published = d ? `
    <h3>3 · What the ${who} reported using</h3>${tagHint}${evidence}
    <h3>4 · What it filled in</h3>
    <table class="diff"><tr><th>Field</th><th>Consumer sent</th><th>Returned</th><th>From</th></tr>${changed}</table>
    <h3>5 · What it decided</h3>
    <div class="row" style="justify-content:flex-start;gap:12px">
      ${a ? `<span>Flink ${level(a.severity)} →</span>` : ""}<span>${who} ${level(ad.urgencyLevel)}</span>
      <span class="muted">score ${show(ad.urgencyScore)} / 10</span></div>
    ${kv([["actions", (ad.recommendedActions || []).join(", ")], ["why these", ad.actionRationale],
      ["reorder", ro.shouldReorder === false ? "No" : ro.reorderQuantity !== undefined
        ? `${ro.reorderQuantity} units · ${ro.reorderPriority || ""} · ${ro.estimatedLeadTimeHours ?? "?"} h lead time` : undefined],
      ["pricing", pr.shouldAdjustPrice ? `${pr.priceAdjustmentPercent}%${pr.expectedDuration ? ` for ${pr.expectedDuration}` : ""}`
        : pr.shouldAdjustPrice === false ? "No change" : undefined],
      ["pricing why", pr.adjustmentRationale],
      ["notify", (nt.notifyTeams || []).join(", ") || undefined],
      ["escalate", nt.escalationRequired ? `Yes: ${nt.escalationReason || ""}` : nt.escalationRequired === false ? "No" : undefined],
      ["summary", ad.analystSummary]])}`
    : `<h3>3 · Published decision</h3><div class="muted">No decision for this alert on
       fashion.agent.responses yet${MODE === "live" ? " — is the agent consumer running (Section 6)?" : "."}</div>`;

  document.getElementById("panel-title").textContent = "What the agent does with this alert";
  document.getElementById("panel-body").innerHTML = `
    <h3>1 · What Flink sent</h3>
    ${a ? kv([["product", a.sku], ["store", a.storeId], ["severity", a.severity],
      ["velocity", `${a.currentVelocity} units/h · ${Number(a.velocityRatio).toFixed(1)}× baseline`],
      ["stock", `${a.currentStock} units · ${a.hoursToStockout} h to stockout`]])
      : `<div class="muted">The matching alert hasn't arrived on the dashboard yet.</div>`}
    <h3>2 · Watch the agent execute</h3>${executionSection(a, d)}
    ${published}
    <h3>6 · What it did</h3>${(() => {
      const did = actionsFor(key);
      return did.length ? did.map(x => actionCard(x.kind, x.p, key).replace(" clickable", "")).join("")
        : `<div class="muted">No orders or tickets for this alert${d ? " — the agent decided not to act, or it has no action tools yet (Section 4.7)." : " yet."}</div>`;
    })()}
    ${a ? rulebookSection(a, d) : ""}`;

  document.getElementById("copy-req")?.addEventListener("click", ev => {
    navigator.clipboard.writeText(JSON.stringify(agentRequest(a), null, 2))
      .then(() => { ev.target.textContent = "Copied — paste it into the wxO preview chat"; },
            () => { ev.target.textContent = "Copy failed"; });
  });
  document.getElementById("run-agent")?.addEventListener("click", async ev => {
    const button = ev.target, box = document.getElementById("exec"), t0 = Date.now();
    button.disabled = true;
    const tick = setInterval(() => { button.textContent = `Agent working… ${Math.round((Date.now() - t0) / 1000)}s`; }, 250);
    let result;
    try {
      const response = await fetch("/execute", { method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify(stripPrivate(a)) });
      result = await response.json();
    } catch (err) {
      result = { ok: false, error: String(err) };
    }
    clearInterval(tick);
    box.innerHTML = renderExecution(result, a, d) +
      `<button class="btn" id="run-again" style="margin-top:4px">Run it again</button>`;
    document.getElementById("run-again").addEventListener("click", () => openPanel(key));
  });
  document.getElementById("panel").showModal();
}

document.addEventListener("click", ev => {
  const card = ev.target.closest(".card.clickable");
  if (card) openPanel(card.dataset.key);
});

let pending = false, seq = 0;
const source = new EventSource("/stream");
source.onmessage = ev => {
  const { lane, payload } = JSON.parse(ev.data);
  payload._seq = seq++;
  data[lane].push(payload);
  keyOf(lane, payload);
  if (!pending) { pending = true; requestAnimationFrame(() => { pending = false; render(); }); }
};
render();
</script>
</body>
</html>
"""


def main() -> None:
    parser = argparse.ArgumentParser(description="Live dashboard for the event-driven AI pipeline")
    parser.add_argument("--replay", "--demo", dest="replay", action="store_true",
                        help="replay the lab's test data without Kafka or the agent")
    parser.add_argument("--workspace", type=Path, default=Path(__file__).resolve().parent.parent,
                        help="the retail-inventory-optimization folder (default: this script's parent)")
    parser.add_argument("--port", type=int, default=8050)
    args = parser.parse_args()

    try:
        lab: Optional[LabData] = LabData(args.workspace)
    except FileNotFoundError as exc:
        hint = (f"Can't find the lab files ({exc.filename}). Put this script in "
                "retail-inventory-optimization/fashion-inventory-consumer/, or pass --workspace.")
        if args.replay:
            raise SystemExit(hint)
        print(f"{hint}\nContinuing without the rulebook check.")
        lab = None

    try:
        from dotenv import load_dotenv

        load_dotenv()
    except ImportError:
        pass  # replay mode without the lab's virtual environment

    if args.replay:
        reader = lambda: run_replay_reader(lab)
    else:
        missing = [name for name in ("KAFKA_BOOTSTRAP_SERVERS", "KAFKA_API_KEY", "KAFKA_API_SECRET")
                   if not os.getenv(name)]
        if missing:
            raise SystemExit(f"Missing {', '.join(missing)} -- fill in .env next to this script "
                             "(lab Section 3.1), or use --replay.")
        reader = lambda: run_kafka_reader(lab)
    threading.Thread(target=reader, daemon=True).start()

    server = ThreadingHTTPServer(("127.0.0.1", args.port), Handler)
    server.replay = args.replay
    print(f"Pipeline dashboard on http://localhost:{args.port}  (Ctrl+C to stop)")
    print("Agent execution: " + ("enabled" if wxo_ready() else "off -- add the WXO_* settings to .env (lab Section 5)"))
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
