#!/usr/bin/env python3
"""
Live pipeline dashboard for the event-driven AI lab.

Reads all three topics and shows each message as it moves through the pipeline:

    fashion.inventory.events  ->  fashion.velocity.anomalies  ->  fashion.agent.responses
         (raw POS sales)            (Flink velocity alerts)        (agent decisions)

Usage (from retail-inventory-optimization/fashion-inventory-consumer/):
    uv run pipeline_dashboard.py            # live, reads your .env
    uv run pipeline_dashboard.py --demo     # simulated data, no Kafka needed

Then open http://localhost:8050

The dashboard joins its own throwaway consumer group and never commits offsets, so it
does not affect the agent consumer -- run it alongside everything else.
"""

from __future__ import annotations

import argparse
import json
import os
import queue
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Dict, List

TOPICS = {
    "fashion.inventory.events": "event",
    "fashion.velocity.anomalies": "alert",
}

_history: List[Dict[str, Any]] = []
_clients: List[queue.Queue] = []
_lock = threading.Lock()


def publish(lane: str, payload: Dict[str, Any]) -> None:
    message = {"lane": lane, "payload": payload, "seenAt": time.time()}
    with _lock:
        _history.append(message)
        for client in _clients:
            client.put(message)


def decode_value(raw: bytes) -> Dict[str, Any]:
    # Flink and the Schema Registry serializer prefix JSON with a 5-byte header
    # (magic byte 0 + 4-byte schema id). The agent consumer publishes plain JSON.
    if raw[:1] == b"\x00":
        raw = raw[5:]
    return json.loads(raw.decode("utf-8"))


def run_kafka_reader() -> None:
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
            publish(lanes[message.topic()], decode_value(message.value()))
        except (ValueError, KeyError) as exc:
            print(f"Skipping unreadable message on {message.topic()}: {exc}")


def run_demo_reader() -> None:
    """Replays the lab's test CSV with the Flink rule applied and simulated agent decisions."""
    base = {"storeId": "STORE-NYC-001", "productId": "JACKET-001", "sku": "JACKET-001-M-BLACK"}
    stock = 100
    sales = [1] * 7 + [5] * 18
    for index, sold in enumerate(sales, start=1):
        time.sleep(0.8)
        stock -= sold
        publish("event", {**base, "eventId": f"evt-{index:03d}", "eventType": "SALE",
                          "quantityChange": -sold, "quantityAfter": stock, "unitPrice": 89.99})
        if sold < 5:
            continue

        alert = {**base, "alertId": f"ALERT-DEMO-{index:03d}", "anomalyType": "VELOCITY_SPIKE",
                 "severity": "CRITICAL" if sold > 20 else "HIGH" if sold > 10 else "MEDIUM",
                 "currentVelocity": float(sold), "baselineVelocity": 2.0,
                 "velocityRatio": sold / 2.0, "currentStock": stock,
                 "hoursToStockout": stock // sold}
        time.sleep(0.3)
        publish("alert", alert)
        threading.Thread(target=_demo_decision, args=(alert,), daemon=True).start()


def _demo_decision(alert: Dict[str, Any]) -> None:
    time.sleep(2.5)
    hours = alert["hoursToStockout"]
    if hours >= 12:
        level, score, actions = "MEDIUM", 5, ["STANDARD_REORDER", "MONITOR"]
    elif hours >= 5:
        level, score, actions = "HIGH", 7, ["RUSH_REORDER", "SURGE_PRICING"]
    else:
        level, score, actions = "CRITICAL", 9, ["RUSH_REORDER", "SURGE_PRICING", "STOCK_TRANSFER"]
    publish("decision", {
        "alertId": alert["alertId"], "sku": alert["sku"], "storeId": alert["storeId"],
        "stockAnalysis": {"currentStock": alert["currentStock"], "hoursToStockout": hours},
        "agentDecision": {
            "urgencyLevel": level, "urgencyScore": score, "recommendedActions": actions,
            "reasoning": [f"Selling {alert['velocityRatio']}x baseline",
                          f"{alert['currentStock']} units left, about {hours} hours of cover",
                          "Simulated decision (demo mode)"],
            "analystSummary": f"Simulated: {hours}h of stock left at {alert['velocityRatio']}x baseline.",
        },
    })


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args: Any) -> None:
        pass

    def do_GET(self) -> None:
        if self.path == "/stream":
            self.stream()
        elif self.path == "/":
            body = PAGE.replace("__MODE__", "demo" if self.server.demo else "live").encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        else:
            self.send_error(404)

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
    --hl: #fff4c2;
  }
  @media (prefers-color-scheme: dark) {
    :root {
      --bg: #111418; --panel: #1a1e24; --text: #e6e9ed; --muted: #98a1ad; --line: #2c323a;
      --event: #6b9bf0; --alert: #e3a04f; --decision: #4cc28a;
      --low: #98a1ad; --medium: #e0c04a; --high: #f08a4b; --critical: #f0606b;
      --hl: #3a3420;
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
  .flow { display: grid; grid-template-columns: 1fr auto 1fr auto 1fr; gap: 8px;
          align-items: center; padding: 16px; max-width: 1200px; margin: 0 auto; }
  .stage { background: var(--panel); border: 1px solid var(--line); border-radius: 10px;
           padding: 12px; border-top: 4px solid var(--c); }
  .stage .n { font-size: 28px; font-weight: 650; font-variant-numeric: tabular-nums; }
  .stage .what { font-weight: 600; }
  .stage .note { color: var(--muted); font-size: 12px; }
  .arrow { color: var(--muted); font-size: 22px; text-align: center; }
  .arrow small { display: block; font-size: 11px; }
  .lanes { display: grid; grid-template-columns: repeat(3, 1fr); gap: 16px;
           padding: 0 16px 24px; max-width: 1200px; margin: 0 auto; }
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
  <h1>Event-driven AI pipeline <span class="demo" id="demo" hidden>DEMO · simulated data</span></h1>
  <p class="sub">Hover any card to follow one sale through all three stages.</p>
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
</section>

<section class="lanes">
  <div class="lane"><h2>1 · Sales <code>quantityChange</code></h2><div class="cards" id="lane-event"></div></div>
  <div class="lane"><h2>2 · Flink alert <code>severity</code></h2><div class="cards" id="lane-alert"></div></div>
  <div class="lane"><h2>3 · Agent decision <code>urgencyLevel</code></h2><div class="cards" id="lane-decision"></div></div>
</section>

<script>
const MODE = "__MODE__";
if (MODE === "demo") document.getElementById("demo").hidden = false;

const data = { event: [], alert: [], decision: [] };
const esc = s => String(s ?? "").replace(/[&<>"]/g, c => ({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;"}[c]));
const level = l => `<span class="chip" style="--k: var(--${String(l || "low").toLowerCase()})">${esc(l || "?")}</span>`;

// A sale and its alert share sku + remaining stock; the agent copies currentStock through.
const stockOf = (lane, p) => lane === "event" ? p.quantityAfter
  : lane === "alert" ? p.currentStock : p.stockAnalysis?.currentStock;
// Replaying the producer repeats the same stock levels, so the nth match pairs with the nth.
const keyOf = (lane, p) => p._key ??= (() => {
  const base = `${p.sku}|${stockOf(lane, p)}`;
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
    return `<div class="card" style="--c: var(--alert)" data-key="${esc(keyOf("alert", a))}">
      <div class="row"><span class="mono">${esc(a.sku)}</span>${level(a.severity)}</div>
      <div>${Number(a.velocityRatio).toFixed(1)}× baseline · ${esc(a.currentStock)} units ·
        ${esc(a.hoursToStockout)} h to stockout</div>${verdict}</div>`;
  });

  fill("decision", data.decision, d => {
    const ad = d.agentDecision || {};
    const orphan = !alertKeys.has(keyOf("decision", d));
    return `<div class="card" style="--c: var(--decision)" data-key="${esc(keyOf("decision", d))}">
      <div class="row"><span class="mono">${esc(d.sku)}</span>
        <span>${level(ad.urgencyLevel)} <span class="muted">score ${esc(ad.urgencyScore)}</span></span></div>
      <div class="actions">${(ad.recommendedActions || []).map(esc).join(" · ")}</div>
      <div class="muted">${esc(ad.analystSummary)}</div>
      ${orphan ? '<div class="muted">Matching alert not seen yet</div>' : ""}
      <details><summary>Why?</summary><ul>${(ad.reasoning || []).map(r => `<li>${esc(r)}</li>`).join("")}</ul></details>
    </div>`;
  });

  for (const lane of Object.keys(data)) {
    if (!data[lane].length) {
      document.getElementById(`lane-${lane}`).innerHTML =
        `<div class="empty">${{event: "No sales yet — run the producer.",
          alert: "No alerts yet — is the Flink job running?",
          decision: "No decisions yet — run the agent consumer."}[lane]}</div>`;
    }
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

let pending = false;
const source = new EventSource("/stream");
source.onmessage = ev => {
  const { lane, payload } = JSON.parse(ev.data);
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
    parser.add_argument("--demo", action="store_true", help="simulate the pipeline without Kafka")
    parser.add_argument("--port", type=int, default=8050)
    args = parser.parse_args()

    if not args.demo:
        from dotenv import load_dotenv

        load_dotenv()
        missing = [name for name in ("KAFKA_BOOTSTRAP_SERVERS", "KAFKA_API_KEY", "KAFKA_API_SECRET")
                   if not os.getenv(name)]
        if missing:
            raise SystemExit(f"Missing {', '.join(missing)} -- fill in .env next to this script "
                             "(lab Section 3.1), or use --demo.")

    reader = run_demo_reader if args.demo else run_kafka_reader
    threading.Thread(target=reader, daemon=True).start()

    server = ThreadingHTTPServer(("127.0.0.1", args.port), Handler)
    server.demo = args.demo
    print(f"Pipeline dashboard on http://localhost:{args.port}  (Ctrl+C to stop)")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
