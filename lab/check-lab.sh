#!/usr/bin/env bash
# check-lab.sh — preflight check for the Event-Driven AI Agents lab
#
# Verifies everything the lab needs, in the order the lab needs it, and tells
# you which section to go back to when something is missing.
#
# Usage:
#   bash check-lab.sh              # all offline + API checks (safe, read-only)
#   bash check-lab.sh --e2e        # also run a real end-to-end pipeline test
#   bash check-lab.sh --help
#
# Run it from your bobchestrate-confluent/ workspace folder.

set -uo pipefail

RUN_E2E=0
for arg in "$@"; do
  case "$arg" in
    --e2e)  RUN_E2E=1 ;;
    --help|-h)
      sed -n '2,/^$/p' "$0" | sed 's/^#\{1,\} \{0,1\}//; s/^#$//'
      exit 0 ;;
    *) echo "Unknown option: $arg (try --help)" >&2; exit 2 ;;
  esac
done

# ── Output helpers ───────────────────────────────────────────────────────────
if [ -t 1 ]; then
  BOLD=$'\033[1m'; RED=$'\033[31m'; GRN=$'\033[32m'; YLW=$'\033[33m'; DIM=$'\033[2m'; RST=$'\033[0m'
else
  BOLD=""; RED=""; GRN=""; YLW=""; DIM=""; RST=""
fi

PASS=0; FAIL=0; WARN=0
CREDS_CONFLUENT=0
CREDS_PLACEHOLDER=0
FAILED_ITEMS=()

ok()   { printf '  %s✓%s %s\n' "$GRN" "$RST" "$1"; PASS=$((PASS+1)); }
bad()  { printf '  %s✗%s %s\n' "$RED" "$RST" "$1"; [ $# -gt 1 ] && printf '      %s↳ %s%s\n' "$DIM" "$2" "$RST"; FAIL=$((FAIL+1)); FAILED_ITEMS+=("$1"); }
warn() { printf '  %s!%s %s\n' "$YLW" "$RST" "$1"; [ $# -gt 1 ] && printf '      %s↳ %s%s\n' "$DIM" "$2" "$RST"; WARN=$((WARN+1)); }
head_() { printf '\n%s%s%s\n' "$BOLD" "$1" "$RST"; }

# Portable timeout. GNU `timeout` is absent on stock macOS, and on Git Bash for
# Windows `timeout` resolves to Windows' timeout.exe, which takes /T and needs a
# console handle — unusable here. Probe for --version to confirm it's GNU; if it
# isn't, fall back to the background watchdog below.
TIMEOUT_BIN=""
for t in timeout gtimeout; do
  if command -v "$t" >/dev/null 2>&1 && "$t" --version >/dev/null 2>&1; then
    TIMEOUT_BIN="$t"
    break
  fi
done

run_limited() {  # run_limited <seconds> <command...>
  local secs="$1"; shift
  if [ -n "$TIMEOUT_BIN" ]; then
    "$TIMEOUT_BIN" "$secs" "$@"
  else
    "$@" &
    local pid=$!
    ( sleep "$secs"; kill -TERM "$pid" 2>/dev/null ) &
    local watcher=$!
    wait "$pid" 2>/dev/null
    local rc=$?
    kill "$watcher" 2>/dev/null
    return "$rc"
  fi
}

# Read one key from .env without executing the file. Empty if unset/missing.
ENV_FILE=""
get_env() {
  [ -n "$ENV_FILE" ] && [ -f "$ENV_FILE" ] || return 0
  grep -E "^[[:space:]]*$1=" "$ENV_FILE" 2>/dev/null | tail -1 | cut -d= -f2- \
    | sed 's/^["'"'"']//; s/["'"'"']$//; s/[[:space:]]*#.*$//; s/[[:space:]]*$//'
}

# ── 1. Local tooling ─────────────────────────────────────────────────────────
head_ "1. Local tooling"

PY=""
for c in python3 python; do command -v "$c" >/dev/null 2>&1 && { PY="$c"; break; }; done
if [ -z "$PY" ]; then
  bad "Python not found" "Install Python 3.11-3.13 — Setup, Step 4"
else
  PYV=$("$PY" -c 'import sys; print("%d.%d" % sys.version_info[:2])' 2>/dev/null)
  case "$PYV" in
    3.11|3.12|3.13) ok "Python $PYV" ;;
    *) bad "Python $PYV is unsupported" "The lab needs 3.11, 3.12 or 3.13 — Setup, Step 4" ;;
  esac
fi

command -v uv >/dev/null 2>&1 \
  && ok "uv $(uv --version 2>/dev/null | awk '{print $2}')" \
  || bad "uv not installed" "Setup, Step 5"

command -v orchestrate >/dev/null 2>&1 \
  && ok "watsonx Orchestrate ADK $(orchestrate --version 2>/dev/null | grep -oE '[0-9][0-9.-]*[0-9]' | head -1)" \
  || bad "orchestrate CLI not found" "Activate your venv, then Setup, Step 10"

command -v curl >/dev/null 2>&1 && ok "curl" || bad "curl not found"

# ── 2. Workspace layout ──────────────────────────────────────────────────────
head_ "2. Workspace layout"

ROOT=""
for cand in . .. ./bobchestrate-confluent; do
  if [ -d "$cand/retail-inventory-optimization" ]; then ROOT="$cand"; break; fi
done

if [ -z "$ROOT" ]; then
  bad "retail-inventory-optimization/ not found" "Run this from your bobchestrate-confluent/ folder — Setup, Step 6"
else
  ok "workspace root: $(cd "$ROOT" && pwd)"
  [ -d "$ROOT/.bob" ] && ok ".bob/ configuration present" \
    || warn ".bob/ not found" "Bob won't have WXO Agent Architect mode — Setup, Step 6"

  CONSUMER="$ROOT/retail-inventory-optimization/fashion-inventory-consumer"
  SETUP="$ROOT/retail-inventory-optimization/fashion-inventory-setup"

  for f in produce_inventory_events.py consume_velocity_alerts.py \
           consume_velocity_alerts_with_agent.py orchestrate_client.py \
           agent_payload_builder.py agent_response_producer.py response_validator.py; do
    [ -f "$CONSUMER/$f" ] && ok "$f" || bad "missing $f" "Re-extract the workspace zip"
  done

  for f in schemas/fashion-inventory-event.schema.json \
           schemas/velocity-anomaly-alert.schema.json \
           schemas/agent-response.schema.json \
           sql/velocity_anomaly_detection.sql \
           data/test_winter_jacket_spike.csv \
           data/store_locations.csv; do
    [ -f "$SETUP/$f" ] && ok "$(basename "$f")" || bad "missing $f" "Re-extract the workspace zip"
  done

  KB="$ROOT/retail-inventory-optimization/labs/part2-watsonx-orchestrate/inventory-alert-demo-knowledge"
  if [ -d "$KB" ]; then
    n=$(find "$KB" -maxdepth 1 -type f | wc -l | tr -d ' ')
    [ "$n" -ge 6 ] && ok "knowledge base documents ($n files)" \
      || warn "knowledge base has $n files, expected 6" "Section 4.4 uploads all six"
  else
    bad "knowledge base folder missing" "Re-extract the workspace zip"
  fi
fi

# ── 3. Credentials in .env ───────────────────────────────────────────────────
head_ "3. Credentials (.env)"

[ -n "${CONSUMER:-}" ] && ENV_FILE="$CONSUMER/.env"
if [ -z "$ENV_FILE" ] || [ ! -f "$ENV_FILE" ]; then
  bad ".env not found" "cp .env.example .env in fashion-inventory-consumer/ — Section 2"
else
  ok ".env found"

  for v in KAFKA_BOOTSTRAP_SERVERS KAFKA_API_KEY KAFKA_API_SECRET \
           SCHEMA_REGISTRY_URL SCHEMA_REGISTRY_API_KEY SCHEMA_REGISTRY_API_SECRET \
           WXO_INSTANCE_URL WXO_API_KEY WXO_AGENT_ID_OR_NAME WXO_INSTANCE_CLOUD; do
    val=$(get_env "$v")
    if [ -z "$val" ]; then
      bad "$v is empty"
    elif printf '%s' "$val" | grep -qiE 'your_|xxxxx|changeme|<.*>|placeholder|your-'; then
      bad "$v still holds a placeholder" "value: $val"
      CREDS_PLACEHOLDER=1
    else
      ok "$v is set"
    fi
  done

  CLOUD=$(get_env WXO_INSTANCE_CLOUD)
  case "$CLOUD" in
    ibmcloud|aws|"") : ;;
    *) warn "WXO_INSTANCE_CLOUD='$CLOUD' is unusual" "expected 'ibmcloud' or 'aws' — Section 5.1" ;;
  esac

  KBS=$(get_env KAFKA_BOOTSTRAP_SERVERS)
  if [ -n "$KBS" ] && ! printf '%s' "$KBS" | grep -qE ':[0-9]+$'; then
    warn "KAFKA_BOOTSTRAP_SERVERS has no port" "expected host:9092 — Section 2.7"
  fi
fi

# ── 4. Confluent Cloud ───────────────────────────────────────────────────────
head_ "4. Confluent Cloud"

SR_URL=$(get_env SCHEMA_REGISTRY_URL)
SR_KEY=$(get_env SCHEMA_REGISTRY_API_KEY)
SR_SEC=$(get_env SCHEMA_REGISTRY_API_SECRET)

if [ "$CREDS_PLACEHOLDER" -eq 1 ]; then
  warn "skipping Confluent checks" "fill in the Confluent credentials first — Section 2.7"
elif [ -z "$SR_URL" ] || [ -z "$SR_KEY" ]; then
  warn "skipping Schema Registry checks" "credentials not set"
else
  SUBJECTS=$(curl -s --max-time 20 -u "$SR_KEY:$SR_SEC" "$SR_URL/subjects" 2>/dev/null)
  if printf '%s' "$SUBJECTS" | grep -q '^\['; then
    ok "Schema Registry reachable and credentials accepted"
    for t in fashion.inventory.events fashion.velocity.anomalies fashion.agent.responses; do
      printf '%s' "$SUBJECTS" | grep -q "$t" \
        && ok "schema registered for $t" \
        || warn "no schema registered for $t" "Section 2.4"
    done
  else
    bad "Schema Registry rejected the request" "check SCHEMA_REGISTRY_* values — Section 2.7"
  fi
fi

KBS=$(get_env KAFKA_BOOTSTRAP_SERVERS)
K_KEY=$(get_env KAFKA_API_KEY)
K_SEC=$(get_env KAFKA_API_SECRET)

if [ "$CREDS_PLACEHOLDER" -eq 1 ]; then
  :
elif [ -z "$KBS" ] || [ -z "$K_KEY" ] || [ -z "${ROOT:-}" ]; then
  warn "skipping Kafka checks" "credentials or workspace not available"
else
  KAFKA_OUT=$(cd "$ROOT/retail-inventory-optimization" && \
    KBS="$KBS" K_KEY="$K_KEY" K_SEC="$K_SEC" uv run python - <<'PYEOF' 2>&1
import os, sys
try:
    from confluent_kafka.admin import AdminClient
except ImportError:
    print("NODEPS"); sys.exit(0)
try:
    admin = AdminClient({
        "bootstrap.servers": os.environ["KBS"],
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": os.environ["K_KEY"],
        "sasl.password": os.environ["K_SEC"],
    })
    md = admin.list_topics(timeout=20)
    print("TOPICS " + " ".join(sorted(md.topics)))
except Exception as e:
    print("ERROR " + str(e).replace("\n", " ")[:160])
PYEOF
  )
  KAFKA_MARK=$(printf '%s' "$KAFKA_OUT" | grep -E '^(TOPICS|ERROR|NODEPS)' | head -1)
  case "$KAFKA_MARK" in
    NODEPS*)
      warn "confluent-kafka not installed" "run: cd retail-inventory-optimization && uv sync --locked" ;;
    TOPICS*)
      ok "Kafka broker reachable, credentials accepted"
      for t in fashion.inventory.events fashion.velocity.anomalies fashion.agent.responses; do
        printf '%s' "$KAFKA_MARK" | tr ' ' '\n' | grep -qx "$t" \
          && ok "topic $t exists" \
          || bad "topic $t missing" "Section 2.3" ;
      done ;;
    ERROR*)
      bad "cannot reach Kafka" "${KAFKA_MARK#ERROR }" ;;
    *)
      warn "Kafka check did not run" "$(printf '%s' "$KAFKA_OUT" | tail -1 | cut -c1-120)" ;;
  esac
fi

# ── 5. watsonx Orchestrate ───────────────────────────────────────────────────
head_ "5. watsonx Orchestrate"

if ! command -v orchestrate >/dev/null 2>&1; then
  warn "skipping wxO checks" "orchestrate CLI not on PATH"
else
  if ENVS=$(orchestrate env list 2>&1); then
    ok "ADK responds"
  else
    bad "orchestrate env list failed" "$(printf '%s' "$ENVS" | head -1)"
  fi

  TOOLS=$(orchestrate tools list 2>/dev/null || echo "")
  if [ -z "$TOOLS" ]; then
    bad "cannot list tools" "session may have expired — re-run: orchestrate env activate <env> -a <key>"
  else
    for t in get_store_location get_weather_forecast; do
      printf '%s' "$TOOLS" | grep -q "$t" && ok "tool $t" || bad "tool $t missing" "Section 4.1 / 4.2"
    done
  fi

  KBL=$(orchestrate knowledge-bases list 2>/dev/null || echo "")
  printf '%s' "$KBL" | grep -q "inventory-alert-knowledge" \
    && ok "knowledge base inventory-alert-knowledge" \
    || bad "knowledge base missing" "Section 4.4"

  AGENTS=$(orchestrate agents list 2>/dev/null || echo "")
  printf '%s' "$AGENTS" | grep -qiE "fashion.*inventory|Fashion_Inventory_Alert_Processor" \
    && ok "agent Fashion_Inventory_Alert_Processor" \
    || bad "agent missing" "Section 4.5"
fi

# ── 6. Optional end-to-end pipeline test ─────────────────────────────────────
if [ "$RUN_E2E" -eq 1 ]; then
  head_ "6. End-to-end pipeline (--e2e)"
  if [ -z "${CONSUMER:-}" ] || [ ! -f "$CONSUMER/produce_inventory_events.py" ]; then
    bad "cannot run e2e" "workspace not found"
  elif [ "$FAIL" -gt 0 ]; then
    warn "skipping e2e" "fix the failures above first"
  else
    echo "  Starting agent consumer (60s budget)..."
    ( cd "$CONSUMER" && run_limited 60 uv run consume_velocity_alerts_with_agent.py ) > /tmp/lab-e2e-consumer.log 2>&1 &
    CPID=$!
    sleep 10
    echo "  Producing test events..."
    if ( cd "$CONSUMER" && uv run produce_inventory_events.py \
          --csv-file ../fashion-inventory-setup/data/test_winter_jacket_spike.csv ) \
          > /tmp/lab-e2e-producer.log 2>&1; then
      ok "test producer completed"
    else
      bad "producer failed" "see /tmp/lab-e2e-producer.log"
    fi
    echo "  Waiting for the agent to respond (up to 45s)..."
    for _ in $(seq 1 45); do
      grep -q "RESULT PUBLISHED" /tmp/lab-e2e-consumer.log 2>/dev/null && break
      sleep 1
    done
    wait $CPID 2>/dev/null
    if grep -q "AGENT DECISION READY" /tmp/lab-e2e-consumer.log 2>/dev/null; then
      ok "agent returned a schema-valid decision"
    else
      bad "no agent decision observed" "see /tmp/lab-e2e-consumer.log"
    fi
    grep -q "RESULT PUBLISHED" /tmp/lab-e2e-consumer.log 2>/dev/null \
      && ok "decision published to fashion.agent.responses" \
      || bad "nothing published downstream" "see /tmp/lab-e2e-consumer.log"
  fi
fi

# ── Summary ──────────────────────────────────────────────────────────────────
printf '\n%s────────────────────────────────────────────────%s\n' "$BOLD" "$RST"
printf '  %s%d passed%s   %s%d failed%s   %s%d warnings%s\n' \
  "$GRN" "$PASS" "$RST" "$RED" "$FAIL" "$RST" "$YLW" "$WARN" "$RST"

if [ "$FAIL" -eq 0 ]; then
  printf '  %sReady to run the lab.%s\n' "$GRN" "$RST"
  [ "$RUN_E2E" -eq 0 ] && printf '  %sTip: bash check-lab.sh --e2e runs the full pipeline.%s\n' "$DIM" "$RST"
else
  printf '  %sFix these before starting:%s\n' "$RED" "$RST"
  for i in "${FAILED_ITEMS[@]}"; do printf '    · %s\n' "$i"; done
fi
printf '%s────────────────────────────────────────────────%s\n\n' "$BOLD" "$RST"

[ "$FAIL" -eq 0 ] || exit 1
