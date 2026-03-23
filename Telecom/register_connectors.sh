#!/usr/bin/env bash
# register_connectors.sh
# Waits for Kafka Connect REST API, then registers source + sink connectors.
# Runs as the connector_setup service in docker-compose.

set -euo pipefail

CONNECT_URL="http://kafka-connect:8083"
SOURCE="/connectors/mysql-source-connector.json"
SINK="/connectors/postgres-sink-connector.json"

echo ""
echo "╔══════════════════════════════════════════════════════╗"
echo "║   Telecom CDC — Connector Registration               ║"
echo "╚══════════════════════════════════════════════════════╝"
echo ""

# ── Wait for Kafka Connect ────────────────────────────────────
echo "⏳ Waiting for Kafka Connect at ${CONNECT_URL} ..."
attempt=0
until curl -sf "${CONNECT_URL}/connectors" > /dev/null 2>&1; do
    attempt=$((attempt + 1))
    if [ $attempt -ge 40 ]; then
        echo "❌ Kafka Connect not ready after 40 attempts. Exiting."
        exit 1
    fi
    printf "   attempt %d/40\r" "$attempt"
    sleep 5
done
echo "✅ Kafka Connect is ready"
echo ""

# ── Helper ─────────────────────────────────────────────────────
register() {
    local file="$1"
    local name
    name=$(python3 -c "import json; print(json.load(open('${file}'))['name'])")
    local cfg
    cfg=$(python3 -c "import json; print(json.dumps(json.load(open('${file}'))['config']))")

    local status
    status=$(curl -s -o /dev/null -w "%{http_code}" "${CONNECT_URL}/connectors/${name}/status")

    if [ "$status" = "200" ]; then
        echo "🔄 Updating existing connector: ${name}"
        curl -sf -X PUT  "${CONNECT_URL}/connectors/${name}/config" \
            -H "Content-Type: application/json" -d "${cfg}" | python3 -m json.tool --no-ensure-ascii || true
    else
        echo "🆕 Creating connector: ${name}"
        curl -sf -X POST "${CONNECT_URL}/connectors" \
            -H "Content-Type: application/json" -d @"${file}" | python3 -m json.tool --no-ensure-ascii || true
    fi
    echo ""
}

register "$SOURCE"
register "$SINK"

# ── Print status ──────────────────────────────────────────────
echo "📊 Connector status:"
sleep 5
curl -sf "${CONNECT_URL}/connectors?expand=status" | python3 -c "
import json, sys
data = json.load(sys.stdin)
for name, info in data.items():
    state = info.get('status', {}).get('connector', {}).get('state', 'UNKNOWN')
    icon  = '✅' if state == 'RUNNING' else '⚠️ '
    print(f'  {icon} {name:<45} [{state}]')
" 2>/dev/null || echo "  (status not yet available)"

echo ""
echo "╔══════════════════════════════════════════════════════╗"
echo "║  CDC Pipeline Active                                 ║"
echo "║  MySQL binlog → Kafka → Postgres raw schema          ║"
echo "║  Kafka UI : http://localhost:8090                    ║"
echo "╚══════════════════════════════════════════════════════╝"
