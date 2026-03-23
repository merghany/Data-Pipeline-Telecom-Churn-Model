#!/usr/bin/env bash
# dbt_runner.sh
# Installs packages, runs dbt on first boot, then re-runs every INTERVAL seconds.
# Designed to run inside the dbt Docker container.

set -euo pipefail

DBT_DIR="/dbt/telecom_churn"
INTERVAL="${DBT_INTERVAL:-300}"   # default: re-run every 5 minutes

echo ""
echo "╔══════════════════════════════════════════════════════╗"
echo "║   Telecom CDC — dbt Runner                           ║"
echo "║   Project : ${DBT_DIR}                    ║"
echo "║   Interval: every ${INTERVAL}s                              ║"
echo "╚══════════════════════════════════════════════════════╝"
echo ""

cd "$DBT_DIR"

# ── Wait for Postgres ─────────────────────────────────────────
echo "⏳ Waiting for Postgres..."
until pg_isready -h "${DBT_HOST:-postgres}" -p "${DBT_PORT:-5432}" -U "${DBT_USER:-dbt_user}" > /dev/null 2>&1; do
    sleep 2
done
echo "✅ Postgres is ready"
echo ""

# ── Install dbt packages once ─────────────────────────────────
echo "📦 Installing dbt packages..."
dbt deps --profiles-dir .
echo ""

# ── Run loop ──────────────────────────────────────────────────
run_number=0
while true; do
    run_number=$((run_number + 1))
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "  dbt run #${run_number}  —  $(date '+%Y-%m-%d %H:%M:%S')"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

    # Run all models
    dbt run --profiles-dir . && echo "  ✅ dbt run complete" || echo "  ⚠️  dbt run had errors"

    # Run tests every 5 runs
    if [ $((run_number % 5)) -eq 0 ]; then
        echo ""
        echo "  🧪 Running dbt tests..."
        dbt test --profiles-dir . && echo "  ✅ All tests passed" || echo "  ⚠️  Some tests failed"
    fi

    echo ""
    echo "  💤 Sleeping ${INTERVAL}s until next run..."
    sleep "$INTERVAL"
done
