#!/bin/bash
set -e

echo "Registering Debezium MySQL source connector..."
curl -sf -X POST http://kafka-connect:8083/connectors \
  -H 'Content-Type: application/json' \
  -d @/connectors/mysql-source-connector.json \
  && echo "  Source connector registered" \
  || echo "  Source connector may already exist"

echo "Registering Postgres sink connector..."
curl -sf -X POST http://kafka-connect-sink:8084/connectors \
  -H 'Content-Type: application/json' \
  -d @/connectors/postgres-sink-connector.json \
  && echo "  Sink connector registered" \
  || echo "  Sink connector may already exist"

echo "Connector setup complete"
