#!/bin/bash
set -e
BS="kafka:29092"

echo "=== Creating Kafka Connect internal topics ==="

# Debezium source worker — config MUST have exactly 1 partition + compaction
kafka-topics --bootstrap-server $BS --create --if-not-exists \
  --topic debezium-connect-configs \
  --partitions 1 --replication-factor 1 \
  --config cleanup.policy=compact \
  --config min.insync.replicas=1
echo "  debezium-connect-configs  (1 partition, compact)"

kafka-topics --bootstrap-server $BS --create --if-not-exists \
  --topic debezium-connect-offsets \
  --partitions 25 --replication-factor 1 \
  --config cleanup.policy=compact \
  --config min.insync.replicas=1
echo "  debezium-connect-offsets  (25 partitions, compact)"

kafka-topics --bootstrap-server $BS --create --if-not-exists \
  --topic debezium-connect-status \
  --partitions 5 --replication-factor 1 \
  --config cleanup.policy=compact \
  --config min.insync.replicas=1
echo "  debezium-connect-status   (5 partitions, compact)"

# JDBC sink worker — separate topic set, same constraints
kafka-topics --bootstrap-server $BS --create --if-not-exists \
  --topic jdbc-connect-configs \
  --partitions 1 --replication-factor 1 \
  --config cleanup.policy=compact \
  --config min.insync.replicas=1
echo "  jdbc-connect-configs      (1 partition, compact)"

kafka-topics --bootstrap-server $BS --create --if-not-exists \
  --topic jdbc-connect-offsets \
  --partitions 25 --replication-factor 1 \
  --config cleanup.policy=compact \
  --config min.insync.replicas=1
echo "  jdbc-connect-offsets      (25 partitions, compact)"

kafka-topics --bootstrap-server $BS --create --if-not-exists \
  --topic jdbc-connect-status \
  --partitions 5 --replication-factor 1 \
  --config cleanup.policy=compact \
  --config min.insync.replicas=1
echo "  jdbc-connect-status       (5 partitions, compact)"

# CDC data topics (one per source table)
kafka-topics --bootstrap-server $BS --create --if-not-exists --topic telecom.telecom_churn.customers             --partitions 1 --replication-factor 1
kafka-topics --bootstrap-server $BS --create --if-not-exists --topic telecom.telecom_churn.subscription_plans    --partitions 1 --replication-factor 1
kafka-topics --bootstrap-server $BS --create --if-not-exists --topic telecom.telecom_churn.customer_subscriptions --partitions 1 --replication-factor 1
kafka-topics --bootstrap-server $BS --create --if-not-exists --topic telecom.telecom_churn.usage_records         --partitions 1 --replication-factor 1
kafka-topics --bootstrap-server $BS --create --if-not-exists --topic telecom.telecom_churn.billing_records       --partitions 1 --replication-factor 1
kafka-topics --bootstrap-server $BS --create --if-not-exists --topic telecom.telecom_churn.support_tickets       --partitions 1 --replication-factor 1
kafka-topics --bootstrap-server $BS --create --if-not-exists --topic telecom.telecom_churn.network_quality       --partitions 1 --replication-factor 1
kafka-topics --bootstrap-server $BS --create --if-not-exists --topic telecom.dlq                                 --partitions 1 --replication-factor 1
kafka-topics --bootstrap-server $BS --create --if-not-exists --topic schema-changes.telecom_churn                --partitions 1 --replication-factor 1
echo "  CDC + DLQ topics created"

echo "=== All topics ready ==="
kafka-topics --bootstrap-server $BS --list
