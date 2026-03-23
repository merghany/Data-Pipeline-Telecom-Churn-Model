-- Runs automatically on first MySQL startup (after 01_schema.sql)
-- Creates a dedicated replication user for Debezium

CREATE USER IF NOT EXISTS 'debezium'@'%' IDENTIFIED WITH mysql_native_password BY 'debezium_pass';
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'debezium'@'%';
FLUSH PRIVILEGES;
