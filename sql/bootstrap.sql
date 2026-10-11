\set ON_ERROR_STOP on

-- Local development bootstrap. Run as a PostgreSQL administrator:
-- psql -U postgres -f sql/bootstrap.sql

SELECT 'CREATE ROLE etloom LOGIN PASSWORD ''etloom_dev'''
WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'etloom')\gexec

SELECT 'CREATE DATABASE etl_source OWNER etloom'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'etl_source')\gexec

SELECT 'CREATE DATABASE etl_target OWNER etloom'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'etl_target')\gexec

SELECT 'CREATE DATABASE etl_control OWNER etloom'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'etl_control')\gexec

\connect etl_source

CREATE SCHEMA IF NOT EXISTS source AUTHORIZATION etloom;
CREATE TABLE IF NOT EXISTS source.transactions (
    transaction_id TEXT PRIMARY KEY,
    merchant_id TEXT,
    transaction_status TEXT NOT NULL,
    amount NUMERIC(18, 2),
    refund_amount NUMERIC(18, 2),
    transaction_timestamp TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);
ALTER TABLE source.transactions OWNER TO etloom;

INSERT INTO source.transactions VALUES
    ('txn-001', 'merchant-101', 'SUCCESS', 1250.00, 0.00, '2026-10-09T10:00:00Z', '2026-10-09T10:01:00Z'),
    ('txn-002', 'merchant-101', 'SUCCESS', 750.00, 50.00, '2026-10-09T10:05:00Z', '2026-10-09T10:06:00Z'),
    ('txn-003', 'merchant-202', 'FAILED', 500.00, 0.00, '2026-10-09T10:10:00Z', '2026-10-09T10:11:00Z'),
    ('txn-004', NULL, 'SUCCESS', 300.00, 0.00, '2026-10-09T10:15:00Z', '2026-10-09T10:16:00Z'),
    ('txn-005', 'merchant-303', 'SUCCESS', -25.00, 0.00, '2026-10-09T10:20:00Z', '2026-10-09T10:21:00Z')
ON CONFLICT (transaction_id) DO NOTHING;

\connect etl_target

CREATE SCHEMA IF NOT EXISTS analytics AUTHORIZATION etloom;
CREATE TABLE IF NOT EXISTS analytics.clean_transactions (
    transaction_id TEXT PRIMARY KEY,
    merchant_id TEXT NOT NULL,
    transaction_status TEXT NOT NULL,
    amount NUMERIC(18, 2) NOT NULL,
    refund_amount NUMERIC(18, 2),
    transaction_timestamp TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    net_amount NUMERIC(18, 2) NOT NULL,
    transaction_date DATE NOT NULL
);
ALTER TABLE analytics.clean_transactions OWNER TO etloom;

\connect etl_control

GRANT CREATE ON DATABASE etl_control TO etloom;
