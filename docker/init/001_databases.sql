CREATE DATABASE etl_source;
CREATE DATABASE etl_target;
CREATE DATABASE etl_control;

\connect etl_source

CREATE SCHEMA source;

CREATE TABLE source.transactions (
    transaction_id TEXT PRIMARY KEY,
    merchant_id TEXT,
    transaction_status TEXT NOT NULL,
    amount NUMERIC(18, 2),
    refund_amount NUMERIC(18, 2),
    transaction_timestamp TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

INSERT INTO source.transactions VALUES
    ('txn-001', 'merchant-101', 'SUCCESS', 1250.00, 0.00, '2026-10-09T10:00:00Z', '2026-10-09T10:01:00Z'),
    ('txn-002', 'merchant-101', 'SUCCESS', 750.00, 50.00, '2026-10-09T10:05:00Z', '2026-10-09T10:06:00Z'),
    ('txn-003', 'merchant-202', 'FAILED', 500.00, 0.00, '2026-10-09T10:10:00Z', '2026-10-09T10:11:00Z'),
    ('txn-004', NULL, 'SUCCESS', 300.00, 0.00, '2026-10-09T10:15:00Z', '2026-10-09T10:16:00Z'),
    ('txn-005', 'merchant-303', 'SUCCESS', -25.00, 0.00, '2026-10-09T10:20:00Z', '2026-10-09T10:21:00Z');

\connect etl_target

CREATE SCHEMA analytics;

CREATE TABLE analytics.clean_transactions (
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

