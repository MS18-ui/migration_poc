-- 01_schema.sql : source schema for the Sqoop POC (PostgreSQL)
-- Deliberately awkward for a Sqoop import: enum, JSONB, array, timestamptz, composite PK,
-- a table with no PK, nullable columns, NUMERIC with scale, free text with delimiters/newlines.

DROP SCHEMA IF EXISTS retail CASCADE;
CREATE SCHEMA retail;

CREATE TYPE retail.segment_t AS ENUM ('RETAIL', 'SME', 'CORPORATE', 'PRIVATE');

CREATE TABLE retail.branches (
    branch_code  VARCHAR(10) PRIMARY KEY,
    branch_name  TEXT        NOT NULL,
    city         TEXT,
    state_code   CHAR(2),
    opened_date  DATE,
    is_active    BOOLEAN     NOT NULL DEFAULT TRUE
);

CREATE TABLE retail.customers (
    customer_id    BIGSERIAL PRIMARY KEY,
    customer_ref   VARCHAR(12)  NOT NULL UNIQUE,
    full_name      TEXT         NOT NULL,
    email          TEXT,                                  -- nullable
    segment        retail.segment_t NOT NULL,             -- enum (Sqoop cannot map it directly)
    country        CHAR(2)      NOT NULL,
    kyc_status     VARCHAR(10)  NOT NULL CHECK (kyc_status IN ('VERIFIED', 'PENDING', 'EXPIRED')),
    pep_flag       BOOLEAN      NOT NULL DEFAULT FALSE,
    date_of_birth  DATE,                                  -- nullable
    onboard_ts     TIMESTAMPTZ  NOT NULL,                 -- time zone aware
    address        JSONB,                                 -- semi-structured
    tags           TEXT[],                                -- array, nullable, sometimes empty
    notes          TEXT,                                  -- commas, quotes, newlines, tabs, unicode
    updated_at     TIMESTAMP    NOT NULL DEFAULT now()
);

CREATE TABLE retail.accounts (
    account_id    BIGSERIAL PRIMARY KEY,
    account_no    VARCHAR(14)   NOT NULL UNIQUE,
    customer_id   BIGINT        NOT NULL REFERENCES retail.customers (customer_id),
    account_type  VARCHAR(10)   NOT NULL,
    currency      CHAR(3)       NOT NULL,
    branch_code   VARCHAR(10)   REFERENCES retail.branches (branch_code),
    balance       NUMERIC(18,2) NOT NULL DEFAULT 0,       -- can be negative
    credit_limit  NUMERIC(18,2),                          -- only for cards
    status        VARCHAR(8)    NOT NULL,
    open_date     DATE          NOT NULL,
    close_date    DATE,                                   -- null unless CLOSED
    updated_at    TIMESTAMP     NOT NULL DEFAULT now()    -- used for lastmodified incremental
);

-- many-to-many with a COMPOSITE primary key (Sqoop needs --split-by for this one)
CREATE TABLE retail.account_holders (
    account_id   BIGINT       NOT NULL REFERENCES retail.accounts (account_id),
    customer_id  BIGINT       NOT NULL REFERENCES retail.customers (customer_id),
    holder_role  VARCHAR(10)  NOT NULL,
    share_pct    NUMERIC(5,2) NOT NULL,
    since_date   DATE,
    PRIMARY KEY (account_id, customer_id)
);

-- big table: used for parallel mappers and append-style incremental import
CREATE TABLE retail.transactions (
    txn_id        BIGSERIAL PRIMARY KEY,
    account_id    BIGINT        NOT NULL REFERENCES retail.accounts (account_id),
    txn_ts        TIMESTAMP     NOT NULL,
    txn_type      VARCHAR(10)   NOT NULL,
    amount        NUMERIC(18,2) NOT NULL,
    status        VARCHAR(10)   NOT NULL,
    channel       VARCHAR(10),
    counterparty  VARCHAR(40),
    memo          TEXT,
    updated_at    TIMESTAMP     NOT NULL DEFAULT now()
);

-- NO primary key and contains duplicate rows (Sqoop falls back to 1 mapper or needs --split-by)
CREATE TABLE retail.fx_rates_log (
    rate_date  DATE          NOT NULL,
    currency   CHAR(3)       NOT NULL,
    usd_rate   NUMERIC(18,6) NOT NULL,
    source     VARCHAR(12),
    loaded_at  TIMESTAMP     NOT NULL DEFAULT now()
);

CREATE INDEX idx_accounts_customer     ON retail.accounts (customer_id);
CREATE INDEX idx_accounts_updated      ON retail.accounts (updated_at);
CREATE INDEX idx_transactions_account  ON retail.transactions (account_id);
CREATE INDEX idx_transactions_ts       ON retail.transactions (txn_ts);
