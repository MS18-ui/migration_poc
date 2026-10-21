-- Test fixture: source tables of the Sqoop POC (Postgres schema "retail"), rebuilt from the column lists in the
-- POC notebook. Not the POC's 01_schema.sql; customers (enum, JSONB, text[]) is left out on purpose because the
-- POC imports it through a free-form query.
CREATE TABLE retail.branches (
    branch_code  VARCHAR(8)   PRIMARY KEY,
    branch_name  VARCHAR(80)  NOT NULL,
    city         VARCHAR(60),
    state_code   CHAR(2),
    opened_date  DATE,
    is_active    BOOLEAN      NOT NULL DEFAULT true
);

CREATE TABLE retail.accounts (
    account_id    BIGSERIAL     PRIMARY KEY,
    account_no    VARCHAR(20)   NOT NULL UNIQUE,
    customer_id   BIGINT        NOT NULL REFERENCES retail.customers (customer_id),
    account_type  VARCHAR(12)   NOT NULL,
    currency      CHAR(3)       NOT NULL,
    branch_code   VARCHAR(8)    REFERENCES retail.branches (branch_code),
    balance       NUMERIC(18,2) NOT NULL,
    credit_limit  NUMERIC(18,2),
    status        VARCHAR(10)   NOT NULL,
    open_date     DATE          NOT NULL,
    close_date    DATE,
    updated_at    TIMESTAMP     NOT NULL DEFAULT now()
);

CREATE TABLE retail.account_holders (
    account_id   BIGINT       NOT NULL REFERENCES retail.accounts (account_id),
    customer_id  BIGINT       NOT NULL REFERENCES retail.customers (customer_id),
    holder_role  VARCHAR(10)  NOT NULL,
    share_pct    NUMERIC(5,2) NOT NULL,
    since_date   DATE,
    PRIMARY KEY (account_id, customer_id)
);

CREATE TABLE retail.transactions (
    txn_id        BIGSERIAL     PRIMARY KEY,
    account_id    BIGINT        NOT NULL REFERENCES retail.accounts (account_id),
    txn_ts        TIMESTAMP     NOT NULL,
    txn_type      VARCHAR(12)   NOT NULL,
    amount        NUMERIC(18,2) NOT NULL,
    status        VARCHAR(10)   NOT NULL,
    channel       VARCHAR(10),
    counterparty  VARCHAR(120),
    memo          TEXT,
    updated_at    TIMESTAMP     NOT NULL DEFAULT now()
);

CREATE TABLE retail.fx_rates_log (
    rate_date  DATE          NOT NULL,
    currency   CHAR(3)       NOT NULL,
    usd_rate   NUMERIC(18,6) NOT NULL,
    source     VARCHAR(20),
    loaded_at  TIMESTAMP     NOT NULL DEFAULT now()
);
