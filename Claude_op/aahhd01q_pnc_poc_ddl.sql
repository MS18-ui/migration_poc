-- Database aahhd01q_pnc_poc: the tables read and written by daily_txn_risk_job.py.
-- Exported from create_tables_spark_sql.py (same statements, same LOCATION paths).
-- The catalog snapshot reads this file so that table and column names match the job exactly.

CREATE TABLE IF NOT EXISTS aahhd01q_pnc_poc.customers (
    customer_id     STRING,
    full_name       STRING,
    segment         STRING,
    addresses       ARRAY<STRUCT<type:STRING, line1:STRING, city:STRING, country:STRING, is_primary:BOOLEAN>>,
    kyc_status      STRING,
    pep_flag        BOOLEAN,
    onboard_date    DATE,
    last_updated_ts TIMESTAMP
)
STORED AS PARQUET
LOCATION 'hdfs://172.16.0.4:9000/user/hive/warehouse/aahhd01q_pnc_poc.db/customers';

CREATE TABLE IF NOT EXISTS aahhd01q_pnc_poc.accounts (
    account_id      STRING,
    customer_id     STRING,
    account_type    STRING,
    currency        STRING,
    branch_code     STRING,
    attributes      MAP<STRING,STRING>,
    status          STRING,
    open_date       DATE,
    close_date      DATE,
    last_updated_ts TIMESTAMP
)
STORED AS PARQUET
LOCATION 'hdfs://172.16.0.4:9000/user/hive/warehouse/aahhd01q_pnc_poc.db/accounts';

CREATE TABLE IF NOT EXISTS aahhd01q_pnc_poc.transactions (
    txn_id        STRING,
    account_id    STRING,
    txn_type      STRING,
    amount        DECIMAL(18,2),
    currency      STRING,
    status        STRING,
    channel       STRING,
    counterparty  STRUCT<name:STRING, bank_bic:STRING, account_no:STRING, country:STRING>,
    txn_ts        TIMESTAMP,
    txn_date      DATE
)
USING PARQUET
PARTITIONED BY (txn_date)
LOCATION 'hdfs://172.16.0.4:9000/user/hive/warehouse/aahhd01q_pnc_poc.db/transactions';

CREATE TABLE IF NOT EXISTS aahhd01q_pnc_poc.fx_rates (
    currency   STRING,
    usd_rate   DECIMAL(18,6),
    source     STRING,
    rate_date  DATE
)
USING PARQUET
PARTITIONED BY (rate_date)
LOCATION 'hdfs://172.16.0.4:9000/user/hive/warehouse/aahhd01q_pnc_poc.db/fx_rates';

CREATE TABLE IF NOT EXISTS aahhd01q_pnc_poc.high_risk_countries (
    country      STRING,
    risk_level   STRING,
    list_source  STRING
)
STORED AS PARQUET
LOCATION 'hdfs://172.16.0.4:9000/user/hive/warehouse/aahhd01q_pnc_poc.db/high_risk_countries';

CREATE TABLE IF NOT EXISTS aahhd01q_pnc_poc.customer_daily_risk (
    customer_id            STRING,
    segment                STRING,
    residence_country      STRING,
    address_countries      ARRAY<STRING>,
    multi_country_flag     BOOLEAN,
    txn_count              BIGINT,
    total_amount_usd       DOUBLE,
    large_txn_count        BIGINT,
    structuring_flag       BOOLEAN,
    counterparty_countries ARRAY<STRING>,
    exposure_by_currency   MAP<STRING,DOUBLE>,
    risk_reasons           ARRAY<STRUCT<rule_id:STRING, hit_count:BIGINT, total_amount_usd:DOUBLE, max_score:INT>>,
    risk_score             INT,
    run_date               STRING
)
USING PARQUET
PARTITIONED BY (run_date)
LOCATION 'hdfs://172.16.0.4:9000/user/hive/warehouse/aahhd01q_pnc_poc.db/customer_daily_risk';
