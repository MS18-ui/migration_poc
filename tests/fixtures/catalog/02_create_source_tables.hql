-- 02_create_source_tables.hql  (with nested complex types)

-- curated.customers : addresses ARRAY<STRUCT>
CREATE TABLE IF NOT EXISTS curated.customers (
  customer_id     STRING    COMMENT 'Unique customer identifier (CIF)',
  full_name       STRING    COMMENT 'Customer legal name',
  segment         STRING    COMMENT 'RETAIL | SME | CORPORATE | PRIVATE',
  addresses       ARRAY<STRUCT<
                    type:STRING,
                    line1:STRING,
                    city:STRING,
                    country:STRING,
                    is_primary:BOOLEAN>>
                            COMMENT 'All known addresses; type = HOME | WORK | MAILING',
  kyc_status      STRING    COMMENT 'VERIFIED | PENDING | EXPIRED',
  pep_flag        BOOLEAN   COMMENT 'Politically exposed person',
  onboard_date    DATE      COMMENT 'Date customer was onboarded',
  last_updated_ts TIMESTAMP COMMENT 'Last record update in source'
)
COMMENT 'Customer master (one row per customer)'
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY', 'data.owner'='customer-data', 'pii'='true');

-- curated.accounts : attributes MAP<STRING,STRING>
CREATE TABLE IF NOT EXISTS curated.accounts (
  account_id      STRING    COMMENT 'Unique account number',
  customer_id     STRING    COMMENT 'FK to curated.customers.customer_id',
  account_type    STRING    COMMENT 'SAVINGS | CURRENT | LOAN | CARD',
  currency        STRING    COMMENT 'Account base currency (ISO 4217)',
  branch_code     STRING    COMMENT 'Home branch',
  attributes      MAP<STRING,STRING>
                            COMMENT 'Product attributes e.g. product_code, overdraft_limit, joint_holder, interest_tier',
  status          STRING    COMMENT 'ACTIVE | DORMANT | CLOSED',
  open_date       DATE      COMMENT 'Account opening date',
  close_date      DATE      COMMENT 'Account closing date (null if open)',
  last_updated_ts TIMESTAMP COMMENT 'Last record update in source'
)
COMMENT 'Account master (one row per account)'
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY', 'data.owner'='account-data');

-- curated.transactions : counterparty STRUCT (now the job's transaction source)
CREATE TABLE IF NOT EXISTS curated.transactions (
  txn_id        STRING        COMMENT 'Unique transaction id',
  account_id    STRING        COMMENT 'FK to curated.accounts.account_id',
  txn_type      STRING        COMMENT 'CASH_DEP | CASH_WDL | TRANSFER | CARD | CHEQUE',
  amount        DECIMAL(18,2) COMMENT 'Amount in transaction currency',
  currency      STRING        COMMENT 'Transaction currency (ISO 4217)',
  status        STRING        COMMENT 'POSTED | PENDING | REVERSED | FAILED',
  channel       STRING        COMMENT 'BRANCH | ATM | ONLINE | MOBILE',
  counterparty  STRUCT<
                  name:STRING,
                  bank_bic:STRING,
                  account_no:STRING,
                  country:STRING>
                              COMMENT 'Beneficiary / remitter details',
  txn_ts        TIMESTAMP     COMMENT 'Transaction timestamp'
)
COMMENT 'Core banking transactions (ingested from Oracle CBS.TRANSACTIONS + payments messages)'
PARTITIONED BY (txn_date DATE)
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY', 'data.owner'='payments');

-- curated.fx_rates (external; read by the job from HDFS)
CREATE EXTERNAL TABLE IF NOT EXISTS curated.fx_rates (
  currency   STRING        COMMENT 'ISO 4217 currency code',
  usd_rate   DECIMAL(18,6) COMMENT 'Multiply local amount by this to get USD',
  source     STRING        COMMENT 'Rate provider'
)
COMMENT 'Daily FX rates to USD'
PARTITIONED BY (rate_date DATE)
STORED AS PARQUET
LOCATION 'hdfs:///data/ref/fx_rates/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

MSCK REPAIR TABLE curated.fx_rates;

-- ref.high_risk_countries
CREATE TABLE IF NOT EXISTS ref.high_risk_countries (
  country      STRING COMMENT 'ISO 3166 alpha-2',
  risk_level   STRING COMMENT 'HIGH | VERY_HIGH',
  list_source  STRING COMMENT 'e.g. FATF, internal'
)
COMMENT 'Reference list of high-risk jurisdictions'
STORED AS PARQUET;
