-- 03_create_target_tables.hql  (with nested complex types)

CREATE TABLE IF NOT EXISTS risk.customer_daily_risk (
  customer_id            STRING   COMMENT 'FK to curated.customers',
  segment                STRING   COMMENT 'Customer segment',
  residence_country      STRING   COMMENT 'Country of primary address',
  address_countries      ARRAY<STRING>
                                  COMMENT 'Distinct countries across all addresses',
  multi_country_flag     BOOLEAN  COMMENT 'Addresses span more than one country',
  txn_count              BIGINT   COMMENT 'Posted transactions on run_date',
  total_amount_usd       DOUBLE   COMMENT 'Sum of amounts converted to USD',
  large_txn_count        BIGINT   COMMENT 'Transactions >= 10,000 USD',
  structuring_flag       BOOLEAN  COMMENT '3+ cash txns of 8,000-9,999.99 USD within 3 days',
  counterparty_countries ARRAY<STRING>
                                  COMMENT 'Distinct counterparty countries transacted with',
  exposure_by_currency   MAP<STRING,DOUBLE>
                                  COMMENT 'USD-equivalent total per transaction currency',
  risk_reasons           ARRAY<STRUCT<
                           rule_id:STRING,
                           hit_count:BIGINT,
                           total_amount_usd:DOUBLE,
                           max_score:INT>>
                                  COMMENT 'AML rules fired, one element per rule',
  risk_score             INT      COMMENT 'Max score among fired rules, 10 if none'
)
COMMENT 'Daily customer risk summary for AML monitoring'
PARTITIONED BY (run_date STRING COMMENT 'Business date yyyy-MM-dd')
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY', 'data.owner'='aml-risk');
