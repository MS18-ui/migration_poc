-- 05_oracle_source_and_housekeeping.hql

-- Oracle source DDL (run in Oracle, not Hive) - CBS.TRANSACTIONS read via JDBC
/*
CREATE TABLE CBS.TRANSACTIONS (
  TXN_ID      VARCHAR2(30)   PRIMARY KEY,
  ACCOUNT_ID  VARCHAR2(20)   NOT NULL,
  TXN_DATE    DATE           NOT NULL,
  TXN_TS      TIMESTAMP,
  TXN_TYPE    VARCHAR2(20),
  AMOUNT      NUMBER(18,2)   NOT NULL,
  CURRENCY    CHAR(3)        NOT NULL,
  STATUS      VARCHAR2(10),
  CHANNEL     VARCHAR2(10)
);
CREATE INDEX CBS.IDX_TXN_DATE ON CBS.TRANSACTIONS (TXN_DATE);
*/

-- Hive housekeeping: refresh stats after each load
ANALYZE TABLE risk.customer_daily_risk PARTITION (run_date='2026-10-18') COMPUTE STATISTICS;
ANALYZE TABLE curated.accounts  COMPUTE STATISTICS FOR COLUMNS;
ANALYZE TABLE curated.customers COMPUTE STATISTICS FOR COLUMNS;
