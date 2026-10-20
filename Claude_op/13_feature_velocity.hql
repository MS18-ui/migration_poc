-- 13_feature_velocity.hql : rolling 7-day / 30-day transaction velocity (one row per customer per day)
SET hivevar:run_date=2026-10-18;
USE aahhd01q_pnc_poc;

WITH daily AS (
  SELECT customer_id, txn_date, COUNT(*) AS cnt, SUM(amount_usd) AS amt, MAX(amount_usd) AS max_amt
  FROM v_txn_enriched
  GROUP BY customer_id, txn_date
)
SELECT customer_id, txn_date,
       SUM(cnt)     OVER w7  AS txn_cnt_7d,
       SUM(amt)     OVER w7  AS txn_amt_7d,
       SUM(cnt)     OVER w30 AS txn_cnt_30d,
       SUM(amt)     OVER w30 AS txn_amt_30d,
       MAX(max_amt) OVER w30 AS max_txn_amt_30d
FROM daily
WINDOW w7  AS (PARTITION BY customer_id ORDER BY datediff(txn_date, '1970-01-01') RANGE BETWEEN 6  PRECEDING AND CURRENT ROW),
       w30 AS (PARTITION BY customer_id ORDER BY datediff(txn_date, '1970-01-01') RANGE BETWEEN 29 PRECEDING AND CURRENT ROW);
