-- 16_feature_behavior.hql : amount deviation (z-score), channel mix, night-time activity
SET hivevar:run_date=2026-10-18;
USE aahhd01q_pnc_poc;

WITH hist AS (
  SELECT customer_id, AVG(amount_usd) AS mu, STDDEV_POP(amount_usd) AS sigma, COUNT(*) AS n
  FROM v_txn_enriched
  WHERE txn_date <  '${hivevar:run_date}'
    AND txn_date >= date_sub('${hivevar:run_date}', 90)
  GROUP BY customer_id
),
today AS (
  SELECT customer_id, MAX(amount_usd) AS max_amt_today, COUNT(*) AS cnt_today,
         AVG(CASE WHEN channel = 'ONLINE' THEN 1.0 ELSE 0.0 END)            AS online_ratio,
         AVG(CASE WHEN channel = 'ATM'    THEN 1.0 ELSE 0.0 END)            AS atm_ratio,
         AVG(CASE WHEN hour(txn_ts) BETWEEN 0 AND 5 THEN 1.0 ELSE 0.0 END)  AS night_txn_ratio
  FROM v_txn_enriched
  WHERE txn_date = '${hivevar:run_date}'
  GROUP BY customer_id
)
SELECT t.customer_id, t.max_amt_today, t.cnt_today, t.online_ratio, t.atm_ratio, t.night_txn_ratio,
       h.mu AS hist_avg_amt, h.sigma AS hist_std_amt,
       CASE WHEN h.sigma > 0 THEN (t.max_amt_today - h.mu) / h.sigma END AS amt_zscore
FROM today t
LEFT JOIN hist h ON t.customer_id = h.customer_id;
