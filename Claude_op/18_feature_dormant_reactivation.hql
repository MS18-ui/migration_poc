-- 18_feature_dormant_reactivation.hql : dormant / long-idle accounts that transact on run_date
SET hivevar:run_date=2026-10-18;
USE aahhd01q_pnc_poc;

WITH prev AS (
  SELECT account_id, MAX(txn_date) AS last_prev_txn
  FROM v_txn_enriched
  WHERE txn_date < '${hivevar:run_date}'
  GROUP BY account_id
),
today AS (
  SELECT account_id, COUNT(*) AS cnt_today, SUM(amount_usd) AS amt_today
  FROM v_txn_enriched
  WHERE txn_date = '${hivevar:run_date}'
  GROUP BY account_id
)
SELECT a.account_id, a.customer_id, t.cnt_today, t.amt_today,
       COALESCE(datediff('${hivevar:run_date}', p.last_prev_txn), 9999) AS days_since_last_txn,
       CASE WHEN a.status = 'DORMANT' THEN 1 ELSE 0 END                 AS dormant_reactivation_flag
FROM accounts a
JOIN today t     ON a.account_id = t.account_id
LEFT JOIN prev p ON a.account_id = p.account_id
WHERE a.status = 'DORMANT'
   OR COALESCE(datediff('${hivevar:run_date}', p.last_prev_txn), 9999) > 90;
