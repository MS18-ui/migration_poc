-- 14_feature_cash_structuring.hql : cash intensity and structuring features
SET hivevar:run_date=2026-10-18;
USE aahhd01q_pnc_poc;

SELECT customer_id,
       MAX(near_thr_cnt_3d)                                    AS near_thr_cnt_3d,
       MAX(CASE WHEN near_thr_cnt_3d >= 3 THEN 1 ELSE 0 END)   AS structuring_flag,
       SUM(CASE WHEN is_cash THEN amount_usd ELSE 0 END) / SUM(amount_usd) AS cash_amt_ratio,
       AVG(CASE WHEN amount_usd % 1000 = 0 THEN 1.0 ELSE 0.0 END)          AS round_amt_ratio
FROM (
  SELECT customer_id, txn_date, amount_usd, is_cash,
         SUM(CASE WHEN is_cash AND amount_usd BETWEEN 8000 AND 9999.99 THEN 1 ELSE 0 END)
           OVER (PARTITION BY customer_id ORDER BY unix_timestamp(txn_ts)
                 RANGE BETWEEN 259200 PRECEDING AND CURRENT ROW) AS near_thr_cnt_3d
  FROM v_txn_enriched
  WHERE txn_date BETWEEN date_sub('${hivevar:run_date}', 3) AND '${hivevar:run_date}'
) x
WHERE txn_date = '${hivevar:run_date}'
GROUP BY customer_id;
