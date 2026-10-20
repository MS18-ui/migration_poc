-- 17_feature_currency_exposure.hql : currency mix and exposure map (30 days)
SET hivevar:run_date=2026-10-18;
USE aahhd01q_pnc_poc;

SELECT customer_id,
       COUNT(DISTINCT currency) AS currency_cnt,
       SUM(CASE WHEN currency <> acct_currency THEN usd ELSE 0 END) / SUM(usd) AS foreign_ccy_ratio,
       str_to_map(concat_ws(',', collect_list(concat(currency, ':', CAST(ROUND(usd, 2) AS STRING))))) AS exposure_by_currency
FROM (
  SELECT customer_id, currency, acct_currency, SUM(amount_usd) AS usd
  FROM v_txn_enriched
  WHERE txn_date BETWEEN date_sub('${hivevar:run_date}', 29) AND '${hivevar:run_date}'
  GROUP BY customer_id, currency, acct_currency
) g
GROUP BY customer_id;
