-- 15_feature_geo_risk.hql : counterparty and geographic risk features (30 days)
SET hivevar:run_date=2026-10-18;
USE aahhd01q_pnc_poc;

WITH f_profile AS (
  SELECT c.customer_id,
         MAX(CASE WHEN x.addr.is_primary THEN x.addr.country END) AS residence_country
  FROM customers c
  LATERAL VIEW OUTER explode(c.addresses) x AS addr
  GROUP BY c.customer_id
)
SELECT e.customer_id,
       COUNT(DISTINCT e.cp_country) AS cp_country_cnt,
       COUNT(DISTINCT e.cp_name)    AS cp_cnt,
       SUM(CASE WHEN h.country IS NOT NULL THEN 1 ELSE 0 END)                   AS high_risk_cp_txn_cnt,
       SUM(CASE WHEN h.country IS NOT NULL THEN e.amount_usd ELSE 0 END)        AS high_risk_cp_amt_usd,
       AVG(CASE WHEN e.cp_country <> p.residence_country THEN 1.0 ELSE 0.0 END) AS cross_border_ratio
FROM v_txn_enriched e
JOIN f_profile p ON e.customer_id = p.customer_id
LEFT JOIN high_risk_countries h ON e.cp_country = h.country
WHERE e.txn_date BETWEEN date_sub('${hivevar:run_date}', 29) AND '${hivevar:run_date}'
GROUP BY e.customer_id;
