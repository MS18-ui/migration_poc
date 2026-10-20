-- 19_customer_features_assemble.hql : self-contained assembly of customer_features (one row per customer per feature_date)
-- Requires 10_feature_base_view.hql to have been run.
SET hivevar:run_date=2026-10-18;
USE aahhd01q_pnc_poc;

CREATE TABLE IF NOT EXISTS customer_features (
  customer_id            STRING,
  segment                STRING,
  kyc_status             STRING,
  pep_flag               BOOLEAN,
  tenure_days            INT,
  address_cnt            BIGINT,
  address_country_cnt    BIGINT,
  residence_country      STRING,
  acct_cnt               BIGINT,
  dormant_acct_cnt       BIGINT,
  total_overdraft_limit  DOUBLE,
  has_joint_acct         INT,
  product_cnt            BIGINT,
  txn_cnt_7d             BIGINT,
  txn_amt_7d             DOUBLE,
  txn_cnt_30d            BIGINT,
  txn_amt_30d            DOUBLE,
  near_thr_cnt_3d        BIGINT,
  structuring_flag       INT,
  cash_amt_ratio         DOUBLE,
  round_amt_ratio        DOUBLE,
  cp_country_cnt         BIGINT,
  high_risk_cp_txn_cnt   BIGINT,
  high_risk_cp_amt_usd   DOUBLE,
  cross_border_ratio     DOUBLE,
  amt_zscore             DOUBLE,
  night_txn_ratio        DOUBLE,
  foreign_ccy_ratio      DOUBLE,
  exposure_by_currency   MAP<STRING,STRING>
)
PARTITIONED BY (feature_date STRING)
STORED AS PARQUET;

INSERT OVERWRITE TABLE customer_features PARTITION (feature_date='${hivevar:run_date}')
WITH f_profile AS (
  SELECT c.customer_id, c.segment, c.kyc_status, c.pep_flag,
         datediff('${hivevar:run_date}', c.onboard_date)          AS tenure_days,
         COUNT(x.addr.country)                                     AS address_cnt,
         COUNT(DISTINCT x.addr.country)                            AS address_country_cnt,
         MAX(CASE WHEN x.addr.is_primary THEN x.addr.country END)  AS residence_country
  FROM customers c
  LATERAL VIEW OUTER explode(c.addresses) x AS addr
  GROUP BY c.customer_id, c.segment, c.kyc_status, c.pep_flag, c.onboard_date
),
f_account AS (
  SELECT customer_id,
         COUNT(*)                                                          AS acct_cnt,
         SUM(CASE WHEN status = 'DORMANT' THEN 1 ELSE 0 END)               AS dormant_acct_cnt,
         SUM(CAST(attributes['overdraft_limit'] AS DOUBLE))                AS total_overdraft_limit,
         MAX(CASE WHEN attributes['joint_holder'] = 'Y' THEN 1 ELSE 0 END) AS has_joint_acct,
         COUNT(DISTINCT attributes['product_code'])                        AS product_cnt
  FROM accounts
  WHERE status <> 'CLOSED'
  GROUP BY customer_id
),
vel_daily AS (
  SELECT customer_id, txn_date, COUNT(*) AS cnt, SUM(amount_usd) AS amt
  FROM v_txn_enriched
  GROUP BY customer_id, txn_date
),
f_velocity AS (
  SELECT customer_id, txn_cnt_7d, txn_amt_7d, txn_cnt_30d, txn_amt_30d
  FROM (
    SELECT customer_id, txn_date,
           SUM(cnt) OVER w7  AS txn_cnt_7d,  SUM(amt) OVER w7  AS txn_amt_7d,
           SUM(cnt) OVER w30 AS txn_cnt_30d, SUM(amt) OVER w30 AS txn_amt_30d
    FROM vel_daily
    WINDOW w7  AS (PARTITION BY customer_id ORDER BY datediff(txn_date, '1970-01-01') RANGE BETWEEN 6  PRECEDING AND CURRENT ROW),
           w30 AS (PARTITION BY customer_id ORDER BY datediff(txn_date, '1970-01-01') RANGE BETWEEN 29 PRECEDING AND CURRENT ROW)
  ) r
  WHERE txn_date = '${hivevar:run_date}'
),
f_struct AS (
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
  GROUP BY customer_id
),
f_geo AS (
  SELECT e.customer_id,
         COUNT(DISTINCT e.cp_country) AS cp_country_cnt,
         SUM(CASE WHEN h.country IS NOT NULL THEN 1 ELSE 0 END)                   AS high_risk_cp_txn_cnt,
         SUM(CASE WHEN h.country IS NOT NULL THEN e.amount_usd ELSE 0 END)        AS high_risk_cp_amt_usd,
         AVG(CASE WHEN e.cp_country <> p.residence_country THEN 1.0 ELSE 0.0 END) AS cross_border_ratio
  FROM v_txn_enriched e
  JOIN f_profile p ON e.customer_id = p.customer_id
  LEFT JOIN high_risk_countries h ON e.cp_country = h.country
  WHERE e.txn_date BETWEEN date_sub('${hivevar:run_date}', 29) AND '${hivevar:run_date}'
  GROUP BY e.customer_id
),
beh_hist AS (
  SELECT customer_id, AVG(amount_usd) AS mu, STDDEV_POP(amount_usd) AS sigma
  FROM v_txn_enriched
  WHERE txn_date <  '${hivevar:run_date}'
    AND txn_date >= date_sub('${hivevar:run_date}', 90)
  GROUP BY customer_id
),
beh_today AS (
  SELECT customer_id, MAX(amount_usd) AS max_amt_today,
         AVG(CASE WHEN hour(txn_ts) BETWEEN 0 AND 5 THEN 1.0 ELSE 0.0 END) AS night_txn_ratio
  FROM v_txn_enriched
  WHERE txn_date = '${hivevar:run_date}'
  GROUP BY customer_id
),
f_behavior AS (
  SELECT t.customer_id, t.night_txn_ratio,
         CASE WHEN h.sigma > 0 THEN (t.max_amt_today - h.mu) / h.sigma END AS amt_zscore
  FROM beh_today t
  LEFT JOIN beh_hist h ON t.customer_id = h.customer_id
),
f_ccy AS (
  SELECT customer_id,
         SUM(CASE WHEN currency <> acct_currency THEN usd ELSE 0 END) / SUM(usd) AS foreign_ccy_ratio,
         str_to_map(concat_ws(',', collect_list(concat(currency, ':', CAST(ROUND(usd, 2) AS STRING))))) AS exposure_by_currency
  FROM (
    SELECT customer_id, currency, acct_currency, SUM(amount_usd) AS usd
    FROM v_txn_enriched
    WHERE txn_date BETWEEN date_sub('${hivevar:run_date}', 29) AND '${hivevar:run_date}'
    GROUP BY customer_id, currency, acct_currency
  ) g
  GROUP BY customer_id
)
SELECT p.customer_id, p.segment, p.kyc_status, p.pep_flag,
       p.tenure_days, p.address_cnt, p.address_country_cnt, p.residence_country,
       a.acct_cnt, a.dormant_acct_cnt, a.total_overdraft_limit, a.has_joint_acct, a.product_cnt,
       v.txn_cnt_7d, v.txn_amt_7d, v.txn_cnt_30d, v.txn_amt_30d,
       s.near_thr_cnt_3d, s.structuring_flag, s.cash_amt_ratio, s.round_amt_ratio,
       g.cp_country_cnt, g.high_risk_cp_txn_cnt, g.high_risk_cp_amt_usd, g.cross_border_ratio,
       b.amt_zscore, b.night_txn_ratio, c.foreign_ccy_ratio, c.exposure_by_currency
FROM f_profile p
LEFT JOIN f_account  a ON p.customer_id = a.customer_id
LEFT JOIN f_velocity v ON p.customer_id = v.customer_id
LEFT JOIN f_struct   s ON p.customer_id = s.customer_id
LEFT JOIN f_geo      g ON p.customer_id = g.customer_id
LEFT JOIN f_behavior b ON p.customer_id = b.customer_id
LEFT JOIN f_ccy      c ON p.customer_id = c.customer_id;
