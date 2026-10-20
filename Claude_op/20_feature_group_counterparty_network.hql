-- 20_feature_group_counterparty_network.hql
-- Feature group : counterparty_network   (entity: customer_id, as-of: feature_date)
-- Purpose       : counterparty concentration, novelty and sharing signals for AML / mule detection
-- Sources       : transactions (counterparty STRUCT), accounts, fx_rates
-- Override run date with: beeline --hivevar run_date=2026-10-18 (remove the SET below)
SET hivevar:run_date=2026-10-18;
USE aahhd01q_pnc_poc;

CREATE TABLE IF NOT EXISTS feature_grp_counterparty_network (
  customer_id            STRING  COMMENT 'Entity key',
  cp_unique_30d          BIGINT  COMMENT 'Distinct external counterparties (bic|account_no) in last 30 days',
  cp_country_cnt_30d     BIGINT  COMMENT 'Distinct counterparty countries in last 30 days',
  top_cp_share_30d       DOUBLE  COMMENT 'Share of USD volume going to the single largest counterparty',
  cp_hhi_30d             DOUBLE  COMMENT 'Herfindahl index of counterparty USD volume (1 = single counterparty)',
  repeat_cp_ratio_30d    DOUBLE  COMMENT 'Share of transactions with counterparties used 2+ times',
  shared_cp_cnt_30d      BIGINT  COMMENT 'Counterparties also used by at least one other customer',
  max_cp_customers_30d   BIGINT  COMMENT 'Max number of customers sharing any one of this customer counterparties',
  new_cp_cnt_today       BIGINT  COMMENT 'Counterparties used on feature_date never seen in prior 89 days'
)
COMMENT 'Feature group: counterparty network and concentration (customer level, daily)'
PARTITIONED BY (feature_date STRING)
STORED AS PARQUET
TBLPROPERTIES (
  'feature_group'='counterparty_network',
  'entity'='customer_id',
  'version'='1',
  'owner'='aml-risk',
  'window'='30d (novelty 90d)'
);

INSERT OVERWRITE TABLE feature_grp_counterparty_network PARTITION (feature_date='${hivevar:run_date}')
WITH cp_txn AS (
  -- external counterparties only, last 90 days, USD-converted
  SELECT a.customer_id, t.txn_date,
         concat(t.counterparty.bank_bic, '|', t.counterparty.account_no) AS cp_key,
         t.counterparty.country AS cp_country,
         CAST(t.amount * COALESCE(f.usd_rate, 1.0) AS DOUBLE) AS amount_usd
  FROM transactions t
  JOIN accounts a ON t.account_id = a.account_id
  LEFT JOIN fx_rates f ON t.currency = f.currency AND t.txn_date = f.rate_date
  WHERE t.status = 'POSTED'
    AND t.counterparty.name <> 'Self'
    AND t.txn_date BETWEEN date_sub('${hivevar:run_date}', 89) AND '${hivevar:run_date}'
),
win30 AS (
  SELECT * FROM cp_txn WHERE txn_date >= date_sub('${hivevar:run_date}', 29)
),
cp_cust AS (   -- per customer x counterparty
  SELECT customer_id, cp_key, COUNT(*) AS txn_cnt, SUM(amount_usd) AS amt
  FROM win30
  GROUP BY customer_id, cp_key
),
cust_tot AS (
  SELECT customer_id, SUM(amt) AS tot_amt, SUM(txn_cnt) AS tot_cnt
  FROM cp_cust
  GROUP BY customer_id
),
cp_shared AS ( -- how many customers use each counterparty
  SELECT cp_key, COUNT(DISTINCT customer_id) AS cust_cnt
  FROM win30
  GROUP BY cp_key
),
ctry AS (
  SELECT customer_id, COUNT(DISTINCT cp_country) AS cp_country_cnt_30d
  FROM win30
  GROUP BY customer_id
),
net AS (
  SELECT c.customer_id,
         COUNT(*)                                                      AS cp_unique_30d,
         MAX(c.amt) / tt.tot_amt                                       AS top_cp_share_30d,
         SUM(POW(c.amt / tt.tot_amt, 2))                               AS cp_hhi_30d,
         SUM(CASE WHEN c.txn_cnt >= 2 THEN c.txn_cnt ELSE 0 END) / tt.tot_cnt AS repeat_cp_ratio_30d,
         SUM(CASE WHEN s.cust_cnt >= 2 THEN 1 ELSE 0 END)              AS shared_cp_cnt_30d,
         MAX(s.cust_cnt)                                               AS max_cp_customers_30d
  FROM cp_cust c
  JOIN cust_tot tt  ON c.customer_id = tt.customer_id
  JOIN cp_shared s  ON c.cp_key = s.cp_key
  GROUP BY c.customer_id, tt.tot_amt, tt.tot_cnt
),
new_cp AS (
  SELECT t.customer_id, COUNT(DISTINCT t.cp_key) AS new_cp_cnt_today
  FROM cp_txn t
  LEFT JOIN (SELECT DISTINCT customer_id, cp_key
             FROM cp_txn
             WHERE txn_date < '${hivevar:run_date}') p
         ON t.customer_id = p.customer_id AND t.cp_key = p.cp_key
  WHERE t.txn_date = '${hivevar:run_date}' AND p.cp_key IS NULL
  GROUP BY t.customer_id
)
SELECT n.customer_id,
       n.cp_unique_30d,
       y.cp_country_cnt_30d,
       n.top_cp_share_30d,
       n.cp_hhi_30d,
       n.repeat_cp_ratio_30d,
       n.shared_cp_cnt_30d,
       n.max_cp_customers_30d,
       COALESCE(w.new_cp_cnt_today, 0) AS new_cp_cnt_today
FROM net n
LEFT JOIN ctry   y ON n.customer_id = y.customer_id
LEFT JOIN new_cp w ON n.customer_id = w.customer_id;
