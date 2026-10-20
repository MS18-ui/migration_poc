-- 12_feature_account_product.hql : account/product features from MAP attributes
SET hivevar:run_date=2026-10-18;
USE aahhd01q_pnc_poc;

SELECT customer_id,
       COUNT(*)                                                          AS acct_cnt,
       SUM(CASE WHEN status = 'ACTIVE'  THEN 1 ELSE 0 END)               AS active_acct_cnt,
       SUM(CASE WHEN status = 'DORMANT' THEN 1 ELSE 0 END)               AS dormant_acct_cnt,
       SUM(CAST(attributes['overdraft_limit'] AS DOUBLE))                AS total_overdraft_limit,
       MAX(CASE WHEN attributes['joint_holder'] = 'Y' THEN 1 ELSE 0 END) AS has_joint_acct,
       COUNT(DISTINCT attributes['product_code'])                        AS product_cnt,
       MIN(datediff('${hivevar:run_date}', open_date))                   AS newest_acct_age_days
FROM accounts
WHERE status <> 'CLOSED'
GROUP BY customer_id;
