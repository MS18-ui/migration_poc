-- 10_feature_base_view.hql : common enriched transaction view (run first)
-- Override run date with: beeline --hivevar run_date=2026-10-18 (remove the SET below)
SET hivevar:run_date=2026-10-18;
USE aahhd01q_pnc_poc;

CREATE VIEW IF NOT EXISTS v_txn_enriched AS
SELECT t.txn_id, t.txn_ts, t.txn_date, t.txn_type, t.channel,
       t.account_id, a.customer_id, a.account_type, a.status AS acct_status,
       a.currency AS acct_currency, t.currency,
       t.counterparty.name    AS cp_name,
       t.counterparty.country AS cp_country,
       CAST(t.amount * COALESCE(f.usd_rate, 1.0) AS DOUBLE) AS amount_usd,
       (t.txn_type IN ('CASH_DEP','CASH_WDL')) AS is_cash
FROM transactions t
JOIN accounts a ON t.account_id = a.account_id
LEFT JOIN fx_rates f ON t.currency = f.currency AND t.txn_date = f.rate_date
WHERE t.status = 'POSTED';
