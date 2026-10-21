-- 03_incremental_changes.sql : simulate a day of source activity between two Sqoop runs
--   * 5,000 new transactions  -> picked up by the append incremental job (check column txn_id)
--   * 300 accounts changed    -> picked up by the lastmodified incremental job (check column updated_at)
--   * 50 new accounts         -> also picked up by lastmodified

INSERT INTO retail.transactions
    (account_id, txn_ts, txn_type, amount, status, channel, counterparty, memo, updated_at)
SELECT 1 + floor(random() * 15000)::bigint,
       now()::timestamp - random() * interval '2 hours',
       (ARRAY['CASH_DEP','CASH_WDL','TRANSFER','CARD','CHEQUE'])[1 + floor(random() * 5)::int],
       round((random() * 1500)::numeric, 2),
       'POSTED',
       (ARRAY['BRANCH','ATM','ONLINE','MOBILE'])[1 + floor(random() * 4)::int],
       'REF' || lpad((floor(random() * 999999999))::bigint::text, 9, '0'),
       'incremental batch',
       now()::timestamp
FROM generate_series(1, 5000);

UPDATE retail.accounts
SET balance    = balance + round((random() * 500)::numeric, 2),
    status     = CASE WHEN status = 'DORMANT' THEN 'ACTIVE' ELSE status END,
    updated_at = now()
WHERE account_id IN (SELECT account_id FROM retail.accounts ORDER BY random() LIMIT 300);

INSERT INTO retail.accounts
    (account_no, customer_id, account_type, currency, branch_code, balance, credit_limit, status, open_date, updated_at)
SELECT 'AC' || lpad((15000 + g)::text, 12, '0'),
       1 + floor(random() * 10000)::bigint,
       'SAVINGS', 'USD',
       'BR' || lpad((1 + floor(random() * 40))::int::text, 3, '0'),
       round((random() * 1000)::numeric, 2),
       NULL, 'ACTIVE', CURRENT_DATE, now()
FROM generate_series(1, 50) g;
