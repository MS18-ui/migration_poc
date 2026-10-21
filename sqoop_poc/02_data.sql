-- 02_data.sql : generates the data (server side, deterministic via setseed)
--   branches 40 | customers 10,000 | accounts 15,000 | account_holders ~18,000
--   transactions 200,000 | fx_rates_log 900 + ~90 duplicates

SELECT setseed(0.42);

-- ------------------------------------------------------------------ branches
INSERT INTO retail.branches (branch_code, branch_name, city, state_code, opened_date, is_active)
SELECT 'BR' || lpad(g::text, 3, '0'),
       'Branch ' || g,
       (ARRAY['Pittsburgh','Philadelphia','Newark','Cleveland','Baltimore','Atlanta','Chicago','Dallas'])[1 + g % 8],
       (ARRAY['PA','PA','NJ','OH','MD','GA','IL','TX'])[1 + g % 8],
       DATE '1990-01-01' + ((g * 173) % 9000),
       (g % 11) <> 0
FROM generate_series(1, 40) g;

-- ------------------------------------------------------------------ customers
INSERT INTO retail.customers
    (customer_ref, full_name, email, segment, country, kyc_status, pep_flag,
     date_of_birth, onboard_ts, address, tags, notes, updated_at)
SELECT 'CIF' || lpad(g::text, 8, '0'),
       (ARRAY['Aisha','Omar','Priya','José','Sofia','Chen','Fatima','Noah','Elena','Ravi','Zoë','Hassan'])[1 + floor(random() * 12)::int]
         || ' ' ||
       (ARRAY['Khan','Patel','O''Brien','Müller','Garcia','Smith','Nguyen','Ahmed','李','Rossi','Silva','Taylor'])[1 + floor(random() * 12)::int],
       CASE WHEN random() < 0.08 THEN NULL ELSE 'user' || g || '@example.com' END,
       (ARRAY['RETAIL','RETAIL','RETAIL','SME','CORPORATE','PRIVATE'])[1 + floor(random() * 6)::int]::retail.segment_t,
       (ARRAY['US','US','US','GB','DE','IN','PK','AE','SG','CA'])[1 + floor(random() * 10)::int],
       (ARRAY['VERIFIED','VERIFIED','VERIFIED','VERIFIED','PENDING','EXPIRED'])[1 + floor(random() * 6)::int],
       random() < 0.02,
       CASE WHEN random() < 0.05 THEN NULL ELSE DATE '1950-01-01' + floor(random() * 20000)::int END,
       TIMESTAMPTZ '2012-01-01 00:00:00+00' + random() * interval '5000 days',
       jsonb_build_object(
           'line1', (1 + floor(random() * 9998))::int || ' Main St, Apt ' || (1 + g % 50),
           'city',  (ARRAY['Pittsburgh','Newark','Austin','London','Berlin','Mumbai'])[1 + floor(random() * 6)::int],
           'zip',   lpad((floor(random() * 99999))::int::text, 5, '0'),
           'geo',   jsonb_build_object('lat', round((random() * 60)::numeric, 4), 'lon', round((random() * 120)::numeric, 4))),
       CASE WHEN random() < 0.30 THEN NULL
            WHEN random() < 0.15 THEN '{}'::text[]
            WHEN random() < 0.50 THEN ARRAY['mobile', 'paperless']
            ELSE ARRAY['branch-visitor'] END,
       CASE g % 7
            WHEN 0 THEN NULL
            WHEN 1 THEN 'Prefers email, not phone'
            WHEN 2 THEN 'Said "call after 5pm"'
            WHEN 3 THEN E'Line one\nLine two'
            WHEN 4 THEN E'Tab\tseparated\tnote'
            WHEN 5 THEN 'Unicode: 北京 / São Paulo'
            ELSE '' END,
       now() - random() * interval '365 days'
FROM generate_series(1, 10000) g;

-- ------------------------------------------------------------------ accounts
WITH base AS (
    SELECT g,
           (ARRAY['ACTIVE','ACTIVE','ACTIVE','ACTIVE','DORMANT','CLOSED'])[1 + floor(random() * 6)::int] AS status,
           (ARRAY['SAVINGS','CURRENT','CURRENT','LOAN','CARD'])[1 + floor(random() * 5)::int]           AS account_type,
           DATE '2012-01-01' + floor(random() * 4500)::int                                              AS open_date
    FROM generate_series(1, 15000) g
)
INSERT INTO retail.accounts
    (account_no, customer_id, account_type, currency, branch_code, balance, credit_limit,
     status, open_date, close_date, updated_at)
SELECT 'AC' || lpad(g::text, 12, '0'),
       1 + floor(random() * 10000)::bigint,
       account_type,
       (ARRAY['USD','USD','USD','EUR','GBP','INR','AED','SGD','CAD','AUD'])[1 + floor(random() * 10)::int],
       'BR' || lpad((1 + floor(random() * 40))::int::text, 3, '0'),
       CASE WHEN random() < 0.05 THEN -round((random() * 5000)::numeric, 2)
            ELSE round((random() * 90000)::numeric, 2) END,
       CASE WHEN account_type = 'CARD' THEN round((1000 + random() * 20000)::numeric, 2) END,
       status,
       open_date,
       CASE WHEN status = 'CLOSED' THEN LEAST(open_date + floor(random() * 1500)::int, CURRENT_DATE) END,
       now() - random() * interval '90 days'
FROM base;

-- ------------------------------------------------------------------ account_holders (composite PK)
INSERT INTO retail.account_holders (account_id, customer_id, holder_role, share_pct, since_date)
SELECT account_id, customer_id, 'PRIMARY', 100.00, open_date
FROM retail.accounts;

INSERT INTO retail.account_holders (account_id, customer_id, holder_role, share_pct, since_date)
SELECT a.account_id, a.joint_id, 'JOINT', 50.00, a.open_date
FROM (SELECT account_id, customer_id, open_date, 1 + floor(random() * 10000)::bigint AS joint_id
      FROM retail.accounts
      WHERE random() < 0.20) a
WHERE a.joint_id <> a.customer_id
ON CONFLICT DO NOTHING;

UPDATE retail.account_holders
SET share_pct = 50.00
WHERE holder_role = 'PRIMARY'
  AND account_id IN (SELECT account_id FROM retail.account_holders WHERE holder_role = 'JOINT');

-- ------------------------------------------------------------------ transactions (200k)
INSERT INTO retail.transactions
    (account_id, txn_ts, txn_type, amount, status, channel, counterparty, memo, updated_at)
SELECT s.account_id,
       s.txn_ts,
       s.txn_type,
       CASE WHEN random() < 0.001 THEN round((250000 + random() * 250000)::numeric, 2)   -- outliers
            WHEN random() < 0.01  THEN 0.00                                              -- zero amounts
            ELSE round((random() * 2000)::numeric, 2) END,
       (ARRAY['POSTED','POSTED','POSTED','POSTED','POSTED','PENDING','REVERSED','FAILED'])[1 + floor(random() * 8)::int],
       CASE WHEN random() < 0.10 THEN NULL
            ELSE (ARRAY['BRANCH','ATM','ONLINE','MOBILE'])[1 + floor(random() * 4)::int] END,
       CASE WHEN s.txn_type IN ('CASH_DEP', 'CASH_WDL') THEN NULL
            ELSE 'REF' || lpad((floor(random() * 999999999))::bigint::text, 9, '0') END,
       CASE (floor(random() * 8))::int
            WHEN 0 THEN NULL
            WHEN 1 THEN 'Invoice 1042, March'
            WHEN 2 THEN 'Rent "May" payment'
            WHEN 3 THEN E'Split bill\nsecond line'
            WHEN 4 THEN E'Ref\tTAB\tseparated'
            WHEN 5 THEN 'Café ☕ Zürich'
            WHEN 6 THEN ''
            ELSE 'ordinary memo' END,
       s.txn_ts
FROM (SELECT 1 + floor(random() * 15000)::bigint AS account_id,
             now()::timestamp - random() * interval '60 days' AS txn_ts,
             (ARRAY['CASH_DEP','CASH_WDL','TRANSFER','TRANSFER','CARD','CARD','CHEQUE'])[1 + floor(random() * 7)::int] AS txn_type
      FROM generate_series(1, 200000)) s;

-- ------------------------------------------------------------------ fx_rates_log (no PK, duplicates)
INSERT INTO retail.fx_rates_log (rate_date, currency, usd_rate, source)
SELECT CURRENT_DATE - d,
       c.ccy,
       round((c.base * (1 + (random() - 0.5) * 0.02))::numeric, 6),
       (ARRAY['ECB','REUTERS','BLOOMBERG'])[1 + floor(random() * 3)::int]
FROM generate_series(0, 89) d,
     (VALUES ('USD', 1.0), ('EUR', 1.08), ('GBP', 1.27), ('INR', 0.012), ('PKR', 0.0036),
             ('AED', 0.2723), ('SGD', 0.74), ('CAD', 0.73), ('AUD', 0.66), ('JPY', 0.0067)) AS c(ccy, base);

-- ~10% of rows loaded twice (duplicate natural keys, slightly different rate/source)
INSERT INTO retail.fx_rates_log (rate_date, currency, usd_rate, source)
SELECT rate_date, currency, round((usd_rate * 1.0005)::numeric, 6), 'REUTERS'
FROM retail.fx_rates_log
WHERE random() < 0.10;

ANALYZE;
