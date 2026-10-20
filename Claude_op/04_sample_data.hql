-- 04_sample_data.hql (test data only; complex types need INSERT ... SELECT)

INSERT INTO curated.customers
SELECT 'C001','Asha Rao','RETAIL',
       array(named_struct('type','HOME','line1','12 MG Road','city','Pune','country','IN','is_primary',true),
             named_struct('type','WORK','line1','1 Canary Wharf','city','London','country','GB','is_primary',false)),
       'VERIFIED', false, CAST('2019-03-12' AS DATE), current_timestamp()
UNION ALL
SELECT 'C002','Globex Ltd','CORPORATE',
       array(named_struct('type','WORK','line1','5 King St','city','Manchester','country','GB','is_primary',true)),
       'VERIFIED', false, CAST('2017-07-01' AS DATE), current_timestamp();

INSERT INTO curated.accounts
SELECT 'A1001','C001','SAVINGS','INR','BR01',
       map('product_code','SAV-PLUS','overdraft_limit','0','joint_holder','N','interest_tier','T2'),
       'ACTIVE', CAST('2019-03-12' AS DATE), CAST(NULL AS DATE), current_timestamp()
UNION ALL
SELECT 'A2001','C002','CURRENT','GBP','BR07',
       map('product_code','BIZ-CUR','overdraft_limit','50000','joint_holder','Y'),
       'ACTIVE', CAST('2017-07-01' AS DATE), CAST(NULL AS DATE), current_timestamp();

INSERT INTO curated.fx_rates PARTITION (rate_date='2026-10-18') VALUES
 ('INR',0.012000,'RBI'), ('GBP',1.310000,'ECB'), ('USD',1.000000,'FED');

INSERT INTO ref.high_risk_countries VALUES
 ('IR','VERY_HIGH','FATF'), ('KP','VERY_HIGH','FATF'), ('MM','HIGH','FATF');

-- Transactions for run_date 2026-10-18: C001 structuring (3 near-threshold cash deposits) + one high-risk transfer
INSERT INTO curated.transactions PARTITION (txn_date='2026-10-18')
SELECT 'T1','A1001','CASH_DEP',CAST(700000.00 AS DECIMAL(18,2)),'INR','POSTED','BRANCH',
       named_struct('name','Self','bank_bic','HDFCINBB','account_no','A1001','country','IN'),
       CAST('2026-10-18 09:10:00' AS TIMESTAMP)
UNION ALL
SELECT 'T2','A1001','CASH_DEP',CAST(720000.00 AS DECIMAL(18,2)),'INR','POSTED','BRANCH',
       named_struct('name','Self','bank_bic','HDFCINBB','account_no','A1001','country','IN'),
       CAST('2026-10-18 11:30:00' AS TIMESTAMP)
UNION ALL
SELECT 'T3','A1001','CASH_DEP',CAST(750000.00 AS DECIMAL(18,2)),'INR','POSTED','ATM',
       named_struct('name','Self','bank_bic','HDFCINBB','account_no','A1001','country','IN'),
       CAST('2026-10-18 15:45:00' AS TIMESTAMP)
UNION ALL
SELECT 'T4','A2001','TRANSFER',CAST(15000.00 AS DECIMAL(18,2)),'GBP','POSTED','ONLINE',
       named_struct('name','Persian Trade Co','bank_bic','BMJIIRTH','account_no','IR123456','country','IR'),
       CAST('2026-10-18 13:00:00' AS TIMESTAMP);
