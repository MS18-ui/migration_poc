-- 11_feature_customer_profile.hql : profile features from nested ARRAY<STRUCT> addresses
SET hivevar:run_date=2026-10-18;
USE aahhd01q_pnc_poc;

SELECT c.customer_id, c.segment, c.kyc_status, c.pep_flag,
       datediff('${hivevar:run_date}', c.onboard_date)          AS tenure_days,
       COUNT(x.addr.country)                                     AS address_cnt,
       COUNT(DISTINCT x.addr.country)                            AS address_country_cnt,
       MAX(CASE WHEN x.addr.is_primary THEN x.addr.country END)  AS residence_country
FROM customers c
LATERAL VIEW OUTER explode(c.addresses) x AS addr
GROUP BY c.customer_id, c.segment, c.kyc_status, c.pep_flag, c.onboard_date;
