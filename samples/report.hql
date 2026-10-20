SET hivevar:dt=2026-01-01;
INSERT OVERWRITE TABLE sales_summary
SELECT /*+ MAPJOIN(c) */ c.region, SUM(o.amount) AS total
FROM orders o JOIN customers c ON o.cust_id = c.id
WHERE o.order_date >= '${hivevar:dt}'
GROUP BY c.region;
