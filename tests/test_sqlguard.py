"""Cluster-mode write protection (agentic/sqlguard.py). Pure Python."""
import unittest

from agentic.sqlguard import (assert_scratch_table, check_sql, rewrite_names, scan_cluster_safety,
                              split_statements)

S = "runcheck_tmp_1"
OK = ["SELECT * FROM curated.transactions", "WITH a AS (SELECT 1) SELECT * FROM a", "-- c\nSELECT 1", "SET spark.sql.x=1",
      f"INSERT OVERWRITE TABLE {S}.t PARTITION (d='1') SELECT 1", f"INSERT INTO `{S}`.`t` SELECT 1",
      f"CREATE TABLE {S}.t AS SELECT 1", "CREATE OR REPLACE TEMPORARY VIEW v AS SELECT 1",
      f"DROP TABLE IF EXISTS {S}.t", f"CREATE DATABASE IF NOT EXISTS {S}", f"DROP DATABASE {S} CASCADE",
      f"ANALYZE TABLE {S}.t COMPUTE STATISTICS", "SELECT 1;\nSELECT 2;", "SELECT ';' AS s, 'a;b' FROM curated.t"]
BAD = ["INSERT OVERWRITE TABLE risk.customer_daily_risk SELECT 1", "INSERT INTO t SELECT 1", "DROP TABLE curated.accounts",
       "DROP DATABASE risk CASCADE", "TRUNCATE TABLE risk.customer_daily_risk",
       "ALTER TABLE curated.customers ADD COLUMNS (x INT)", "INSERT OVERWRITE DIRECTORY '/x' SELECT 1",
       f"CREATE EXTERNAL TABLE {S}.t (a INT) LOCATION '/data/real'",
       "MERGE INTO a.b USING c ON 1=1 WHEN MATCHED THEN DELETE", "DELETE FROM curated.transactions",
       "ANALYZE TABLE curated.accounts COMPUTE STATISTICS", "CREATE DATABASE other", "GRANT ALL ON x TO y",
       "SELECT 1; DROP TABLE curated.accounts", "-- sneaky\nDROP TABLE curated.accounts",
       "LOAD DATA INPATH '/x' INTO TABLE curated.accounts", "insert overwrite table RISK.customer_daily_risk select 1"]


class SqlGuard(unittest.TestCase):
    def test_allowed(self):
        for s in OK:
            with self.subTest(s=s):
                check_sql(s, S)

    def test_blocked(self):
        for s in BAD:
            with self.subTest(s=s):
                with self.assertRaises(PermissionError):
                    check_sql(s, S)

    def test_scratch_table_rule(self):
        assert_scratch_table(f"{S.upper()}.t", S)
        for t in ("risk.customer_daily_risk", "customer_daily_risk"):
            with self.assertRaises(PermissionError):
                assert_scratch_table(t, S)

    def test_rewrite_whole_identifiers_only(self):
        m = {"curated.transactions": "real.transactions"}
        self.assertEqual(rewrite_names("SELECT * FROM CURATED.TRANSACTIONS t JOIN curated.transactions_x x", m),
                         "SELECT * FROM real.transactions t JOIN curated.transactions_x x")

    def test_split_respects_quotes(self):
        self.assertEqual(len(split_statements("SELECT ';'; SELECT 2")), 2)

    def test_apostrophe_in_a_comment_cannot_hide_a_following_statement(self):
        for sql in ("-- it's fine\nSELECT 1; DROP TABLE curated.accounts",
                    "/* don't */ SELECT 1; DROP TABLE curated.accounts",
                    "SELECT 1 -- the job's table\n; DROP TABLE curated.accounts"):
            with self.subTest(sql=sql):
                self.assertEqual(len([s for s in split_statements(sql) if s.strip()]), 2)
                with self.assertRaises(PermissionError):
                    check_sql(sql, S)

    def test_comment_markers_inside_strings_are_kept(self):
        self.assertEqual(len(split_statements("SELECT '-- not a comment'; SELECT '/* nor this */'")), 2)
        check_sql("SELECT '-- x' FROM curated.t", S)

    def test_code_scan(self):
        bad = scan_cluster_safety("import subprocess\nspark._jvm.x()\ndf.write.jdbc(u,t)\nimport os\nos.system('x')")
        self.assertGreaterEqual(len(bad), 4)
        self.assertEqual(scan_cluster_safety("df = spark.table('a.b')\ndf.write.insertInto('c.d')"), [])


if __name__ == "__main__":
    unittest.main()
