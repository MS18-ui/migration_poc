# PySpark 3.x - populate source tables of the banking risk use case (15K rows each)
# Target DB: aahhd01q_pnc_poc  (tables created by create_tables_spark_sql.py)
from pyspark.sql import functions as F
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("populate_txn_risk_tables")
    .config("spark.sql.catalogImplementation", "hive")
    .master("spark://spark-master:7077")
    .config("spark.hadoop.fs.defaultFS", "hdfs://172.16.0.4:9000")
    .config("spark.sql.warehouse.dir", "hdfs://172.16.0.4:9000/user/hive/warehouse")
    .config("hive.metastore.uris", "thrift://172.16.0.4:9083")
    .config("spark.sql.hive.metastore.version", "4.1.0")
    .config("spark.sql.hive.metastore.jars", "maven")
    .enableHiveSupport()
    .getOrCreate()
)
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
spark.conf.set("hive.exec.dynamic.partition", "true")
spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")

DB = "aahhd01q_pnc_poc"
N = 15000
RUN_DATE = "2026-10-18"
N_STRUCT_ACCTS = 100                 # accounts seeded with structuring pattern (3 txns each)
N_TXN_RANDOM = N - 3 * N_STRUCT_ACCTS


def pick(values, seed):
    """Random element of a python list (repeat values to weight them)."""
    arr = F.array(*[F.lit(v) for v in values])
    return arr.getItem((F.rand(seed) * len(values)).cast("int"))


def cust_id(i):  return F.concat(F.lit("C"), F.lpad(i.cast("string"), 6, "0"))
def acct_id(i):  return F.concat(F.lit("A"), F.lpad(i.cast("string"), 7, "0"))


COUNTRIES = ["IN"] * 6 + ["US"] * 4 + ["GB"] * 3 + ["DE", "SG", "AE", "CA", "AU", "FR", "CH"]
HIGH_RISK = ["IR", "KP", "MM", "SY"]
CURRENCIES = ["USD", "EUR", "GBP", "INR", "JPY", "CAD", "AUD", "CHF",
              "SGD", "AED", "CNY", "MXN", "BRL", "ZAR", "HKD"]
BASE_RATE = {"USD": 1.0, "EUR": 1.08, "GBP": 1.31, "INR": 0.012, "JPY": 0.0067,
             "CAD": 0.73, "AUD": 0.65, "CHF": 1.12, "SGD": 0.74, "AED": 0.272,
             "CNY": 0.138, "MXN": 0.055, "BRL": 0.19, "ZAR": 0.054, "HKD": 0.128}

# ------------------------------------------------------------------
# 1. customers  (addresses ARRAY<STRUCT>)
# ------------------------------------------------------------------
def addr(i, type_, primary):
    return F.struct(F.lit(type_).alias("type"),
                    F.concat(F.lit("Street "), (F.rand(100 + i) * 500).cast("int").cast("string")).alias("line1"),
                    pick(["Pune", "Mumbai", "London", "New York", "Berlin", "Singapore", "Dubai"], 110 + i).alias("city"),
                    pick(COUNTRIES, 120 + i).alias("country"),
                    F.lit(primary).alias("is_primary"))

customers = (spark.range(1, N + 1).withColumnRenamed("id", "n")
    .withColumn("customer_id", cust_id(F.col("n")))
    .withColumn("full_name", F.concat(
        pick(["Asha", "Ravi", "John", "Maria", "Chen", "Fatima", "Liam", "Olga", "Sanjay", "Emma"], 1), F.lit(" "),
        pick(["Rao", "Smith", "Khan", "Garcia", "Wang", "Patel", "Brown", "Ivanov", "Singh", "Jones"], 2)))
    .withColumn("segment", pick(["RETAIL"] * 14 + ["SME"] * 3 + ["CORPORATE"] * 2 + ["PRIVATE"], 3))
    .withColumn("addr_all", F.array(addr(1, "HOME", True), addr(2, "WORK", False), addr(3, "MAILING", False)))
    .withColumn("n_addr", (F.lit(1) + (F.rand(4) * 3).cast("int")))
    .withColumn("addresses", F.expr("slice(addr_all, 1, n_addr)"))
    .withColumn("kyc_status", pick(["VERIFIED"] * 18 + ["PENDING", "EXPIRED"], 5))
    .withColumn("pep_flag", F.rand(6) < 0.01)
    .withColumn("onboard_date", F.expr("date_sub(to_date('%s'), cast(rand(7) * 3650 as int))" % RUN_DATE))
    .withColumn("last_updated_ts", F.current_timestamp())
    .select("customer_id", "full_name", "segment", "addresses", "kyc_status",
            "pep_flag", "onboard_date", "last_updated_ts"))

customers.write.mode("overwrite").insertInto("%s.customers" % DB)

# ------------------------------------------------------------------
# 2. accounts  (attributes MAP<STRING,STRING>)
# ------------------------------------------------------------------
accounts = (spark.range(1, N + 1).withColumnRenamed("id", "n")
    .withColumn("account_id", acct_id(F.col("n")))
    .withColumn("customer_id", cust_id((F.rand(11) * N).cast("int") + 1))
    .withColumn("account_type", pick(["SAVINGS"] * 5 + ["CURRENT"] * 3 + ["LOAN", "CARD"], 12))
    .withColumn("currency", pick(["USD", "INR", "GBP", "EUR"], 13))
    .withColumn("branch_code", F.concat(F.lit("BR"), F.lpad(((F.rand(14) * 50).cast("int") + 1).cast("string"), 2, "0")))
    .withColumn("attributes", F.create_map(
        F.lit("product_code"),    pick(["SAV-PLUS", "BIZ-CUR", "PRM-CARD", "HOME-LN", "BASIC"], 15),
        F.lit("overdraft_limit"), pick(["0", "0", "5000", "25000", "50000"], 16),
        F.lit("joint_holder"),    pick(["N"] * 4 + ["Y"], 17),
        F.lit("interest_tier"),   pick(["T1", "T2", "T3"], 18)))
    .withColumn("status", pick(["ACTIVE"] * 17 + ["DORMANT"] * 2 + ["CLOSED"], 19))
    .withColumn("open_date", F.expr("date_sub(to_date('%s'), cast(rand(20) * 3650 as int))" % RUN_DATE))
    .withColumn("close_date", F.when(F.col("status") == "CLOSED",
                                     F.expr("date_sub(to_date('%s'), cast(rand(21) * 365 as int))" % RUN_DATE))
                               .cast("date"))
    .withColumn("last_updated_ts", F.current_timestamp())
    .select("account_id", "customer_id", "account_type", "currency", "branch_code",
            "attributes", "status", "open_date", "close_date", "last_updated_ts"))

accounts.write.mode("overwrite").insertInto("%s.accounts" % DB)

# ------------------------------------------------------------------
# 3. transactions  (counterparty STRUCT, partitioned by txn_date)
# ------------------------------------------------------------------
def counterparty():
    country = F.when(F.rand(31) < 0.05, pick(HIGH_RISK, 32)).otherwise(pick(COUNTRIES, 33))
    return F.struct(
        F.concat(pick(["Acme", "Globex", "Initech", "Umbrella", "Stark", "Wayne", "Hooli"], 34),
                 F.lit(" "), pick(["Trading", "Logistics", "Holdings", "Services", "Exports"], 35)).alias("name"),
        pick(["HDFCINBB", "BARCGB22", "DEUTDEFF", "CITIUS33", "DBSSSGSG", "EBILAEAD"], 36).alias("bank_bic"),
        F.concat(F.lit("AC"), (F.rand(37) * 1e9).cast("long").cast("string")).alias("account_no"),
        country.alias("country"))

random_txn = (spark.range(1, N_TXN_RANDOM + 1).withColumnRenamed("id", "n")
    .withColumn("txn_id", F.concat(F.lit("T"), F.lpad(F.col("n").cast("string"), 8, "0")))
    .withColumn("account_id", acct_id((F.rand(41) * N).cast("int") + 1))
    .withColumn("txn_type", pick(["TRANSFER"] * 4 + ["CARD"] * 3 + ["CASH_DEP"] * 2 + ["CASH_WDL", "CHEQUE"], 42))
    .withColumn("amount", F.round(F.exp(F.lit(3.0) + F.rand(43) * 7.0), 2).cast("decimal(18,2)"))
    .withColumn("currency", pick(["USD"] * 4 + ["EUR", "GBP", "INR", "INR"], 44))
    .withColumn("status", pick(["POSTED"] * 18 + ["PENDING", "REVERSED", "FAILED"], 45))
    .withColumn("channel", pick(["BRANCH", "ATM", "ONLINE", "ONLINE", "MOBILE", "MOBILE"], 46))
    .withColumn("counterparty", counterparty())
    .withColumn("txn_date", F.expr("date_add(to_date('2026-10-14'), least(4, cast(rand(47) * 7 as int)))"))
    .withColumn("txn_ts",
                (F.col("txn_date").cast("timestamp").cast("long")
                 + (F.rand(48) * 86399).cast("int")).cast("timestamp"))
    .select("txn_id", "account_id", "txn_type", "amount", "currency", "status",
            "channel", "counterparty", "txn_ts", "txn_date"))

# Seeded structuring pattern: 3 near-threshold cash deposits on RUN_DATE for 100 accounts
struct_accts = (accounts.select("account_id").orderBy(F.rand(51)).limit(N_STRUCT_ACCTS)
                .withColumn("k", F.monotonically_increasing_id()))
structuring_txn = (struct_accts
    .withColumn("seq", F.explode(F.array(F.lit(1), F.lit(2), F.lit(3))))
    .withColumn("txn_id", F.concat(F.lit("TS"), F.lpad(F.col("k").cast("string"), 5, "0"), F.col("seq").cast("string")))
    .withColumn("txn_type", F.lit("CASH_DEP"))
    .withColumn("amount", F.round(F.lit(8000) + F.rand(52) * 1999.99, 2).cast("decimal(18,2)"))
    .withColumn("currency", F.lit("USD"))
    .withColumn("status", F.lit("POSTED"))
    .withColumn("channel", F.lit("BRANCH"))
    .withColumn("counterparty", F.struct(F.lit("Self").alias("name"), F.lit("HDFCINBB").alias("bank_bic"),
                                         F.col("account_id").alias("account_no"), F.lit("IN").alias("country")))
    .withColumn("txn_date", F.to_date(F.lit(RUN_DATE)))
    .withColumn("txn_ts", F.expr("cast(concat('%s ', lpad(cast(8 + seq * 3 as string), 2, '0'), ':00:00') as timestamp)" % RUN_DATE))
    .select("txn_id", "account_id", "txn_type", "amount", "currency", "status",
            "channel", "counterparty", "txn_ts", "txn_date"))

random_txn.unionByName(structuring_txn).write.mode("overwrite").insertInto("%s.transactions" % DB)

# ------------------------------------------------------------------
# 4. fx_rates : 15 currencies x 1000 days = 15,000 rows, partitioned by rate_date
# ------------------------------------------------------------------
base_map = F.create_map(*[x for c in CURRENCIES for x in (F.lit(c), F.lit(BASE_RATE[c]))])
# NOTE: built with explode(array(...)) instead of createDataFrame(list) so no Python
# worker is needed (avoids driver/worker Python version mismatch)
fx = (spark.range(1).select(F.explode(F.array(*[F.lit(c) for c in CURRENCIES])).alias("currency"))
      .crossJoin(spark.range(0, 1000).withColumnRenamed("id", "d"))
      .withColumn("rate_date", F.expr("date_sub(to_date('%s'), cast(d as int))" % RUN_DATE))
      .withColumn("usd_rate", F.when(F.col("currency") == "USD", F.lit(1.0))
                  .otherwise(base_map.getItem(F.col("currency")) * (F.lit(1.0) + (F.rand(61) - 0.5) * 0.02))
                  .cast("decimal(18,6)"))
      .withColumn("source", pick(["ECB", "RBI", "FED"], 62))
      .select("currency", "usd_rate", "source", "rate_date"))

fx.write.mode("overwrite").insertInto("%s.fx_rates" % DB)
spark.sql("MSCK REPAIR TABLE %s.fx_rates" % DB)

# ------------------------------------------------------------------
# 5. high_risk_countries : small reference list (not 15K - a real list is tiny)
# ------------------------------------------------------------------
(spark.sql("""
    SELECT * FROM VALUES
      ('IR','VERY_HIGH','FATF'), ('KP','VERY_HIGH','FATF'), ('MM','HIGH','FATF'),
      ('SY','HIGH','internal'), ('AF','HIGH','FATF'), ('YE','HIGH','internal'),
      ('LY','HIGH','internal'), ('SO','HIGH','internal')
    AS t(country, risk_level, list_source)
""").write.mode("overwrite").insertInto("%s.high_risk_countries" % DB))

# ------------------------------------------------------------------
# Verification
# ------------------------------------------------------------------
for t in ["customers", "accounts", "transactions", "fx_rates", "high_risk_countries"]:
    print(t, spark.table("%s.%s" % (DB, t)).count())
print("txns on run_date:", spark.table("%s.transactions" % DB).filter(F.col("txn_date") == RUN_DATE).count())
