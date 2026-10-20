from pyspark import SparkContext
from pyspark.sql import SQLContext, HiveContext
from pyspark.sql.functions import to_date

sc = SparkContext(appName="legacy_sales")
sqlContext = HiveContext(sc)
df = sqlContext.read.parquet("s3://bucket/raw/")
df = df.withColumn("tx_date", to_date(df.tx_date, "yyyy-MM-dd"))
df.registerTempTable("sales")
print "rows:", df.count()
df.write.mode("overwrite").parquet("s3://bucket/clean/")
