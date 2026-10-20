from pyspark.sql import SparkSession


def get_spark(app_name="Bigdata-POC"):
    spark = (
        SparkSession.builder
        .appName(app_name)
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
    # runtime-settable options (also apply when a session already exists)
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    return spark
