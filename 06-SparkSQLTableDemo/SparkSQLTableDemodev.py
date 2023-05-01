from pyspark.sql import *

from lib.logger import Log4j

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .master("local[3]") \
        .appName("SparkSQLTableDemo") \
        .enableHiveSupport() \
        .getOrCreate()

    logger = Log4j(spark)

    flight_rdd = spark.read \
        .format("parquet") \
        .load("dataSource/flight-time.parquet")
    spark.sql("CREATE DATABASE IF NOT EXISTS AIRLINES_DB")
    spark.catalog.setCurrentDatabase("AIRLINES_DB")

    flight_rdd.write \
        .format("csv") \
        .mode("overwrite") \
        .bucketBy(5, "ORIGIN") \
        .saveAsTable("flight_table")

    print(spark.catalog.listTables("AIRLINES_DB"))
