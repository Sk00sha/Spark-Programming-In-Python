from pyspark.sql import *
from pyspark.sql.functions import spark_partition_id

from lib.logger import Log4j

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .master("local[3]") \
        .appName("SparkSchemaDemo") \
        .getOrCreate()

    logger = Log4j(spark)

    flightTime = spark.read \
        .format("parquet") \
        .load("dataSource/flight*.parquet")

    print(flightTime.rdd.getNumPartitions())
    flightTime = flightTime.repartition(5)

    flightTime.write\
        .format("json").mode("overwrite")\
        .option("path", "dataSink/json/")\
        .partitionBy("OP_CARRIER", "ORIGIN")\
        .save()
