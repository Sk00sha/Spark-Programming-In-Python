from pyspark.sql import SparkSession
from pyspark.sql.functions import col, monotonically_increasing_id, when, expr, regexp_replace
from pyspark.sql.types import *

from lib.logger import Log4j

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Misc Demo") \
        .master("local[2]") \
        .getOrCreate()

    logger = Log4j(spark)

    data_list = [("Ravi", "28", "NAN"),
                 ("Abdul", "23", "NAN"),  # 1981
                 ("John", "12", "NAN"),  # 2006
                 ("Rosy", "7", "NAN"),  # 1963
                 ("Abdul", "23", "NAN")]  # 1981

    raw_df = spark.createDataFrame(data_list).toDF("name", "age", "AgeRestriction").repartition(3)
    raw_df.printSchema()

    final_df = raw_df.withColumn("id", monotonically_increasing_id()) \
        .withColumn("day", col("age").cast(IntegerType())) \
        .withColumn("dob", 2023 - col("age")) \
        .sort(col("dob").desc())

    final_df = final_df.withColumn("AgeRestriction", when(col("age") < 18, regexp_replace("AgeRestriction", "NAN", "Underage"))
                                   .otherwise(regexp_replace("AgeRestriction", "NAN", "Adult"))).drop("age")

    final_df.show(5)
