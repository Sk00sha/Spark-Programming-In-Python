import re
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

from lib.logger import Log4j


def generate_only_canada(country):
    if country == "Canada":
        return "YeesCanada"
    return "NooCanada"


if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("UDF Demo") \
        .master("local[2]") \
        .getOrCreate()

    logger = Log4j(spark)

    survey_df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv("data/survey.csv")
    registered_udf = udf(generate_only_canada, StringType())
    survey_df = survey_df.withColumn("Country", registered_udf("Country"))

    survey_df.show(10)
