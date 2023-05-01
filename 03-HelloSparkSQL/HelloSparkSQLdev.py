import sys
from collections import namedtuple

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

SurveyRecord = namedtuple("SurveyRecord", ["Age", "Gender", "Country", "State"])

if __name__ == "__main__":
    conf = SparkConf() \
        .setMaster("local[3]") \
        .setAppName("Hello RDD")

    spark = SparkSession \
        .builder \
        .config(conf=conf) \
        .getOrCreate()

    surveydata = spark.read\
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv("data/sample.csv")

    surveydata.createOrReplaceTempView("survey_tbl")

    counts = spark.sql("select Country, count(1) as count from survey_tbl where Age<40 group by Country")

    counts.show()
