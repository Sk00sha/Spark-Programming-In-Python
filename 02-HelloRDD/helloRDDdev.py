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

    sc = spark.sparkContext

    rdd = sc.textFile(sys.argv[1])

    repartition = rdd.repartition(2)

    cols = repartition.map(lambda line: line.replace('"', '').split(","))

    selectedRdd = cols.map(lambda columns: SurveyRecord(int(columns[1]), int(columns[2]), int(columns[3]), int(columns[4])))

    