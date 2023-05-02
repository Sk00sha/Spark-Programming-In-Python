from pyspark.sql import *
from pyspark.sql.functions import regexp_extract, substring_index, expr

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .master("local[3]") \
        .appName("LogFileDemo") \
        .getOrCreate()

    flight = spark.read \
        .format("parquet") \
        .load("dataSource/flight*.parquet")

    selected_rdd = flight.select("OP_CARRIER", "FL_DATE", "ORIGIN")
    #column expression
    new_rdd = selected_rdd.filter(selected_rdd.OP_CARRIER == "DL")
    #sql syntax
    another_rdd = selected_rdd.selectExpr("OP_CARRIER", "OP_CARRIER == DL")
    print(another_rdd)
