from pyspark.sql import *
from pyspark.sql.functions import regexp_extract, substring_index

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .master("local[3]") \
        .appName("LogFileDemo") \
        .getOrCreate()

    logs_df = spark.read.option("header", "false").text("data/apache_logs.txt")
    log_reg = r'^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+) (\S+)" (\d{3}) (\S+) "(\S+)" "([^"]*)'

    logs_transform = logs_df.select(
        regexp_extract('value', log_reg, 1).alias("ip"),
        regexp_extract('value', log_reg, 4).alias("date"),
        regexp_extract('value', log_reg, 6).alias("request"),
        regexp_extract('value', log_reg, 6).alias("referrer")
               )
    logs_transform.show(5)
