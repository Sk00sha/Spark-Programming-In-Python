from pyspark.sql import SparkSession
from pyspark.sql import functions as f

from lib.logger import Log4j

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Agg Demo") \
        .master("local[2]") \
        .getOrCreate()

    logger = Log4j(spark)

    def_df = spark.read \
        .format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load("data/invoices.csv")

    def_df.select(f.count("*").alias("Count of all"),
                  f.avg("UnitPrice").alias("Average unit price"),
                  f.countDistinct("InvoiceNo").alias("Count distinct invoices")) \
        .show()

    def_df.groupBy("Country") \
        .agg(f.count("*").alias("NumInvoices")
             , f.sum("Quantity").alias("TotalQuantity")
             , f.sum("UnitPrice").alias("InvoiceValue")
             ).dropDuplicates().show()

    #def_df.createOrReplaceTempView("invoice_table")

    #spark.sql("SELECT StockCode,COUNT(*) as `Count of stock codes`,AVG(UnitPrice) as `Average unit Price` FROM "
    #          "invoice_table GROUP BY StockCode").show()
