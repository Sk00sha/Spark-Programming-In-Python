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

    invoice_df = spark.read \
        .format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load("data/invoices.csv")
    numInvoices = f.countDistinct("InvoiceNo").alias("NumInvoices")
    totalQuantity = f.sum("UnitPrice").alias("Sum of unit price")
    invoiceValue = f.expr("round(sum(Quantity * UnitPrice),2) as InvoiceValue")

    invoice_df\
        .withColumn("InvoiceDate", f.to_date(f.col("InvoiceDate"), "dd-MM-yyyy H.mm")) \
        .where("year(InvoiceDate) == 2010") \
        .withColumn("WeekNumber", f.weekofyear(f.col("InvoiceDate"))) \
        .groupBy("Country", "WeekNumber")\
        .agg(numInvoices, totalQuantity, invoiceValue).show()

    invoice_df.write\
        .format("parquet")\
        .save("outdata/file.parquet")

