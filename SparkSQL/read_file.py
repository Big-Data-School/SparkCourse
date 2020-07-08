import pyspark
from pyspark import SparkContext
sc = SparkContext.getOrCreate()
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("dataframe").getOrCreate()

df = spark.read.format("csv")\
    .option("header", "true")\
    .option("inferschema", "true")\
    .load("data/retail-data/all/*.csv")\
    .coalesce(5)

df.cache()

df.createOrReplaceTempView("dfTable")

df.count()

df.columns

df.printSchema()

from pyspark.sql.functions import count
from pyspark.sql.functions import countDistinct
from pyspark.sql.functions import first, last
from pyspark.sql.functions import min, max
from pyspark.sql.functions import sum
from pyspark.sql.functions import sumDistinct

df.select(sum("Quantity")).show()

df.select(sumDistinct("Quantity")).show()

df.select(min("Quantity"), max("Quantity")).show()

df.select(count("StockCode")).show()
df.select(countDistinct("StockCode")).show()

df.select(first("StockCode"), last("StockCode")).show()

df.groupBy("InvoiceNo", "CustomerId").count().show(5)

# select count(*) from dfTable group by InvoiceNo, CustomerId

from pyspark.sql.functions import count, expr

df.groupBy("InvoiceNo").agg(
    count("Quantity").alias("quan"),
    expr("count(Quantity)")).show(5)


###
from pyspark.sql.functions import col, to_date
dfWithDate = df.withColumn("date", to_date(col("InvoiceDate"), "MM/d/yyyy H:mm"))
dfWithDate.createOrReplaceTempView("dfWithDate")

from pyspark.sql.window import Window
from pyspark.sql.functions import desc

windowSpec = Window\
.partitionBy("CustomerId", "date")\
.orderBy(desc("Quantity"))\
.rowsBetween(Window.unboundedPreceding, Window.currentRow)

from pyspark.sql.functions import max
maxPurchaseQuantity = max(col("Quantity")).over(windowSpec)

from pyspark.sql.functions import dense_rank, rank
purchaseDenseRank = dense_rank().over(windowSpec)
purchaseRank = rank().over(windowSpec)

from pyspark.sql.functions import col
dfWithDate.where("CustomerId IS NOT NULL").orderBy("CustomerId")\
.select(
col("CustomerId"),
col("date"),
col("Quantity"),
purchaseRank.alias("quantityRank"),
purchaseDenseRank.alias("quantityDenseRank"),
maxPurchaseQuantity.alias("maxPurchaseQuantity")).show(10)