import pyspark
from pyspark import SparkContext #SC
sc = SparkContext.getOrCreate() #place SparkContext into a Variable
from pyspark.sql import SparkSession #place SparkSession into a Variable
spark = SparkSession.builder.appName("readData").getOrCreate()

#from pyspark.sql.functions import max, min, avg, asc, desc, window, column, col

flight = spark\
    .read\
    .option("inferSchema","true")\
    .option("header","true")\
    .csv("data/flight-data/csv/2015-summary.csv")

flight.take(3)
flight.sort("count").explain()

flight.createOrReplaceTempView("flight_tbl")

sql = spark.sql("""select DEST_COUNTRY_NAME,count(1)
from flight_tbl group by DEST_COUNTRY_NAME""")

df_way = flight.groupBy("DEST_COUNTRY_NAME").count()

sql.explain()
df_way.explain()


flight.select(max("Count")).take(1)
flight.select(min("Count")).take(1)
flight.select(avg("Count")).take(1)

maxSal = spark.sql("""
select DEST_COUNTRY_NAME, sum(count) as total
from flight_tbl
group by DEST_COUNTRY_NAME
order by sum(count ) desc
limit 5
""")

maxSal.show()

###

flight.groupBy("DEST_COUNTRY_NAME").sum("count")\
.withColumnRenamed("sum(count)","total")\
.sort(desc("total"))\
.limit(5)\
.explain()

###
flight.printSchema()
flight.select("DEST_COUNTRY_NAME").show(5)
flight.select(flight['DEST_COUNTRY_NAME'], flight['count']*1000).show()
flight.filter(flight['count']>5000).show()
flight.groupBy('DEST_COUNTRY_NAME').count().show()


staticDF = spark.read.format("csv")\
    .option("header", "true")\
    .option("inferschema","true")\
    .load("data/retail-data/by-day/*.csv")

staticDF.createOrReplaceTempView("retail_data")
staticSchema = staticDF.schema

staticDF.head()
staticSchema

staticDF.selectExpr(
    "CustomerId",
    "(UnitPrice*Quantity) as total_cost",
    "InvoiceDate")\
    .groupBy(
    col("CustomerId"), window(col("InvoiceDate"), "1 day"))\
    .sum("total_cost")\
    .show(5)

spark.conf.set("spark.sql.shuffle.partitions", "5")

#streaming
streamingDF = spark.readStream\
    .schema(staticSchema)\
    .option("maxFilesPerTrigger", 1)\
    .format("csv")\
    .option("header", "true")\
    .load("data/retail-data/by-day/*.csv")

streamingDF.isStreaming
'''
