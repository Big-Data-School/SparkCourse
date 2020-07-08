from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
sc = SparkContext('local')
spark = SparkSession(sc)

flight = spark\
    .read\
    .option("inferSchema","true")\
    .option("header","true")\
    .csv("data/flight-data/csv/2015-summary.csv")

flight.take(3)
flight.sort("count").explain()