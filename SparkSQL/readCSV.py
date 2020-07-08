from pyspark import SparkContext
sc = SparkContext.getOrCreate()
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("readCSV").getOrCreate()

flight = spark\
    .read\
    .option("inferSchema", "true")\
    .option("header","true")\
    .csv("data/flight-data/csv/2015-summary.csv")

flight.take(3)
#sort the dataframe based on count column
flight.sort("count").explain()

flight.createOrReplaceTempView("flight_tbl")
sql = spark.sql("""select DEST_COUNTRY_NAME, count(1) from flight_tbl group by DEST_COUNTRY_NAME""")

sql.explain()
df_way = flight.groupBy("DEST_COUNTRY_NAME").count()

df_way.explain()