import pyspark
from pyspark import SparkContext #SC
sc = SparkContext.getOrCreate() #place SparkContext into a Variable
from pyspark.sql import SparkSession #place SparkSession into a Variable
spark = SparkSession.builder.appName("readData").getOrCreate()


# in Python
spark.read.json("data/flight-data/json/2015-summary.json")\
    .createOrReplaceTempView("myView") # DF => SQL

spark.sql("""
SELECT DEST_COUNTRY_NAME, sum(count)
FROM some_sql_view GROUP BY DEST_COUNTRY_NAME
""")\
    .where("DEST_COUNTRY_NAME like 'S%'").where("`sum(count)` > 10")\
    .count() # SQL => DF