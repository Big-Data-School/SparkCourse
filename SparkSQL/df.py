import pyspark
from pyspark import SparkContext #SC
sc = SparkContext.getOrCreate() #place SparkContext into a Variable
from pyspark.sql import SparkSession #place SparkSession into a Variable
spark = SparkSession.builder.appName("df57").getOrCreate()

df = spark.range(1000).toDF("nums")
spark.range(5).collect()
df.select(df["nums"] + 10)

df = spark.read.format("json").load("data/flight-data/json/2015-summary.json")
dfS = spark.read.format("json").load("data/flight-data/json/2015-summary.json").schema
dfS
df.printSchema()

from pyspark.sql.types import StructField, StructType, StringType, LongType

myManualSchema = StructType([
    StructField("DEST_COUNTRY_NAME", StringType(), True),
    StructField("ORIGIN_COUNTRY_NAME", StringType(), True),
    StructField("count", LongType(), False, metadata={"hello":"world"}),
])
df = spark.read.format("json").schema(myManualSchema)\
    .load("data/flight-data/json/2015-summary.json")
df

from pyspark.sql.functions import col, column
df.col("count")

from pyspark.sql import Row

myManualSchema = StructType([
    StructField("id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("rank", LongType(), False)
])
myRow = Row("1", "Mohammad", 142)
my2Row = Row("2", "Ali", "200")
myDf = spark.createDataFrame([myRow], myManualSchema)
myDf = spark.createDataFrame([myRow], myManualSchema)
myDf.show()
