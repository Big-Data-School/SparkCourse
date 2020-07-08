import pyspark
from pyspark import SparkContext #SC
sc = SparkContext.getOrCreate() #place SparkContext into a Variable
from pyspark.sql import SparkSession #place SparkSession into a Variable
spark = SparkSession.builder.appName("RDD").getOrCreate()

words = sc.parallelize (
  ["python",
  "java",
  "hadoop",
  "c",
  "C++",
  "spark and hadoop",
  "pyspark and spark"]
)

words_filter = words.filter(lambda x: 'spark' in x)
filtered = words_filter.collect()

print("Filtered RDD : %s" % (filtered))