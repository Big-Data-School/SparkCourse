from pyspark import SparkContext #SC
sc = SparkContext.getOrCreate() #place SparkContext into a Variable
from pyspark.sql import SparkSession #place SparkSession into a Variable
spark = SparkSession.builder.appName("name").getOrCreate()

words = sc.parallelize (
 ["python",
  "java",
  "hadoop",
  "C",
  "C++",
  "spark vs hadoop",
  "pyspark and spark"]
)
words_map = words.map(lambda x: (x, 1))
mapping = words_map.collect()
print("Key value pair -> %s" % (mapping))