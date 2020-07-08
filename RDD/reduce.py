import pyspark
from pyspark import SparkContext #SC
sc = SparkContext.getOrCreate() #place SparkContext into a Variable
from pyspark.sql import SparkSession #place SparkSession into a Variable
spark = SparkSession.builder.appName("RDD").getOrCreate()

from operator import add

sum = sc.parallelize([1, 2, 3, 4, 5])
adding = sum.reduce(add)
print("Adding all the elements in RDDs : %i" % (adding))