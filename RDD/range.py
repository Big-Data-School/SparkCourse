import pyspark
from pyspark import SparkContext
sc = SparkContext.getOrCreate()
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("RDD").getOrCreate()

spark.range(1, 100, 10).collect()

spark.range(10).rdd
spark.range(10).rdd.toDF()

myCollection = "Apache Spark is a unified analytics engine for big data processing, " \
"with built-in modules for streaming, SQL, machine learning and graph processing. " \
"Apache Spark is an open-source distributed general-purpose cluster-computing framework. " \
"Spark provides an interface for programming entire clusters with implicit data parallelism and fault tolerance"\
    .split(" ")

words = spark.sparkContext.parallelize(myCollection, 1)

words.setName("myWords")
words.name()

words.count()
words.distinct().count()

#wich words starts with S
def startsWithS(individual):
    return individual.startswith("S")

def startsWithM(individual):
    return individual.startswith("m")

words.filter(lambda word: startsWithS(word)).collect()
words.filter(lambda word: startsWithM(word)).collect()

words2 = words.map(lambda word: (word, word[0], word.startswith("S")))