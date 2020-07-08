from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
sc = SparkContext('local')
spark = SparkSession(sc)

'''lines = sc.textFile('mapreduce/words.txt')
lines.count()
lines.first()
'''

lines = sc.textFile('mapreduce/words.txt')
lines.count()
lines.first()