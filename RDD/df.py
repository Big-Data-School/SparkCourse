from pyspark import SparkContext #SC
sc = SparkContext.getOrCreate() #place SparkContext into a Variable
from pyspark.sql import SparkSession #place SparkSession into a Variable
spark = SparkSession.builder.appName("name").getOrCreate()

student = spark.createDataFrame([
('1', 'Mohammad', '95%', 'Big Data'),
('2', 'Ali', '80%', 'Data Science'),
('3', 'Hassan', '94%', 'Software'),
('4', 'Hossein', '98%', 'Analytics')],
['id', 'Name', 'Percentage','Department']
)

student.show()
