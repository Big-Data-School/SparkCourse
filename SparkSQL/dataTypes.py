import pyspark
from pyspark import SparkContext #SC
sc = SparkContext.getOrCreate() #place SparkContext into a Variable
from pyspark.sql import SparkSession #place SparkSession into a Variable
spark = SparkSession.builder.appName("dt").getOrCreate()

from pyspark.sql.functions import lit

