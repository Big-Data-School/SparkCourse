from pyspark import SparkContext #SC
sc = SparkContext.getOrCreate() #place SparkContext into a Variable
from pyspark.sql import SparkSession #place SparkSession into a Variable
spark = SparkSession.builder.appName("joint_RDD").getOrCreate()

from operator import add

sum = sc.parallelize([1,2,3,4,5,6,7,8,9])
adding = sum.reduce(add)

print("adding all the elements in RDD : %i" % (adding))

words = sc.parallelize(
    ["python", "data", "big data", "spark", "apache spark", "hadoop", "data science"]
)

words_map = words.map(lambda x: (x, 1))
mapping = words_map.collect()

print("key value pair -> %s" % (mapping))

#counts = words.count()
#print ("number of elements present in RDD -> %i" % (counts))
#words_filter = words.filter(lambda x: 'spark' in x)
#filtered = words_filter.collect()
from pyspark import SparkContext #SC
sc = SparkContext.getOrCreate() #place SparkContext into a Variable
from pyspark.sql import SparkSession #place SparkSession into a Variable
spark = SparkSession.builder.appName("dataframe").getOrCreate()

students = spark.createDataFrame([
    ('1','Mohammad','Heydari','52','Big Data','Msc'),
    ('2','Ali','Rezaei','123','Data Science','MSc'),
    ('3','Mahdi','Ehsanpour','1000','Data Analytic','PhD'),
    ('4','Milad','Miladi','1','Stream Dep','Post Doc')],
    ['Id','Name','Family','Rank','Dep','Deg']
)

students.show()

#join
#It returns RDD with the matching keys with their values in paired form.
# We will get two different RDDs for two pair of element

a = sc.parallelize([("spark", 1), ("hadoop", 3)])
b = sc.parallelize([("spark", 2), ("hadoop", 4)])
#c = sc.parallelize([("spark", 3), ("hadoop", 5)])

joined = a.join(b)
mapped = joined.collect()

print("Join RDD -> %s" % (mapped))




