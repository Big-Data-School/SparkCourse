import pyspark
from pyspark import SparkContext #SC
sc = SparkContext.getOrCreate() #place SparkContext into a Variable
from pyspark.sql import SparkSession #place SparkSession into a Variable
spark = SparkSession.builder.appName("name").getOrCreate()

words = sc.parallelize (
  ["python",
  "java",
  "hadoop",
  "c",
  "C++",
  "spark and hadoop",
  "pyspark and spark"]
)
counts = words.collect()

print(counts)

#Part of speech tagging.
nltk.download('averaged_perceptron_tagger')
def pos_tag(x):
  import nltk
  return nltk.pos_tag([x])

pos_word = filtered_data.map(pos_tag)
print (pos_word.collect())

#NER
nltk.download('maxent_ne_chunker')
nltk.download('words')

def named_entity_recog(x):
  import nltk
  return nltk.ne_chunk([x])

NER_word = filtered_data.map(named_entity_recog)
print (NER_word.collect())

#Stemming and Lemmatization
nltk.download('wordnet')

def lemma(x):
  import nltk
  from nltk.stem import WordNetLemmatizer
  lemmatizer = WordNetLemmatizer()
  return lemmatizer.lemmatize(x)

lem_words = filtered_data.map(lemma)
print (lem_words.collect())

#Text Classification .
#find the words which has the highest frequency and sort them in decreasing order of their frequency.
text_Classifi = filtered_data.flatMap(lambda x : nltk.FreqDist(x.split(",")).most_common()).map(lambda x: x).reduceByKey(lambda x,y : x+y).sortBy(lambda x: x[1], ascending = False)
topcommon_data = text_Classifi.take(100) #take first 100 most common words

topcommon_data