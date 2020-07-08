from pyspark import SparkConf
from pyspark import SparkContext

conf = SparkConf()
conf.setAppName('spark-NLTK')
sc = SparkContext(conf = conf).getOrCreate()

import nltk

data = sc.textFile('NLP/Nixon.txt')

#word tokenization
def word_tokenize(x):
    lowerW = x.lower()
    return nltk.word_tokenize(x)

words = data.flatMap(word_tokenize)
words.collect()















#remove stop words

from nltk.corpus import stopwords
stop_words = set(stopwords.words('english'))
stopW = words.filter(lambda word: word[0] not in stop_words and word[0] != '')
stopW.collect()


# remove punctuations

import string
list_punct = list(string.punctuation)
filtered_data = stopW.filter(lambda punct: punct not in list_punct)

filtered_data.collect()

#!()=[] {} ;" \<>

# POS Tagger

nltk.download('averaged_perceptron_tagger')

def pos_tag(x):
    return nltk.pos_tag([x])

pos_words = filtered_data.map(pos_tag)

pos_words.collect()


# root

def lemma(x):
    nltk.download('wordnet')
    from nltk.stem import WordNetLemmatizer
    lemm = WordNetLemmatizer()
    return lemm.lemmatize(x)

lem_words = filtered_data.map(lemma)
lem_words.collect()


#text classifi
text_classifi = filtered_data\
    .flatMap(lambda x: nltk.FreqDist(x.split(","))\
    .most_common())\
    .map(lambda x:x)\
    .reduceByKey(lambda x,y: x+y)\
    .sortBy(lambda x: x[1], ascending=False)

topCommonData = text_classifi.take(100)
print(topCommonData)






