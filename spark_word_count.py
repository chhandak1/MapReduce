import findspark
findspark.init()

import sys 
from pyspark import SparkContext, SparkConf
import re

# create Spark context with necessary configuration
sc = SparkContext("local","PySpark Word Count Exmaple")

# preprocessing steps folowed using Transformations
# read data from text file 
# remove punctuations joining two words like 'Scene I.—ELSINORE', 'Hamlet.—Come' and replace them with a space. Keep "'" to save words like"Hamlet's"
# and split text into words using " " and "\n"
words = sc.textFile("inputFile2.txt").map(lambda text: re.sub('[^a-zA-Z0-9\.\']', ' ', text)).flatMap(lambda line: re.split("\n| ",line))
# remove all characters other than alphabets and '. Convert to lower case.
words_strip_punctuation = words.map(lambda word: re.sub(r'[^a-zA-Z0-9\']', '', word)).map(lambda word: word.lower())
#remove empty strings
words_strip_punctuation = words_strip_punctuation.filter(lambda word: word!='')
# count the occurrence of each word
wordCounts = words_strip_punctuation.map(lambda word: (word, 1)).reduceByKey(lambda a,b:a +b)

# Action
list1 = wordCounts.collect()

#write in file
with open("true_outputFile2.txt", 'w') as f:
    for (word, count) in list1:
        f.write("%s\t%i\n" % (word, count))