'''
Counting Words

@Author Amruta Abhyankar
Date - 06/12/2021

'''
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext.getOrCreate(conf = conf)

input = sc.textFile("file:/Users/amrutaabhyankar/Documents/ApacheSparkCourse/Book.txt")
words = input.flatMap(lambda x: x.split())
wordCounts = words.countByValue()

for word, count in wordCounts.items():
    cleanWord = word.encode('ascii', 'ignore')
    if (cleanWord):
        print(cleanWord.decode() + " " + str(count))
