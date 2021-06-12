'''
Better Counting Words Program

@Author Amruta Abhyankar
Date - 06/12/2021

'''
import re
from pyspark import SparkConf, SparkContext


# Regular Expression to get only words from a text file
def normalizeWords(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext.getOrCreate(conf = conf)

input = sc.textFile("file:/Users/amrutaabhyankar/Documents/ApacheSparkCourse/Book.txt")
words = input.flatMap(normalizeWords)
wordCounts = words.countByValue()

for word, count in wordCounts.items():
    cleanWord = word.encode('ascii', 'ignore')
    if (cleanWord):
        print(cleanWord.decode() + " " + str(count))
