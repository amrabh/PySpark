'''
Rating Histogram - Spark Program to count the sum of each movie ratings

@Author Amruta Abhyankar
Date - 06/12/2021

'''


from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext.getOrCreate(conf = conf);

lines = sc.textFile("file:/Users/amrutaabhyankar/Documents/ApacheSparkCourse/ml-100k/u.data")
ratings = lines.map(lambda x: x.split()[2])
result = ratings.countByValue()

sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.items():
    print("%s %i" % (key, value))
