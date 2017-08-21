from pyspark import SparkConf, SparkContext
import re


def normalizeWords(line):
    # re.compile firts compiles a pattern.
    # Then the split function is used to split at every instance where the pattern
    # is satisfied.
    return re.compile(r'\W+', re.UNICODE).split(line.lower())

conf = SparkConf().setMaster("local").setAppName("wordCount")
sc = SparkContext(conf=conf)

book = sc.textFile("Book.txt")
# Converting lines to words by splitting at space
words = book.flatMap(normalizeWords)
wordCounts = words.countByValue()

for word, count in wordCounts.items():
    cleanword = word.encode('ascii', 'ignore')
    if(cleanword):
        print("{}: {}".format(cleanword.decode(), count))
