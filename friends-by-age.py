from pyspark import SparkConf, SparkContext


def parseLine(line):
    fields = line.split(",")
    return (int(fields[2]), int(fields[3]))

conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf=conf)

data = sc.textFile("fakefriends.csv")

friendsByAge = data.map(parseLine)
friendsByAge = friendsByAge.mapValues(lambda x: (x, 1)).\
        reduceByKey(lambda x,y: (x[0]+y[0], x[1]+y[1]))

averageFriends = friendsByAge.mapValues(lambda x: x[0]/x[1])

results = averageFriends.collect()

for result in results:
    print(result)

