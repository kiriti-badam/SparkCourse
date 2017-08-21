from pyspark import SparkConf, SparkContext

def loadMovieNames():
    movieNames = {}
    # The u.item file couldn't be decoded with utf-8
    with open("ml-100k/u.item", encoding="ISO-8859-1") as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames

conf = SparkConf().setMaster("local").setAppName("PopularMovies")
sc = SparkContext(conf = conf)

# broadcast enables the Python object to be sent to all the executors and 
# be available whenever needed. This transfers to executors only once.
nameDict = sc.broadcast(loadMovieNames())
#nameDict = sc.broadcast(loadMovieNames())

lines = sc.textFile("ml-100k/u.data")
movies = lines.map(lambda x: (int(x.split()[1]), 1))
movieCounts = movies.reduceByKey(lambda x, y: x + y)

flipped = movieCounts.map( lambda x : (x[1], x[0]))
sortedMovies = flipped.sortByKey()

sortedMoviesWithNames = sortedMovies.map(lambda countMovie : (nameDict.value[countMovie[1]], countMovie[0]))

results = sortedMoviesWithNames.collect()

for result in results:
    print (result)
