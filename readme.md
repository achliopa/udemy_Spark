# Udemy Course: The Ultimate Hands-On Hadoop - Tame your Big Data!

* [Course Link](https://www.udemy.com/course/taming-big-data-with-apache-spark-hands-on/)
* [Course Repo]()

### Section 1: Getting Started with Spark

### Lecture 4. [Activity]Getting Set Up: Installing Python, a JDK, Spark, and its Dependencies.

* [Installation Instructions](https://sundog-education.com/spark-python/)
* [Apache Spark](http://www.tutorialspoint.com/apache_spark/apache_spark_installation.htm)
* we choose to deploy on AWS EC2 Instance (small) using the instructions from DSMLBootcamp course
* we need a Anaconda env with python3
* we choose an Unbuntu medium instance and use our hadoop keys to connect (our instanc eis in eu_central_1b) our security group rules are launch-wizard-2. we open the necessary ports
* make sure we set the right permissiong to mycert.pem (ubuntu:ubuntu) better DONOT sudo openssl
* we install spark 2.4.5 `wget http://archive.apache.org/dist/spark/spark-2.4.5/spark-2.4.5-bin-hadoop2.7.tgz`
* do not install default-jre but `sudo apt install openjdk-8-jre-headless`
* export spark to python
```
$ export SPARK_HOME='/home/ubuntu/spark-2.4.5-bin-hadoop2.7'
$ export PATH=$SPARK_HOME:$PATH
$ export PYTHONPATH=$SPARK_HOME/python:$PYTHONPATH
$ export PATH=$PATH:$SPARK_HOME/bin
```
* better add it to .bashrc and source
* `spark-shell` works
* `pyspark` works
```
rdd = sc.textFile("README.md")
rdd.count()
```
* the following in Notebook web works
```
from pyspark import SparkContext
sc = SparkContext()
```

### Lecture 5. [Activity] Installing the MovieLens Movie Rating Dataset

* we will get the movielens dataset from grouplens
* `wget http://files.grouplens.org/datasets/movielens/ml-100k.zip`
* get the course repo from github

### Lecture 6. [Activity] Run your first Spark program! Ratings histogram example.

* we will create a histogram out of the ml dataset counting the ratings depending on the rating num
* we use the 'ratings-counter.py' script
```
from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf = conf)

lines = sc.textFile("./ml-100k/u.data")
ratings = lines.map(lambda x: x.split()[2])
result = ratings.countByValue()

sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.items():
    print("%s %i" % (key, value))
```
* we import pyspark
* le load and parse the file
* to run the script on spark being in the script dir we use `spark-submit ratings-counter.py`
* we see a lot of additional printouts from spark. but IT WORKS

## Section 2: Spark Basics and Simple Examples

### Lecture 7. What's new in Spark 3?

* MLLib is being deprecated
* same RDDs are phased out. dataframes are the norm
* Use Dataframes
* Spark 3 is faster
* python 2 deprecated
* Spark 2 now utilizes GPU and exposes them to addons (aka TensorFlow)
* Binary file Support. we can load it to dataframe
* SparkGraph added for graph processing
* Cypher query lang (SQL like) for graphs
* ACID support in data lakes (amazon S3) with Delta Lake

### Lecture 8. Introduction to Spark

* Spark is scalable. it run on a cluster
* Spark scripts are in Scala,Python. spark ditributes the code to the cluster
* Spark Script => DriverProgram (Spark Context) => Cluster Manager (Spark, YARN) => Executor Node (Cache, Tasks)
* After the manager distributed the tasks The driver program sends tasks directly to workers
* if one worker falls our task does not stop
* horizontal scalability
* Spark is FAST:
    * runs programs up to 100x faster than Hadoop MapReduce in memory or 10x fater on disk
    * DAG engine (directed acyclic graph) optimizes workflows
* HOT HOT HOT
    * Big Players use it. Amazon,Ebay.NASA,TripAdvisor etc.
* Relaitevly easy
    * Code in Python,Java,Scala
    * Built on RDDs (Resilient Distributed Dataset)
* Spark Ecosystem: on Spark Core we have
    * Spark Streaming
    * SparkSQL
    * GraphX
    * MLLib
* Why Python?
    * No need to compile, manage dependencies
    * less coding overhead
    * many people know python
    * Focus on concepts than a new language
* Why Scala?
    * more popular in Spark
    * scala is native in spark
    * new feats
* Python Spark is much like Scala
    * python code to square nums in dataset
```
num = sc.parallelize([1,2,3,4])
squared = nums.map(lambda x:x*x).collect()
```
    * equivalent scala code
```
val nums = sc.parallelize(List[1,2,3,4])
val squared = nums.map(x => x*x).collect()
``` 
### Lecture 9. The Resilient Distributed Dataset (RDD)

* RDD is a dataset. an abstraction for a big dataset
* spread out along the cluster
* SparkContext
    * Created by the driver program
    * IS responsible for making RDDs resilient and distributed
    * Creates RDDs
    * Spark shell creates an "sc" object for us
* How to create RDDs
    * `nums = parallelize([1,2,3,4])`
    * `sc.textfile("./path")` or `s3n://` or `hdfs://`
    * `hiveCtx = HiveContext(sc); rows=hiveCtx.sql("SELECT name, age FROM users")`
    * Can also be created from: JDBC,Cassandra,HBase,ElasticSearch, JSON,CSV, object files, seq files
* Transforming RDDs
    * map()
    * flatmap(): multiple values for each values
    * filter()
    * distinct()
    * sample()
    * union(), intersection(), subtract(), cartesian(): combine
* lanbda methods used heavily, many RDD methods accept a function as a parameter
    * functional programming concept
    * python terminology
* RDD actions
    * collect
    * count
    * countByValue
    * take
    * top
    * reduce : combine values
* nothing happens in our driver program untill an action is called
* before the graph is built but is run when we call the action

### Lecture 10. Ratings Histogram Walkthrough

* first we do the imports from pyspark
* SparkContext is the father of all the connection to driver
* collection lib is used to give us methods for sorting
```
from pyspark import SparkConf, SparkContext
import collections
```

* next we create the context. this code is similar to every project
* local means the code will run one one machine, one thread
```
conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf = conf)
```

* next we load up the file, using the textFile.
* file is split to lines `lines = sc.textFile("./ml-100k/u.data")`
* a transformation method exracts data from line `ratings = lines.map(lambda x: x.split()[2])`
* RDD action produces tuples `result = ratings.countByValue()`
* straight python to present results
```
sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.items():
    print("%s %i" % (key, value))
```

* to run python on spark we use `spark-submit` not `python`

### Lecture 11. Key/Value RDD's, and the Average Friends by Age Example

* RDD's can hold key/value pairs
    * e.g number of friends by age
    * key is age value is number of friends
    * instead of a list of ages or a list of friends, we can store (age,#of friends),(age,# of friends)...
* Creating a Key/Value RDD
    * nothing special in python
    * just map pairs if data in the RDD `totalsByAge = rdd.map(lambda x: (x,1))`
    * we now have a key value pair RDD
    * its OK to have lists as values as well
* Spark can do special stuff with Key/Value Data
    * `reduceByKey()`: combine values with the same key using some function `rdd.reduceByKey(lambda x,y: x+y)` adds them up
    * `groupByKey()`: group values with the same key
    * `sortByKey()`: Sort RDD by key values
    * `keys()`, `values()`: Create an RDDof just the keys or just the values
* We can do SQL style joins on 2 key/value RDDs
    * join, rightOuterJoin, leftOuterJoin, cogroup, subtractByKey
* Mapping just the values of a key/value pair
    * with key/value data, use `mapValues()` and `flatMapValues()` if your transformation doesn't affect the keys
    * its more efficient
* Example (Friendsbyage)
    * input data: ID, name, age, number of friends `0,Will,22,387`
```
def parseLine(line):
    fileds = line.split(",")
    age = int(fields[2])
    numFriends = int(fileds[3])
    return (age,numFriends)

lines = sc.textFile("./fakefriends.csv")
rdd = lines.map(parseLine)
```

* output is key value pairs (age,numFriends) `33,385`
* Count up sum of friends and number of entries per age
```
totalsByAge = rdd.mapValues(lambda x: (x,1)).reduceByKey(lambda x,y: (x[0]+y[0],x[1]+y[1]))
```

* `rdd.mapValues(lambda x: (x,1))` (33,385) => (33, (385,1))
* `reduceByKey(lambda x,y: (x[0]+y[0],x[1]+y[1]))` adds up all values for each uniqie key (33, (387,2))
* get the average `averageByAge = totalsByAge.mapValues(lambda x: x[0]/x[1])` (33,(387,2)) => (33, 193,5)
* print out results
```
results = averageByAge.collect()
for result in results:
    print result
```

### Lecture 12. [Activity] Running the Average Friends by Age Example

* the script complete
```
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf=conf)


def parseLine(line):
    fields = line.split(',')
    age = int(fields[2])
    numFriends = int(fields[3])
    return (age, numFriends)


lines = sc.textFile("./fakefriends.csv")
rdd = lines.map(parseLine)
totalsByAge = rdd.mapValues(lambda x: (x, 1)).reduceByKey(
    lambda x, y: (x[0] + y[0], x[1] + y[1]))
averagesByAge = totalsByAge.mapValues(lambda x: x[0] / x[1])
results = averagesByAge.collect()
for result in results:
    print(result)
```
* nothing fancy just run the script `spark-submit friends-by-age.py`

### Lecture 13. Filtering RDD's, and the Minimum Temperature by Location Example

* we have weather data from 1800 and we will find the minimum temp for each station
* we will use filtering to strip out unnecesary info
* filter() removes data from your RDD
    * just takes a function that returns a boolean
    * for example, we want to filter out entries entries that don;t have "TMIN" in the first item of a list of data `minTemps = parsedLines.filter(lambda x: "TMIN" in x[1])`
* our csv data
```
...
ITE00100554,18000101,TMAX,-75,,,E,
ITE00100554,18000101,TMIN,-148,,,E,
...
```

* Parse (Map) the input data
```
def parseLine(line):
    fileds = line.split(',')
    stationID = fileds[0]
    entryType = filends[2]
    temperature = float(fileds[3])*0.1*(9.0/5.0)+32.0
    return (stationID,entryType,temperature)

lines = sc.textFile("./1800.csv")
parsedLines = lines.map(parseLine)
```

* output is (stationID, entryType,temperature)
* Filter out all but TMIN entries `minTemps = parsedLines.filter(lambda x: "TMIN" in x[1])`
* Create (station,temperature) key/value pairs `stationTemps = minTemps.map(lambda x: (x[0],x[2]))`
* Find Miimum tempeature by stationID `minTemps = stationTemps.reduceByKey(lambda x,y: min(x,y))`
* Collect and print results
```
results = minTemps.collect()
for result in results:
    print result[0] + "\t{:.2f}F".format(result[1])
```

### Lecture 14. [Activity]Running the Minimum Temperature Example, and Modifying it for Maximums

* the complete script
```
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("MinTemperatures")
sc = SparkContext(conf=conf)


def parseLine(line):
    fields = line.split(',')
    stationID = fields[0]
    entryType = fields[2]
    temperature = float(fields[3]) * 0.1 * (9.0 / 5.0) + 32.0
    return (stationID, entryType, temperature)


lines = sc.textFile("./1800.csv")
parsedLines = lines.map(parseLine)
minTemps = parsedLines.filter(lambda x: "TMIN" in x[1])
stationTemps = minTemps.map(lambda x: (x[0], x[2]))
minTemps = stationTemps.reduceByKey(lambda x, y: min(x, y))
results = minTemps.collect()

for result in results:
    print(result[0] + "\t{:.2f}F".format(result[1]))
```

* we run it with `spark-submit min-temperatures.py`

### Lecture 15. [Activity] Running the Maximum Temperature by Location Example

* the only change in previous code is `minTemps = parsedLines.filter(lambda x: "TMAX" in x[1])` and 
```
maxTemps = stationTemps.reduceByKey(lambda x, y: max(x, y))
results = maxTemps.collect()
```

### Lecture 16. [Activity] Counting Word Occurrences using flatmap()

* `map()` transforms each element of an RDD into one new element (1:1 relation)
* "the quick red fox jumped over" => `rageCaps = lines.map(lambda x: x.upper())` => "THE QUICK RED FOX JUMPED OVER"
* `flatmap()` can create many new elements from each one 
* "the quick red fox jumped over" => `words = lines.flatMap(lambda x: x.split())` => ["the","quick","red","fox"."jumped","over"]
* we will count the words in a text 'Book.txt'
* the script 'word-count.py' uses flatmap
```
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf=conf)

input = sc.textFile("./book.txt")
words = input.flatMap(lambda x: x.split())
wordCounts = words.countByValue()

for word, count in wordCounts.items():
    cleanWord = word.encode('ascii', 'ignore')
    if (cleanWord):
        print(cleanWord.decode() + " " + str(count))
```

* `wordCounts = words.countByValue()` creates a tuple of word plus occurences
* `wordCounts.items():` contains the tuple
* run with `spark-submit word-count.py` 
* words are not preporcessed

### Lecture 17. [Activity] Improving the Word Count Script with Regular Expressions

* we will use regex to clean up the words from the list
* Τext normalization
    * Problem: word variants with different capitalization, punctuation etc
    * THere are fancy natural language processing toolkits like NLTK
    * but we will keep it simple, and use regex
```
import re
from pyspark import SparkConf, SparkContext


def normalizeWords(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())


conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf=conf)

input = sc.textFile("./book.txt")
words = input.flatMap(normalizeWords)
wordCounts = words.countByValue()

for word, count in wordCounts.items():
    cleanWord = word.encode('ascii', 'ignore')
    if (cleanWord):
        print(cleanWord.decode() + " " + str(count))
```

* normalize words method dos the trick extracting meaningful words and lowercasing them
* we run the script on spark `spark-submit word-count-better.py`

### Lecture 18. [Activity] Sorting the Word Count Results


* we could sort what `countByValue()` returns but we will use RDD's to keep it scalable
* First, do `countByValue()` the hard way `wordCounts = words.map(lambda x: (w,1)).reduceByKey(lambda x,y: x+y)` => convert each word to a key/value pair with value of 1, then count the all up
* flip the (word,count) pairs to (count,word)
* then use `sortByKey()` to sort by count (count is the key) `wordCountsSorted = wordCounts.map(lambda (x,y): (y,x)).sortByKey()`
* the complete code is:
```
import re
from pyspark import SparkConf, SparkContext

def normalizeWords(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

input = sc.textFile("file:///sparkcourse/book.txt")
words = input.flatMap(normalizeWords)

wordCounts = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
wordCountsSorted = wordCounts.map(lambda x: (x[1], x[0])).sortByKey()
results = wordCountsSorted.collect()

for result in results:
    count = str(result[0])
    word = result[1].encode('ascii', 'ignore')
    if (word):
        print(word.decode() + ":\t\t" + count)
```
* we run `spark-submit word-count-better-sorted.py`

### Lecture 19. [Exercise] Find the Total Amount Spent by Customer

* Add Up amount spent by customer
* input CSV is UserID,ItemID,AmountSpent
* we need to add up the time spent for each user UserID,sum(AmountSpent)
* Strategy
    * split each commadelimited line into fields
    * map each line to key/value pairs of customer ID and dollar amount
    * Use reduceBykey to add up amount spent by customeID
    * collect() the results and print Them
    * Review Previous Examples
    * split comma-delimited fileds `fields=line.split(',')`
    * treat field 0 as an integer, and field 2 s a floating point num `return (int(fields[0],float(fields[2]))`

### Lecture 20. [Excercise] Check your Results, and Now Sort them by Total Amount Spent.

* our solution
```
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("TotalAmount")
sc = SparkContext(conf=conf)

def parseLine(line):
    fields = line.split(',')
    customer = int(fields[0])
    amount = float(fields[2])
    return (customer,amount)

lines = sc.textFile("./customer-orders.csv")
rdd = lines.map(parseLine)
totalAmount = rdd.reduceByKey(lambda x, y: x + y)
results = totalAmount.collect()
for result in results:
    print(result)
```
* we run it with `spark-submit total-spent-by-customer.py`

### Lecture 21. Check Your Sorted Implementation and Results Against Mine.

* our code
```
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("TotalAmount")
sc = SparkContext(conf=conf)


def parseLine(line):
    fields = line.split(',')
    customer = int(fields[0])
    amount = float(fields[2])
    return (customer, amount)


lines = sc.textFile("./customer-orders.csv")
rdd = lines.map(parseLine)
totalAmount = rdd.reduceByKey(lambda x, y: x + y)
totalAmountSorted = totalAmount.map(lambda x: (x[1], x[0])).sortByKey()
results = totalAmountSorted.collect()
for result in results:
    print(result)
```

* we run it as `spark-submit total-spent-by-customer-sorted.py`
* his solution differs only at `totalAmount.map(lambda (x,y):(y,x)).sortByKey()`

## Section 3: Advanced Examples of Spark Programs

### Lecture 22. [Activity] Find the Most Popular Movie

* u.data data file (ratings) format is UserID,MovieID,Rating,Timestamp
* the code for finding the most viewed movie (most rated) is
```
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("PopularMovies")
sc = SparkContext(conf=conf)

lines = sc.textFile("./ml-100k/u.data")
movies = lines.map(lambda x: (int(x.split()[1]), 1))
movieCounts = movies.reduceByKey(lambda x, y: x + y)

flipped = movieCounts.map(lambda xy: (xy[1], xy[0]))
sortedMovies = flipped.sortByKey()

results = sortedMovies.collect()

for result in results:
    print(result)
```

* nothing new here.. run as `spark-submit popular-movies.py`

### Lecture 23. [Activity] Use Broadcast Variables to Display Movie Names Instead of ID Numbers

* we will use broadcast values to combine data from different files
* We want to display movie names not IDs (from u.items\ file)
* we could just keep a table loaded in the driver program
* or we could even let Spark automatically forward it to each executor when needed
* but what if the table were massive? we'd only want to transfer it once to each executor and keep it there
* Broadcase variables do this
    * Broadcast objects to the executors, such that they are always there whenever needed
    * Just use sc.broadcast() to ship off whatever we want
    * then use .value() to get the object back
```
def loadMovieNames():
    movieNames = {}
    with open("./ml-100k/u.item") as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames
...
nameDict = sc.broadcast(loadMovieNames())
...
sortedMoviesWithNames = sortedMovies.map(
    lambda countMovie: (nameDict.value[countMovie[1]], countMovie[0]))
```
* what we do in the helper method is build a hash table (a python distionary or key value pair)
* we extract the value of the dict (movieName) with `nameDict.value[countMovie[1]]`
* the complete code is 
```
from pyspark import SparkConf, SparkContext


def loadMovieNames():
    movieNames = {}
    with open("./ml-100k/u.item") as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames


conf = SparkConf().setMaster("local").setAppName("PopularMovies")
sc = SparkContext(conf=conf)

nameDict = sc.broadcast(loadMovieNames())

lines = sc.textFile("./ml-100k/u.data")
movies = lines.map(lambda x: (int(x.split()[1]), 1))
movieCounts = movies.reduceByKey(lambda x, y: x + y)

flipped = movieCounts.map(lambda x: (x[1], x[0]))
sortedMovies = flipped.sortByKey()

sortedMoviesWithNames = sortedMovies.map(
    lambda countMovie: (nameDict.value[countMovie[1]], countMovie[0]))

results = sortedMoviesWithNames.collect()

for result in results:
    print(result)
```
* we run it `spark-submit popular-movies-nicer.py`

### Lecture 24. Find the Most Popular Superhero in a Social Graph

* Superhero Social Network... WTF
* Input Data Format (marvel-graph.txt) HeroID,HeroID,HeroID,..,HeroID
* each line is a comic and the IDs are the heros that appear in it
* a hero may appear in multiple lines
* 'Marvel-names.txt' HeroID "HeroName"
* Strategy
    * Map input data to (heroID,number of co-occurencies) per line
    * Add up co-occurence by heroID using `reduceByKey()`
    * Flip (map) RDD to (number,heroID) so we can...
    * Use `max()` on the RDD to find the hero with the most co-occurencies
    * look up the name of the winner and from pyspark import SparkConf, SparkContext

### Lecture 25. [Activity] Run the Script - Discover Who the Most Popular Superhero is!

* the code is:
```
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("PopularHero")
sc = SparkContext(conf=conf)

def countCoOccurences(line):
    elements = line.split()
    return (int(elements[0]), len(elements) - 1)

def parseNames(line):
    fields = line.split('\"')
    return (int(fields[0]), fields[1].encode("utf8"))

names = sc.textFile("./marvel-names.txt")
namesRdd = names.map(parseNames)

lines = sc.textFile("./marvel-graph.txt")

pairings = lines.map(countCoOccurences)
totalFriendsByCharacter = pairings.reduceByKey(lambda x, y: x + y)
flipped = totalFriendsByCharacter.map(lambda xy: (xy[1], xy[0]))

mostPopular = flipped.max()

mostPopularName = namesRdd.lookup(mostPopular[1])[0]

print(str(mostPopularName) + " is the most popular superhero, with " +
      str(mostPopular[0]) + " co-appearances.")
```

* we run the code `spark-submit most-popular-superhero.py`

### Lecture 26. Superhero Degrees of Separation: Introducing Breadth-First Search

* Degrees of separation is how far connected are 2 people in a social network
* Breath-First-Search algorithm with accumulators
* Its a Graph problem. each hero is a node in the graph, edges are connections
* its node has a distance from the node we are interested in
* before we process anode the distance is infinity
    * we start from Start hero (mark it visit) and put 0 as distance, mark its neighbours with the distance and start as visited
    * pick one with lower distance mark it as processed and its neighbours as visited
    * its like dijkstas algorithm with a fixed cost of 1

### Lecture 27. Superhero Degrees of Separation: Accumulators, and Implementing BFS in Spark

* Implement BFS in Spark:
    * Represent each line as a node with connections, a color and a distance. e.g: 5983 1163 3836 4361 1282 => (5983, (1165,3836,4361,1282),9999,WHITE)
    * the initial condition is that a nod is infinite distant (9999) and WHITE
* Map Function tp conver 'marvel-graph.txt' to BFS nodes
```
def convertToBFS(line):
    fields = line.split()
    heroID = int(fields[0])
    connections = []
    for connection in fields[1:]:
        connections.append(int(connection))
    
    color = "WHITE"
    distance = 9999

    if(heroID == startCharadterID):
        color = 'GRAY'
        distance = 0
    
    return (heroID,(connections,distance,color))
```

* Itertively Process the RDD
    * go through looking for GREY nodes to expand
    * color nodes wa are done with black
    * update the distances as we go
* BFS as a Map & Reduce job
* Mapper:
    * creates new nodes for each connection of gray nodes, with a distance incrmented by one, color gray and no connections
    * colors the procesed gray nodes black
    * copies the node in the results
* Reducer: 
    * Combines together all nodes for the same heroID
    * preserves the shortest distance and the darkest color found
    * preseves the list of connections from the original node
* How do we know when we're done?
    * An accumulator allows many executors to increment a shared
    * For example `hitCounter = sc.accumulator(0)` sets up a shared accumulator with an initial value of 0
    * For each iteration, if the character we're interested in is hit, we increment the hitCounter accumulator
    * fter each iteration, we check if hitCounter is greater than one. if so we are done
* An accumulator is a value shared across all nodes in cluster. here is used as a flag to signal the executors on various nodes to stop onece result is found

### Lecture 28. [Activity] Superhero Degrees of Separation: Review the Code and Run it

* we load the file and create the nodes
* we run flatMap using the bfsMap
```
def bfsMap(node):
    characterID = node[0]
    data = node[1]
    connections = data[0]
    distance = data[1]
    color = data[2]

    results = []

    #If this node needs to be expanded...
    if (color == 'GRAY'):
        for connection in connections:
            newCharacterID = connection
            newDistance = distance + 1
            newColor = 'GRAY'
            if (targetCharacterID == connection):
                hitCounter.add(1)

            newEntry = (newCharacterID, ([], newDistance, newColor))
            results.append(newEntry)

        #We've processed this node, so color it black
        color = 'BLACK'

    #Emit the input node so we don't lose it.
    results.append((characterID, (connections, distance, color)))
    return results
```

* it does one iteration visiting the neighbour nodes incrementing dist
* flatmap is a transformation. spark uses lazy evaluation... it needs an action to run. we cheat using `count()`

* if the target is not found and we need a new iteration we reduce (reduceBYKey) to build the updated graph for next iteration
```
def bfsReduce(data1, data2):
    edges1 = data1[0]
    edges2 = data2[0]
    distance1 = data1[1]
    distance2 = data2[1]
    color1 = data1[2]
    color2 = data2[2]

    distance = 9999
    color = color1
    edges = []

    # See if one is the original node with its connections.
    # If so preserve them.
    if (len(edges1) > 0):
        edges.extend(edges1)
    if (len(edges2) > 0):
        edges.extend(edges2)
    # Preserve minimum distance
    if (distance1 < distance):
        distance = distance1
    if (distance2 < distance):
        distance = distance2
    # Preserve darkest color
    if (color1 == 'WHITE' and (color2 == 'GRAY' or color2 == 'BLACK')):
        color = color2
    if (color1 == 'GRAY' and color2 == 'BLACK'):
        color = color2
    if (color2 == 'WHITE' and (color1 == 'GRAY' or color1 == 'BLACK')):
        color = color1
    if (color2 == 'GRAY' and color1 == 'BLACK'):
        color = color1
    return (edges, distance, color)
```
* run script `spark-submit degress-of-separation.py`

### Lecture 29. Item-Based Collaborative Filtering in Spark, cache(), and persist()

* Collaborative Filtering is used for suggestions (person who likes this also likes ...)
* We will apply Item-Based Collaborative Filtering to find similar movies using Spark and Movielens dataset
* we will learn about caching RDDs
* Item-Based Collaborative Filtering
    * Find every pair of movies that were watched by the same person
    * Measure the similiariy of their ratings accross all user watched both
    * sort by movie then by similaity strength
    * this is one way to do it
* Make it a Spark Problem
    * Map input ratings to (userID,(movieID,rating))
    * Find every movie pair rated by the same user, this can be done with a "self join" operation. at this point we have (userID,((movieID1,rating1),(movieID2,rating2)))
    * Filter out duplicate pairs
    * make the moviepairs the key: map to ((movieID1,movieID2),(rating1,rating2))
    * groupByKey() to get every rating pair found for each movie pair
    * compute similarity between ratings for each movie in the pair
    * sort , save and dispaly results
* self join blows dat size... be careful
* this is some heavy lifting in processing. we need all the cores of the machine
```
conf = SparkConf().setmaster("local[*]").setAppName("MovieSimilarities")
sc = SparkContext(conf=conf)
```
* `local[*]` tells the driver to run the code on multiple executors on each core treating it as a separate node
* Caching RDD's
    * in this example we will query the final RDD of movie similarities a couple of times
    * Any time we will perform more than one action on an RDD we must cache it. otherwise Spark might reevaluate the whole RDD al over
    * Use `.cache()` or `.persist()` to do this.
    * Persist optionally lets us cache it to the disk, instead of just memory, in case node fails

### Lecture 30. [Activity] Running the Similar Movies Script using Spark's Cluster Manager

* we load the movie names
* we load the ratings
* transform data to key value pair
* we do self-join to find all ratings from same user `joinedRatings = ratings.join(ratings)` join is doen on the key
* we filter duplicates
```
def filterDuplicates(userRatings):
    ratings = userRatings[1]
    (movie1, rating1) = ratings[0]
    (movie2, rating2) = ratings[1]
    return movie1 < movie2
```
* due to complexity instead of lambdas we use proper functions
* we cache results
```
moviePairSimilarities = moviePairRatings.mapValues(
    computeCosineSimilarity).cache()
```
* if we choose to store intermediate reults to files we will get different file for each executor
* our output shows how code is split to each core
* run the code with `spark-submit movie-similarities.py 260` where 260 is the ID for Starwrs for which we want to get the similarities

### Lecture 31. [Exercise] Improve the Quality of Similar Movies

* Improve the results!
    * Discard bad ratings.only recomend good movies
    * try different similarity metrics (Pearson Correlation Coefficient,Jaccard Coefficient, Conditional Probability)
    * Adjust the Thresholds for minimum co-raters or minimum score
    * Invent  new similaity metric that takes the number of co-raters into account
    * unse genre information in u.items to boost scores from movies in the same genre

## Section 4: Running Spark on a Cluster

### Lecture 32. Introducing Elastic MapReduce

* Running Spark in the Cloud with AWS Elastic mapReduce
    * Very quick and easy way to rent time on a cluster of your own
    * Set up a default spark configuration for us on top of Hadoop's YARN cluster manager (Well we use Hadoop anyway even in a local deployment)
    * Spark also has a built-in standalone cluster manager, and scripts to setup its own EC2-based cluster (AWS console is easier)
* Spark on EMR isn't that expensive but is no cheap either
    * unlike MapReduce with MRjobs we will run m3.xlarge instances
    * running a few hours can cost 30+USD
    * shut down instances when done!!!!!
    * if we want to go cheap we can run on a subset of data locally
* Always run locally on a subset of data. Always... when code is fine then fire up the cluster, do the job, kill it..... stay low, stay cheap

### Lecture 33. [Activity] Setting up your AWS / Elastic MapReduce Account and Setting Up PuTTY

* Geting Set up on EMR
    * Make an AWS account. check
    * Create an EC2 key pair and store it to an .pem file. check
    * fire up a terminal. connect to EC2.check

### Lecture 34. Partitioning

* we need to reorm our code to run on cluster
* Partitioning
    * we need to think how our data is partitioned
    * Running our movie similarity script as-is wont work at all on cluster. sel-join is expensive and spark wont distribute it at all. no gains
    * Use `.partiionBy()` on an RDD before running a large operaion that benefits from partitioning such as `join()` `cogroup()` `groupWith()` `Join()` `leftOuterJoin()` `rightOuterJoin()` `groupByKey()` `reduceByKey()` `combineByKey()` `lookup()` these operations will preserve the partitioning in the result to
* Choosing a partition Size
    * too few partitions won't take full advantage of our cluster
    * too many results in too much overhead from shuffling data
    * at least as many partitions as we have cores, or executors that fit within our available memory
    * partitionBy(100) is usually a reasonable place to start for large operration
    * lets examine our modified movie similarity script for 1million movies on the cluster

### Lecture 35. Create Similar Movies from One Million Ratings - Part 1

* we get the 1million dataset `wget http://files.grouplens.org/datasets/movielens/ml-1m.zip`
* our `modified movie-similarities-1m.py` script does the job
* ratings data is loaded from amazon s3 bucket `data = sc.textFile("s3n://sundog-spark/ml-1m/ratings.dat")`
* SparkConf() config is not hardcoded but will be passed as script arguments when we will run it. also it will use the config files from amazon

* we use partition 2 times before join() and groupByKey() (Reducer operations or RDD actions)
```
# Emit every movie rated together by the same user.
# Self-join to find every combination.
ratingsPartitioned = ratings.partitionBy(100)
joinedRatings = ratingsPartitioned.join(ratingsPartitioned)
# At this point our RDD consists of userID => ((movieID, rating), (movieID, rating))
# Filter out duplicate pairs
uniqueJoinedRatings = joinedRatings.filter(filterDuplicates)
# Now key by (movie1, movie2) pairs.
moviePairs = uniqueJoinedRatings.map(makePairs).partitionBy(100)
# We now have (movie1, movie2) => (rating1, rating2)
# Now collect all ratings for each movie pair and compute similarity
moviePairRatings = moviePairs.groupByKey()
```

### Lecture 36. [Activity] Create Similar Movies from One Million Ratings - Part 2

* Specifying memory per executor:
    * Just use an empty, defult SparkConf() in your driver - this way we will use the defaults EMR set up instead, as well as any command line options we pass into `spark-submit` from our master node
    * in our example, the default executor memory budget of 512MB is insufficient  for processing 1million movie ratings. So instead we do: `spark-submit --executor-memory 1g movie-similarities-1m.py 260` from the master node in our cluster
    * executors will keep failing for the dataset with the default mem setings
* Running on Cluster
    * Get your scripts & data someplace where EMR can access them easily: AWS S3 is a good choice - just use `s3n://URL` when specing file paths, and make sure your file permissions make them accessible
    * Spin up an EMR cluster for Spark using  the AWS console (billing starts)
    * Get the external DNS name for the master node, and log into it using the "Hadoop" user account and our private key file
    * copy our driver program and the files it needs. (using aws s3://bucket-name/file-name)
    * Rin `spark-submit` and watch the output
    * Terminate cluster when done
* Click EMR in AWS
* Create New Cluster
* name it
* disable logging
* latest emr release
* application => Spark
* m3.xlarge instances
* default is 3 use 5 for more reliable results
* set the EC2 key pair to use to connect to master node
* Create Cluster
    * it starts provisioning 1 for master and 4 cores
    * get the dns name, click SSH to see instructions
* use scp to copy data or `aws cp s3://URL ./`
* run script `spark-submit --executor-memory 1g movie-similarities-1m.py 260` where 260 is the ID for starwars for which we want the suggestions

### Lecture 37. Create Similar Movies from One Million Ratings - Part 3

* output in console is a lot but our results are there

### Lecture 38. Troubleshooting Spark on a Cluster

* Its like a dark art
* our master will run a console on port 4040 for debugging 
* in EMR its nearly impossible to connect to it from outside
* if we run a private cluster its easier
* lets take a look on our local machine
* we visit it from browser. we have a visualizaion of our task as it is distributed in the cluster
* its accessible only when job is running
* we can see the graph etc

### Lecture 39. More Troubleshooting, and Managing Dependencies

* Logs
    * in standalone mode they are in the Web UI
    * in YARN on cluster logs are distributed, we need to collect them after the fact using `yarn logs --applicationID <AppID>`
* While our driver script runs it will log errors like executors failing to issue heartbeats
    * this generally means we are asking to much from each executor
    * we many need more of them (more machines)
    * each executor might need more memory
    * or use patitionBy() to demand less work from individual executors by using smaller partitions
* Managing Dependencies
    * Remember our executors arent necessarily on the same box as our driver script
    * Use broadcast variables to share data outside of RDD's
    * Need some Python package that's not preloaded to EMR? => setup a step in EMR to run pip for what we need on each worker machine, or use -py files with spark-submit to add individual libraries that are on the master
    * try to just avoid using obscure packages you don't need in the first palce. time is money on the cluster, and you' re better off not fiddle with it

## Section 5: SparkSQL, DataFrames, and DataSets

### Lecture 40. Introducing SparkSQL

* Working with Structured Data
    * Extends RDD to a "DataFrame" object
* Dataframes:
    * Contain Row objects
    * Can Run SQL queries
    * Has a schema (leading to more efficient storage)
    * Read and Write  to JSON,Hive,parquet
    * Communicates wth JDBC,ODBS,Tableau
* Using Spqrk SQL in Python
    * `from pyspark.sql import SQLContext,Roq`
    * `hiveContext=HiveContext(sc)`
    * `inputData=spark.read.json(dataFile)`
    * `inputData.createOrReplaceTempView("myStructuredStuff")`
    * `myResultDataFrame=hiveContext.sql(""SELECT foo FROM bar ORDER BY foobar"")`
* to convert RDDs to DataFrames we need to know its schema
* Other stuff we can do with Dataframes
    * `myResultDataFrame.show()`
    * `myResultDataFrame.select("someFieldName")`
    * `myResultDataFrame.filter(myResultDataFrame("someFiledName") > 200)`
    * `myResultDataFrame.groupBY(myResultDataFrame("someFieldName")).mean()`
    * `myResultDataFrame.rdd().map(mapperFuncton)`
* In Spark2 DataFrame is a DataSet of Row objects
* DataSets can wrap known, typed data too. But this is mostly transparent to us in Python, since Python is untyped
* So dont sweat it too much with Python.  The Spark2 way is to use DataSets instead of DataFrames when we can
* Shell Access
    * Spark SQL exposes a JDBC/ODBC server (if we build Spark with Hive Support)
    * Start it with `sbin/start-thriftserver.sh`
    * listens to port 10000 by default
    * connect using `bin/beeline -u jdbc:hive2://localhost:10000`
    * this way we have a SQL shell to Spark SQL
    * we can createnew tables, or query existing ones that were cached using `hiveCtx.cacheTale("tableName")`
* UserDefinedFunctions (UDFs)
```
from pyspark.sql.types import IntegerType
hiveCtx.registerFunction("square",lambda x: x*x,IntegerType())
df=hiveCtx.sql("SELECT square('someNumericField)" FROM tableName)
```

### Lecture 41. Executing SQL commands and SQL-style functions on a DataFrame

* do the imports
```
from pyspark.sql import SparkSession
from pyspark.sql import Row
```

* create a spark session instead of a sparkcontext. we get backa sql context as well
```
spark = SparkSession.builder.appName("SparkSQL").getOrCreate()
```

* we will use SQl queries to analyze the fake social network file
* data is unstructured. we will use a mapper to convert to row obkects (DataFrame is an RDD of row objects) before importing it to SparkSession

```
def mapper(line):
    fields = line.split(',')
    return Row(ID=int(fields[0]), name=str(fields[1].encode("utf-8")), age=int(fields[2]), numFriends=int(fields[3]))

lines = spark.sparkContext.textFile("fakefriends.csv")
people = lines.map(mapper)
```

* we pass it in infering the schema, `schemaPeople = spark.createDataFrame(people).cache()`
* we issue SQL queries. we create a temp in memory table and register the DataFrame as a table `schemaPeople.createOrReplaceTempView("people")`
* we issue query `teenagers = spark.sql("SELECT * FROM people WHERE age >= 13 AND age <= 19")`
* we can just `.show()` the results or trat them as RDD and use colect
```
for teen in teenagers.collect():
  print(teen)
```

* we can alternatively chain functions instead of SQL queries `schemaPeople.groupBy("age").count().orderBy("age").show()`
* run as `spark-submit spark-sql.py`

### Lecture 42. Using DataFrames instead of RDD's

* we do the imports
```
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions
```

* create a sparksession `spark = SparkSession.builder.appName("PopularMovies").getOrCreate()`
* we load movie names as dictionaries
* our u.data is a txt file unstructured. so again we need to impart a structure on it. again we dot RDD->Row obj => DataFrame
```
lines = spark.sparkContext.textFile("./ml-100k/u.data")
movies = lines.map(lambda x: Row(movieID=int(x.split()[1])))
movieDataset = spark.createDataFrame(movies)
```
* then we use function to imply sql query like `topMovieIDs = movieDataset.groupBy("movieID").count().orderBy(
    "count", ascending=False).cache()`
* we `.show()` results
* in the end we `.stop()` the session
* run it as `spark-submit popular-movies-dataframe.py`

## Section 6: Other Spark Technologies and Libraries

### Lecture 43. Introducing MLLib

* MLLib Capabilities
    * Feature extraction: term frequency / inverse document frequency (TF/IDF) useful for search
    * basic statistics: Chi-squared test, Pearson or Spearman correlation, min,max,mean,variance
    * Linear Regression, logistic regression
    * Support Vector machines
    * Naive Bayes Classifier
    * Decision Trees
    * K-Means Clustering
    * Principal component analysis, singular value decomposition
    * Recommendations using Alternating Least Squares
* Special MLLib Data Types
    * Vectors (Dense or Sparse)
    * LabeledPoint
    * Rating
* For more info check [Advanced Analytcs with Spark](http://shop.oreilly.com/product/0636920035091.do)
* We ll make some movie recommendations using MLLib
* MLLib uses RDDs under the hood. if we use an RDD action more than once we need to cache it

### Lecture 44. [Activity] Using MLLib to Produce Movie Recommendations

* for this to work we need to add a dummy user in the dataset u.data with 3 fake reviwes
```
0   50  5   881250949
0   172 5   881250949
0   133 1   881250949
```
* we import ALS from mllib `from pyspark.mllib.recommendation import ALS, Rating`
* data loading is like always
* the ALS specific code is 
```
# Build the recommendation model using Alternating Least Squares
print("\nTraining recommendation model...")
rank = 10
# Lowered numIterations to ensure it works on lower-end systems
numIterations = 6
model = ALS.train(ratings, rank, numIterations)

userID = int(sys.argv[1])

print("\nRatings for user ID " + str(userID) + ":")
userRatings = ratings.filter(lambda l: l[0] == userID)
for rating in userRatings.collect():
    print(nameDict[int(rating[1])] + ": " + str(rating[2]))

print("\nTop 10 recommendations:")
recommendations = model.recommendProducts(userID, 10)
for recommendation in recommendations:
    print(nameDict[int(recommendation[1])] +
          " score " + str(recommendation[2]))
```

### Lecture 45. Analyzing the ALS Recommendations Results

* results are not ok
* seems like algorithm is not working