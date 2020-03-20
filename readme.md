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

* 