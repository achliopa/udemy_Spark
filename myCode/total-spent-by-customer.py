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
