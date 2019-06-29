from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("SpendByCustomer")
sc = SparkContext(conf = conf)


def parseLine(line):
    fields = line.split(',')
    return (int(fields[0]), float(fields[2]))


lines = sc.textFile("file:///Users/kesar500/SparkCourse/customer-orders.csv")
custRDD = lines.map(parseLine)
totalByCustomer = custRDD.reduceByKey(lambda x, y: x + y)
totalByCustomerSorted = totalByCustomer.map(lambda x: (x[1], x[0])).sortByKey()

results = totalByCustomerSorted.collect();
for result in results:
    print(result)