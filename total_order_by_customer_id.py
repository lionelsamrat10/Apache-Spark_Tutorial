from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("SpendByCustomers")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    customerID = int(fields[0])
    amountInDollars = float(fields[2])
    return (customerID, amountInDollars)

lines = sc.textFile("file:////Users/samratmitra1999/SparkCourse/customer-orders.csv")
parsedLines = lines.map(parseLine)

#need to change from here

#mapping each line to kwy/value pairs of customerID and Amount in dollars
sumOfAmountsPerCustomerRDD = parsedLines.reduceByKey(lambda x, y: x+y)
resultsRDD = sumOfAmountsPerCustomerRDD.collect();

for result in resultsRDD:
    print(result)
