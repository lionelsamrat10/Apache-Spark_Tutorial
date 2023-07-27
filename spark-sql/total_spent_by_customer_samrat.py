from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType

spark = SparkSession.builder.appName("TotalSpentByCustomer").master("local[*]").getOrCreate()


# create custom schema
customerSchemaSamrat = StructType([ \
                        StructField("customerID", IntegerType(), True), \
                        StructField("itemID", IntegerType(), True), \
                        StructField("amount", FloatType(), True)])

customersDF = spark.read.schema(customerSchemaSamrat).csv("file:////Users/samratmitra1999/SparkCourse/spark-sql/customer-orders.csv")
customersDF.printSchema()

# Grouping by customerId and summing up the amount
totalAmountByCustomer = customersDF.groupby("customerID").agg(func.round(func.sum("amount"), 2)\
                                      .alias("total_spent_by_customer"))

totalAmountByCustomerSorted = totalAmountByCustomer.sort("total_spent_by_customer")

totalAmountByCustomerSorted.show(totalAmountByCustomerSorted.count())

spark.stop()