# Program to handle data where first row is header
from pyspark.sql import SparkSession
from pyspark.sql import functions as func

spark = SparkSession.builder.appName("SparkSQLExerciseGetFriendsByAge").getOrCreate()

people = spark.read.option("header", "true").option("inferSchema", "true")\
    .csv("file:////Users/samratmitra1999/SparkCourse/fakefriends-header.csv")
    
print("Here is our inferred schema:")
people.printSchema()

#Getting the necessary columns and creating a new dataframe
new_df = people.select("age", "friends")
new_df.show()


print("Average number of friends group by age:")
new_df.groupBy("age").avg("friends").orderBy("age").show()

# Formatted more nicely
new_df.groupBy("age").agg(func.round(func.avg("friends"), 2)).sort("age").show()

# With a custom column name
new_df.groupBy("age").agg(func.round(func.avg("friends"), 2)
  .alias("friends_avg")).sort("age").show()


spark.stop()

