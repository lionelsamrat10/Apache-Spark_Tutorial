import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("RDD_sample")
sc = SparkContext(conf = conf)

#nums = parallelize([1,2,3,4])