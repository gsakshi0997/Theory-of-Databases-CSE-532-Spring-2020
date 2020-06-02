from __future__ import print_function
from pyspark import SparkContext, broadcast
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark import SparkContext 
from operator import add

import time
import sys
import os

if __name__== "__main__":

    startTime = time.time()
    if len(sys.argv)!=5:
        print("Usage: SparkCovid19_1.py <input file> <start date> <end date> <output file>", file=sys.stderr)
        sys.exit(-1)

    spark = SparkSession.builder.appName("SparkCovid19_1.py").getOrCreate()
    sc = spark.sparkContext

    st_date=sys.argv[2]
    en_date=sys.argv[3]
    
    if en_date > "2020-04-08" or st_date < "2019-12-31" or st_date > en_date:
        print("Invalid Date Range")
        sys.exit(-1)

    input = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])

    rdd_1=input.map(lambda x: x.split(','))
    rdd_2=rdd_1.filter(lambda x : x[0] >= st_date).filter(lambda x : x[0] <= en_date)
    rdd_3=rdd_2.map(lambda x : (x[1], int(x[3]))).reduceByKey(add).sortByKey()  
    
    output = rdd_3.collect()
    output = sorted(output, key=lambda x:x[0])
    output_file = ''
    
    output_file = open(sys.argv[4] + "/task_1.txt", "a")

    for (country, deaths) in output:
        print("%s %d" % (country, int(deaths)))
        output_file.write("%s %d\n" % (country, deaths))  

    endTime = time.time()    
    print("Execution time for Task 1(Spark)=%i milliseconds"%(endTime-startTime)) 
    spark.stop()