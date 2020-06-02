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
    if len(sys.argv) != 4 :
        print("Usage: SparkCovid19_2.py <input file> <cache file> <output file>", file=sys.stderr)
        sys.exit(-1)
    
    spark = SparkSession.builder.appName("SparkCovid19_2.py").getOrCreate()
    sc = spark.sparkContext
    
    pop = spark.read.format("CSV").option("header","true").load(sys.argv[2]).collect() 
    pop_map = {}

    for i in range(0, len(pop)):
        pop_map[pop[i]['location']] = pop[i]['population']
    
    broadPop = sc.broadcast(pop_map).value

    input = spark.read.format("CSV").option("header","true").load(sys.argv[1])
    Input = input.rdd.map(lambda r: r)
    counts = Input.map(lambda x: (x["location"], int(x["new_cases"]))).reduceByKey(add)
    output = counts.collect()
    output = sorted(output, key=lambda x:x[0])
    output_file = ''
    
    output_file = open(sys.argv[3] + "/task_2.txt", "a")

    for (location, cases) in output:
        if location in broadPop and broadPop[location] != None:
            res = float(float(cases * 1000000) / float(broadPop[location])) 
            print("%s %f" % (location, res))
            output_file.write("%s %f\n" % (location, res))

    endTime = time.time()
    millis = endTime - startTime
    print("Execution Time for Task2(Spark) = %i milliseconds"%(endTime-startTime))
    spark.stop()