from pyspark import SparkConf
from pyspark.sql import SparkSession
from operator import add
import sys
from pyspark import SparkContext
 

conf= SparkConf()
conf.setAppName('Assignment3')
conf.set('spark.executor.memory', '2g')
sc= SparkContext(conf= conf)
spark=SparkSession(sc)

def function1(param):
    a = param[0]
    b = param[1]
    w = a[1] * b[1]
    return (a[0], b[0], w)


def function2(param):
    list=[]
    for i in range(len(param)):
        for j in range(i+1, len(param)):
            bc = (param[i],param[j])
            list.append(bc)

    return list
    


df1 = spark.read.format("com.databricks.spark.avro").load("/bigd37/avro_output2.avro")
rdd1 = df1.rdd
rdd9=rdd1.map(lambda (a,b) : (b))
rdd10 = rdd9.map(function2).flatMap(lambda x:[y for y in x])
rdd11 = rdd10.map(function1).map(lambda (x,y,z):((x,y),z)).reduceByKey(add, numPartitions=5)
rdd12 = rdd11.map(lambda (x,y):(y,x)).sortByKey(0,1).map(lambda (x,y):(y,x))
rdd13=rdd12.zipWithIndex().filter(lambda vi: vi[1]<10).keys()

df2=rdd13.toDF()
df2.write.format("com.databricks.spark.avro").save("/bigd37/avro_matrix_output2.avro")






