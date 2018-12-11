import sys
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark import SparkConf, SparkContext
from operator import add

conf= SparkConf()
conf.setAppName('Assignment3')
conf.set('spark.executor.memory', '2g')
sc= SparkContext(conf= conf)
spark=SparkSession(sc)

def lower_case(l):
    l=l.lower()
    return l

def weight(w):
    count = len(w[1].split())
    return (w[0],w[1],count)
    
def function(param):
    weight = float(param[3])/param[1]
    return (param[0],param[2],weight)

#Question1
myfile=sc.textFile('/cosc6339_hw2/gutenberg-500/')
lower=myfile.map(lower_case)
stopwords=["the","a","an","is","am","are","on","to","in","or","i","on","by","it","if","he","she","as","are","will","and",".",",",";","-"]
allwords = lower.flatMap(lambda line:line.split())
filterwords = allwords.filter(lambda x: x not in stopwords)
wordcounts= filterwords.map(lambda w: (w, 1) )
counts = wordcounts.reduceByKey(add, numPartitions=5).map(lambda(x,y):(y,x)).sortByKey(0,1).map(lambda(x,y):(y,x))
filtered_data=counts.zipWithIndex().filter(lambda vi: vi[1]<1000).keys()

#Question2
rdd0 = sc.wholeTextFiles("/cosc6339_hw2/gutenberg-500/")
rdd1 = rdd0.map(weight)
rdd2 = rdd1.flatMap(lambda (path,contents,count):[(path,count,word) for word in contents.lower().split()])
rdd3 = rdd2.map(lambda (file,count,word): (word,count,file))
rdd4 = rdd3.map(lambda (word,count,file): ((word,count,file), 1)).reduceByKey(lambda a,b: a+b,numPartitions=5)


rdd5 = rdd4.map(lambda ((word,count,file), num):(word,count,file,num)).map(function)
rdd6 = rdd5.map(lambda (word,file,n):(word,(file,n)))
rdd7 = rdd6.groupByKey(numPartitions=5).map(lambda x:(x[0],list(x[1])))
df1=rdd7.toDF()
df1.write.format("com.databricks.spark.avro").save("/bigd37/avro_output2.avro")

