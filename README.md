# Apache-Spark

Just like main function in java and other programming languages, spark has a main entry point called spark context(SC) which resides at the master machine. 
When we write a SC command the blocks of files are copied in the memory from different clusters. These blocks are combinely called as RDD(Resilient distributed data). hence RDD is the distributed data sitting in the memory. Resilient means reliable.
RDD's are immutable i.e. processing is done using a block B and new block is created say B' to store the results in memory. When we use RDD we are only doing input output operation once, after that it is always in memory, hence it is faster.Which is why spark is faster than MapReduce. Even when the RAM is slow spark can handle it by pipelining concept. 
Spark doesn't have print statement,it uses collect in its place. This step of collecting or printing data is called action step. The step before this was transformation steps where we were making RDD's. 
Spark can work on real time data where as Map Reduce only works on historic data. 
When the data blocks are loaded in memory and form RDD's, they are initially empty. Once the collect(action) statement is called, the data is loaded on each block in RDD's. This is called Lazy Evaluation. 

The space of RAM where we keep all the data blocks is called as an Executor. In spark we do not have a data node rather it is called as a worker node. The code you are executing in RDD it stored in memory in Task.

Spark streaming is used for processing real-time streaming data. It provides high through-put and fault tolerence stream processing of live data streams. 

Spark core is used to create RDD by combining many blocks of data. 
When we get the data file we store it in blocks and whenever required to transfer a copy of it to memory to perform actions and work on it. All this is done using spark streaming. 

SparkSQL is faster than Hadoop.

MlLib can handle supervised and unsupervised learning. It has classification,collaborative filtering etc. Classification for example is like spam folder in emails. The mails are automatically classified as spam depending upon certain characteristic. collaborative filtering for example is like the suggestions we get on amazon depending on our past purchases.

GraphX, we have vertices or leave and we have edges which connect the vertex and show relationship. It can be directed or un-directed. Examples like searching for shotest path in google maps, or uber app finding nearest driver. 
