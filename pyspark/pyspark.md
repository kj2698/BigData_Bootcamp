# Abstracting Data with RDDs
In this chapter, we will cover how to work with Apache Spark Resilient Distributed Datasets. You will learn the following recipes:

Creating RDDs

Reading data from files

Overview of RDD transformations

Overview of RDD actions

Pitfalls of using RDDs

# Introduction
Resilient Distributed Datasets (RDDs) are collections of immutable JVM objects that are distributed across an Apache Spark cluster. Data in an RDD is split into chunks based on a key and then dispersed across all the executor nodes. RDDs are highly resilient, that is, there are able to recover quickly from any issues as the same data chunks are replicated across multiple executor nodes. Thus, even if one executor fails, another will still process the data. This allows you to perform your functional calculations against your dataset very quickly by harnessing the power of multiple nodes. RDDs keep a log of all the execution steps applied to each chunk. This, on top of the data replication, speeds up the computations and, if anything goes wrong, RDDs can still recover the portion of the data lost due to an executor error.

While it is common to lose a node in distributed environments (for example, due to connectivity issues, hardware problems), distribution and replication of the data defends against data loss, while data lineage allows the system to recover quickly.

To quickly create an RDD, run PySpark on your machine via the bash terminal, or you can run the same query in a Jupyter notebook. There are two ways to create an RDD in PySpark: you can either use the parallelize() method—a collection (list or an array of some elements) or reference a file (or files) located either locally or through an external source, as noted in subsequent recipes.

The following code snippet creates your RDD (myRDD) using the sc.parallelize() method:

`myRDD = sc.parallelize([('Mike', 19), ('June', 18), ('Rachel',16), ('Rob', 18), ('Scott', 17)])`

To view what is inside your RDD, you can run the following code snippet:

`myRDD.take(5)`

The output is as follows:

`Out[10]: [('Mike', 19), ('June', 18), ('Rachel',16), ('Rob', 18), ('Scott', 17)]`

# Spark context parallelize method
Under the covers, there are quite a few actions that happened when you created your RDD. Let's start with the RDD creation and break down this code snippet:

`myRDD = sc.parallelize([('Mike', 19), ('June', 18), ('Rachel',16), ('Rob', 18), ('Scott', 17)])`
 
Focusing first on the statement in the sc.parallelize() method, we first created a Python list (that is, [A, B, ..., E]) composed of a list of arrays (that is, ('Mike', 19), ('June', 19), ..., ('Scott', 17)). The sc.parallelize() method is the SparkContext's parallelize method to create a parallelized collection. This allows Spark to distribute the data across multiple nodes, instead of depending on a single node to process the data:

![image](https://github.com/kj2698/BigData_Bootcamp/assets/101991863/dff84514-5744-42a7-b27e-43adff9a7a3a)

Now that we have created myRDD as a parallelized collection, Spark can operate against this data in parallel. Once created, the distributed dataset (distData) can be operated on in parallel. For example, we can call myRDD.reduceByKey(add) to add up the grouped by keys of the list;

# .take(...) method
Now that you have created your RDD (myRDD), we will use the take() method to return the values to the console (or notebook cell). We will now execute an RDD action (more information on this in subsequent recipes), take(). Note that a common approach in PySpark is to use collect(), which returns all values in your RDD from the Spark worker nodes to the driver. There are performance implications when working with a large amount of data as this translates to large volumes of data being transferred from the Spark worker nodes to the driver. For small amounts of data (such as this recipe), this is perfectly fine, but, as a matter of habit, you should pretty much always use the take(n) method instead; it returns the first n elements of the RDD instead of the whole dataset. It is a more efficient method because it first scans one partition and uses those statistics to determine the number of partitions required to return the results.
