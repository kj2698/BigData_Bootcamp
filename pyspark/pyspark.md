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

# Reading data from files
![image](https://github.com/kj2698/BigData_Bootcamp/assets/101991863/e1fc36fd-2d2e-4ac5-a2de-c6dfd867d037)

we will be reading a tab-delimited (or comma-delimited) file, so please ensure that you have a text (or CSV) file available. For your convenience, you can download the airport-codes-na.txt and departuredelays.csv files from https://github.com/drabastomek/learningPySpark/tree/master/Chapter03/flight-data. Ensure your local Spark cluster can access this file (for example, ~/data/flights/airport-codes-na.txt).

Once you start the PySpark shell via the bash terminal (or you can run the same query within Jupyter notebook), execute the following query:
```
myRDD = (
    sc
    .textFile(
        '~/data/flights/airport-codes-na.txt'
        , minPartitions=4
        , use_unicode=True
    ).map(lambda element: element.split("\t"))
)
```

When running the query:

`myRDD.take(5)`

The resulting output is:

`Out[22]:  [[u'City', u'State', u'Country', u'IATA'], [u'Abbotsford', u'BC', u'Canada', u'YXX'], [u'Aberdeen', u'SD', u'USA', u'ABR'], [u'Abilene', u'TX', u'USA', u'ABI'], [u'Akron', u'OH', u'USA', u'CAK']]`

Diving in a little deeper, let's determine the number of rows in this RDD. Note that more information on RDD actions such as count() is included in subsequent recipes:
```
myRDD.count()

# Output
# Out[37]: 527
```
Also, let's find out the number of partitions that support this RDD:
```
myRDD.getNumPartitions()

# Output
# Out[33]: 4
```

The first code snippet to read the file and return values via take can be broken down into its two components: sc.textFile() and map().

# .textFile(...) method
To read the file, we are using SparkContext's textFile() method via this command:
```
(
    sc
    .textFile(
        '~/data/flights/airport-codes-na.txt'
        , minPartitions=4
        , use_unicode=True
    )
)
```
Only the first parameter is required, which indicates the location of the text file as per ~/data/flights/airport-codes-na.txt. There are two optional parameters as well:

minPartitions: Indicates the minimum number of partitions that make up the RDD. The Spark engine can often determine the best number of partitions based on the file size, but you may want to change the number of partitions for performance reasons and, hence, the ability to specify the minimum number.

use_unicode: Engage this parameter if you are processing Unicode data.
Note that if you were to execute this statement without the subsequent map() function, the resulting RDD would not reference the tab-delimiter—basically a list of strings that is:
```
myRDD = sc.textFile('~/data/flights/airport-codes-na.txt')
myRDD.take(5)

# Out[35]:  [u'City\tState\tCountry\tIATA', u'Abbotsford\tBC\tCanada\tYXX', u'Aberdeen\tSD\tUSA\tABR', u'Abilene\tTX\tUSA\tABI', u'Akron\tOH\tUSA\tCAK']
```

# .map(...) method
To make sense of the tab-delimiter with an RDD, we will use the .map(...) function to transform the data from a list of strings to a list of lists:
```
myRDD = (
    sc
    .textFile('~/data/flights/airport-codes-na.txt')
    .map(lambda element: element.split("\t"))
)
```
The key components of this map transformation are:

lambda: An anonymous function (that is, a function defined without a name) composed of a single expression
split: We're using PySpark's split function (within pyspark.sql.functions) to split a string around a regular expression pattern; in this case, our delimiter is a tab (that is, \t)
Putting the sc.textFile() and map() functions together allows us to read the text file and split by the tab-delimiter to produce an RDD composed of a parallelized list of lists collection:
```
Out[22]:  [[u'City', u'State', u'Country', u'IATA'], [u'Abbotsford', u'BC', u'Canada', u'YXX'], [u'Aberdeen', u'SD', u'USA', u'ABR'], [u'Abilene', u'TX', u'USA', u'ABI'], [u'Akron', u'OH', u'USA', u'CAK']]
```

# Partitions and performance
Earlier in this recipe, if we had run sc.textFile() without specifying minPartitions for this dataset, we would only have two partitions:
```
myRDD = (
    sc
    .textFile('/databricks-datasets/flights/airport-codes-na.txt')
    .map(lambda element: element.split("\t"))
)

myRDD.getNumPartitions()

# Output
Out[2]: 2
```
But as noted, if the minPartitions flag is specified, then you would get the specified four partitions (or more):
```
myRDD = (
    sc
    .textFile(
        '/databricks-datasets/flights/airport-codes-na.txt'
        , minPartitions=4
    ).map(lambda element: element.split("\t"))
)

myRDD.getNumPartitions()

# Output
Out[6]: 4
```
A key aspect of partitions for your RDD is that the more partitions you have, the higher the parallelism. Potentially, having more partitions will improve your query performance. For this portion of the recipe, let's use a slightly larger file, departuredelays.csv: 
```
# Read the `departuredelays.csv` file and count number of rows
myRDD = (
    sc
    .textFile('/data/flights/departuredelays.csv')
    .map(lambda element: element.split(","))
)

myRDD.count()

# Output Duration: 3.33s
Out[17]: 1391579

# Get the number of partitions
myRDD.getNumPartitions()

# Output:
Out[20]: 2
```
As noted in the preceding code snippet, by default, Spark will create two partitions and take 3.33 seconds (on my small cluster) to count the 1.39 million rows in the departure delays CSV file.

Executing the same command, but also specifying minPartitions (in this case, eight partitions), you will notice that the count() method completed in 2.96 seconds (instead of 3.33 seconds with eight partitions). Note that these values may be different based on your machine's configuration, but the key takeaway is that modifying the number of partitions may result in faster performance due to parallelization. Check out the following code:
```
# Read the `departuredelays.csv` file and count number of rows
myRDD = (
    sc
    .textFile('/data/flights/departuredelays.csv', minPartitions=8)
    .map(lambda element: element.split(","))
)

myRDD.count()

# Output Duration: 2.96s
Out[17]: 1391579

# Get the number of partitions
myRDD.getNumPartitions()

# Output:
Out[20]: 8
```

# Overview of RDD transformations
As noted in preceding sections, there are two types of operation that can be used to shape data in an RDD: `transformations` and `actions`. A transformation, as the name suggests, transforms one RDD into another. In other words, it takes an existing RDD and transforms it into one or more output RDDs. In the preceding recipes, we had used a map() function, which is an example of a transformation to split the data by its tab-delimiter.

Transformations are lazy (unlike actions). They only get executed when an action is called on an RDD. For example, calling the count() function is an action; more information is available in the following section on actions.

# Getting ready
This recipe will be reading a tab-delimited (or comma-delimited) file, so please ensure that you have a text (or CSV) file available. For your convenience, you can download the airport-codes-na.txt and departuredelays.csv files from https://github.com/drabastomek/learningPySpark/tree/master/Chapter03/flight-data. Ensure your local Spark cluster can access this file (for example, ~/data/flights/airport-codes-na.txt).

If you are running Databricks, the same file is already included in the /databricks-datasets folder; the command is 

myRDD = sc.textFile('/databricks-datasets/flights/airport-codes-na.txt').map(lambda line: line.split("\t"))

Many of the transformations in the next section will use the RDDs airports or flights; let's set them up using this code snippet:
```
# Setup the RDD: airports
airports = (
    sc
    .textFile('~/data/flights/airport-codes-na.txt')
    .map(lambda element: element.split("\t"))
)

airports.take(5)

# Output
Out[11]:  
[[u'City', u'State', u'Country', u'IATA'], 
 [u'Abbotsford', u'BC', u'Canada', u'YXX'], 
 [u'Aberdeen', u'SD', u'USA', u'ABR'], 
 [u'Abilene', u'TX', u'USA', u'ABI'], 
 [u'Akron', u'OH', u'USA', u'CAK']]
```
```
# Setup the RDD: flights
flights = (
    sc
    .textFile('/databricks-datasets/flights/departuredelays.csv')
    .map(lambda element: element.split(","))
)

flights.take(5)

# Output
[[u'date', u'delay', u'distance', u'origin', u'destination'],  
 [u'01011245', u'6', u'602', u'ABE', u'ATL'],  
 [u'01020600', u'-8', u'369', u'ABE', u'DTW'],  
 [u'01021245', u'-2', u'602', u'ABE', u'ATL'],  
 [u'01020605', u'-4', u'602', u'ABE', u'ATL']]
 ```

# How to do it...
In this section, we list common Apache Spark RDD transformations and code snippets. A more complete list can be found at https://spark.apache.org/docs/latest/rdd-programming-guide.html#transformations, https://spark.apache.org/docs/latest/api/python/pyspark.html#pyspark.RDD and https://training.databricks.com/visualapi.pdf.

The transformations include the following common tasks:
```
Removing the header line from your text file: zipWithIndex()
Selecting columns from your RDD: map()
Running a WHERE (filter) clause: filter()
Getting the distinct values: distinct()
Getting the number of partitions: getNumPartitions()
Determining the size of your partitions (that is, the number of elements within each partition): mapPartitionsWithIndex()
```
