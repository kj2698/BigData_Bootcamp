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

# .map(...) transformation
The map(f) transformation returns a new RDD formed by passing each element through a function, f.

Look at the following code snippet:
```
# Use map() to extract out the first two columns
airports.map(lambda c: (c[0], c[1])).take(5)
This will produce the following output:

# Output
[(u'City', u'State'),  
 (u'Abbotsford', u'BC'),  
 (u'Aberdeen', u'SD'),

 (u'Abilene', u'TX'),  
 (u'Akron', u'OH')] 
```

# .filter(...) transformation
The filter(f)  transformation returns a new RDD based on selecting elements for which the f function returns true. Therefore, look at the following code snippet:

## User filter() to filter where second column == "WA"
```
(
    airports
    .map(lambda c: (c[0], c[1]))
    .filter(lambda c: c[1] == "WA")
    .take(5)
)
This will produce the following output:

# Output
[(u'Bellingham', u'WA'),
 (u'Moses Lake', u'WA'),  
 (u'Pasco', u'WA'),  
 (u'Pullman', u'WA'),  
 (u'Seattle', u'WA')]
```

# .flatMap(...) transformation
The flatMap(f) transformation is similar to map, but the new RDD flattens out all of the elements (that is, a sequence of events). Let's look at the following snippet:
```
# Filter only second column == "WA", 
# select first two columns within the RDD,
# and flatten out all values
(
    airports
    .filter(lambda c: c[1] == "WA")
    .map(lambda c: (c[0], c[1]))
    .flatMap(lambda x: x)
    .take(10)
)
The preceding code will produce the following output:

# Output
[u'Bellingham',  
 u'WA',  
 u'Moses Lake',  
 u'WA',  
 u'Pasco',  
 u'WA',  
 u'Pullman',  
 u'WA',  
 u'Seattle',  
 u'WA']
```

# .distinct() transformation
The distinct() transformation returns a new RDD containing the distinct elements of the source RDD. So, look at the following code snippet:
```
# Provide the distinct elements for the 
# third column of airports representing
# countries
(
    airports
    .map(lambda c: c[2])
    .distinct()
    .take(5)
)
This will return the following output:

# Output
[u'Canada', u'USA', u'Country']
```

# .sample(...) transformation
The sample(withReplacement, fraction, seed) transformation samples a fraction of the data, with or without replacement (the withReplacement parameter), based on a random seed. 

Look at the following code snippet:
```
# Provide a sample based on 0.001% the
# flights RDD data specific to the fourth
# column (origin city of flight)
# without replacement (False) using random
# seed of 123 
(
    flights
    .map(lambda c: c[3])
    .sample(False, 0.001, 123)
    .take(5)
)
We can expect the following result:

# Output
[u'ABQ', u'AEX', u'AGS', u'ANC', u'ATL']
```

# .join(...) transformation
The join(RDD') transformation returns an RDD of (key, (val_left, val_right)) when calling RDD (key, val_left) and RDD (key, val_right). Outer joins are supported through left outer join, right outer join, and full outer join. 

Look at the following code snippet:
```
# Flights data
#  e.g. (u'JFK', u'01010900')
flt = flights.map(lambda c: (c[3], c[0]))

# Airports data
# e.g. (u'JFK', u'NY')
air = airports.map(lambda c: (c[3], c[1]))

# Execute inner join between RDDs
flt.join(air).take(5)
This will give you the following result:

# Output
[(u'JFK', (u'01010900', u'NY')),  
 (u'JFK', (u'01011200', u'NY')),  
 (u'JFK', (u'01011900', u'NY')),  
 (u'JFK', (u'01011700', u'NY')),  
 (u'JFK', (u'01010800', u'NY'))]
```

# .repartition(...) transformation
The repartition(n) transformation repartitions the RDD into n partitions by randomly reshuffling and uniformly distributing data across the network. As noted in the preceding recipes, this can improve performance by running more parallel threads concurrently. Here's a code snippet that does precisely that:
```
# The flights RDD originally generated has 2 partitions 
flights.getNumPartitions()

# Output
2 

# Let's re-partition this to 8 so we can have 8 
# partitions
flights2 = flights.repartition(8)

# Checking the number of partitions for the flights2 RDD
flights2.getNumPartitions()

# Output
8
```

# .zipWithIndex() transformation
The zipWithIndex() transformation appends (or ZIPs) the RDD with the element indices. This is very handy when wanting to remove the header row (first row) of a file.

Look at the following code snippet:
```
# View each row within RDD + the index 
# i.e. output is in form ([row], idx)
ac = airports.map(lambda c: (c[0], c[3]))
ac.zipWithIndex().take(5)
This will generate this result:

# Output
[((u'City', u'IATA'), 0),  
 ((u'Abbotsford', u'YXX'), 1),  
 ((u'Aberdeen', u'ABR'), 2),  
 ((u'Abilene', u'ABI'), 3),  
 ((u'Akron', u'CAK'), 4)]
To remove the header from your data, you can use the following code:

# Using zipWithIndex to skip header row
# - filter out row 0
# - extract only row info
(
    ac
    .zipWithIndex()
    .filter(lambda (row, idx): idx > 0)
    .map(lambda (row, idx): row)
    .take(5)
)
The preceding code will skip the header, as shown as follows:

# Output
[(u'Abbotsford', u'YXX'),  
 (u'Aberdeen', u'ABR'),  
 (u'Abilene', u'ABI'),  
 (u'Akron', u'CAK'),  
 (u'Alamosa', u'ALS')]
```

# .reduceByKey(...) transformation
The reduceByKey(f) transformation reduces the elements of the RDD using f by the key. The f function should be commutative and associative so that it can be computed correctly in parallel.

Look at the following code snippet:
```
r1=sc.textFile('/config/workspace/departuredelays.csv',minPartitions=6)
r1.take(5)
['date,delay,distance,origin,destination', '01011245,6,602,ABE,ATL', '01020600,-8,369,ABE,DTW', '01021245,-2,602,ABE,ATL', '01020605,-4,602,ABE,ATL']

r1_s1=r1.map(lambda e:e.split(','))
r1_s1.take(5)
[['date', 'delay', 'distance', 'origin', 'destination'], ['01011245', '6', '602', 'ABE', 'ATL'], ['01020600', '-8', '369', 'ABE', 'DTW'], ['01021245', '-2', '602', 'ABE', 'ATL'], ['01020605', '-4', '602', 'ABE', 'ATL']]

r1_s2=r1_s1.zipWithIndex()
r1_s2.take(5)
[(['date', 'delay', 'distance', 'origin', 'destination'], 0), (['01011245', '6', '602', 'ABE', 'ATL'], 1), (['01020600', '-8', '369', 'ABE', 'DTW'], 2), (['01021245', '-2', '602', 'ABE', 'ATL'], 3), (['01020605', '-4', '602', 'ABE', 'ATL'], 4)]

r1_s3=r1_s2.filter(lambda e:e[1]>0).map(lambda e:e[0]).map(lambda e:(e[3],int(e[1])))
r1_s3.take(5)
[('ABE', 6), ('ABE', -8), ('ABE', -2), ('ABE', -4), ('ABE', -4)]

r1_s4=r1_s3.reduceByKey(lambda x,y:x+y)
r1_s4.take(10)
[('ABE', 5113), ('ACT', 392), ('ADQ', -254), ('AEX', 10193), ('AUS', 108638), ('BFL', 4022), ('BHM', 44355), ('BMI', 7817), ('BQN', 3943), ('CEC', 2832)]
```

## reduceByKey is only working for (K,V) pair of values, if we try for (K,V1,V2) its failing.

# .sortByKey(...) transformation
The sortByKey(asc) transformation orders (key, value) RDD by key and returns an RDD in ascending or descending order. Look at the following code snippet:

```
r1=sc.textFile('/config/workspace/departuredelays.csv',minPartitions=6)
r1_s1=r1.map(lambda e:e.split(','))
r1_s2=r1_s1.zipWithIndex()
r1_s3=r1_s2.filter(lambda e:e[1]>0).map(lambda e:e[0]).map(lambda e:(e[3],int(e[1])))
r1_s4=r1_s3.reduceByKey(lambda x,y:x+y).sortByKey()
r1_s4.take(10)

[('ABE', 5113), ('ABI', 5128), ('ABQ', 64422), ('ABY', 1554), ('ACT', 392), ('ACV', 8429), ('ADQ', -254), ('AEX', 10193), ('AGS', 5003), ('ALB', 22362)]
```

# .union(...) transformation
The union(RDD) transformation returns a new RDD that is the union of the source and argument RDDs. Look at the following code snippet:
```
# Create `a` RDD of Washington airports
a = (
    airports
    .zipWithIndex()
    .filter(lambda (row, idx): idx > 0)
    .map(lambda (row, idx): row)
    .filter(lambda c: c[1] == "WA")
)

# Create `b` RDD of British Columbia airports
b = (
    airports
    .zipWithIndex()
    .filter(lambda (row, idx): idx > 0)
    .map(lambda (row, idx): row)
    .filter(lambda c: c[1] == "BC")
)

# Union WA and BC airports
a.union(b).collect()
This will generate the following output:

# Output
[[u'Bellingham', u'WA', u'USA', u'BLI'],
 [u'Moses Lake', u'WA', u'USA', u'MWH'],
 [u'Pasco', u'WA', u'USA', u'PSC'],
 [u'Pullman', u'WA', u'USA', u'PUW'],
 [u'Seattle', u'WA', u'USA', u'SEA'],
...
 [u'Vancouver', u'BC', u'Canada', u'YVR'],
 [u'Victoria', u'BC', u'Canada', u'YYJ'], 
 [u'Williams Lake', u'BC', u'Canada', u'YWL']]
```

# .mapPartitionsWithIndex(...) transformation
The mapPartitionsWithIndex(f) is similar to map but runs the f function separately on each partition and provides an index of the partition. It is useful to determine the data skew within partitions (check the following snippet):
```
# Source: https://stackoverflow.com/a/38957067/1100699
def partitionElementCount(idx, iterator):
  count = 0
  for _ in iterator:
    count += 1
  return idx, count

# Use mapPartitionsWithIndex to determine 
flights.mapPartitionsWithIndex(partitionElementCount).collect()
The preceding code will produce the following result:

# Output
[0,  
 174293,  
 1,  
 174020,  
 2,  
 173849,  
 3,  
 174006,  
 4,  
 173864,  
 5,  
 174308,  
 6,  
 173620,  
 7,  
 173618]
```

# .collect() action
We have also cautioned you about using this action; collect() returns all of the elements from the workers to the driver. Thus, look at the following code:
```
# Return all airports elements
# filtered by WA state
airports.filter(lambda c: c[1] == "WA").collect()
This will generate the following output:
# Output
[[u'Bellingham', u'WA', u'USA', u'BLI'],  [u'Moses Lake', u'WA', u'USA', u'MWH'],  [u'Pasco', u'WA', u'USA', u'PSC'],  [u'Pullman', u'WA', u'USA', u'PUW'],  [u'Seattle', u'WA', u'USA', u'SEA'],  [u'Spokane', u'WA', u'USA', u'GEG'],  [u'Walla Walla', u'WA', u'USA', u'ALW'],  [u'Wenatchee', u'WA', u'USA', u'EAT'],  [u'Yakima', u'WA', u'USA', u'YKM']]
```

# .reduce(...) action
The reduce(f) action aggregates the elements of an RDD by f. The f function should be commutative and associative so that it can be computed correctly in parallel. Look at the following code:
```
# Calculate the total delays of flights
# between SEA (origin) and SFO (dest),
# convert delays column to int 
# and summarize
flights\
 .filter(lambda c: c[3] == 'SEA' and c[4] == 'SFO')\
 .map(lambda c: int(c[1]))\
 .reduce(lambda x, y: x + y)
This will produce the following result:

# Output
22293
We need to make an important note here, however. When using reduce(), the reducer function needs to be associative and commutative; that is, a change in the order of elements and operands does not change the result.

Associativity rule: (6 + 3) + 4 = 6 + (3 + 4)
Commutative rule:  6 + 3 + 4 = 4 + 3 + 6

Error can occur if you ignore the aforementioned rules.

As an example, see the following RDD (with one partition only!):

data_reduce = sc.parallelize([1, 2, .5, .1, 5, .2], 1)
Reducing data to divide the current result by the subsequent one, we would expect a value of 10:

works = data_reduce.reduce(lambda x, y: x / y)
Partitioning the data into three partitions will produce an incorrect result:

data_reduce = sc.parallelize([1, 2, .5, .1, 5, .2], 3) data_reduce.reduce(lambda x, y: x / y)
It will produce 0.004.
```

# .count() action
The count() action returns the number of elements in the RDD. See the following code:

(
    flights
    .zipWithIndex()
    .filter(lambda (row, idx): idx > 0)
    .map(lambda (row, idx): row)
    .count()
)
This will produce the following result:
```
# Output
1391578
```

# .saveAsTextFile(...) action
The saveAsTextFile() action saves your RDD into a text file; note that each partition is a separate file. See the following snippet:
```
# Saves airports as a text file
#   Note, each partition has their own file

# saveAsTextFile
airports.saveAsTextFile("/tmp/denny/airports")
This will actually save the following files:

# Review file structure
# Note that `airports` is a folder with two
# files (part-zzzzz) as the airports RDD is 
# comprised of two partitions.
/tmp/denny/airports/_SUCCESS
/tmp/denny/airports/part-00000
/tmp/denny/airports/part-00001
```

# How it works...
Recall that actions return a value to the driver after running a computation on the dataset, typically on the workers. Examples of some Spark actions include count() and take(); for this section, we will be focusing on reduceByKey():
```
# Determine delays by originating city
# - remove header row via zipWithIndex() 
#   and map() 
flights.zipWithIndex()\
  .filter(lambda (row, idx): idx > 0)\
  .map(lambda (row, idx): row)\
  .map(lambda c: (c[3], int(c[1])))\
  .reduceByKey(lambda x, y: x + y)\
  .take(5)

# Output
[(u'JFK', 387929),  
 (u'MIA', 169373),  
 (u'LIH', -646),  
 (u'LIT', 34489),  
 (u'RDM', 3445)]
```
To better understand what is happening when running this join, let's review the Spark UI. Every Spark Session launches a web-based UI, which is, by default, on port 4040, for example, http://localhost:4040. It includes the following information:

A list of scheduler stages and tasks
A summary of RDD sizes and memory usage
Environmental information
Information about the running executors
For more information, please refer to the Apache Spark Monitoring documentation page at https://spark.apache.org/docs/latest/monitoring.html.

To dive deeper into Spark internals, a great video is Patrick Wendell's Tuning and Debugging in Apache Spark video, which is available at https://www.youtube.com/watch?v=kkOG_aJ9KjQ.
Here is the DAG visualization of the preceding code snippet, which is executed when the reduceByKey() action is called; note that Job 14 represents only the reduceByKey() of part the DAG. A previous job had executed and returned the results based on the zipWithIndex() transformation, which is not included in Job 14:
![image](https://github.com/kj2698/BigData_Bootcamp/assets/101991863/96a92228-56cc-4d35-a94b-320a214b00ca)

Digging further into the tasks that make up each stage, notice that the bulk of the work is done in Stage 18. Note the eight parallel tasks that end up processing data, from extracting it from the file (/tmp/data/departuredelays.csv) to executing reduceByKey() in parallel:
![image](https://github.com/kj2698/BigData_Bootcamp/assets/101991863/703a94a7-acfa-4d52-9458-858ff50e7f8d)

A few important callouts are as follows:

Spark's reduceByKey(f) assumes the f function is commutative and associative so that it can be computed correctly in parallel. As noted in the Spark UI, all eight tasks are processing the data extraction (sc.textFile) and reduceByKey() in parallel, providing faster performance.

As noted in the Getting ready section of this recipe, we executed sc.textFile($fileLocation, minPartitions=8)... This forced the RDD to have eight partitions (at least eight partitions), which translated to eight tasks being executed in parallel:
![image](https://github.com/kj2698/BigData_Bootcamp/assets/101991863/06be1f2c-2c9f-44c2-9ec6-2eeff47f73a9)

Now that you have executed reduceByKey(), we will run take(5), which executes another stage that shuffles the eight partitions from the workers to the single driver node; that way, the data can be collected for viewing in the console. 

# Pitfalls of using RDDs
The key concern associated with using RDDs is that they can take a lot of time to master. The flexibility of running functional operators such as map, reduce, and shuffle allows you to perform a wide variety of transformations against your data. But with this power comes great responsibility, and it is potentially possible to write code that is inefficient, such as the use of GroupByKey; more information can be found in Avoid GroupByKey at https://databricks.gitbooks.io/databricks-spark-knowledge-base/content/best_practices/prefer_reducebykey_over_groupbykey.html.

Generally, you will typically have slower performance when using RDDs compared to Spark DataFrames, as noted in the following diagram:
![image](https://github.com/kj2698/BigData_Bootcamp/assets/101991863/91770641-a49f-449a-90e5-94948a4c8fd5)

The reason RDDs are slow—especially within the context of PySpark—is because whenever a PySpark program is executed using RDDs, there is a potentially large overhead to execute the job. As noted in the following diagram, in the PySpark driver, the Spark Context uses Py4j to launch a JVM using JavaSparkContext. Any RDD transformations are initially mapped to PythonRDD objects in Java.

Once these tasks are pushed out to the Spark worker(s), PythonRDD objects launch Python subprocesses using pipes to send both code and data to be processed in Python:
![image](https://github.com/kj2698/BigData_Bootcamp/assets/101991863/72ee4292-0111-476e-9fa0-e84c2d6a4ede)

While this approach allows PySpark to distribute the processing of the data to multiple Python subprocesses on multiple workers, as you can see, there is a lot of context switching and communications overhead between Python and the JVM.

An excellent resource on PySpark performance is Holden Karau’s Improving PySpark Performance: Spark Performance Beyond the JVM at http://bit.ly/2bx89bn.

This is even more apparent when using Python UDFs, as the performance is significantly slower because all of the data will need to be transferred to the driver prior to using a Python UDF. Note that vectorized UDFs were introduced as part of Spark 2.3 and will improve PySpark UDF performance. For more information, please refer to Introducing Vectorized UDFs for PySpark at https://databricks.com/blog/2017/10/30/introducing-vectorized-udfs-for-pyspark.html.

# Getting ready
As in the previous sections, let's make use of the flights dataset and create an RDD and a DataFrame against this dataset:
```
## Create flights RDD
flights = sc.textFile('/databricks-datasets/flights/departuredelays.csv')\
  .map(lambda line: line.split(","))\
  .zipWithIndex()\
  .filter(lambda (row, idx): idx > 0)\
  .map(lambda (row, idx): row)

# Create flightsDF DataFrame
flightsDF = spark.read\
  .options(header='true', inferSchema='true')
  .csv('~/data/flights/departuredelays.csv')
flightsDF.createOrReplaceTempView("flightsDF")
```

## How to do it...
In this section, we will run the same group by statement—one via an RDD using reduceByKey(), and one via a DataFrame using Spark SQL GROUP BY. For this query, we will sum the time delays grouped by originating city and sort according to the originating city:
```
# RDD: Sum delays, group by and order by originating city
flights.map(lambda c: (c[3], int(c[1]))).reduceByKey(lambda x, y: x + y).sortByKey().take(50)

# Output (truncated)
# Duration: 11.08 seconds
[(u'ABE', 5113),  
 (u'ABI', 5128),  
 (u'ABQ', 64422),  
 (u'ABY', 1554),  
 (u'ACT', 392),
 ... ]
```

For this particular configuration, it took 11.08 seconds to extract the columns, execute reduceByKey() to summarize the data, execute sortByKey() to order it, and then return the values to the driver:
```
# RDD: Sum delays, group by and order by originating city
spark.sql("select origin, sum(delay) as TotalDelay from flightsDF group by origin order by origin").show(50)

# Output (truncated)
# Duration: 4.76s
+------+----------+ 
|origin|TotalDelay| 
+------+----------+ 
| ABE  |      5113| 
| ABI  |      5128|
| ABQ  |     64422| 
| ABY  |      1554| 
| ACT  |       392|
...
+------+----------+ 
```
There are many advantages of Spark DataFrames, including, but not limited to the following:

You can execute Spark SQL statements (not just through the Spark DataFrame API)
There is a schema associated with your data so you can specify the column name instead of position
In this configuration and example, the query completes in 4.76 seconds, while RDDs complete in 11.08 seconds

It is impossible to improve your RDD query by specifying minPartitions within sc.textFile() when originally loading the data to increase the number of partitions:
flights = sc.textFile('/databricks-datasets/flights/departuredelays.csv', minPartitions=8), ...
`flights = sc.textFile('/databricks-datasets/flights/departuredelays.csv', minPartitions=8), ...`
For this configuration, the same query returned in 6.63 seconds. While this approach is faster, its still slower than DataFrames; in general, DataFrames are faster out of the box with the default configuration. 

## How it works...
To better understand the performance of the previous RDD and DataFrame, let's return to the Spark UI. For starters, when we run the flights RDD query, three separate jobs are executed, as can be seen in Databricks Community Edition in the following screenshot:

![image](https://github.com/kj2698/BigData_Bootcamp/assets/101991863/409440bb-3e92-4952-b147-b38de74c5e26)

Each of these jobs spawn their own set of stages to initially read the text (or CSV) file, execute  reduceByKey(), and execute the sortByKey() functions:

![image](https://github.com/kj2698/BigData_Bootcamp/assets/101991863/f1b5f59d-42d2-4888-a010-d15c7b13d6b5)

With two additional jobs to complete the sortByKey() execution:
![image](https://github.com/kj2698/BigData_Bootcamp/assets/101991863/777cf0ff-bcb8-4d71-8b84-26d09fa1f7c3)
![image](https://github.com/kj2698/BigData_Bootcamp/assets/101991863/9b157400-bec2-4373-a050-856d1ab96c44)

As can be observed, by using RDDs directly, there can potentially be a lot of overhead, generating multiple jobs and stages to complete a single query.

In the case of Spark DataFrames, for this query it is much simpler for it to consist of a single job with two stages. Note that the Spark UI has a number of DataFrame-specific set tasks, such as WholeStageCodegen and Exchange, that significantly improve the performance of Spark dataset and DataFrame queries. More information about the Spark SQL engine catalyst optimizer can be found in the next chapter.
![image](https://github.com/kj2698/BigData_Bootcamp/assets/101991863/907fbc99-5e4a-4b50-9293-1d037167d933)


![image](https://github.com/kj2698/BigData_Bootcamp/assets/101991863/865f244f-32b4-4470-b4ba-6e615a3901dc)


# Data Analysis with Python and PySpark
## Jonathan Rioux

![image](https://github.com/kj2698/BigData_Bootcamp/assets/101991863/92a36877-e367-4ed3-bdb6-f63f2961caf4)

An RDD versus a data frame. In the RDD, we think of each record as an independent entity. With the data frame, we mostly interact with columns, performing functions on them. We still can access the rows of a data frame, via RDD, if necessary.

# 1.1 What is PySpark?
What’s in a name? Actually, quite a lot. Just by separating PySpark in two, you can already deduce that this will be related to Spark and Python. And you would be right!

At its core, PySpark can be summarized as being the Python API to Spark. While this is an accurate definition, it doesn’t give much unless you know the meaning of Python and Spark. Still, let’s break down the summary definition by first answering “What is Spark?” With that under our belt, we then will look at why Spark becomes especially powerful when combined with Python and its incredible array of analytical (and machine learning) libraries.

# 1.1.2 PySpark = Spark + Python
PySpark provides an entry point to Python in the computational model of Spark. Spark itself is coded in Scala.2 The authors did a great job of providing a coherent interface between languages while preserving the idiosyncrasies of each language where appropriate. It will, therefore, be quite easy for a Scala/Spark programmer to read your PySpark program, as well as for a fellow Python programmer who hasn’t jumped into the deep end (yet).

Python is a dynamic, general-purpose language, available on many platforms and for a variety of tasks. Its versatility and expressiveness make it an especially good fit for PySpark. The language is one of the most popular for a variety of domains, and currently it is a major force in data analysis and science. The syntax is easy to learn and read, and the number of libraries available means that you’ll often find one (or more!) that’s just the right fit for your problem.

# 1.2 Your very own factory: How PySpark works
In this section, we cover how Spark processes a program. It can be a little odd to present the workings and underpinnings of a system that we claimed, a few paragraphs ago, hides that complexity. Still, it is important to have a working knowledge of how Spark is set up, how it manages data, and how it optimizes queries. With this, you will be able to reason with the system, improve your code, and figure out quickly when it doesn’t perform the way you want.

If we keep the factory analogy, we can imagine that the cluster of computers Spark is sitting on is the building. If we look at figure 1.1, we can see two different ways to interpret a data factory. On the left, we see how it looks from the outside: a cohesive unit where projects come in and results come out. This is how it will appear to you most of the time. Under the hood, it looks more like what’s on the right: you have some workbenches that some workers are assigned to. The workbenches are like the computers in our Spark cluster: there is a fixed amount of them. Some modern Spark implementations, such as Databricks (see appendix B), allow for auto-scaling the number of machines at runtime. Some require more planning, especially if you run on the premises and own your hardware. The workers are called executors in Spark’s literature: they perform the actual work on the machines/nodes.

![image](https://github.com/kj2698/BigData_Bootcamp/assets/101991863/bf8c31f0-7ebd-45c6-b815-c4750245ae54)

Figure 1.1 A totally relatable data factory, outside and in. Ninety percent of the time we care about the whole factory, but knowing how it’s laid out helps when reflecting on our code performance.

One of the little workers looks spiffier than the other. That top hat definitely makes him stand out from the crowd. In our data factory, he’s the manager of the work floor. In Spark terms, we call this the master.4 The master here sits on one of the workbenches/machines, but it can also sit on a distinct machine (or even your computer!) depending on the cluster manager and deployment mode. The role of the master is crucial to the efficient execution of your program, so section 1.2.2 is dedicated to this.

# 1.2.1 Some physical planning with the cluster manager
Upon reception of the task, which is called a driver program in the Spark world, the factory starts running. This doesn’t mean that we get straight to processing. Before that, the cluster needs to plan the capacity it will allocate for your program. The entity or program taking care of this is aptly called the cluster manager. In our factory, this cluster manager will look at the workbenches with available space and secure as many as necessary, and then start hiring workers to fill the capacity. In Spark, it will look at the machines with available computing resources and secure what’s necessary before launching the required number of executors across them.

`NOTE` Spark provides its own cluster manager, called Standalone, but can also play well with other ones when working in conjunction with Hadoop or another big data platform. If you read about YARN, Mesos, or Kubernetes in the wild, know that they are used (as far as Spark is concerned) as cluster managers.

Any directions about capacity (machines and executors) are encoded in a SparkContext representing the connection to our Spark cluster. If our instructions don’t mention any specific capacity, the cluster manager will allocate the default capacity prescribed by our Spark installation.

As an example, let’s try the following operation. Using the same sample.csv file in listing 1.1 (available in the book’s repository), let’s compute a simplified version of the program: return the arithmetic average of the values of old_column. Let’s assume that our Spark instance has four executors, each working on its own worker node. The data processing will be approximately split between the four executors: each will have a small portion of the data frame that it will work with.

```
less data/list_of_numbers/sample.csv
 
 
old_column
1
4
4
5
7
7
7
10
14
1
4
8
```
Figure 1.2 depicts one way that PySpark could process the average of our old_column in our small data frame. I chose the average because it is not trivially distributable, unlike the sum or the count, where you sum the intermediate values from each worker. In the case of computing the average, each worker independently computes the sum of the values and their counts before moving the result—not all the data!—over to a single worker (or the master directly, when the intermediate result is really small) that will process the aggregation into a single number, the average.

For a simple example like this, mapping the thought process of PySpark is an easy and fun exercise. The size of our data and the complexity of our programs will grow and will get more complicated, and we will not be able to easily map our code to exact physical steps performed by our Spark instance. Chapter 11 covers the mechanism Spark uses to give us visibility into the work performed as well as the health of our factory.

![image](https://github.com/kj2698/BigData_Bootcamp/assets/101991863/e1765a7f-a5dc-45ff-a430-81ef9a6c4696)

Figure 1.2 Computing the average of our small data frame, PySpark style: each worker works on a distinct piece of data. As necessary, the data gets moved/shuffled around to complete the instructions.

This section took a simple example—computing the average of a data frame of numbers—and we mapped a blueprint of the physical steps performed by Spark to give us the right answer. In the next section, we get to one of Spark’s best, and most misunderstood, features: laziness. In the case of big data analysis, hard work pays off, but smart work is better!

Some language convention: Data frame vs. DataFrame

Since this book will talk about data frames more than anything else, I prefer using the noncapitalized nomenclature (i.e., “data frame”). I find this more readable than using capital letters or even “dataframe” without a space.

When referring to the PySpark object directly, I’ll use DataFrame but with a fixed-width font. This will help differentiate between “data frame” the concept and DataFrame the object.

# 1.2.2 A factory made efficient through a lazy leader
This section introduces one of the most fundamental aspects of Spark: its lazy evaluation capabilities. In my time teaching PySpark and troubleshooting data scientists’ programs, I would say that laziness is the concept in Spark that creates the most confusion. It’s a real shame because laziness is (in part) how Spark achieves its incredible processing speed. By understanding at a high level how Spark makes laziness work, you will be able to explain a lot of its behavior and better tune for performance.

Just like in a large-scale factory, you don’t go to each employee and give them a list of tasks. No, here, the master/manager is responsible for the workers. The driver is where the action happens. Think of a driver as a floor lead: you provide them your list of steps and let them deal with it. In Spark, the driver/floor lead takes your instructions (carefully written in Python code), translates them into Spark steps, and then processes them across the worker. The driver also manages which worker/table has which slice of the data, and makes sure you don’t lose some bits in the process. The executor/factory worker sits atop the workers/tables and performs the actual work on the data.

As a summary:

The master is like the factory owner, allocating resources as needed to complete the jobs.

The driver is responsible for completing a given job. It requests resources from the master as needed.

A worker is a set of computing/memory resources, like a workbench in our factory.

Executors sit atop a worker and perform the work sent by the driver, like employees at a workbench.

We’ll review the terminology in practice in chapter 11.

Taking the example of listing 1.1 and breaking each instruction one by one, PySpark won’t start performing the work until the write instruction. If you use regular Python or a pandas data frame, which are not lazy (we call this eager evaluation), each instruction is performed one by one as it’s being read.

Your floor lead/driver has all the qualities a good manager has: it’s smart, cautious, and lazy. Wait, what? You read me right. Laziness in a programming context—and, one could argue, in the real world too—can be a very good thing. Every instruction you’re providing in Spark can be classified into two categories: transformations and actions. Actions are what many programming languages would consider I/O. The most typical actions are the following:

Printing information on the screen

Writing data to a hard drive or cloud bucket

Counting the number of records

In Spark, we’ll see those instructions most often via the show(), write(), and count() methods on a data frame.

![image](https://github.com/kj2698/BigData_Bootcamp/assets/101991863/d8f6364c-c371-4077-9cf7-fa91bab35193)

Figure 1.3 Breaking down the data frame instructions as a series of transformations and one action. Each “job” Spark will perform consists of zero or more transformations and one action.

Transformations are pretty much everything else. Some examples of transformations are as follows:

Adding a column to a table

Performing an aggregation according to certain keys

Computing summary statistics

Training a machine learning model

Why the distinction, you might ask? When thinking about computation over data, you, as the developer, are only concerned about the computation leading to an action. You’ll always interact with the results of an action because this is something you can see. Spark, with its lazy computation model, will take this to the extreme and avoid performing data work until an action triggers the computation chain. Before that, the driver will store your instructions. This way of dealing with computation has many benefits when dealing with large-scale data.

NOTE As we see in chapter 5, count() is a transformation when applied as an aggregation function (where it counts the number of records of each group) but an action when applied on a data frame (where it counts the number of records in a data frame).

First, storing instructions in memory takes much less space than storing intermediate data results. If you are performing many operations on a data set and are materializing the data each step of the way, you’ll blow your storage much faster, although you don’t need the intermediate results. We can all agree that less waste is better.

Second, by having the full list of tasks to be performed available, the driver can optimize the work between executors much more efficiently. It can use the information available at run time, such as the node where specific parts of the data are located. It can also reorder, eliminate useless transformations, combine multiple operations, and rewrite some portion of the program more effectively, if necessary.

![image](https://github.com/kj2698/BigData_Bootcamp/assets/101991863/aace657c-23e9-4766-82fe-928da5ce904e)

Figure 1.4 Eager versus lazy evaluation: storing (and computing on the fly) transformation saves memory by reducing the need for intermediate data frames. It also makes it easier to recreate the data frame if one of the nodes fails.

Third, should one node fail during processing—computers fail!—Spark will be able to recreate the missing chunks of data since it has the instructions cached. It’ll read the relevant chunk of data and process it up to where you are without the need for you to do anything. With this, you can focus on the data-processing aspect of your code, offloading the disaster and recovery part to Spark. Check out chapter 11 for more information about compute and memory resources, and how to monitor for failures.

Finally, during interactive development, you don’t have to submit a huge block of commands and wait for the computation to happen. Instead, you can iteratively build your chain of transformation, one at a time, and when you’re ready to launch the computation, you can add an action and let Spark work its magic.

Lazy computation is a fundamental aspect of Spark’s operating model and part of the reason it’s so fast. Most programming languages, including Python, R, and Java, are eagerly evaluated. This means that they process instructions as soon as they receive them. With PySpark, you get to use an eager language—Python—with a lazy framework—Spark. This can look a little foreign and intimidating, but you don’t need to worry. The best way to learn is by doing, and this book provides explicit examples of laziness when relevant. You’ll be a lazy pro in no time!

One aspect to remember is that Spark will not preserve the results of actions (or the intermediate data frames) for subsequent computations. If you submit the same program twice, PySpark will process the data twice. We use caching to change this behavior and optimize certain hot spots in our code (most noticeably when training an ML model), and chapter 11 provides you with how and when to cache (spoiler: not as often as you’d think).

NOTE Reading data, although being I/O, is considered a transformation by Spark. In most cases, reading data doesn’t perform any visible work for the user. You, therefore, won’t read data until you need to perform some work on it (writing, reading, inferring schema; see chapter 6 for more information).

What’s a manager without competent employees? Once the task, with its action, has been received, the driver starts allocating data to what Spark calls executors. Executors are processes that run computations and store data for the application. Those executors sit on what’s called a worker node, which is the actual computer. In our factory analogy, an executor is an employee performing the work, while the worker node is a workbench where many employees/executors can work.

That concludes our factory tour. Let’s summarize our typical PySpark program:

We first encode our instructions in Python code, forming a driver program.

When submitting our program (or launching a PySpark shell), the cluster manager allocates resources for us to use. Those will mostly stay constant (with the exception of auto-scaling) for the duration of the program.

The driver ingests your code and translates it into Spark instructions. Those instructions are either transformations or actions.

Once the driver reaches an action, it optimizes the whole computation chain and splits the work between executors. Executors are processes performing the actual data work, and they reside on machines labeled worker nodes.

That’s it! As we can see, the overall process is quite simple, but it’s obvious that Spark hides a lot of the complexity that arises from efficient distributed processing. For a developer, this means shorter and clearer code, and a faster development cycle.

# 2 Your first data program in PySpark
This chapter covers

Launching and using the pyspark shell for interactive development
Reading and ingesting data into a data frame
Exploring data using the DataFrame structure
Selecting columns using the select() method
Reshaping single-nested data into distinct records using explode()
Applying simple functions to your columns to modify the data they contain
Filtering columns using the where() method
Data-driven applications, no matter how complex, all boil down to what we can think of as three meta steps, which are easy to distinguish in a program:

1. We start by loading or reading the data we wish to work with.

2. We transform the data, either via a few simple instructions or a very complex machine learning model.

3. We then export (or sink) the resulting data, either into a file or by summarizing our findings into a visualization.

# 2.1 Setting up the PySpark shell
Once everything is set up, the easiest way to ensure that everything is running is by launching the PySpark shell by inputting pyspark into your terminal. You should see an ASCII-art version of the Spark logo, as well as some useful information. Listing 2.1 shows what happens on my local machine. In section 2.1.1, you’ll find a less magical alternative to running pyspark as a command that will help you with integrating PySpark into an existing Python REPL.

## Listing 2.1 Launching pyspark on a local machine
```
$ pyspark
 
Python 3.8.8 | packaged by conda-forge | (default, Feb 20 2021, 15:50:57)
[Clang 11.0.1 ] on darwin
Type "help", "copyright", "credits" or "license" for more information.
21/08/23 07:28:16 WARN Utils: Your hostname, gyarados-2.local resolves to a loopback address: 
    127.0.0.1; using 192.168.2.101 instead (on interface en0)
21/08/23 07:28:16 WARN Utils: Set SPARK_LOCAL_IP if you need to bind to another address
21/08/23 07:28:17 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... 
    using builtin-java classes where applicable                                              ❶
Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel). ❷
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /__ / .__/\_,_/_/ /_/\_\   version 3.2.0                                                  ❸
      /_/
 
Using Python version 3.8.8 (default, Feb 20 2021 15:50:57)                                   ❹
Spark context Web UI available at http:/ /192.168.2.101:4040                                 ❺
Spark context available as 'sc' (master = local[*], app id = local-1629718098205).           ❻
SparkSession available as 'spark'.                                                           ❻
  
+In [1]:
```

# 2.1.1 The SparkSession entry point
PySpark uses a builder pattern through the SparkSession.builder object. For those familiar with object-oriented programming, a builder pattern provides a set of methods to create a highly configurable object without having multiple constructors.

In listing 2.2, we start the builder pattern and then chain a configuration parameter that defined the application name. This isn’t necessary, but when monitoring your jobs (see chapter 11), having a unique and well-thought-out job name will make it easier to know what’s what. We finish the builder pattern with the .getOrCreate() method to materialize and instantiate our SparkSession.
```
Listing 2.2 Creating a SparkSession entry point from scratch

from pyspark.sql import SparkSession                                   ❶
  
spark = (SparkSession
         .builder                                                      ❷
         .appName("Analyzing the vocabulary of Pride and Prejudice.")  ❸
         .getOrCreate())
```

❶ The SparkSession entry point is located in the pyspark.sql package, providing the functionality for data transformation.

❷ PySpark provides a builder pattern abstraction for constructing a SparkSession, where we chain the methods to configure the entry point.

❸ Providing a relevant appName helps in identifying which programs run on your Spark cluster (see chapter 11).

`NOTE` By using the getOrCreate() method, your program will work in both interactive and batch mode by avoiding the creation of a new SparkSession if one already exists. Note that if a session already exists, you won’t be able to change certain configuration settings (mostly related to JVM options). If you need to change the configuration of your SparkSession, kill everything and start from scratch to avoid any confusion.

In chapter 1, we spoke briefly about the Spark entry point called SparkContext, which is the liaison between your Python REPL and the Spark cluster. SparkSession is a superset of that. It wraps the SparkContext and provides functionality for interacting with the Spark SQL API, which includes the data frame structure we’ll use in most of our programs. Just to prove our point, see how easy it is to get to the SparkContext from our SparkSession object—just call the sparkContext attribute from spark:
```
$ spark.sparkContext
# <SparkContext master=local[*] appName=Analyzing the vocabulary of [...]>
```

# 2.1.2 Configuring how chatty spark is: The log level
This section covers the log level, probably the most overlooked (and annoying) element of a PySpark program. Monitoring your PySpark jobs is an important part of developing a robust program. PySpark provides many levels of logging, from nothing at all to a full description of everything happening on the cluster. The pyspark shell defaults on WARN, which can be a little chatty when we’re learning. More importantly, a non-interactive PySpark program (which is how you’ll run your scripts for the most part) defaults to the oversharing INFO level. Fortunately, we can change the settings for your session by using the code in the next listing.

```
Listing 2.3 Deciding how chatty you want PySpark to be

spark.sparkContext.setLogLevel("KEYWORD")
```
Table 2.1 lists the available keywords you can pass to setLogLevel (as strings). Each subsequent keyword contains all the previous ones, with the obvious exception of OFF, which doesn’t show anything.

![image](https://github.com/kj2698/BigData_Bootcamp/assets/101991863/2099e173-d11c-4ec3-b679-8bb4c3a2c50f)

# 2.2 Mapping our program
In this chapter’s introduction, we introduced our problem statement: “What are the most popular words used in the English language?” Before we can even hammer out code in the REPL, we have to start by mapping the major steps our program will need to perform:

1. Read—Read the input data (we’re assuming a plain text file).

2. Token—Tokenize each word.

3. Clean—Remove any punctuation and/or tokens that aren’t words. Lowercase each word.

4. Count—Count the frequency of each word present in the text.

5. Answer—Return the top 10 (or 20, 50, 100).
Visually, a simplified flow of our program would look like figure 2.1.

![image](https://github.com/kj2698/BigData_Bootcamp/assets/101991863/4c886f60-77a8-4340-8d01-f036c0424967)

# 2.3 Ingest and explore: Setting the stage for data transformation
This section covers the three operations every PySpark program will encounter, regardless of the nature of your program: ingesting data into a structure, printing the structure (or schema) to see how the data is organized, and finally showing a sample of the data for review. Those operations are fundamental to any data analysis, whether it is text (this chapter and chapter 3), tabular (most chapters, but especially chapter 4 and 5), or even binary or hierarchical data (chapter 6); the general blueprint and methods will apply everywhere in your PySpark journey.

# 2.3.1 Reading data into a data frame with spark.read
The first step of our program is to ingest the data in a structure we can perform work in. This section introduces the basic functionality PySpark provides for reading data and how it is specialized for plain text.

Before ingesting any data, we need to choose where it’s going to go. PySpark provides two main structures for storing data when performing manipulations:

The RDD

The data frame

The RDD was the only structure for a long time. It looks like a distributed collection of objects (or rows). I visualize this as a bag that you give orders to. You pass orders to the RDD through regular Python functions over the items in the bag.

The data frame is a stricter version of the RDD. Conceptually, you can think of it like a table, where each cell can contain one value. The data frame makes heavy usage of the concept of columns, where you operate on columns instead of on records, like in the RDD. Figure 2.2 provides a visual summary of the two structures. The data frame is now the dominant data structure, and we will almost exclusively use it in this book; chapter 8 covers the RDD (a more general and flexible structure, from which the data frame inherits) for cases that need record-by-record flexibility.


![image](https://github.com/kj2698/BigData_Bootcamp/assets/101991863/c9bc7ef0-dffd-4a71-8d39-4c5f27e8bbd6)

Figure 2.2 An RDD versus a data frame. In the RDD, we think of each record as an independent entity. With the data frame, we mostly interact with columns, performing functions on them. We still can access the rows of a data frame via RDD if necessary.

If you’ve used SQL in the past, you’ll find that the data frame implementation takes a lot of inspiration from SQL. The module name for data organization and manipulation is even named pyspark.sql! Furthermore, chapter 7 teaches how to mix PySpark and SQL code within the same program.

Reading data into a data frame is done through the DataFrameReader object, which we can access through spark.read. The code in listing 2.4 displays the object, as well as the methods it exposes. We recognize a few file formats: CSV stands for comma-separated values (which we’ll use as early as chapter 4), JSON for JavaScript Object Notation (a popular data exchange format), and text is, well, just plain text.
```
Listing 2.4 The DataFrameReader object

In [3]: spark.read
Out[3]: <pyspark.sql.readwriter.DataFrameReader at 0x115be1b00>
 
In [4]: dir(spark.read)
Out[4]: [<some content removed>, _spark', 'csv', 'format', 'jdbc', 'json',
'load', 'option', 'options', 'orc', 'parquet', 'schema', 'table', 'text']
```

PySpark reads your data

PySpark can accommodate the different ways you can process data. Under the hood, spark.read.csv() will map to spark.read.format('csv').load(), and you may encounter this form in the wild. I usually prefer using the direct csv method as it provides a handy reminder of the different parameters the reader can take.

orc and parquet are also data formats that are especially well suited for big data processing. ORC (which stands for “optimized row columnar”) and Parquet are competing data formats that pretty much serve the same purpose. Both are open sourced and now part of the Apache project, just like Spark.

PySpark defaults to using Parquet when reading and writing files, and we’ll use this format to store our results throughout the book. I’ll provide a longer discussion about the usage, advantages, and trade-offs of using Parquet or ORC as a data format in chapter 6.

Let’s read our data file in listing 2.5. I am assuming you launched PySpark at the root of this book’s repository. Depending on your case, you might need to change the path where the file is located. The code is all available on the book’s companion repository on GitHub (http://mng.bz/6ZOR).

```
Listing 2.5 “Reading” our Jane Austen novel in record time

book = spark.read.text("./data/gutenberg_books/1342-0.txt")
 
book
# DataFrame[value: string]
```
We get a data frame, as expected! If you input your data frame, conveniently named book, into the shell, you see that PySpark doesn’t output any data to the screen. Instead, it prints the schema, which is the name of the columns and their type. In PySpark’s world, each column has a type: it represents how the value is represented by Spark’s engine. By having the type attached to each column, you can instantly know what operations you can do on the data. With this information, you won’t inadvertently try to add an integer to a string: PySpark won’t let you add 1 to “blue.” Here, we have one column, named value, composed of a string. A quick graphical representation of our data frame would look like figure 2.3: each line of text (separated by a newline character) is a record. Besides being a helpful reminder of the content of the data frame, types are integral to how Spark processes data quickly and accurately. We will explore the subject extensively in chapter 6.

![image](https://github.com/kj2698/BigData_Bootcamp/assets/101991863/cc5889e4-ebfc-4deb-ba3e-17dc5e06e4ac)
When working with a larger data frame (think hundreds or even thousands of columns), you may want to see the schema displayed more clearly. PySpark provides printSchema() to display the schema in a tree form. I use this method probably more than any other one as it gives you direct information on the structure of the data frame. Since printSchema() directly prints to the REPL with no other option, should you want to filter the schema, you can use the dtypes attributes of the data frame, which gives you a list of tuples (column_name, column_type). You can also access the schema programmatically (as a data structure) using the schema attribute (see chapter 6 for more information).
```
Listing 2.6 Printing the schema of our data frame

book.printSchema()
 
# root                                   ❶
#  |-- value: string (nullable = true)   ❷
 
print(book.dtypes)
 
# [('value', 'string')]                  ❸
```
❶ Each data frame tree starts with a root, which the columns are attached to.

❷ We have one column value, containing strings that can be null (or None in Python terms).

❸ The same information is stored as a list of tuples under the data frame’s dtypes attribute.

In this section, we ingested our textual data into a data frame. This data frame inferred a simple columnar structure that we can explore through the variable name in the REPL, the printSchema() method, or the dtypes attribute. In the next section, we go beyond the structure to peek at the data inside.

# 2.3.2 From structure to content: Exploring our data frame with show()
Enter the show() method, which displays a few rows of the data back to you—nothing more, nothing less. With printSchema(), this method will become one of your best friends when performing data exploration and validation. By default, it will show 20 rows and truncate long values. The code in listing 2.8 shows the default behavior of the method applied to our book data frame. 

```
Listing 2.8 Showing a little data using the .show() method

book.show()
 
# +--------------------+
# |               value|    ❶
# +--------------------+
# |The Project Guten...|
# |                    |
# |This eBook is for...|
# |almost no restric...|
# |re-use it under t...|
# |with this eBook o...|
# |                    |
# |                    |
# |Title: Pride and ...|
# |                    |
# | [... more records] |
# |Character set enc...|
# |                    |
# +--------------------+
# only showing top 20 rows
```
❶ Spark displays the data from the data frame in an ASCII art-like table, limiting the length of each cell to 20 characters. If the contents spill over the limit, an ellipsis is added at the end.

The show() method takes three optional parameters:

n can be set to any positive integer and will display that number of rows.

truncate, if set to true, will truncate the columns to display only 20 characters. Set to False, it will display the whole length, or any positive integer to truncate to a specific number of characters.

vertical takes a Boolean value and, when set to True, will display each record as a small table. If you need to check records in detail, this is a very useful option.
```
Listing 2.9 Showing less length, more width with the show() method

book.show(10, truncate=50)
 
# +--------------------------------------------------+
# |                                             value|
# +--------------------------------------------------+
# |The Project Gutenberg EBook of Pride and Prejud...|
# |                                                  |
# |This eBook is for the use of anyone anywhere at...|
# |almost no restrictions whatsoever.  You may cop...|
# |re-use it under the terms of the Project Gutenb...|
# |    with this eBook or online at www.gutenberg.org|
# |                                                  |
# |                                                  |
# |                        Title: Pride and Prejudice|
# |                                                  |
# +--------------------------------------------------+
# only showing top 10 rows
```

We can now start the real work: performing transformations on the data frame to accomplish our goal. Let’s take some time to review the five steps we outlined at the beginning of the chapter:

1. `[DONE]`Read—Read the input data (we’re assuming a plain text file).

2. Token—Tokenize each word.

3. Clean—Remove any punctuation and/or tokens that aren’t words. Lowercase each word.

4. Count—Count the frequency of each word present in the text.

5. Answer—Return the top 10 (or 20, 50, 100).

`show()` is an action, since it performs the visible work of printing data on the screen. As savvy PySpark programmers, we want to avoid accidentally triggering the chain of computations, so the Spark developers made show() explicit. When building a complicated chain of transformations, triggering its execution is a lot more annoying and time-consuming than having to type the show() method when you’re ready.

That being said, there are some moments, especially when learning, when you want your data frames to be evaluated after each transformation (which we call eager evaluation). Since Spark 2.4.0, you can configure the SparkSession object to support printing to screen. We will cover how to create a SparkSession object in greater detail in chapter 3, but if you want to use eager evaluation in the shell, you can paste the following code in your shell:
```
from pyspark.sql import SparkSession
 
spark = (SparkSession.builder
                     .config("spark.sql.repl.eagerEval.enabled", "True")
                     .getOrCreate())
```

# 2.4 Simple column transformations: Moving from a sentence to a list of words
When ingesting our selected text into a data frame, PySpark created one record for each line of text and provided a value column of type String. To tokenize each word, we need to split each string into a list of distinct words. This section covers simple transformations using select(). We will split our lines of text into words so we can count them.
```
Listing 2.10 Splitting our lines of text into arrays or words

from pyspark.sql.functions import split
 
lines = book.select(split(book.value, " ").alias("line"))
 
lines.show(5)
 
# +--------------------+
# |                line|
# +--------------------+
# |[The, Project, Gu...|
# |                  []|
# |[This, eBook, is,...|
# |[almost, no, rest...|
# |[re-use, it, unde...|
# +--------------------+
# only showing top 5 rows
```
More specifically, we learn about the following:

1. The select() method and its canonical usage, which is selecting data

2. The alias() method to rename transformed columns

3. Importing column functions from pyspark.sql.functions and using them

# 2.4.1 Selecting specific columns using select()
In PySpark’s world, a data frame is made out of Column objects, and you perform transformations on them. The most basic transformation is the identity, where you return exactly what was provided to you. If you’ve used SQL in the past, you might think that this sounds like a SELECT statement, and you’d be right! You also get a free pass: the method name is also conveniently named select().
```
Listing 2.11 The simplest select statement ever

book.select(book.value)
```
PySpark provides for each column in its data frame a dot notation that refers to the column. This is the simplest way to select a column, as long as the name doesn’t contain any funny characters: PySpark will accept $!@# as a column name, but you won’t be able to use the dot notation for this column.

PySpark provides more than one way to select columns. I display the four most common in the next listing.
```
Listing 2.12 Selecting the value column from the book data frame

from pyspark.sql.functions import col
 
book.select(book.value)
book.select(book["value"])
book.select(col("value"))
book.select("value")
```
The first way to select a column is the trusty dot notation we got acquainted with a few paragraphs ago. The second one uses brackets instead of the dot to name the column. It addresses the $!@# problem since you pass the name of the column as a string.

The third one uses the col function from the pyspark.sql.functions module. The main difference here is that you don’t specify that the column comes from the book data frame. This will become very useful when working with more complex data pipelines in part 2 of the book. I’ll use the col object as much as I can since I consider its usage more idiomatic and it’ll prepare us for more complex use cases, such as performing column transformation (see chapter 4 and 5).

Finally, the fourth one only uses the name of the column as a string. PySpark is smart enough to infer that we mean a column here. For simple select statements (and other methods that I’ll cover later), using the name of the column directly can be a viable option. That being said, it’s not as flexible as the other options, and the moment your code requires column transformations, like in section 2.4.2, you’ll have to use another option.

# 2.4.2 Transforming columns: Splitting a string into a list of words
PySpark provides a split() function in the pyspark.sql.functions module for splitting a longer string into a list of shorter strings. The most popular use case for this function is to split a sentence into words. The split() function takes two or three parameters:

A column object containing strings

A Java regular expression delimiter to split the strings against

An optional integer about how many times we apply the delimiter (not used here)

```
Listing 2.13 Splitting our lines of text into lists of words

from pyspark.sql.functions import col, split
 
lines = book.select(split(col("value"), " "))
 
lines
 
# DataFrame[split(value,  , -1): array<string>]
 
lines.printSchema()
 
# root
#  |-- split(value,  , -1): array (nullable = true)
#  |    |-- element: string (containsNull = true)
 
lines.show(5)
 
# +--------------------+
# | split(value,  , -1)|
# +--------------------+
# |[The, Project, Gu...|
# |                  []|
# |[This, eBook, is,...|
# |[almost, no, rest...|
# |[re-use, it, unde...|
# +--------------------+
# only showing top 5 rows
```
https://spark.apache.org/docs/latest/api/python/_modules/pyspark/sql/functions.html#split

# 2.4.3 Renaming columns: alias and withColumnRenamed
There is an implicit assumption that you’ll want to rename the resulting column yourself, using the alias() method. Its usage isn’t very complicated: when applied to a column, it takes a single parameter and returns the column it was applied to, with the new name. A simple demonstration is provided in the next listing.
```
Listing 2.14 Our data frame before and after the aliasing

book.select(split(col("value"), " ")).printSchema()
# root
#  |-- split(value,  , -1): array (nullable = true)    ❶
#  |    |-- element: string (containsNull = true)
 
book.select(split(col("value"), " ").alias("line")).printSchema()
 
# root
#  |-- line: array (nullable = true)                   ❷
#  |    |-- element: string (containsNull = true)
```
❶ Our new column is called split(value, , -1), which isn’t really pretty.

❷ We aliased our column to the name line. Much better!

alias() provides a clean and explicit way to name your columns after you’ve performed work on it. On the other hand, it’s not the only renaming player in town. Another equally valid way to do so is by using the .withColumnRenamed() method on the data frame. It takes two parameters: the current name of the column and the wanted name of the column. Since we’re already performing work on the column with split, chaining alias makes a lot more sense than using another method. Listing 2.15 shows you the two different approaches.

When writing your code, choosing between those two options is pretty easy:

When you’re using a method where you’re specifying which columns you want to appear, like the select() method, use alias().

If you just want to rename a column without changing the rest of the data frame, use .withColumnRenamed. Note that, should the column not exist, PySpark will treat this method as a no-op and not perform anything.

```
Listing 2.15 Renaming a column, two ways

# This looks a lot cleaner
lines = book.select(split(book.value, " ").alias("line"))
# This is messier, and you have to remember the name PySpark assigns automatically
lines = book.select(split(book.value, " "))
lines = lines.withColumnRenamed("split(value,  , -1)", "line")
```
We have a list of words, but we need each token or word to be its own record:

1. `[DONE]`Read—Read the input data (we’re assuming a plain text file).

2. `[IN PROGRESS]`Token—Tokenize each word.

3. Clean—Remove any punctuation and/or tokens that aren’t words. Lowercase each word.

4. Count—Count the frequency of each word present in the text.

5. Answer—Return the top 10 (or 20, 50, 100).

# 2.4.4 Reshaping your data: Exploding a list into rows
When working with data, a key element in data preparation is making sure that it “fits the mold”; this means making sure that the structure containing the data is logical and appropriate for the work at hand. At the moment, each record of our data frame contains multiple words into an array of strings. It would be better to have one record for each word.

Enter the `explode()` function. When applied to a column containing a container-like data structure (such as an array), it’ll take each element and give it its own row. This is much easier explained visually rather than using words, and figure 2.4 explains the process.

![image](https://github.com/kj2698/BigData_Bootcamp/assets/101991863/813bb192-f4e1-4a22-983e-5e2a48c58a39)
Figure 2.4 Exploding a data frame of array[String] into a data frame of String. Each element of each array becomes its own record.
```
Listing 2.16 Exploding a column of arrays into rows of elements

from pyspark.sql.functions import explode, col
 
words = lines.select(explode(col("line")).alias("word"))
 
words.show(15)
# +----------+
# |      word|
# +----------+
# |       The|
# |   Project|
# | Gutenberg|
# |     EBook|
# |        of|
# |     Pride|
# |       and|
# |Prejudice,|
# |        by|
# |      Jane|
# |    Austen|
# |          |
# |      This|
# |     eBook|
# |        is|
# +----------+
# only showing top 15 rows
```
Before continuing our data-processing journey, we can take a step back and look at a sample of the data. Just by looking at the 15 rows returned, we can see that Prejudice, has a comma and that the cell between Austen and This contains the empty string. That gives us a good blueprint of the next steps that need to be performed before we start analyzing word frequency.
Looking back at our five steps, we can now conclude step 2, and our words are tokenized. Let’s attack the third one, where we’ll clean our words to simplify the counting:

1. `[DONE]`Read—Read the input data (we’re assuming a plain text file).

2. `[DONE]`Token—Tokenize each word.

3. Clean—Remove any punctuation and/or tokens that aren’t words. Lowercase each word.

4. Count—Count the frequency of each word present in the text.

5. Answer—Return the top 10 (or 20, 50, 100).

# 2.4.5 Working with words: Changing case and removing punctuation
Let’s get right to it. Listing 2.17 contains the source code to lower the case of all the words in the data frame. The code should look very familiar: we select a column transformed by lower, a PySpark function lowering the case of the data inside the column passed as a parameter. We then alias the resulting column to word_lower to avoid PySpark’s default nomenclature.
```
Listing 2.17 Lower the case of the words in the data frame

from pyspark.sql.functions import lower
words_lower = words.select(lower(col("word")).alias("word_lower"))
 
words_lower.show()
 
# +-----------+
# | word_lower|
# +-----------+
# |        the|
# |    project|
# |  gutenberg|
# |      ebook|
# |         of|
# |      pride|
# |        and|
# | prejudice,|
# |         by|
# |       jane|
# |     austen|
# |           |
# |       this|
# |      ebook|
# |         is|
# |        for|
# |        the|
# |        use|
# |         of|
# |     anyone|
# +-----------+
# only showing top 20 rows
```
Next, we want to clean our words of any punctuation and other non-useful characters; in this case, we’ll keep only the letters using a regular expression (see the end of the section for a reference on regular expressions [or regex]). This can be a little trickier: we won’t improvise a full NLP (Natural Language Processing) library here, and instead rely on the functionality PySpark provides in its data manipulation toolbox. In the spirit of keeping this exercise simple, we’ll keep the first contiguous group of letters as the word, and remove the rest. It will effectively remove punctuation, quotation marks, and other symbols, at the expense of being less robust with more exotic word construction. The next listing shows the code in all its splendor.
```
Listing 2.18 Using regexp_extract to keep what looks like a word

from pyspark.sql.functions import regexp_extract
words_clean = words_lower.select(
    regexp_extract(col("word_lower"), "[a-z]+", 0).alias("word")   ❶
)
 
words_clean.show()
 
# +---------+
# |     word|
# +---------+
# |      the|
# |  project|
# |gutenberg|
# |    ebook|
# |       of|
# |    pride|
# |      and|
# |prejudice|
# |       by|
# |     jane|
# |   austen|
# |         |
# |     this|
# |    ebook|
# |       is|
# |      for|
# |      the|
# |      use|
# |       of|
# |   anyone|
# +---------+
# only showing top 20 rows
```
❶ We only match for multiple lowercase characters (between a and z). The plus sign (+) will match for one or more occurrences.

Our data frame of words looks pretty regular by now, except for the empty cell between austen and this. In the next section, we cover the filtering operation by removing any empty records.

## Exercise 2.1

Given the following exo_2_1_df data frame, how many records will the solution_ 2_1_df data frame contain? (Note: No need to write code to solve this problem.)

exo_2_1_df.show()
``` 
# +-------------------+
# |            numbers|
# +-------------------+
# |    [1, 2, 3, 4, 5]|
# |[5, 6, 7, 8, 9, 10]|
# +-------------------+
```
 
`solution_2_1_df = exo_2_1_df.select(explode(col("numbers")))`

# 2.5 Filtering rows
Conceptually, we should be able to provide a test to perform on each record. If it returns true, we keep the record. False? You’re out! PySpark provides not one, but two identical methods to perform this task. You can use either .filter() or its alias .where(). This duplication is to ease the transition for users coming from other data-processing engines or libraries; some use one, some the other. PySpark provides both, so no arguments are possible! I prefer filter(), because w maps to more data frame methods (withColumn() in chapter 4 or withColumnRenamed() in chapter 3). If we look at the next listing, we can see that columns can be compared to values using the usual Python comparison operators. In this case, we’re using “not equal,” or `!=`.
```
Listing 2.19 Filtering rows in your data frame using where or filter

words_nonull = words_clean.filter(col("word") != "")
 
words_nonull.show()
 
# +---------+
# |     word|
# +---------+
# |      the|
# |  project|
# |gutenberg|
# |    ebook|
# |       of|
# |    pride|
# |      and|
# |prejudice|
# |       by|
# |     jane|
# |   austen|
# |     this|     ❶
# |    ebook|
# |       is|
# |      for|
# |      the|
# |      use|
# |       of|
# |   anyone|
# | anywhere|
# +---------+
# only showing top 20 rows
```
❶ The blank cell is gone!

`TIP` If you want to negate a whole expression in a filter() method, PySpark provides the ~ operator. We could theoretically use filter(~(col("word") == "")). Look at the exercises at the end of the chapter to see them in an application. You can also use SQL-style expression; check out chapter 7 for an alternative syntax.

We’re ready for counting and displaying the results of our analysis:

1. `[DONE]`Read—Read the input data (we’re assuming a plain text file).

2. `[DONE]`Token—Tokenize each word.

3. `[DONE]`Clean—Remove any punctuation and/or tokens that aren’t words. Lowercase each word.

4. Count—Count the frequency of each word present in the text.

5. Answer—Return the top 10 (or 20, 50, 100).

## Summary
Almost all PySpark programs will revolve around three major steps: reading, transforming, and exporting data.

PySpark provides a REPL (read, evaluate, print, loop) via the pyspark shell where you can experiment interactively with data.

PySpark data frames are a collection of columns. You operate on the structure using chained transformations. PySpark will optimize the transformations and perform the work only when you submit an action, such as show(). This is one of the pillars of PySpark’s performance.

PySpark’s repertoire of functions that operate on columns is located in pyspark .sql.functions.

You can select columns or transformed columns via the select() method.

You can filter columns using the where() or filter() methods and by providing a test that will return True or False; only the records returning True will be kept.

PySpark can have columns of nested values, like arrays of elements. In order to extract the elements into distinct records, you need to use the explode() method.

# 3 Submitting and scaling your first PySpark program

This chapter covers

Summarizing data using groupby and a simple aggregate function
Ordering results for display
Writing data from a data frame
Using spark-submit to launch your program in batch mode
Simplifying PySpark writing using method chaining
Scaling your program to multiple files at once

# 3.1 Grouping records: Counting word frequencies
Intuitively, we count the number of each word by creating groups: one for each word. Once those groups are formed, we can perform an aggregation function on each one of them. In this specific case, we count the number of records for each group, which will give us the number of occurrences for each word in the data frame. Under the hood, PySpark represents a grouped data frame in a GroupedData object; think of it as a transitional object that awaits an aggregation function to become a transformed data frame.

![image](https://github.com/kj2698/BigData_Bootcamp/assets/101991863/31396941-3bdb-44f3-9303-b37f7691cf80)

Listing 3.1 A schematic representation of our groups object. Each small box represents a record.

The easiest way to count record occurrence is to use the groupby() method, passing the columns we wish to group as a parameter. The groupby() method in listing 3.1 returns a GroupedData and awaits further instructions. Once we apply the count() method, we get back a data frame containing the grouping column word, as well as the count column containing the number of occurrences for each word.

```
Listing 3.1 Counting word frequencies using groupby() and count()

groups = words_nonull.groupby(col("word"))
 
print(groups)
 
# <pyspark.sql.group.GroupedData at 0x10ed23da0>
 
results = words_nonull.groupby(col("word")).count()
 
print(results)
 
# DataFrame[word: string, count: bigint]
 
results.show()
 
# +-------------+-----+
# |         word|count|
# +-------------+-----+
# |       online|    4|
# |         some|  203|
# |        still|   72|
# |          few|   72|
# |         hope|  122|
# [...]
# |       doubts|    2|
# |    destitute|    1|
# |    solemnity|    5|
# |gratification|    1|
# |    connected|   14|
# +-------------+-----+
# only showing top 20 rows
```
Peeking at the results data frame in listing 3.1, we see that the results are in no specific order. As a matter of fact, I’d be very surprised if you had the exact same order of words that I do! This has to do with how PySpark manages data. In chapter 1, we learned that PySpark distributes the data across multiple nodes. When performing a grouping function, such as groupby(), each worker performs the work on its assigned data. groupby() and count() are transformations, so PySpark will queue them lazily until we request an action. When we pass the show method to our results data frame, it triggers the chain of computation that we see in figure 3.2.

![image](https://github.com/kj2698/BigData_Bootcamp/assets/101991863/a6bdf2be-1d02-4b94-906a-dc61ddd92b1b)

Listing 3.2 A distributed group by on our words_nonull data frame. The work is performed in a distributed fashion until we need to assemble the results in a cohesive display via show().

`TIP` If you need to create groups based on the values of multiple columns, you can pass multiple columns as parameters to groupby(). We see this in action in chapter 5.

## Exercise 3.1

Starting with the word_nonull seen in this section, which of the following expressions would return the number of words per letter count (e.g., there are X one-letter words, Y two-letter words, etc.)?

Assume that pyspark.sql.functions.col, pyspark.sql.functions.length are imported.

a) words_nonull.select(length(col("word"))).groupby("length").count()

b) words_nonull.select(length(col("word")).alias("length")).groupby("length").count()

c) words_nonull.groupby("length").select("length").count()

d) None of those options would work.

# 3.2 Ordering the results on the screen using orderBy
Just like we use groupby() to group a data frame by the values in one or many columns, we use orderBy() to order a data frame by the values of one or many columns. PySpark provides two different syntaxes to order records:

We can provide the column names as parameters, with an optional `ascending` parameter. By default, we order a data frame in ascending order; by setting `ascending` to false, we reverse the order, getting the largest values first.

Or we can use the Column object directly, via the col function. When we want to reverse the ordering, we use the `desc()` method on the column.

PySpark orders the data frame using each column, one at a time. If you pass multiple columns (see chapter 5), PySpark uses the first column’s values to order the data frame, then the second (and then third, etc.) when there are identical values. Since we have a single column—and no duplicates because of groupby()—the application of orderBy() in the next listing is simple, regardless of the syntax we pick.
```
Listing 3.2 Displaying the top 10 words in Jane Austen’s Pride and Prejudice

results.orderBy("count", ascending=False).show(10)
results.orderBy(col("count").desc()).show(10)
 
# +----+-----+
# |word|count|
# +----+-----+
# | the| 4480|
# |  to| 4218|
# |  of| 3711|
# | and| 3504|
# | her| 2199|
# |   a| 1982|
# |  in| 1909|
# | was| 1838|
# |   i| 1749|
# | she| 1668|
# +----+-----+
# only showing top 10 rows
```

PySpark’s method naming convention zoo

If you are detail-oriented, you might have noticed we used `groupby` (lowercase), but `orderBy` (lowerCamelCase, where you capitalize the first letter of each word but the first word). This seems like an odd design choice.

groupby() is an alias for `groupBy()`, just like `where()` is an alias of `filter()`. I guess that the PySpark developers found that a lot of typing mistakes were avoided by accepting the two cases. `orderBy()` didn’t have that luxury, for a reason that escapes my understanding, so we need to be mindful of this.

Part of this incoherence is due to Spark’s heritage. Scala prefers camelCase for methods. On the other hand, we saw `regexp_extract`, which uses Python’s preferred snake_case (words separated by an underscore) in chapter 2. There is no magic secret here: you’ll have to be mindful of the different case conventions at play in PySpark.

It’s much better to save those results to a file so that we’ll be able to reuse them without having to compute everything each time. The next section covers writing a data frame to a file.

## Exercise 3.2
```
Why isn’t the order preserved in the following code block?

(
    results.orderBy("count", ascending=False)
    .groupby(length(col("word")))
    .count()
    .show(5)
)
# +------------+-----+
# |length(word)|count|
# +------------+-----+
# |          12|  199|
# |           1|   10|
# |          13|  113|
# |           6|  908|
# |          16|    4|
# +------------+-----+
# only showing top 5 rows
```

# 3.3 Writing data from a data frame
Just like we use read() and the SparkReader to read data in Spark, we use write() and the SparkWriter object to write back our data frame to disk. In listing 3.3, I specialize the SparkWriter to export text into a CSV file, naming the output simple_count.csv. If we look at the results, we can see that PySpark didn’t create a results.csv file. Instead, it created a directory of the same name, and put 201 files inside the directory (200 CSVs + 1 _SUCCESS file).
```
Listing 3.3 Writing our results in multiple CSV files, one per partition

results.write.csv("./data/simple_count.csv")
 
# The ls command is run using a shell, not a Python prompt.
# If you use IPython, you can use the bang pattern (! ls -1).
# Use this to get the same results without leaving the IPython console.
 
$ ls -1 ./data/simple_count.csv                               ❶
 
_SUCCESS                                                      ❷
part-00000-615b75e4-ebf5-44a0-b337-405fccd11d0c-c000.csv
[...]
part-00199-615b75e4-ebf5-44a0-b337-405fccd11d0c-c000.csv      ❸
```
❶ The results are written in a directory called simple_count.csv.

❷ The _SUCCESS file means the operation was successful.

❸ We have part-00000 to part-00199, which means our results are split across 200 files.

There it is, folks! The first moment where we have to care about PySpark’s distributed nature. Just like PySpark will distribute the transformation work across multiple workers, it’ll do the same for writing data. While it might look like a nuisance for our simple program, it is tremendously useful when working in distributed environments. When you have a large cluster of nodes, having many smaller files makes it easy to logically distribute reading and writing the data, making it way faster than having a single massive file.

By default, PySpark will give you one file per partition. This means that our program, as run on my machine, yields 200 partitions at the end. This isn’t the best for portability. To reduce the number of partitions, we apply the `coalesce()` method with the desired number of partitions. The next listing shows the difference when using `coalesce(1)` on our data frame before writing to disk. We still get a directory, but there is a single CSV file inside of it. Mission accomplished!
```
Listing 3.4 Writing our results under a single partition

results.coalesce(1).write.csv("./data/simple_count_single_partition.csv")
 
$ ls -1 ./data/simple_count_single_partition.csv/
 
_SUCCESS
part-00000-f8c4c13e-a4ee-4900-ac76-de3d56e5f091-c000.csv
```
`NOTE` You might have realized that we’re not ordering the file before writing it. Since our data here is pretty small, we could have written the words by decreasing order of frequency. If you have a large data set, this operation will be quite expensive. Furthermore, since reading is a potentially distributed operation, what guarantees that it’ll get read the same way? Never assume that your data frame will keep the same ordering of records unless you explicitly ask via orderBy() right before the showing step.

# 3.4 Putting it all together: Counting
The REPL allows you to go back in history using the directional arrows on your keyboard, just like a regular Python REPL. To make things a bit easier, I am providing the step-by-step program in the next listing. This section is dedicated to streamlining and making our code more succinct and readable.
```
Listing 3.5 Our first PySpark program, dubbed “Counting Jane Austen”

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    explode,
    lower,
    regexp_extract,
    split,
)
 
spark = SparkSession.builder.appName(
    "Analyzing the vocabulary of Pride and Prejudice."
).getOrCreate()
 
book = spark.read.text("./data/gutenberg_books/1342-0.txt")
 
lines = book.select(split(book.value, " ").alias("line"))
 
words = lines.select(explode(col("line")).alias("word"))
 
words_lower = words.select(lower(col("word")).alias("word"))
 
words_clean = words_lower.select(
    regexp_extract(col("word"), "[a-z']*", 0).alias("word")
)
 
words_nonull = words_clean.where(col("word") != "")
 
results = words_nonull.groupby(col("word")).count()
 
results.orderBy("count", ascending=False).show(10)
 
results.coalesce(1).write.csv("./simple_count_single_partition.csv")
```

# 3.4.1 Simplifying your dependencies with PySpark’s import conventions
This program uses five distinct functions from the pyspark.sql.functions modules. We should probably replace this with a qualified import, which is Python’s way of importing a module by assigning a keyword to it. While there is no hard rule, the common wisdom is to use F to refer to PySpark’s functions. The next listing shows the before and after.
```
Listing 3.6 Simplifying our PySpark functions import

# Before
from pyspark.sql.functions import col, explode, lower, regexp_extract, split
 
# After
import pyspark.sql.functions as F
```
Since `col`, `explode`, `lower`, `regexp_extract`, and `split` are all in pyspark.sql.functions, we can import the whole module. Since the new import statement imports the entirety of the `pyspark.sql.functions module`, we assign the keyword (or key letter) F. The PySpark community seems to have implicitly settled on using `F` for `pyspark.sql.functions`, and I encourage you to do the same. It’ll make your programs consistent, and since many functions in the module share their name with pandas or Python built-in functions, you’ll avoid name clashes. Each function application in the program will then be prefixed by `F`, just like with regular Python-qualified imports.

`WARNING` It can be very tempting to start an `import like from pyspark.sql.functions import *`. Do not fall into that trap! It’ll make it hard for your readers to know which functions come from PySpark and which come from regular Python. In chapter 8, where we’ll use user-defined functions (UDFs), this separation will become even more important. This is a good coding hygiene rule!

# 3.4.2 Simplifying our program via method chaining
If we look at the transformation methods we applied to our data frames (`select()`, `where()`, `groupBy()`, and `count()`), they all have something in common: they take a structure as a parameter—the data frame or `GroupedData` in the case of `count()`—and return a structure. All transformations can be seen as pipes that ingest a structure and return a modified structure. This section will look at method chaining and how it makes a program less verbose and thus easier to read by eliminating intermediate variables.

In PySpark, every transformation returns an object, which is why we need to assign a variable to the result. This means that PySpark doesn’t perform modifications in place.
We can avoid intermediate variables by chaining the results of one method to the next. Since each transformation returns a data frame (or GroupedData, when we perform the groupby() method), we can directly append the next method without assigning the result to a variable. This means that we can eschew all but one variable assignment. The code in the next listing shows the before and after. Note that we also added the F prefix to our functions to respect the import convention we outlined in section 3.4.1.
```
Listing 3.7 Removing intermediate variables by chaining transformation methods

# Before
book = spark.read.text("./data/gutenberg_books/1342-0.txt")
 
lines = book.select(split(book.value, " ").alias("line"))
 
words = lines.select(explode(col("line")).alias("word"))
 
words_lower = words.select(lower(col("word")).alias("word"))
 
words_clean = words_lower.select(
    regexp_extract(col("word"), "[a-z']*", 0).alias("word")
)
 
words_nonull = words_clean.where(col("word") != "")
 
results = words_nonull.groupby("word").count()
 
# After
import pyspark.sql.functions as F
 
results = (
    spark.read.text("./data/gutenberg_books/1342-0.txt")
    .select(F.split(F.col("value"), " ").alias("line"))
    .select(F.explode(F.col("line")).alias("word"))
    .select(F.lower(F.col("word")).alias("word"))
    .select(F.regexp_extract(F.col("word"), "[a-z']*", 0).alias("word"))
    .where(F.col("word") != "")
    .groupby("word")
    .count()
)
```
![image](https://github.com/kj2698/BigData_Bootcamp/assets/101991863/f0e42c42-1aba-43ad-9eea-7ce142054e50)

Listing 3.3 Method chaining eliminates the need for intermediate variables.
I am not saying that intermediate variables are evil and are to be avoided. But they can hinder your code readability, so you have to make sure they serve a purpose. A lot of burgeoning PySpark developers make it a habit of always writing on top of the same variable. While not dangerous in itself, it makes the code redundant and harder to reason about. If you see yourself doing something like the first two lines of the next listing, chain your methods. You’ll get the same result and more aesthetically pleasing code.
```
Listing 3.8 Chaining for writing over the same variable

df = spark.read.text("./data/gutenberg_books/1342-0.txt")     ❶
df = df.select(F.split(F.col("value"), " ").alias("line"))    ❶
  
df = (
       spark.read.text("./data/gutenberg_books/1342-0.txt")   ❷
       .select(F.split(F.col("value"), " ").alias("line"))    ❷
     )
```
❶ Instead of doing this . . .

❷ . . . you can do this—no variable repetition!

`Make your life easier by using Python’s parentheses`

If you look at the “after” code in listing 3.7, you’ll notice that I start the right side of the equal sign with an opening parenthesis (`spark = ( [...]`). This is a trick I use when I need to chain methods in Python. If you don’t wrap your result into a pair of parentheses, you’ll need to add a `\` character at the end of each line, which adds visual noise to your program. PySpark code is especially prone to line breaks when you use method chaining:
```
results = spark\
          .read.text('./data/ch02/1342-0.txt')\
          ...
```

# 3.5 Using spark-submit to launch your program in batch mode
Unlike the interactive REPL, where the choice of language triggers the program to run, as in listing 3.10, we see that Spark provides a single program, named spark-submit, to submit Spark (Scala, Java, SQL), PySpark (Python), and SparkR (R) programs. The full code for our program is available on the book’s repository under code/Ch02/word_count_submit.py.
```
Listing 3.9 Submitting our job in batch mode

$ spark-submit ./code/Ch03/word_count_submit.py
 
# [...]
# +----+-----+
# |word|count|
# +----+-----+
# | the| 4480|
# |  to| 4218|
# |  of| 3711|
# | and| 3504|
# | her| 2199|
# |   a| 1982|
# |  in| 1909|
# | was| 1838|
# |   i| 1749|
# | she| 1668|
# +----+-----+
# only showing top 10 rows
# [...]
```

`TIP` If you get a deluge of INFO messages, don’t forget that you have control over this: use `spark.sparkContext.setLogLevel("WARN")` right after your spark definition. If your local configuration has `INFO` as a default, you’ll still get a slew of messages until it catches this line, but it won’t obscure your results.

# 3.6 What didn’t happen in this chapter
Chapter 2 and 3 were pretty dense. We learned how to read text data, process it to answer any question, display the results on the screen, and write them to a CSV file. On the other hand, there are many elements we left out on purpose. Let’s quickly look at what we didn’t do in this chapter.

Except for coalescing the data frame to write it into a single file, we didn’t do much with the distribution of the data. We saw in chapter 1 that PySpark distributes data across multiple worker nodes, but our code didn’t pay much attention to this. Not having to constantly think about partitions, data locality, and fault tolerance made our data discovery process much faster.

We didn’t spend much time configuring PySpark. Other than providing a name for our application, no additional configuration was inputted in our SparkSession. It’s not to say we’ll never broach this, but we can start with a bare-bones configuration and tweak as we go. The subsequent chapters will customize the SparkSession to optimize resources (chapter 11) or create connectors to external data repositories (chapter 9).

Finally, we didn’t obsess about planning the order of operations as it relates to processing, focusing instead on readability and logic. We made a point to describe our transformations as logically as they appear to us, and we’re letting Spark optimize this into efficient processing steps. We could potentially reorder some and get the same output, but our program reads well, is easy to reason about, and works correctly.

This echoes the statement I made in chapter 1: PySpark is remarkable not only in what it provides, but also in what it can abstract over. You most often can write your code as a sequence of transformations that will get you to your destination most of the time. For those cases where you want a more finely tuned performance or more control over the physical layout of your data, we’ll see in part 3 that PySpark won’t hold you back. Because Spark is in constant evolution, there are still cases where you need to be a little more careful about how your program translates to physical execution on the cluster. For this, chapter 11 covers the Spark UI, which shows you the work being performed on your data and how you can influence processing.

# 3.7 Scaling up our word frequency program
That example wasn’t big data. I’ll be the first to say it.

Teaching big data processing has a catch-22. While I want to show the power of PySpark to work with massive data sets, I don’t want you to purchase a cluster or rack up a massive cloud bill. It’s easier to show you the ropes using a smaller set of data, knowing that we can scale using the same code.

Let’s take our word-counting example: How can we scale this to a larger corpus of text? Let’s download more files from Project Gutenberg and place them in the same directory:
https://github.com/jonesberg/DataAnalysisWithPythonAndPySpark/tree/trunk
```
$ ls -1 data/gutenberg_books
 
11-0.txt
1342-0.txt
1661-0.txt
2701-0.txt
30254-0.txt
84-0.txt
```
While this is not enough to claim “we’re doing big data,” it’ll be enough to explain the general concept. If you want to scale, you can use appendix B to provision a powerful cluster on the cloud, download more books or other text files, and run the same program for a few dollars.

We modify our `word_count_submit.py` in a very subtle way. Where we `.read.text()`, we’ll change the path to account for all files in the directory. The next listing shows the before and after: we are only changing the `1342-0.txt` to a `*.txt`, which is called a glob pattern. The `*` means that Spark selects all the `.txt` files in the directory.
```
Listing 3.10 Scaling our word count program using the glob pattern

# Before
results = spark.read.text('./data/gutenberg_books/1342-0.txt')    ❶
 
# After
results = spark.read.text('./data/gutenberg_books/*.txt')         ❷
```
❶ Here we have a single file passed as a parameter . . .

❷ . . . and here the star (or glob) picks all the text files within the directory.

`NOTE` You can also just pass the name of the directory if you want PySpark to ingest all the files within the directory.

The results of running the program over all the files in the directory are available in the following listing.
```
Listing 3.11 Results of scaling our program to multiple files

$ spark-submit ./code/Ch02/word_count_submit.py
 
+----+-----+
|word|count|
+----+-----+
| the|38895|
| and|23919|
|  of|21199|
|  to|20526|
|   a|14464|
|   i|13973|
|  in|12777|
|that| 9623|
|  it| 9099|
| was| 8920|
+----+-----+
only showing top 10 rows
```

# Summary
1. You can group records using the `groupby` method, passing the column names you want to group against as a parameter. This returns a `GroupedData` object that waits for an aggregation method to return the results of computation over the groups, such as the `count()` of records.
2. PySpark’s repertoire of functions that operates on columns is located in `pyspark.sql.functions`. The unofficial but well-respected convention is to qualify this import in your program using the `F` keyword.
3. When writing a data frame to a file, PySpark will create a directory and put one file per partition. If you want to write a single file, use the `coaslesce(1)` method.
4. To prepare your program to work in batch mode via `spark-submit`, you need to create a SparkSession. PySpark provides a builder pattern in the `pyspark.sql` module.
5. If your program needs to scale across multiple files within the same directory, you can use a glob pattern to select many files at once. PySpark will collect them in a single data frame.

# 4 Analyzing tabular data with pyspark.sql
This chapter covers

Reading delimited data into a PySpark data frame
Understanding how PySpark represents tabular data in a data frame
Ingesting and exploring tabular or relational data
Selecting, manipulating, renaming, and deleting columns in a data frame
Summarizing data frames for quick exploration

Our first example in chapters 2 and 3 worked with unstructured textual data. Each line of text was mapped to a record in a data frame, and, through a series of transformations, we counted word frequencies from one (and multiple) text files. This chapter goes deeper into data transformation, this time using structured data. Data comes in many shapes and forms: we start with relational (or tabular,1 or row and columns) data, one of the most common formats popularized by SQL and Excel. This chapter and the next follow the same blueprint as we did with our first data analysis. We use the public Canadian television schedule data to identify and measure the proportion of commercials over its total programming.

Just like with every PySpark program, we start by initializing our `SparkSession` object, as in the next listing. I also proactively import the `pyspark.sql.functions` as a qualified F, since we saw in chapter 3 that it helps with readability and avoiding potential name clashes for functions.

```
Listing 4.1 Creating our SparkSession object to start using PySpark

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
 
spark = SparkSession.builder.getOrCreate()
```

# 4.1 What is tabular data?
We call data tabular when we represent it in a two-dimensional table. You have cells, each containing a single (or simple) value, organized into rows and columns. A good example is your grocery list: you may have one column for the item you wish to purchase, one for the quantity, and one for the expected price. Figure 4.1 provides an example of a small grocery list. We have the three columns mentioned, as well as four rows, each representing an entry in our grocery list.
![image](https://github.com/kj2698/BigData_Bootcamp/assets/101991863/319cdd57-2671-475b-a574-dbc494d631ea)

The easiest analogy we can make for tabular data is the spreadsheet format: the interface provides you with a large number of rows and columns where you can input and perform computations on data. SQL databases can be thought of as tables made up of rows and columns. Tabular data is an extremely common data format, and because it’s so popular and easy to reason about, it makes for a perfect first dive into PySpark’s data manipulation API.

PySpark’s data frame structure maps very naturally to tabular data. In chapter 2, I explain that PySpark operates either on the whole data frame structure (via methods such as select() and groupby()) or on Column objects (e.g., when using a function like split()). The data frame is column-major, so its API focuses on manipulating the columns to transform the data. Because of this, we can simplify how we reason about data transformations by thinking about what operations to perform and which columns will be impacted by them.

`**NOTE**` The resilient distributed dataset, briefly introduced in chapter 1, is a good example of a structure that is row-major. Instead of thinking about columns, you are thinking about items (rows) with attributes in which you apply functions. It’s an alternative way of thinking about your data, and chapter 8 contains more information about where/when it can be useful.

# 4.1.1 How does PySpark represent tabular data?
In chapters 2 and 3, our data frame always contained a single column, up to the very end when we counted the occurrence of each word. In other words, we took unstructured data (a body of text), performed some transformations, and created a two-column table containing the information we wanted. Tabular data is, in a way, an extension of this, where we have more than one column to work with.

Let’s take my very healthy grocery list as an example, and load it into PySpark. To make things simple, we’ll encode our grocery list into a list of lists. PySpark has multiple ways to import tabular data, but the two most popular are the list of lists and the pandas data frame. In chapter 9, I briefly cover how to work with pandas. It would be a bit overkill to import a library just for loading four records (four items on our grocery list), so I kept it in a list of lists.
```
Listing 4.2 Creating a data frame out of our grocery list

my_grocery_list = [
    ["Banana", 2, 1.74],
    ["Apple", 4, 2.04],
    ["Carrot", 1, 1.09],
    ["Cake", 1, 10.99],
]                                        ❶
  
df_grocery_list = spark.createDataFrame(
    my_grocery_list, ["Item", "Quantity", "Price"]
)
 
df_grocery_list.printSchema()
# root
#  |-- Item: string (nullable = true)    ❷
#  |-- Quantity: long (nullable = true)  ❷
#  |-- Price: double (nullable = true)   ❷
```
❶ My grocery list is encoded in a list of lists.

❷ PySpark automatically inferred the type of each field from the information Python had about each value.

We can easily create a data frame from the data in our program with the `spark.createDataFrame` function, as listing 4.2 shows. Our first parameter is the data itself. You can provide a list of items (here, a list of lists), a pandas data frame, or a resilient distributed dataset, which I cover in chapter 8. The second parameter is the schema of the data frame. Chapter 6 covers the automatic and manual schema definitions in greater depth. In the meantime, passing a list of column names will make PySpark happy while it infers the types (`string`, `long`, and `double`, respectively) of our columns. Visually, the data frame will look like figure 4.2, although much more simplified. The master node knows about the structure of the data frame, but the actual data is represented on the worker nodes. Each column maps to data stored somewhere on our cluster that is managed by PySpark. We operate on the abstract structure and let the master delegate the work efficiently.
![image](https://github.com/kj2698/BigData_Bootcamp/assets/101991863/1661f78d-e378-4987-8dc4-42bbf455b80a)

Figure 4.2 Each column of our data frame maps to some place on our worker nodes.

PySpark gladly represented our tabular data using our column definitions. This means that all the functions we’ve learned so far apply to our tabular data. By having one flexible structure for many data representations—we’ve covered text and tabular so far—PySpark makes it easy to move from one domain to another. It removes the need to learn yet another set of functions and a whole new abstraction for our data.

This section covered the look and feel of a simple two-dimensional/tabular data frame. In the next section, we ingest and process a more significant data frame. It’s time for some coding!

# 4.2 PySpark for analyzing and processing tabular data
My grocery list was fun, but the potential for analysis work is pretty limited. We’ll get our hands on a larger data set, explore it, and ask a few introductory questions that we might find interesting. This process is called exploratory data analysis (or EDA) and is usually the first step data analysts and scientists undertake when placed in front of new data. Our goal is to get familiar with the data discovery functions and methods, as well as with performing some basic data assembly. Being familiar with those steps will remove the awkwardness of working with data you won’t see transforming before your eyes. This section shows you a blueprint you can reuse when facing new data frames until you can visually process millions of records per second.

Graphical exploratory data analysis

A lot of the EDA work you’ll see in the wild incorporates charts and/or tables. Does this mean that PySpark has the option to do the same?

We saw in chapter 2 how to print a data frame so that we can view the content at a glance. This still applies to summarizing information and displaying it on the screen. If you want to export the table in an easy-to-process format (e.g., to incorporate it in a report), you can use spark.write.csv, making sure you coalesce the data frame in a single file. (See chapter 3 for a refresher on coalesce().) By its very nature, table summaries won’t be very large, so you won’t risk running out of memory.

PySpark doesn’t provide any charting capabilities and doesn’t play with other charting libraries (like Matplotlib, seaborn, Altair, or plot.ly), and this makes a lot of sense: PySpark distributes your data over many computers. It doesn’t make much sense to distribute a chart creation. The usual solution will be to transform your data using PySpark, use the toPandas() method to transform your PySpark data frame into a pandas data frame, and then use your favorite charting library. When using charts, I provide the code I used to generate them.

When using toPandas(), remember that you lose the advantages of working with multiple machines, as the data will accumulate on the driver. Reserve this operation for an aggregated or manageable data set. While this is a crude formula, I usually take the number of rows times the number of columns; if this number is over 100,000 (for a 16 GB driver), I try to reduce it further. This simple trick helps me get a sense of the size of the data I am dealing with, as well as what’s possible given my driver size.

You do not want to move your data between a pandas and a PySpark data frame all the time. Reserve toPandas() for either discrete operations or for moving your data into a pandas data frame once and for all. Moving back and forth will yield a ton of unnecessary work in distributing and collecting the data for nothing. If you need pandas functionality on a Spark data frame, check out pandas UDFs in chapter 9.

For this exercise, we’ll use some open data from the government of Canada, more specifically the CRTC (Canadian Radio-Television and Telecommunications Commission). Every broadcaster is mandated to provide a complete log of the programs and commercials showcased to the Canadian public. This gives us a lot of potential questions to answer, but we’ll select just one: What are the channels with the greatest and least proportion of commercials?

You can download the file on the Canada Open Data portal (http://mng.bz/y4YJ); select the BroadcastLogs_2018_Q3_M8 file. The file is 994 MB to download, which might be too large, depending on your computer. The book’s repository contains a sample of the data under the data/broadcast_logs directory, which you can use in place of the original file. You also need to download the Data Dictionary in .doc form, as well as the Reference Tables zip file, unzipping them into a ReferenceTables directory in data/ broadcast_logs. Once again, the examples assume that the data is downloaded under data/broadcast_logs and that PySpark is launched from the root of the repository.

Before moving to the next section, make sure you have the following. With the exception of the large BroadcastLogs file, the rest is in the repository:

data/BroadcastLogs_2018_Q3_M8.CSV (either download from the website or use the sample from the repo)

data/broadcast_logs/ReferenceTables

data/broadcast_logs/data_dictionary.doc

# 4.3 Reading and assessing delimited data in PySpark
Now that we have tested the waters with a small synthetic tabular data set, we are ready to dive into real data. Just like in chapter 3, our first step is to read the data before we can perform exploration and transformation. This time, we read data that is a little more complex than just some unorganized text. Because of this, I cover the `SparkReader` usage in more detail. As the two-dimensional table is one of the most common organization formats, knowing how to ingest tabular or relational data will become second nature very quickly.

`TIP` Relational data is often in a SQL database. Spark can read from SQL (or SQL-like) data stores very easily: check chapter 9 for an example where I read from Google BigQuery.

In this section, I start by covering the usage of the `SparkReader` for delimited data, or data that is separated by a delimited character (to create this second dimension), by applying it to one of the CRTC data tables. I then review the most common reader’s options, so you can read other types of delimited files with ease.

# 4.3.1 A first pass at the SparkReader specialized for CSV files
The CSV file format stems from a simple idea: we use text, separated in two-dimensional records (rows and columns), that are separated by two types of delimiters. Those delimiters are characters, but they serve a special purpose when applied in the context of a CSV file:

The first one is a row delimiter. The row delimiter splits the file into logical records. There is one and only one record between delimiters.

The second one is a field delimiter. Each record is made up of an identical number of fields, and the field delimiter tells where one field starts and ends.
![image](https://github.com/kj2698/BigData_Bootcamp/assets/101991863/3320dba5-27fc-486b-97c4-c29acb992e06)

CSV files are easy to produce and have a loose set of rules to follow to be considered usable. Because of this, PySpark provides a whopping 25 optional parameters when ingesting a CSV file. Compare this to the two for reading text data. In listing 4.3, I use three configuration parameters: the record delimiter through sep and the presence of a header (column names) row through header, and I finally ask Spark to infer the data types for me with inferSchema (more on this in section 4.3.2). This is enough to parse our data into a data frame.
```
Listing 4.3 Reading our broadcasting information

import os
 
DIRECTORY = "./data/broadcast_logs"
logs = spark.read.csv(
    os.path.join(DIRECTORY, "BroadcastLogs_2018_Q3_M8.CSV"),  ❶
    sep="|",                                                  ❷
    header=True,                                              ❸
    inferSchema=True,                                         ❹
    timestampFormat="yyyy-MM-dd",                             ❺
)
```
❶ We specify the file path where our data resides first.

❷ Our file uses a vertical bar as delimiter/separator, so we pass | as a parameter to sep.

❸ header takes a Boolean. When true, the first row of your file is parsed as the column names.

❹ inferSchema takes a Boolean as well. When true, it’ll pre-parse the data to infer the type of the column.

❺ timestampFormat is used to inform the parser of the format (year, month, day, hour, minutes, seconds, microseconds) of the timestamp fields (see section 4.4.3).

While we were able to read the CSV data for our analysis, this is just one narrow example of the usage of the SparkReader. The next section expands on the most important parameters when reading CSV data and provides more detailed explanations behind the code used in listing 4.3.

# 4.3.2 Customizing the SparkReader object to read CSV data files
This section focuses on how we can specialize the SparkReader object to read delimited data and the most popular configuration parameters to accommodate the various declinations of CSV data.
```
Listing 4.4 The spark.read.csv function, with every parameter explicitly laid out

logs = spark.read.csv(
    path=os.path.join(DIRECTORY, "BroadcastLogs_2018_Q3_M8.CSV"),
    sep="|",
    header=True,
    inferSchema=True,
    timestampFormat="yyyy-MM-dd",
)
```

Reading delimited data can be a dicey business. Because of how flexible and human-editable the format is, a CSV reader needs to provide many options to cover the many use cases possible. There is also a risk that the file is malformed, in which case you will need to treat it as text and gingerly infer the fields manually. I will stay on the happy path and cover the most popular scenario: a single file, properly delimited.

`THE PATH TO THE FILE YOU WANT TO READ AS THE ONLY MANDATORY PARAMETER`

Just like when reading text, the only truly mandatory parameter is the path, which contains the file or files’ path. As we saw in chapter 2, you can use a glob pattern to read multiple files inside a given directory, as long as they have the same structure. You can also explicitly pass a list of file paths if you want specific files to be read.

`PASSING AN EXPLICIT FIELD DELIMITER WITH THE SEP PARAMETER`

The most common variation you’ll encounter when ingesting and producing CSV files is selecting the right delimiter. The comma is the most popular, but it suffers from being a popular character in text, which means you need a way to differentiate which commas are part of the text and which are delimiters. Our file uses the vertical bar character, an apt choice: it’s easily reachable on the keyboard yet infrequent in text.

`NOTE` In French, we use the comma for separating numbers between their integral part and their decimal part (e.g., 1.02 → 1,02). This is pretty awful when in a CSV file, so most French CSVs will use the semicolon (;) as a field delimiter. This is one more example of why you need to be vigilant when using CSV data.

When reading CSV data, PySpark will default to using the comma character as a field delimiter. You can set the optional parameter sep (for separator) to the single character you want to use as a field delimiter.

`QUOTING TEXT TO AVOID MISTAKING A CHARACTER FOR A DELIMITER`

When working with CSVs that use the comma as a delimiter, it’s common practice to quote the text fields to make sure any comma in the text is not mistaken for a field separator. The CSV reader object provides an optional `quote` parameter that defaults to the double-quote character (`"`). Since I am not passing an explicit value to `quote`, we are keeping the default value. This way, we can have a field with the value `"Three | Trois"`, whereas without the quotation characters, we would consider this two fields. If we don’t want to use any character as a quote, we need to explicitly pass the empty string to `quote`.

`USING THE FIRST ROW AS THE COLUMN NAMES`

The `header` optional parameter takes a Boolean flag. If set to true, it’ll use the first row of your file (or files, if you’re ingesting many) and use it to set your column names.
You can also pass an explicit schema (see chapter 6) or a DDL string (see chapter 7) as the schema optional parameter if you wish to explicitly name your columns. If you don’t fill any of those, your data frame will have `_c*` for column names, where the `*` is replaced with increasing integers (`_c0`, `_c1`, . . .).

`INFERRING COLUMN TYPE WHILE READING THE DATA`

PySpark has a schema-discovering capacity. You turn it on by setting `inferSchema` to True (by default, this is turned off). This optional parameter forces PySpark to go over the ingested data twice: one time to set the type of each column, and one time to ingest the data. This makes the ingestion quite a bit longer but helps us avoid writing the schema by hand (I go down to this level of detail in chapter 6). Let the machine do the work!

`TIP` Inferring the schema can be very expensive if you have a lot of data. In chapter 6, I cover how to work with (and extract) schema information; if you read a data source multiple times, it’s a good idea to keep the schema information once inferred! You can also take a small representative data set to infer the schema, followed by reading the large data set.

We are lucky enough that the government of Canada is a good steward of data and provides us with clean, properly formatted files. In the wild, malformed CSV files are legion, and you will run into errors when trying to ingest some of them. Furthermore, if your data is large, you often won’t get the chance to inspect each row to fix mistakes. Chapter 6 covers some strategies to ease the pain and shows you some ways to share your data with the schema included.

Our data frame schema, displayed in the next listing, is coherent with the documentation we’ve downloaded. The column names are properly displayed, and the types make sense. That’s enough to get started with some exploration.

```
Listing 4.5 The schema of our logs data frame

logs.printSchema()
# root
#  |-- BroadcastLogID: integer (nullable = true)
#  |-- LogServiceID: integer (nullable = true)
#  |-- LogDate: timestamp (nullable = true)
#  |-- SequenceNO: integer (nullable = true)
#  |-- AudienceTargetAgeID: integer (nullable = true)
#  |-- AudienceTargetEthnicID: integer (nullable = true)
#  |-- CategoryID: integer (nullable = true)
#  |-- ClosedCaptionID: integer (nullable = true)
#  |-- CountryOfOriginID: integer (nullable = true)
#  |-- DubDramaCreditID: integer (nullable = true)
#  |-- EthnicProgramID: integer (nullable = true)
#  |-- ProductionSourceID: integer (nullable = true)
#  |-- ProgramClassID: integer (nullable = true)
#  |-- FilmClassificationID: integer (nullable = true)
#  |-- ExhibitionID: integer (nullable = true)
#  |-- Duration: string (nullable = true)
#  |-- EndTime: string (nullable = true)
#  |-- LogEntryDate: timestamp (nullable = true)
#  |-- ProductionNO: string (nullable = true)
#  |-- ProgramTitle: string (nullable = true)
#  |-- StartTime: string (nullable = true)
#  |-- Subtitle: string (nullable = true)
#  |-- NetworkAffiliationID: integer (nullable = true)
#  |-- SpecialAttentionID: integer (nullable = true)
#  |-- BroadcastOriginPointID: integer (nullable = true)
#  |-- CompositionID: integer (nullable = true)
#  |-- Producer1: string (nullable = true)
#  |-- Producer2: string (nullable = true)
#  |-- Language1: integer (nullable = true)
#  |-- Language2: integer (nullable = true)
```

## Exercise 4.1

Let’s take the following file, called sample.csv, which contains three columns:
```
Item,Quantity,Price
$Banana, organic$,1,0.99
Pear,7,1.24
$Cake, chocolate$,1,14.50
```

`logs = spark.read.csv("/config/workspace/data/broadcast_logs/sample.csv",sep=",",quote="$",header=True,inferSchema=True)`
```
>>> logs.show()
+---------------+--------+-----+
|           Item|Quantity|Price|
+---------------+--------+-----+
|Banana, organic|       1| 0.99|
|           Pear|       7| 1.24|
|Cake, chocolate|       1| 14.5|
+---------------+--------+-----+
```

# 4.3.3 Exploring the shape of our data universe
When working with tabular data, especially if it comes from a SQL data warehouse, you’ll often find that the data set is split between tables. In our case, our logs table contains a majority of fields suffixed by ID; those IDs are listed in other tables, and we have to link them to get the legend of those IDs. This section briefly introduces star schemas, why they are so frequently encountered, and how we can visually represent them to work with them.

Our data universe (the set of tables we are working with) follows a very common pattern in relational databases: a center table containing a bunch of IDs (or keys) and some ancillary tables containing a legend between each key and its value. This is called a star schema since it looks like a star. Star schemas are common in the relational database world because of normalization, a process used to avoid duplicating pieces of data and improve data integrity. Data normalization is illustrated in figure 4.4, where our center table logs contain IDs that map to the auxiliary tables called link tables. In the case of the CD_Category link table, it contains many fields (e.g., Category_CD and English_description) that are made available to logs when you link the tables with the Category_ID key.

![image](https://github.com/kj2698/BigData_Bootcamp/assets/101991863/edb3b0b2-7e74-4061-86c3-5481562ac988)

Figure 4.4 The logs table ID columns map to other tables, like the CD_category table, which links the Category_ID field.

In Spark’s universe, we often prefer working with a single table instead of linking a multitude of tables to get the data. We call these denormalized tables, or, colloquially, fat tables. We start by assessing the data directly available in the logs table before plumping our table, a topic I cover in chapter 5. By looking at the logs table, its content, and the data documentation, we avoid linking tables that contain data with no real value for our analysis.

The right structure for the right work

Normalization, denormalization—what gives? Isn’t this a book about data analysis?

While this book isn’t about data modelling, it’s important to understand, at least a little, how data might be structured so that we can work with it. Normalized data has many advantages when you’re working with relational information (e.g., our broadcast tables). In addition to being easier to maintain, data normalization reduces the probability of getting anomalies or illogical records in the data. On the flip side, large-scale data systems sometimes embrace denormalized tables to avoid costly join operations.

When dealing with analytics, a single table containing all the data is best. However, having to link/join the data by hand can be tedious, especially when working with dozens or even hundreds of link tables (check out chapter 5 for more information about joins). Fortunately, data warehouses don’t change their structure very often. If you’re faced with a complex star schema one day, befriend one of the database managers. There is a very good chance they’ll provide you with the information to denormalize the tables, most often in SQL, and chapter 7 will show how you can adapt the code into PySpark with minimum effort.

# 4.4 The basics of data manipulation: Selecting, dropping, renaming, ordering, diagnosing
This section shows the most common manipulations done on a data frame in greater detail. I show how you can select, delete, rename, reorder, and create columns so you can customize how a data frame is shown. I also cover summarizing a data frame so you can have a quick diagnostic overview of the data inside your structure. 

# 4.4.1 Knowing what we want: Selecting columns
So far, we’ve learned that typing our data frame variable into the shell prints the structure of the data frame, not the data, unless you’re using eagerly evaluated Spark (referenced in chapter 2). We can also use the `show()` command to display a few records for exploration. I won’t show the results, but if you try it, you’ll see that the table-esque output is garbled because we are showing too many columns at once. This section reintroduces the `select()` method, which, this time, instructs PySpark on the columns you want to keep in your data frame. I also introduce how you can refer to columns when using PySpark methods and functions.

At its simplest, `select()` can take one or more column objects—or strings representing column names—and return a data frame containing only the listed columns. This way, we can keep our exploration tidy and check a few columns at a time. An example is displayed in the next listing.
```
Listing 4.6 Selecting five rows of the first three columns of our data frame

logs.select("BroadcastLogID", "LogServiceID", "LogDate").show(5, False)
 
# +--------------+------------+-------------------+
# |BroadcastLogID|LogServiceID|LogDate            |
# +--------------+------------+-------------------+
# |1196192316    |3157        |2018-08-01 00:00:00|
# |1196192317    |3157        |2018-08-01 00:00:00|
# |1196192318    |3157        |2018-08-01 00:00:00|
# |1196192319    |3157        |2018-08-01 00:00:00|
# |1196192320    |3157        |2018-08-01 00:00:00|
# +--------------+------------+-------------------+
# only showing top 5 rows
```
In chapter 2, you learned that `.show(5, False)` shows five rows without truncating their representation so that we can show the whole content. The `.select()` statement is where the magic happens. In the documentation, select() takes a single parameter, `*cols`; the `*` means that the method will accept an arbitrary number of parameters. If we pass multiple column names, `select()` will simply clump all the parameters in a tuple called `cols`.

Because of this, we can use the same de-structuring trick for selecting columns. From a PySpark perspective, the four statements in listing 4.7 are interpreted the same. Note how prefixing the list with a star removed the container so that each element becomes a parameter of the function. If this looks a little confusing to you, fear not! Appendix C provides you with a good overview of collection unpacking.

```
Listing 4.7 Four ways to select columns in PySpark, all equivalent in terms of results

# Using the string to column conversion
logs.select("BroadCastLogID", "LogServiceID", "LogDate")
logs.select(*["BroadCastLogID", "LogServiceID", "LogDate"])
 
# Passing the column object explicitly
logs.select(
    F.col("BroadCastLogID"), F.col("LogServiceID"), F.col("LogDate")
)
logs.select(
    *[F.col("BroadCastLogID"), F.col("LogServiceID"), F.col("LogDate")]
)
```

When explicitly selecting a few columns, you don’t have to wrap them into a list. If you’re already working on a list of columns, you can unpack them with a `*` prefix. This argument unpacking pattern is worth remembering as many other data frame methods taking columns as input use the same approach.

In the spirit of being clever (or lazy), let’s expand our selection code to see every column in groups of three. This will give us a sense of the content. A data frame keeps track of its columns in the columns attributes; `logs.columns` is a Python list containing all the column names of the logs data frame. In the next listing, I slice the columns into groups of three to display them by small groups rather than in one fell swoop.

```
Listing 4.8 Peeking at the data frame in chunks of three columns

import numpy as np
 
column_split = np.array_split()
    np.array(logs.columns), len(logs.columns) // 3   ❶
)  
 
print(column_split)
 
# [array(['BroadcastLogID', 'LogServiceID', 'LogDate'], dtype='<U22'),
#  [...]
#  array(['Producer2', 'Language1', 'Language2'], dtype='<U22')]'
 
for x in column_split:
    logs.select(*x).show(5, False)
 
# +--------------+------------+-------------------+
# |BroadcastLogID|LogServiceID|LogDate            |
# +--------------+------------+-------------------+
# |1196192316    |3157        |2018-08-01 00:00:00|
# |1196192317    |3157        |2018-08-01 00:00:00|
# |1196192318    |3157        |2018-08-01 00:00:00|
# |1196192319    |3157        |2018-08-01 00:00:00|
# |1196192320    |3157        |2018-08-01 00:00:00|
# +--------------+------------+-------------------+
# only showing top 5 rows
# ... and more tables of three columns
```

❶ The array_split() function comes from the numpy package, imported as np at the beginning of this listing.

Let’s take each line one at a time. We start by splitting the `logs.columns` list into approximate groups of three. To do so, we rely on a function from the numpy package called `array_split()`. The function takes an array and a number of desired sub-arrays, `N`, and returns a list of `N` sub-arrays. We wrap our list of columns, `logs.columns`, into an array via the `np.array` function and pass this as a first parameter. For the number of sub-arrays, we divide the number of columns by three, using an integer division, `//`.

`TIP` To be perfectly honest, the call to `np.array` can be eschewed since `np.array_split()` can work on lists, albeit more slowly. I am still using it because if you are using a static type checker, such as mypy, you’ll get a type error. Chapter 8 has a basic introduction to type checking in your PySpark program.

The last part of listing 4.8 iterates over the list of sub-arrays, using `select()`; select the columns present inside each sub-array and use show() to display them on the screen.

# 4.4.2 Keeping what we need: Deleting columns
The other side of selecting columns is choosing what not to select. We could do the full trip with `select()`, carefully crafting our list of columns to keep just the one we want. Fortunately, PySpark also provides a shorter trip: simply drop what you don’t want.

Let’s get rid of two columns in our current data frame in the spirit of tidying up. Hopefully, it will bring us joy:

`BroadCastLogID` is the primary key of the table and will not serve us in answering our questions.

`SequenceNo` is a sequence number and won’t be useful either.

More will come off later when we start looking at the link tables. The code in the next listing does this simply.
```
Listing 4.9 Getting rid of columns using the drop() method

logs = logs.drop("BroadcastLogID", "SequenceNO")
 
# Testing if we effectively got rid of the columns
 
print("BroadcastLogID" in logs.columns)  # => False
print("SequenceNo" in logs.columns)  # => False
```
Just like `select()`, `drop()` takes a `*cols` and returns a data frame, this time excluding the columns passed as parameters. Just like every other method in PySpark, `drop()` returns a new data frame, so we overwrite our `logs` variable by assigning the result of our code.

`WARNING` Unlike `select()`, where selecting a column that doesn’t exist will return a runtime error, dropping a nonexistent column is a no-op. PySpark will simply ignore the columns it doesn’t find. Be careful with the spelling of your column names!

Depending on how many columns you want to preserve, select() might be a neater way to keep only what you want. We can view drop() and select() as being two sides of the same coin: one drops what you specify; the other keeps what you specify. We could reproduce listing 4.9 with a select() method, and the next listing does just that.
```
Listing 4.10 Getting rid of columns, select style

logs = logs.select(
    *[x for x in logs.columns if x not in ["BroadcastLogID", "SequenceNO"]]
)
```
Advanced topic: An unfortunate inconsistency

In theory, you can also select() columns with a list without unpacking them. This code will work as expected:
```
logs = logs.select(
    [x for x in logs.columns if x not in ["BroadcastLogID", "SequenceNO"]]
)
```
This is not the case for drop(), where you need to explicitly unpack:
```
logs.drop(logs.columns[:])
# TypeError: col should be a string or a Column
 
logs.drop(*logs.columns[:])
# DataFrame[]
```

## Exercise 4.2

What is the printed result of this code?

sample_frame.columns # => ['item', 'price', 'quantity', 'UPC']
 
print(sample_frame.drop('item', 'UPC', 'prices').columns)
a) ['item' 'UPC']

b) ['item', 'upc']

c) ['price', 'quantity']

d) ['price', 'quantity', 'UPC']

e) Raises an error

# 4.4.3 Creating what’s not there: New columns with withColumn()
Creating new columns is such a basic operation that it seems a little far-fetched to rely on` select()`. It also puts a lot of pressure on code readability; for instance, using `drop()` makes it obvious we’re removing columns. It would be nice to have something that signals we’re creating a new column. PySpark named this function `withColumn()`.

Before going crazy with column creation, let’s take a simple example, build what we need iteratively, and then move the data to `withColumn()`. Let’s take the Duration column, which contains the length of each program shown.
```
Listing 4.11 Selecting and displaying the Duration column

logs.select(F.col("Duration")).show(5)
 
# +----------------+
# |        Duration|
# +----------------+
# |02:00:00.0000000|
# |00:00:30.0000000|
# |00:00:15.0000000|
# |00:00:15.0000000|
# |00:00:15.0000000|
# +----------------+
# only showing top 5 rows
 
print(logs.select(F.col("Duration")).dtypes)    ❶
 
# [('Duration', 'string')]
```

❶ The dtypes attribute of a data frame contains the name of the column and its type, wrapped in a tuple.

PySpark doesn’t have a default type for time without dates or duration, so it kept the column as a string. We verified the exact type via the `dtypes` attribute, which returns both the name and type of a data frame’s columns. A string is a safe and reasonable option, but this isn’t remarkably useful for our purpose. Thanks to our peeking, we can see that the string is formatted like `HH:MM:SS.mmmmmm`, where

HH is the duration in hours.

MM is the duration in minutes.

SS is the duration in seconds.

mmmmmmm is the duration in microseconds.

`NOTE` To match an arbitrary date/timestamp pattern, refer to the Spark documentation for date-time patterns at http://mng.bz/M2X2.

In listing 4.12, I use it to extract the hours, minutes, and seconds from the Duration columns. The substr() method takes two parameters. The first gives the position of where the sub-string starts, the first character being 1, not 0 like in Python. The second gives the length of the sub-string we want to extract in a number of characters. The function application returns a string Column that I convert to an Integer via the cast() method. Finally, I provide an alias for each column so that we can easily tell which is which.
```
Listing 4.12 Extracting the hours, minutes, and seconds from the Duration column

logs.select(
    F.col("Duration"),                                                ❶
    F.col("Duration").substr(1, 2).cast("int").alias("dur_hours"),    ❷
    F.col("Duration").substr(4, 2).cast("int").alias("dur_minutes"),  ❸
    F.col("Duration").substr(7, 2).cast("int").alias("dur_seconds"),  ❹
).distinct().show(                                                    ❺
    5
)
 
# +----------------+---------+-----------+-----------+
# |        Duration|dur_hours|dur_minutes|dur_seconds|
# +----------------+---------+-----------+-----------+
# |00:10:06.0000000|        0|         10|          6|
# |00:10:37.0000000|        0|         10|         37|
# |00:04:52.0000000|        0|          4|         52|
# |00:26:41.0000000|        0|         26|         41|
# |00:08:18.0000000|        0|          8|         18|
# +----------------+---------+-----------+-----------+
# only showing top 5 rows
```

❶ The original column, for sanity.

❷ The first two characters are the hours.

❸ The fourth and fifth characters are the minutes.

❹ The seventh and eighth characters are the seconds.

❺ To avoid seeing identical rows, I’ve added a distinct() to the results.

I use the `distinct()` method before `show()`, which de-dupes the data frame. This is explained further in chapter 5. I added `distinct()` to avoid seeing identical occurrences that would provide no additional information when displayed.

In the next listing, we apply addition and multiplication on integer columns, just like if they were simple number values.
```
Listing 4.13 Creating a duration in second field from the Duration column

logs.select(
    F.col("Duration"),
    (
        F.col("Duration").substr(1, 2).cast("int") * 60 * 60
        + F.col("Duration").substr(4, 2).cast("int") * 60
        + F.col("Duration").substr(7, 2).cast("int")
    ).alias("Duration_seconds"),
).distinct().show(5)
 
# +----------------+----------------+
# |        Duration|Duration_seconds|
# +----------------+----------------+
# |00:10:30.0000000|             630|
# |00:25:52.0000000|            1552|
# |00:28:08.0000000|            1688|
# |06:00:00.0000000|           21600|
# |00:32:08.0000000|            1928|
# +----------------+----------------+
# only showing top 5 rows
```

We kept the same definitions, removed the alias, and performed arithmetic directly on the columns. There are 60 seconds in a minute, and 60 * 60 seconds in an hour. PySpark respects operator precedence, so we don’t have to clobber our equation with parentheses. Overall, our code is quite easy to follow, and we are ready to add our column to our data frame.

What if we want to add a column at the end of our data frame? Instead of using `select()` on all the columns plus our new one, let’s use `withColumn()`. Applied to a data frame, it’ll return a data frame with the new column appended. The next listing takes our field and adds it to our `logs` data frame. I also include a sample of the `printSchema()` method so that you can see the column added at the end.

```
Listing 4.14 Creating a new column with withColumn()

logs = logs.withColumn(
    "Duration_seconds",
    (
        F.col("Duration").substr(1, 2).cast("int") * 60 * 60
        + F.col("Duration").substr(4, 2).cast("int") * 60
        + F.col("Duration").substr(7, 2).cast("int")
    ),
)
 
logs.printSchema()
 
# root
#  |-- LogServiceID: integer (nullable = true)
#  |-- LogDate: timestamp (nullable = true)
#  |-- AudienceTargetAgeID: integer (nullable = true)
#  |-- AudienceTargetEthnicID: integer (nullable = true)
#  [... more columns]
#  |-- Language2: integer (nullable = true)
#  |-- Duration_seconds: integer (nullable = true)    ❶
```

❶ Our Duration_seconds columns have been added at the end of our data frame.

`WARNING` If you create a column withColumn() and give it a name that already exists in your data frame, PySpark will happily overwrite the column. This is often very useful for keeping the number of columns manageable, but make sure you are seeking this effect!

We can create columns using the same expression with select() and with withColumn(). Both approaches have their use. select() will be useful when you’re explicitly working with a few columns. When you need to create a few new ones without changing the rest of the data frame, I prefer withColumn(). You’ll quickly gain intuition about which is easiest when faced with the choice.

`WARNING` Creating many (100+) new columns using withColumns() will slow Spark down to a grind. If you need to create a lot of columns at once, use the select() approach. While it will generate the same work, it is less tasking on the query planner.


![image](https://github.com/kj2698/BigData_Bootcamp/assets/101991863/d046f5ac-06ae-4b46-add2-3b1e5942a167)
Figure 4.5 select() versus withColumn(), visually. withColumn() keeps all the preexisting columns without the need the specify them explicitly.

# 4.4.4 Tidying our data frame: Renaming and reordering columns
This section covers how to make the order and names of the columns friendlier. It might seem a little vapid, but after a few hours of hammering code on a particularly tough piece of data, you’ll be happy to have this in your toolbox.

Renaming columns can be done with `select()` and `alias()`, of course. We saw briefly in chapter 3 that PySpark provides you an easier way to do so. Enter `withColumnRenamed()`! In the following listing, I use `withColumnRenamed()` to remove the capital letters of my newly created duration_seconds column.
```
Listing 4.15 Renaming one column at a type, the withColumnRenamed() way

logs = logs.withColumnRenamed("Duration_seconds", "duration_seconds")
 
logs.printSchema()
 
# root
#  |-- LogServiceID: integer (nullable = true)
#  |-- LogDate: timestamp (nullable = true)
#  |-- AudienceTargetAgeID: integer (nullable = true)
#  |-- AudienceTargetEthnicID: integer (nullable = true)
#  [...]
#  |-- Language2: integer (nullable = true)
#  |-- duration_seconds: integer (nullable = true)
```
I’m a huge fan of having column names without capital letters. I’m a lazy typist, and pressing Shift all the time adds up! I could potentially use withColumnRenamed() with a for loop over all the columns to rename them in my data frame. The PySpark developers thought about this and offered a better way to rename all the columns of your data frame in one fell swoop. This relies on a method, toDF(), that returns a new data frame with the new columns. Just like drop(), toDF() takes a *cols, and just like select() and drop(), we need to unpack our column names if they’re in a list. The code in the next listing shows how you can rename all your columns to lowercase in a single line using that method.

```
Listing 4.16 Batch lowercasing using the toDF() method

logs.toDF(*[x.lower() for x in logs.columns]).printSchema()
 
# root
#  |-- logserviceid: integer (nullable = true)
#  |-- logdate: timestamp (nullable = true)
#  |-- audiencetargetageid: integer (nullable = true)
#  |-- audiencetargetethnicid: integer (nullable = true)
#  |-- categoryid: integer (nullable = true)
#  [...]
#  |-- language2: integer (nullable = true)
#  |-- duration_seconds: integer (nullable = true)
```

Our final step is reordering columns. Since reordering columns is equivalent to selecting columns in a different order, select() is the perfect method for the job. For instance, if we wanted to sort the columns alphabetically, we could use the sorted function on the list of our data frame columns, just like in the next listing
```
Listing 4.17 Selecting our columns in alphabetical order using select()

logs.select(sorted(logs.columns)).printSchema()
 
# root
#  |-- AudienceTargetAgeID: integer (nullable = true)
#  |-- AudienceTargetEthnicID: integer (nullable = true)
#  |-- BroadcastOriginPointID: integer (nullable = true)
#  |-- CategoryID: integer (nullable = true)
#  |-- ClosedCaptionID: integer (nullable = true)
#  |-- CompositionID: integer (nullable = true)
#  [...]
#  |-- Subtitle: string (nullable = true)
#  |-- duration_seconds: integer (nullable = true)   ❶
```
❶ Remember that, in most programming languages, capital letters come before lowercase ones.

# 4.4.5 Diagnosing a data frame with describe() and summary()
When working with numerical data, looking at a long column of values isn’t very useful. We’re often more concerned about some key information, which may include count, mean, standard deviation, minimum, and maximum. In this section, I cover how we can quickly explore numerical columns using PySpark’s describe() and summary() methods.

When applied to a data frame with no parameters, describe() will show summary statistics (count, mean, standard deviation, min, and max) on all numerical and string columns. To avoid screen overflow, I display the column descriptions one by one by iterating over the list of columns and showing the output of describe() in the next listing. Note that describe() will (lazily) compute the data frame but won’t display it, just like any transformation, so we have to show() the result.
```
Listing 4.18 Describing everything in one fell swoop

for i in logs.columns:
    logs.describe(i).show()
 
# +-------+------------------+   ❶
# |summary|      LogServiceID|
# +-------+------------------+
# |  count|           7169318|
# |   mean|3453.8804215407936|
# | stddev|200.44137201584468|
# |    min|              3157|
# |    max|              3925|
# +-------+------------------+
#
# [...]
#
# +-------+                      ❷
# |summary|
# +-------+
# |  count|
# |   mean|
# | stddev|
# |    min|
# |    max|
# +-------+
 
# [... many more little tables]
```
❶ Numerical columns will display the information in a description table, like so.

❷ If the type of the column isn’t compatible, PySpark displays only the title column.

It will take more time than doing everything in one fell swoop, but the output will be a lot friendlier. Since we can’t compute the mean or standard deviation of a string, you’ll see null values for those columns. Furthermore, some columns won’t be displayed (you’ll see time tables with only the title column), as describe() will only work for numerical and string columns. For a short line to type, you still get a lot!

describe() is a fantastic method, but what if you want more? summary() to the rescue!

Where describe() will take *cols as a parameter (one or more columns, the same way as select() or drop()), summary() will take *statistics as a parameter. This means that you’ll need to select the columns you want to see before passing the summary() method. On the other hand, we can customize the statistics we want to see. By default, summary() shows everything describe() shows, adding the approximate 25-50% and 75% percentiles. The next listing shows how you can replace describe() for summary() and the result of doing so.

```
Listing 4.19 Summarizing everything in one fell swoop

for i in logs.columns:
    logs.select(i).summary().show()                             ❶
 
# +-------+------------------+
# |summary|      LogServiceID|
# +-------+------------------+
# |  count|           7169318|
# |   mean|3453.8804215407936|
# | stddev|200.44137201584468|
# |    min|              3157|
# |    25%|              3291|
# |    50%|              3384|
# |    75%|              3628|
# |    max|              3925|
# +-------+------------------+
#
# [... many more slightly larger tables]
 
for i in logs.columns:
    logs.select(i).summary("min", "10%", "90%", "max").show()   ❷
 
# +-------+------------+
# |summary|LogServiceID|
# +-------+------------+
# |    min|        3157|
# |    10%|        3237|
# |    90%|        3710|
# |    max|        3925|
# +-------+------------+
#
# [...]
```

❶ By default, we have count, mean, stddev, min, 25%, 50%, 75%, max as statistics.

❷ We can also pass our own, following the same nomenclature convention.

If you want to limit yourself to a subset of those metrics, summary() will accept a number of string parameters representing the statistic. You can input count, mean, stddev, min, or max directly. For approximate percentiles, you need to provide them in XX% format, such as 25%.

Both methods will work only on non-null values. For the summary statistics, it’s the expected behavior, but the “count” entry will also count only the non-null values for each column. This is a good way to see which columns are mostly empty!

`WARNING` describe() and summary() are two very useful methods, but they are not meant to be used for anything other than quickly peeking at data during development. The PySpark developers don’t guarantee that the output will look the same from version to version, so if you need one of the outputs for your program, use the corresponding function in pyspark.sql .functions. They’re all there.

# Summary
1. PySpark uses the SparkReader object to directly read any kind of data in a data frame. The specialized CSVSparkReader is used to ingest CSV files. Just like when reading text, the only mandatory parameter is the source location.

2. The CSV format is very versatile, so PySpark provides many optional parameters to account for this flexibility. The most important ones are the field delimiter, the record delimiter, and the quotation character, all of which have sensible defaults.

3. PySpark can infer the schema of a CSV file by setting the inferSchema optional parameter to True. PySpark accomplishes this by reading the data twice: once for setting the appropriate types for each column and once to ingest the data in the inferred format.

4. Tabular data is represented in a data frame in a series of columns, each having a name and a type. Since the data frame is a column-major data structure, the concept of rows is less relevant.

5. You can use Python code to explore the data efficiently, using the column list as any Python list to expose the elements of the data frame of interest.

6. The most common operations on a data frame are the selection, deletion, and creation of columns. In PySpark, the methods used are select(), drop(), and withColumn(), respectively.

7. select can be used for column reordering by passing a reordered list of columns.

8. You can rename columns one by one with the withColumnRenamed() method, or all at once by using the toDF() method.

9. You can display a summary of the columns with the describe() or summary() methods. describe() has a fixed set of metrics, while summary() will take functions as parameters and apply them to all columns.

## Create a new data frame, logs_clean, that contains only the columns that do not end with ID.
`logs.select(*[x for x in logs.columns if x.lower()[-2:]!='id']).show(5)`
```
+----------+----------------+----------------+------------+------------+--------------------+----------------+--------+---------+---------+---------+---------+---------------+
|   LogDate|        Duration|         EndTime|LogEntryDate|ProductionNO|        ProgramTitle|       StartTime|Subtitle|Producer1|Producer2|Language1|Language2|duration_in_sec|
+----------+----------------+----------------+------------+------------+--------------------+----------------+--------+---------+---------+---------+---------+---------------+
|2018-08-01|02:00:00.0000000|08:00:00.0000000|  2018-08-01|      A39082|   Newlywed and Dead|06:00:00.0000000|    null|     null|     null|       94|     null|           7200|
|2018-08-01|00:00:30.0000000|06:13:45.0000000|  2018-08-01|        null|15-SPECIALTY CHAN...|06:13:15.0000000|    null|     null|     null|     null|     null|             30|
|2018-08-01|00:00:15.0000000|06:14:00.0000000|  2018-08-01|        null|3-PROCTER & GAMBL...|06:13:45.0000000|    null|     null|     null|     null|     null|             15|
|2018-08-01|00:00:15.0000000|06:14:15.0000000|  2018-08-01|        null|12-CREDIT KARMA-B...|06:14:00.0000000|    null|     null|     null|     null|     null|             15|
|2018-08-01|00:00:15.0000000|06:14:30.0000000|  2018-08-01|        null|3-L'OREAL CANADA-...|06:14:15.0000000|    null|     null|     null|     null|     null|             15|
+----------+----------------+----------------+------------+------------+--------------------+----------------+--------+---------+---------+---------+---------+---------------+
only showing top 5 rows
```

# 5 Data frame gymnastics: Joining and grouping
This chapter covers

Joining two data frames together

Selecting the right type of join for your use case

Grouping data and understanding the GroupedData transitional object

Breaking the GroupedData with an aggregation method

Filling null values in your data frame

# 5.1 From many to one: Joining data
What happens when we need to link two sources? This section will introduce joins and how we can apply them when using a star schema setup or another set of tables where values match exactly.

Joining data frames is a common operation when working with related tables. If you’ve used other data-processing libraries, you might have seen the same operation called a merge or a link. Because there are multiple ways to perform joins, the next section sets a common vocabulary to avoid confusion and build understanding on solid ground.

# 5.1.1 What’s what in the world of joins
At the most basic level, a join operation is a way to take the data from one data frame and link it to another one according to a set of rules. To introduce the moving parts of a join, I provide a second table to be joined to our logs data frame in listing 5.1. I use the same parameterization of the SparkReader.csv as used for the logs table to read our new log_identifier table. Once the table is ingested, I filter the data frame to keep only the primary channels, as per the data documentation. With this, we should be good to go.
```
Listing 5.1 Exploring our first link table: log_identifier

DIRECTORY = "./data/broadcast_logs"
log_identifier = spark.read.csv(
    os.path.join(DIRECTORY, "ReferenceTables/LogIdentifier.csv"),
    sep="|",
    header=True,
    inferSchema=True,
)
 
log_identifier.printSchema()
# root
#  |-- LogIdentifierID: string (nullable = true)              ❶
#  |-- LogServiceID: integer (nullable = true)                ❷
#  |-- PrimaryFG: integer (nullable = true)                   ❸
 
log_identifier = log_identifier.where(F.col("PrimaryFG") == 1)
print(log_identifier.count())
# 758
 
log_identifier.show(5)
# +---------------+------------+---------+
# |LogIdentifierID|LogServiceID|PrimaryFG|
# +---------------+------------+---------+
# |           13ST|        3157|        1|
# |         2000SM|        3466|        1|
# |           70SM|        3883|        1|
# |           80SM|        3590|        1|
# |           90SM|        3470|        1|
# +---------------+------------+---------+
# only showing top 5 rows
```
❶ This is the channel identifier.

❷ This is the channel key (which maps to our center table).

❸ This is a Boolean flag: Is the channel primary (1) or (0)? We want only the 1s.

We have two data frames, logs and log_identifier, each containing a set of columns. We are ready to start joining!

The join operation has three major ingredients:

1. Two tables, called a left and a right table, respectively

2. One or more predicates, which are the series of conditions that determine how records between the two tables are joined

3. A method to indicate how we perform the join when the predicate succeeds and when it fails

With these three ingredients, you can construct a join between two data frames in PySpark by filling the blueprint in listing 5.2 with the relevant keywords to accomplish the desired behavior. Every join operation in PySpark will follow the same blueprint. The next few sections will take each keyword and illustrate how they impact the end result.
```
Listing 5.2 A bare-bone recipe for a join in PySpark

[LEFT].join(
    [RIGHT],
    on=[PREDICATES]
    how=[METHOD]
)
```

# 5.1.2 Knowing our left from our right
A join is performed on two tables at a time. In this section, we cover the [LEFT] and [RIGHT] blocks of listing 5.2. Knowing which table is called left and which is called right is helpful when discussing join types, so we start with this useful vocabulary.

Because of the SQL heritage in the data manipulation vocabulary, the two tables are named left and right tables. In PySpark, a neat way to remember which is which is to say that the left table is to the left of the join() method, whereas the right is to the right (inside the parentheses). Knowing which is which is very useful when choosing the join method. Unsurprisingly, there are a left and right join types (see section 5.1.4).

Our tables are now identified, so we can update our join blueprint as in the next listing. We now need to steer our attention to the next parameter, the predicates.
```
Listing 5.3 A bare-bone join in PySpark, with left and right tables filled in

logs.join(            ❶
    log_identifier,   ❷
    on=[PREDICATES]
    how=[METHOD]
)
```
❶ logs is the left table . . .

❷ . . . and log_identifier is the right table.

# 5.1.3 The rules to a successful join: The predicates
This section covers the `[PREDICATES]` block of the join blueprint, which is the cornerstone of determining what records from the left table will match the right table. Most predicates in join operations are simple, but they can grow significantly in complexity depending on the logic you want. I introduce the simplest and most common use cases first before graduating to more complex predicates.

The predicates of a PySpark join are rules between columns of the left and right data frames. A join is performed record-wise, where each record on the left data frame is compared (via the predicates) to each record on the right data frame. If the predicates return `True`, the join is a match and is a no-match if `False`. We can think of this like a two-way `where` (see chapter 2): you match the values from one table to the other, and the (Boolean) result of the predicate block determines if it’s a match.

The best way to illustrate a predicate is to create a simple example and explore the results. For our two data frames, we will build the predicate `logs["LogServiceID"] == log_identifier["LogServiceID"]`. In plain English, this translates to “match the records from the logs data frame to the records from the log_identifier data frame when the value of their LogServiceID column is equal.”
I’ve taken a small sample of the data in both data frames and illustrated the result of applying the predicate in figure 5.1. There are two important points to highlight:

If one record in the left table resolves the predicate with more than one record in the right table (or vice versa), this record will be duplicated in the joined table.

If one record in the left or right table does not resolve the predicate with any record in the other table, it will not be present in the resulting table unless the join method (see section 5.1.4) specifies a protocol for failed predicates.
![image](https://github.com/kj2698/BigData_Bootcamp/assets/101991863/f7af15a9-998d-4784-ab22-d13e11647d2e)

Figure 5.1 A simple join predicate resolution between logs and log_identifier using LogServiceID in both tables and equality testing in the predicate. I show only the four successes in the result table. Our predicate is applied to a sample of our two tables: 3590 in the left table resolves the predicate twice, while 3417 on the left and 3883 on the right have no matches.

In our example, the `3590` record on the left is equal to the two corresponding records on the right, and we see two solved predicates with this number in our result set. On the other hand, the `3417` record does not match anything on the right, and therefore is not present in the result set. The same thing happens with the `3883` record in the right table.

You are not limited to a single test in your predicate. You can use multiple conditions by separating them with Boolean operators such as | (or) or & (and). You can also use a different test than equality. Here are two examples and their plain English translation:

`(logs["LogServiceID"] == log_identifier["LogServiceID"]) & (logs["left_ col"] < log_identifier["right_col"])`—This will only match the records that have the same LogServiceID on both sides and where the value of the left_col in the logs table is smaller than the value of the right_col in the log_identifier table.

`(logs["LogServiceID"] == log_identifier["LogServiceID"]) | (logs["left_ col"] > log_identifier["right_col"])`—This will only match the records that have the same LogServiceID on both sides or where the value of the left_col in the logs table is greater than the value of the right_col in the log_identifier table.

You can make the operations as complicated as you want. I recommend wrapping each condition in parentheses to avoid worrying about operator precedence and to facilitate the reading.

Before adding our predicate to our join in progress, I want to note that PySpark provides a few predicate shortcuts to reduce the complexity of the code. If you have multiple and predicates (such as `(left["col1"] == right["colA"]) & (left["col2"] > right["colB"]) & (left["col3"] != right["colC"]))`, you can put them into a list, such as `[left["col1"] == right["colA"], left["col2"] > right["colB"], left["col3"] != right["colC"]]`. This makes your intent more explicit and avoids counting parentheses for long chains of conditions.

Finally, if you are performing an “equi-join,” where you are testing for equality between identically named columns, you can simply specify the name of the columns as a string or a list of strings as a predicate. In our case, it means that our predicate can only be `"LogServiceID"`. This is what I put in the following listing.
```
Listing 5.4 A join in PySpark, with left and right tables and predicate

logs.join(
    log_identifier,
    on="LogServiceID"
    how=[METHOD]
)
```

# 5.1.4 How do you do it: The join method
A join method boils down to these two questions:

What happens when the return value of the predicates is True?

What happens when the return value of the predicates is False?

Classifying the join methods based on the answer to these questions is an easy way to remember them.

`TIP` PySpark’s joins are essentially the same as SQL’s. If you are already comfortable with them, feel free to skip this section.

INNER JOIN

An inner join `(how="inner")` is the most common join. PySpark will default to an inner join if you don’t pass a join method explicitly. It returns a record if the predicate is true and drops it if false. I consider an inner join the natural way to think of joins because they are very simple to reason about.

If we look at our tables, we have a table very similar to figure 5.1. The record with the `LogServiceID == 3590` on the left will be duplicated because it matches two records in the right table. The result is illustrated in figure 5.2.
![image](https://github.com/kj2698/BigData_Bootcamp/assets/101991863/b592ae34-ceed-4251-9de7-4cda9a40c7de)

Figure 5.2 An inner join. Each successful predicate creates a joined record.

LEFT AND RIGHT OUTER JOIN

Left `(how="left" or how="left_outer")` and right `(how="right" or how="right_ outer")`, as displayed in figure 5.4, are like an inner join in that they generate a record for a successful predicate. The difference is what happens when the predicate is false:

A left (also called a left outer) join will add the unmatched records from the left table in the joined table, filling the columns coming from the right table with `null`.

A right (also called a right outer) join will add the unmatched records from the right in the joined table, filling the columns coming from the left table with `null`.

In practice, this means that your joined table is guaranteed to contain all the records of the table that feed the join (left or right). Visually, figure 5.3 shows this. Although `3417` doesn’t satisfy the predicate, it is still present in the left joined table. The same happens with `3883` and the right table. Just like an inner join, if the predicate is successful more than once, the record will be duplicated.
![image](https://github.com/kj2698/BigData_Bootcamp/assets/101991863/151af501-1867-4123-a694-095b56d1c980)

Figure 5.3 A left and right joined table. All the records of the direction table are present in the resulting table.

Left and right joins are very useful when you are not certain if the link table contains every key. You can then fill the null values (see listing 5.16) or process them knowing you didn’t drop any records.

FULL OUTER JOIN

A full outer `(how="outer", how="full"`, or `how="full_outer")` join is simply the fusion of a left and right join. It will add the unmatched records from the left and the right table, padding with null. It serves a similar purpose to the left and right join but is not as popular since you’ll generally have one (and only one) anchor table where you want to preserve all records.
![image](https://github.com/kj2698/BigData_Bootcamp/assets/101991863/26c465cb-42f5-41ae-84d4-96cfd3b8affa)

Figure 5.4 A left and right joined table. We can see all the records from both tables.

LEFT SEMI-JOIN AND LEFT ANTI-JOIN

The left semi-join and left anti-join are less popular but still quite useful nonetheless.

A left semi-join (`how="left_semi"`) is the same as an inner join, but keeps the columns in the left table. It also won’t duplicate the records in the left table if they fulfill the predicate with more than one record in the right table. Its main purpose is to filter records from a table based on a predicate that is depending on another table.

A left anti-join (`how="left_anti"`) is the opposite of an inner join. It will keep only the records from the left table that do not match the predicate with any record in the right table. If a record from the left table matches a record from the right table, it gets dropped from the join operation.

Our blueprint join is now finalized: we are going with an inner join since we want to keep only the records where the LogServiceID has additional information in our log_identifier table. Since our join is complete, I assign the result to a new variable: logs_and_channels.
```
Listing 5.5 Our join in PySpark, with all the parameters filled in

logs_and_channels = logs.join(
    log_identifier,
    on="LogServiceID",
    how="inner"          ❶
)
```
❶ I could have omitted the how parameter outright, since inner join is the default.

CROSS JOIN: THE NUCLEAR OPTION

A cross join (`how="cross"`) is the nuclear option. It returns a record for every record pair, regardless of the value the predicates return. In our data frame example, our `logs` table contains four records and our `logs_identifier` five records, so the cross join will contain 4 × 5 = 20 records. The result is illustrated in figure 5.5.
![image](https://github.com/kj2698/BigData_Bootcamp/assets/101991863/1e78a254-75c7-4041-8961-be422e24bd43)
Figure 5.5 A visual example of a cross join. Each record on the left is matched to every record on the right.

Cross joins are seldom the operation you want, but they are useful when you want a table that contains every possible combination.

`TIP` PySpark also provides an explicit `crossJoin()` method that takes the right data frame as a parameter.

When joining data in a distributed environment, `“we don’t care about where data is” no longer works`. To be able to process a comparison between records, the data needs to be on the same machine. If not, PySpark will move the data in an operation called a `shuffle`. As you can imagine, moving large amounts of data over the network is very slow, and we should aim to avoid this when possible.
This is one of the instances in which PySpark’s abstraction model shows some weakness. 

# 5.1.5 Naming conventions in the joining world
This section covers how PySpark manages column and data frame names. While this applies beyond the join world, name clashing is most painful when you are trying to assemble many data frames into one. We cover how to prevent name clashing and how to treat it if you inherit an already mangled data frame.

By default, PySpark will not allow two columns to be named the same. If you create a column with `withColumn()` using an existing column name, PySpark will overwrite (or shadow) the column. When joining data frames, the situation is a little more complicated, as displayed in the following listing.
```
logs_and_channels_verbose = logs.join(
    log_identifier, logs["LogServiceID"] == log_identifier["LogServiceID"]
)
 
logs_and_channels_verbose.printSchema()
 
# root
#  |-- LogServiceID: integer (nullable = true)                                   ❶
#  |-- LogDate: timestamp (nullable = true)
#  |-- AudienceTargetAgeID: integer (nullable = true)
#  |-- AudienceTargetEthnicID: integer (nullable = true)
#  [...]
#  |-- duration_seconds: integer (nullable = true)
#  |-- LogIdentifierID: string (nullable = true)
#  |-- LogServiceID: integer (nullable = true)                                   ❷
#  |-- PrimaryFG: integer (nullable = true)
 
try:
    logs_and_channels_verbose.select("LogServiceID")
except AnalysisException as err:
    print(err)
 
# "Reference 'LogServiceID' is ambiguous, could be: LogServiceID, LogServiceID.;"❸
```
❶ This is one LogServiceID column . . .

❷ . . . and this is another.

❸ PySpark doesn’t know which column we mean: is it LogServiceID or LogServiceID?

PySpark happily joins the two data frames but fails when we try to work with the ambiguous column. This is a common situation when working with data that follows the same convention for column naming. To solve this problem, in this section I show three methods, from the easiest to the most general.

First, when performing an equi-join, I prefer using the simplified syntax, since it takes care of removing the second instance of the predicate column. This only works when using an equality comparison, since the data is identical in both columns from the predicate, which prevents information loss. I show the code and schema of the resulting data frame when using a simplified equi-join in the next listing.

```
Listing 5.7 Using the simplified syntax for equi-joins

logs_and_channels = logs.join(log_identifier, "LogServiceID")
 
logs_and_channels.printSchema()
 
# root
#  |-- LogServiceID: integer (nullable = true)
#  |-- LogDate: timestamp (nullable = true)
#  |-- AudienceTargetAgeID: integer (nullable = true)
#  |-- AudienceTargetEthnicID: integer (nullable = true)
#  |-- CategoryID: integer (nullable = true)
#  [...]
#  |-- Language2: integer (nullable = true)
#  |-- duration_seconds: integer (nullable = true)
#  |-- LogIdentifierID: string (nullable = true)    ❶
#  |-- PrimaryFG: integer (nullable = true)         ❶
```
❶ No LogServiceID here: PySpark kept only the first referred column.

The second approach relies on the fact that PySpark-joined data frames remember the origin of the columns. Because of this, we can refer to the `LogServiceID` columns using the same nomenclature as before (i.e., `log_identifier["LogServiceID"]`). We can then rename this column or delete it, and thus solve our issue. I use this approach in the following listing.

```
Listing 5.8 Using the origin name of the column for unambiguous selection

logs_and_channels_verbose = logs.join(
    log_identifier, logs["LogServiceID"] == log_identifier["LogServiceID"]
)
 
logs_and_channels.drop(log_identifier["LogServiceID"]).select(
    "LogServiceID")                                             ❶
 
# DataFrame[LogServiceID: int]
```

❶ By dropping one of the two duplicated columns, we can then use the name for the other without any problem.

The last approach is convenient if you use the `Column` object directly. PySpark will not resolve the origin name when you rely on `F.col()` to work with columns. To solve this in the most general way, we need to `alias()` our tables when performing the join, as shown in the following listing.
```
Listing 5.9 Aliasing our tables to resolve the origin

logs_and_channels_verbose = logs.alias("left").join(        ❶
    log_identifier.alias("right"),                          ❷
    logs["LogServiceID"] == log_identifier["LogServiceID"],
)
 
logs_and_channels_verbose.drop(F.col("right.LogServiceID")).select(
    "LogServiceID"
)                                                           ❸
 
# DataFrame[LogServiceID: int]
```
❶ Our logs table gets aliased as left.

❷ Our log_identifier gets aliased as right.

❸ F.col() will resolve left and right as a prefix for the column names.

All three approaches are valid. The first one works only in the case of equi-joins, but the two others are mostly interchangeable. PySpark gives you a lot of control over the structure and naming of your data frame but requires you to be explicit.

This section packed in a lot of information about joins, a very important tool when working with interrelated data frames. Although the possibilities are endless, the syntax is simple and easy to understand: `left.join(right` decides the first parameter. `on` decides if it’s a match. `how` indicates how to operate on match success and failures.

Now that the first join is done, we will link two additional tables to continue our data discovery and processing. The `CategoryID` table contains information about the types of programs, and the `ProgramClassID` table contains the data that allows us to pinpoint the commercials.

This time, we are performing `left` joins since we are not entirely certain about the existence of the keys in the link table. In listing 5.10, we follow the same process as we did for the `log_identifier` table in one fell swoop:

We read the table using the `SparkReader.csv` and the same configuration as our other tables.

We keep the relevant columns.

We join the data to our `logs_and_channels` table, using PySpark’s method chaining.

```
Listing 5.10 Linking the category and program class tables using two left joins

DIRECTORY = "./data/broadcast_logs"
 
cd_category = spark.read.csv(
    os.path.join(DIRECTORY, "ReferenceTables/CD_Category.csv"),
    sep="|",
    header=True,
    inferSchema=True,
).select(
    "CategoryID",
    "CategoryCD",
    F.col("EnglishDescription").alias("Category_Description"),      ❶
)
 
cd_program_class = spark.read.csv(
    os.path.join(DIRECTORY, "ReferenceTables/CD_ProgramClass.csv"),
    sep="|",
    header=True,
    inferSchema=True,
).select(
    "ProgramClassID",
    "ProgramClassCD",
    F.col("EnglishDescription").alias("ProgramClass_Description"),  ❷
)
 
full_log = logs_and_channels.join(cd_category, "CategoryID", how="left").join(
    cd_program_class, "ProgramClassID", how="left"
)
```
❶ We’re aliasing the EnglishDescription column to remember what it maps to.

❷ We’re also aliasing here, but for the program class.

With our table nicely augmented, let’s move to our last step: summarizing the table using groupings.

## Exercise 5.1

Assume two tables, left and right, each containing a column named my_column. What is the result of this code?

one = left.join(right, how="left_semi", on="my_column")

two = left.join(right, how="left_anti", on="my_column")
 
one.union(two)

```
left=spark.createDataFrame([[1,'hi'],[2,'hey'],[3,'yo'],[4,'yoyo']],['col1','col2'])
>>> left.show()
+----+----+
|col1|col2|
+----+----+
|   1|  hi|
|   2| hey|
|   3|  yo|
|   4|yoyo|
+----+----+

>>> right=spark.createDataFrame([[2,'hello'],[2,'sup']],['col1','col2'])
>>> right.show()
+----+-----+
|col1| col2|
+----+-----+
|   2|hello|
|   2|  sup|
+----+-----+

one=left.join(right,on='col1',how='left_semi')
>>> one.show()
+----+----+
|col1|col2|
+----+----+
|   2| hey|
+----+----+

 two=left.join(right,on='col1',how='left_anti')
>>> two.show()
+----+----+
|col1|col2|
+----+----+
|   1|  hi|
|   3|  yo|
|   4|yoyo|
+----+----+

one.union(two).show()
+----+----+
|col1|col2|
+----+----+
|   2| hey|
|   1|  hi|
|   3|  yo|
|   4|yoyo|
+----+----+
```

##Exercise 5.2

Assume two data frames, red and blue. Which is the appropriate join to use in red.join(blue, ...) if you want to join red and blue and keep all the records satisfying the predicate?

a) Left

b) Right

c) Inner

d) Theta

e) Cross

##Exercise 5.3

Assume two data frames, red and blue. Which is the appropriate join to use in red.join(blue, ...) if you want to join red and blue and keep all the records satisfying the predicate and the records in the blue table?

a) Left

b) Right

c) Inner

d) Theta

e) Cross

# 5.2 Summarizing the data via groupby and GroupedData
This section covers how to summarize a data frame into more granular dimensions (versus the entire data frame) via the `groupby()` method. We already grouped our text data frame in 3; this section goes deeper into the specifics of grouping. Here, I introduce the `GroupedData` object and its usage. In practical terms, we’ll use `groupby()` to answer our original question: what are the channels with the greatest and least proportion of commercials? To answer this, we have to take each channel and sum the `duration_seconds` in two ways:

One to get the number of seconds when the program is a commercial

One to get the number of seconds of total programming

# 5.2.1 A simple groupby blueprint
Since you are already acquainted with the basic syntax of groupby(), this section starts by presenting a full code block that computes the total duration (in seconds) of the program class. In the next listing we perform the grouping, compute the aggregate function, and present the results in decreasing order.
```
Listing 5.11 Displaying the most popular types of programs
(full_log
 .groupby("ProgramClassCD", "ProgramClass_Description")
 .agg(F.sum("duration_seconds").alias("duration_total"))
 .orderBy("duration_total", ascending=False).show(100, False)
 )
 
# +--------------+--------------------------------------+--------------+
# |ProgramClassCD|ProgramClass_Description              |duration_total|
# +--------------+--------------------------------------+--------------+
# |PGR           |PROGRAM                               |652802250     |
# |COM           |COMMERCIAL MESSAGE                    |106810189     |
# |PFS           |PROGRAM FIRST SEGMENT                 |38817891      |
# |SEG           |SEGMENT OF A PROGRAM                  |34891264      |
# |PRC           |PROMOTION OF UPCOMING CANADIAN PROGRAM|27017583      |
# |PGI           |PROGRAM INFOMERCIAL                   |23196392      |
# |PRO           |PROMOTION OF NON-CANADIAN PROGRAM     |10213461      |
# |OFF           |SCHEDULED OFF AIR TIME PERIOD         |4537071       |
# [... more rows]
# |COR           |CORNERSTONE                           |null          |
# +--------------+--------------------------------------+--------------+
```
This small program has a few new parts, so let’s review them one by one.
![image](https://github.com/kj2698/BigData_Bootcamp/assets/101991863/cd6a4a46-99f1-4e0a-97c7-d4d6ab8f880c)

Figure 5.6 The original data frame, with the focus on the columns we are grouping by
Our group routing starts with the `groupby()` method on the data frame shown in figure 5.6. A “grouped by” data frame is no longer a data frame; instead, it becomes a `GroupedData` object and is displayed in all its glory in listing 5.12. This object is a transitional object: you can’t really inspect it (there is no `.show()` method), and it’s waiting for further instructions to become showable again. Illustrated, it would look like the right-hand side of figure 5.7.
![image](https://github.com/kj2698/BigData_Bootcamp/assets/101991863/76e210ed-e03f-4b1e-8f8d-8d3e7ed405c2)

Figure 5.7 The GroupedData object resulting from grouping
`Aggregating for the lazy`

`agg()` also accepts a dictionary in the form `{column_name: aggregation_function}` where both are string. Because of this, we can rewrite listing 5.11 like so:
```
full_log.groupby("ProgramClassCD", "ProgramClass_Description").agg(
    {"duration_seconds": "sum"}
).withColumnRenamed("sum(duration_seconds)", "duration_total").orderBy(
    "duration_total", ascending=False
).show(
    100, False
)
```
This makes rapid prototyping very easy (you can, just like with column objects, use the "*" to refer to all columns). I personally don’t like this approach for most cases since you don’t get to alias your columns when creating them. I am including it since you will see it when reading other people’s code.
```
Listing 5.12 A GroupedData object representation

full_log.groupby()
# <pyspark.sql.group.GroupedData at 0x119baa4e0>
```

n chapter 3, we brought back the `GroupedData` into a data frame by using the `count()` method, which returns the count of each group. There are a few others, such as `min(), max(), mean(), or sum()`. We could have used the sum() method directly, but we wouldn’t have had the option of aliasing the resulting column and would have gotten stuck with sum(duration_seconds) for a name. Instead, we use the oddly named agg().

The `agg()` method, for aggregate (or aggregation), will take one or more aggregate functions from the `pyspark.sql.functions` module we all know and love, and apply them on each group of the `GroupedData` object. In figure 5.8, I start on the left with our GroupedData object. Calling `agg()` with an appropriate aggregate function pulls the column from the group cell, extracts the values, and performs the function, yielding the answer. Compared to using the `sum()` function on the groupby object, agg() trades a few keystrokes for two main advantages:

1. `agg()` takes an arbitrary number of aggregate functions, unlike using a summary method directly. You can’t chain multiple functions on `GroupedData` objects: the first one will transform it into a data frame, and the second one will fail.

2. You can alias resulting columns so that you control their name and improve the robustness of your code.
![image](https://github.com/kj2698/BigData_Bootcamp/assets/101991863/0f69aea3-d3bb-4a3f-9fbe-1847a8461449)

Figure 5.8 A data frame arising from the application of the `agg()` method (aggregate function: `F.sum() on Duration_seconds`)

After the application of the aggregate function on our `GroupedData` object, we again have a data frame. We can then use the `orderBy()` method to order the data by decreasing order of `duration_total`, our newly created column. We finish by showing 100 rows, which is more than what the data frame contains, so it shows everything.
Table 5.1 The types of programs we’ll consider as commercials
![image](https://github.com/kj2698/BigData_Bootcamp/assets/101991863/41d5b283-6eb3-4171-a891-5e8b5930ca62)

agg() is not the only player in town

You can also use groupby(), with the apply() (Spark 2.3+) and applyInPandas() (Spark 3.0+) method, in the creatively named split-apply-combine pattern. We explore this powerful tool in chapter 9. Other less-used (but still useful) methods are also available.

# 5.2.2 A column is a column: Using agg() with custom column definitions
When grouping and aggregating columns in PySpark, we have all the power of the `Column` object at our fingertips. This means that we can group by and aggregate on custom columns! For this section, we will start by building a definition of `duration_ commercial`, which takes the duration of a program only if it is a commercial, and use this in our `agg()` statement to seamlessly compute both the total duration and the commercial duration.

If we encode the contents of table 5.1 into a PySpark definition, it gives us the next listing.
```
Listing 5.13 Computing only the commercial time for each program in our table

F.when(
    F.trim(F.col("ProgramClassCD")).isin(
        ["COM", "PRC", "PGI", "PRO", "PSA", "MAG", "LOC", "SPO", "MER", "SOL"]
    ),
    F.col("duration_seconds"),
).otherwise(0)
```
I think that the best way to describe the code this time is to literally translate it into plain English.

`When` the field of the column ProgramClass, `trimmed` of spaces at the beginning and end of the field, `is in` our list of commercial codes, then take the value of the field in the column `duration_seconds`. `Otherwise`, use `zero` as a value.

The blueprint of the F.when() function is as follows. It is possible to chain multiple when() if we have more than one condition and to omit the otherwise() if we’re okay with having null values when none of the tests are positive:
```
(
F.when([BOOLEAN TEST], [RESULT IF TRUE])
 .when([ANOTHER BOOLEAN TEST], [RESULT IF TRUE])
 .otherwise([DEFAULT RESULT, WILL DEFAULT TO null IF OMITTED])
)
```
We now have a column ready to use. While we could create the column before grouping by, using withColumn(), let’s take it up a notch and use our definition in the agg() clause directly. The following listing does just that, and at the same time, gives us our answer!
`Listing 5.14 Using our new column into agg() to compute our final answer`
```
answer = (
    full_log.groupby("LogIdentifierID")
    .agg(
        F.sum(                                                              ❶
            F.when(                                                         ❶
                F.trim(F.col("ProgramClassCD")).isin(                       ❶
                    ["COM", "PRC", "PGI", "PRO", "LOC", "SPO", "MER", "SOL"]❶
                ),                                                          ❶
                F.col("duration_seconds"),                                  ❶
            ).otherwise(0)                                                  ❶
        ).alias("duration_commercial"),                                     ❶
        F.sum("duration_seconds").alias("duration_total"),
    )
    .withColumn(
        "commercial_ratio", F.col(
            "duration_commercial") / F.col("duration_total")
    )
)
 
answer.orderBy("commercial_ratio", ascending=False).show(1000, False)
 
# +---------------+-------------------+--------------+---------------------+
# |LogIdentifierID|duration_commercial|duration_total|commercial_ratio     |
# +---------------+-------------------+--------------+---------------------+
# |HPITV          |403                |403           |1.0                  |
# |TLNSP          |234455             |234455        |1.0                  |
# |MSET           |101670             |101670        |1.0                  |
# |TELENO         |545255             |545255        |1.0                  |
# |CIMT           |19935              |19935         |1.0                  |
# |TANG           |271468             |271468        |1.0                  |
# |INVST          |623057             |633659        |0.9832686034602207   |
# [...]
# |OTN3           |0                  |2678400       |0.0                  |
# |PENT           |0                  |2678400       |0.0                  |
# |ATN14          |0                  |2678400       |0.0                  |
# |ATN11          |0                  |2678400       |0.0                  |
# |ZOOM           |0                  |2678400       |0.0                  |
# |EURO           |0                  |null          |null                 |
# |NINOS          |0                  |null          |null                 |
# +---------------+-------------------+--------------+---------------------+
```
❶ A column is a column: our F.when() function returns a column object that can be used in F.sum().
Wait a moment—the commercial ratio of some channels is 1.0; are some channels only commercials? If we look at the total duration, we can see that some channels don’t broadcast much. Since one day is 86,400 seconds (24 × 60 × 60), we see that HPITV only has 403 seconds of programming in our data frame. I am not too concerned about this right now, but we always have the option to filter() our way out and remove the channels that broadcast very little (see chapter 2). Still, we accomplished our goal: we identified the channels with the most commercials. We finish this chapter with one last task: processing those null values.

# 5.3 Taking care of null values: Drop and fill
`null` values represent the absence of value. I think this is a great oxymoron: a value for no value? Philosophy aside, we have some `nulls` in our result set, and I would like them gone. This section covers the two easiest ways to deal with `null` values in a data frame: you can either `dropna()` the record containing them or `fillna()` the null with a value. In this section, we explore both options to see which is best for our analysis.

# 5.3.1 Dropping it like it’s hot: Using dropna() to remove records with null values
Our first option is to plainly ignore the records that have null values. In this section, I cover the different ways to use the `dropna()` method to drop records based on the presence of null values.

`dropna()` is pretty easy to use. This data frame method takes three parameters:

`how`, which can take the value `any` or `all`. If `any` is selected, PySpark will drop records where at least one of the fields is null. In the case of `all`, only the records where all fields are null will be removed. By default, PySpark will take the any mode.

`thresh` takes an integer value. If set (its default is None), PySpark will ignore the how parameter and only drop the records with less than thresh `non-null values`.

`subset` will take an optional list of columns that `dropna()` will use to make its decision.

In our case, we want to keep only the records that have a commercial_ratio and that are `non-null`. We just have to pass our column to the subset parameter, like in the next listing.
```
sting 5.15 Dropping only the records that have a null commercial_ratio value

answer_no_null = answer.dropna(subset=["commercial_ratio"])
 
answer_no_null.orderBy(
    "commercial_ratio", ascending=False).show(1000, False)
 
# +---------------+-------------------+--------------+---------------------+
# |LogIdentifierID|duration_commercial|duration_total|commercial_ratio     |
# +---------------+-------------------+--------------+---------------------+
# |HPITV          |403                |403           |1.0                  |
# |TLNSP          |234455             |234455        |1.0                  |
# |MSET           |101670             |101670        |1.0                  |
# |TELENO         |545255             |545255        |1.0                  |
# |CIMT           |19935              |19935         |1.0                  |
# |TANG           |271468             |271468        |1.0                  |
# |INVST          |623057             |633659        |0.9832686034602207   |
# [...]
# |OTN3           |0                  |2678400       |0.0                  |
# |PENT           |0                  |2678400       |0.0                  |
# |ATN14          |0                  |2678400       |0.0                  |
# |ATN11          |0                  |2678400       |0.0                  |
# |ZOOM           |0                  |2678400       |0.0                  |
# +---------------+-------------------+--------------+---------------------+
 
print(answer_no_null.count())  # 322
```

# 5.3.2 Filling values to our heart’s content using fillna()
This section covers the `fillna()` method to replace `null` values.

`fillna()` is even simpler than `dropna()`. This data frame method takes two parameters:

The `value`, which is a Python int, float, string, or bool. PySpark will only fill the compatible columns; for instance, if we were to `fillna("zero")`, our commercial_ratio, being a double, would not be filled.

The same `subset` parameter we encountered in `dropna()`. We can limit the scope of our filling to only the columns we want.

In concrete terms, a null value in any of our numerical columns means that the value should be zero, so the next listing fills the null values with zero.
```
Listing 5.16 Filling our numerical records with zero using the fillna() method

answer_no_null = answer.fillna(0)
 
answer_no_null.orderBy(
    "commercial_ratio", ascending=False).show(1000, False)
 
# +---------------+-------------------+--------------+---------------------+
# |LogIdentifierID|duration_commercial|duration_total|commercial_ratio     |
# +---------------+-------------------+--------------+---------------------+
# |HPITV          |403                |403           |1.0                  |
# |TLNSP          |234455             |234455        |1.0                  |
# |MSET           |101670             |101670        |1.0                  |
# |TELENO         |545255             |545255        |1.0                  |
# |CIMT           |19935              |19935         |1.0                  |
# |TANG           |271468             |271468        |1.0                  |
# |INVST          |623057             |633659        |0.9832686034602207   |
# [...]
# |OTN3           |0                  |2678400       |0.0                  |
# |PENT           |0                  |2678400       |0.0                  |
# |ATN14          |0                  |2678400       |0.0                  |
# |ATN11          |0                  |2678400       |0.0                  |
# |ZOOM           |0                  |2678400       |0.0                  |
# +---------------+-------------------+--------------+---------------------+
 
print(answer_no_null.count())  # 324     ❶
```
❶ We have the two additional records that listing 5.15 dropped.

`The return of the dict`

You can also pass a dict to the fillna() method, with the column names as key and the values as dict values. If we were to use this method for our filling, the code would be like the following code:
```
Filling our numerical records with zero using the fillna() method and a dict
answer_no_null = answer.fillna(
    {"duration_commercial": 0, "duration_total": 0, "commercial_ratio": 0}
)
```

#5.4 What was our question again? Our end-to-end program
At the beginning of the chapter, we gave ourselves an anchor question to start exploring the data and uncover some insights. Throughout the chapter, we’ve assembled a cohesive data set containing the relevant information needed to identify commercial programs and ranked the channels based on how much of their programming is commercial. In listing 5.17, I’ve assembled all the relevant code blocks introduced in the chapter into a single program you can spark-submit. The code is also available in the book’s repository under code/Ch05/commercials.py. The end-of-chapter exercises also use this code.

Not counting data ingestion, comments, or docstring, our code is a rather small hundred or so lines of code. We could play code golf (trying to shrink the number of characters as much as we can), but I think we’ve struck a good balance between terseness and ease of reading. Once again, we haven’t paid much attention to the distributed nature of PySpark. Instead, we took a very descriptive view of our problem and translated it into code via PySpark’s powerful data frame abstraction and rich function ecosystems.

This chapter is the last chapter of the first part of the book. You are now familiar with the PySpark ecosystem and how you can use its main data structure, the data frame, to ingest and manipulate two very common sources of data, textual and tabular. You know a variety and method and functions that can be applied to data frames and columns, and can apply those to your own data problem. You can also leverage the documentation provided through the PySpark docstrings, straight from the PySpark shell.

There is a lot more you can get from the plain data manipulation portion of the book. Because of this, I recommend taking the time to review the PySpark online API and become proficient in navigating its structure. Now that you have a solid understanding of the data model and how to structure simple data manipulation programs, adding new functions to your PySpark quiver will be easy.

The second part of the book builds heavily on what you’ve learned so far:

We dig deeper into PySpark’s data model and find opportunities to refine our code. We will also look at PySpark’s column types, how they bridge to Python’s types, and how to use them to improve the reliability of our code.

We go beyond two-dimensional data frames with complex data types, such as the array, the map, and the struct, by ingesting hierarchical data.

We look at how PySpark modernizes SQL, an influential language for tabular data manipulation, and how you can blend SQL and Python in a single program.

We look at promoting pure Python code to run in the Spark-distributed environment. We formally introduce a lower-level structure, the resilient distributed dataset (RDD) and its row-major model. We also look at UDFs and pandas UDFs as a way to augment the functionality of the data frame.

```
Listing 5.17 Our full program, ordering channels by decreasing proportion of commercials

import os
 
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
 
spark = SparkSession.builder.appName(
    "Getting the Canadian TV channels with the highest/lowest proportion of commercials."
).getOrCreate()
 
spark.sparkContext.setLogLevel("WARN")
 
# Reading all the relevant data sources
 
DIRECTORY = "./data/broadcast_logs"
 
logs = spark.read.csv(
    os.path.join(DIRECTORY, "BroadcastLogs_2018_Q3_M8.CSV"),
    sep="|",
    header=True,
    inferSchema=True,
)
 
log_identifier = spark.read.csv(
    os.path.join(DIRECTORY, "ReferenceTables/LogIdentifier.csv"),
    sep="|",
    header=True,
    inferSchema=True,
)
cd_category = spark.read.csv(
    os.path.join(DIRECTORY, "ReferenceTables/CD_Category.csv"),
    sep="|",
    header=True,
    inferSchema=True,
).select(
    "CategoryID",
    "CategoryCD",
    F.col("EnglishDescription").alias("Category_Description"),
)
 
cd_program_class = spark.read.csv(
    "./data/broadcast_logs/ReferenceTables/CD_ProgramClass.csv",
    sep="|",
    header=True,
    inferSchema=True,
).select(
    "ProgramClassID",
    "ProgramClassCD",
    F.col("EnglishDescription").alias("ProgramClass_Description"),
)
 
# Data processing
 
logs = logs.drop("BroadcastLogID", "SequenceNO")
 
logs = logs.withColumn(
    "duration_seconds",
    (
        F.col("Duration").substr(1, 2).cast("int") * 60 * 60
        + F.col("Duration").substr(4, 2).cast("int") * 60
        + F.col("Duration").substr(7, 2).cast("int")
    ),
)
 
log_identifier = log_identifier.where(F.col("PrimaryFG") == 1)
 
logs_and_channels = logs.join(log_identifier, "LogServiceID")
 
full_log = logs_and_channels.join(cd_category, "CategoryID", how="left").join(
    cd_program_class, "ProgramClassID", how="left"
)
 
answer = (
    full_log.groupby("LogIdentifierID")
    .agg(
        F.sum(
            F.when(
                F.trim(F.col("ProgramClassCD")).isin(
                    ["COM", "PRC", "PGI", "PRO", "LOC", "SPO", "MER", "SOL"]
                ),
                F.col("duration_seconds"),
            ).otherwise(0)
        ).alias("duration_commercial"),
        F.sum("duration_seconds").alias("duration_total"),
    )
    .withColumn(
        "commercial_ratio", F.col("duration_commercial") / F.col("duration_total")
    )
    .fillna(0)
)
 
answer.orderBy("commercial_ratio", ascending=False).show(1000, False)
```

# Summary
1. PySpark implements seven join functionalities, using the common “what?,” “on what?,” and “how?” questions: cross, inner, left, right, full, left semi and left anti. Choosing the appropriate join method depends on how to process the records that resolve the predicates and those that do not.

2. PySpark keeps lineage information when joining data frames. Using this information, we can avoid column naming clashes.

3. You can group similar values using the `groupby()` method on a data frame. The method takes a number of column objects or strings representing columns and returns a GroupedData object.

4. `GroupedData` objects are transitional structures. They contain two types of columns: the key columns, which are the one you “grouped by” with, and the group cell, which is a container for all the other columns. The most common way to return to a data frame is to summarize the values in the column via the `agg()` function or via one of the direct aggregation methods, such as `count()` or `min()`.

5. You can drop records containing `null` values using `dropna()` or replace them with another value with the `fillna()` method.

