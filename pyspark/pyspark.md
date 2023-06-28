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







