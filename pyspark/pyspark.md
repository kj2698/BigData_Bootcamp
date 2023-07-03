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

Make your life easier by using Python’s parentheses
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
