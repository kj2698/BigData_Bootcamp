# creating database with properties and comment.
create database db1
comment 'This is a test db'
with dbproperties("who"="kj","day"="friday");

# key points to remember before dealing with complex data types/ nested complex dtypes
Hive's default delimiters are:

Row Delimiter => Control-A ('\001')
Collection Item Delimiter => Control-B ('\002')
Map Key Delimiter => Control-C ('\003')
If you override these delimiters then overridden delimiters are used during parsing. The preceding description of delimiters is correct for the usual case of flat data structures, where the complex types only contain primitive types. For nested types the level of the nesting determines the delimiter.

For an array of arrays, for example, the delimiters for the outer array are Control-B ('\002') characters, as expected, but for the inner array they are Control-C ('\003') characters, the next delimiter in the list.

Hive actually supports eight levels of delimiters, corresponding to ASCII codes 1, 2, ... 8, but you can only override the first three.

# creating table with nested complex data types.
create table temp1 as select "alex" as name,array('raipur','CG') as addr,map('python',95,'perl',90) as grade,map('key1',array('dev','lead')) as role;
insert into temp1 values('yoyo',array('pune'),map('py',70,'perl',80,'c',90),map('key1',array('dev'),'key2',array('dev','lead')));

hive> select * from temp1;

OK

alex    ["raipur","CG"] {"python":95,"perl":90} {"key1":["dev","lead"]}

yoyo    ["pune"]        {"py":70,"perl":80,"c":90}   {"key1":["dev"],"key2":["dev","lead"]}

Time taken: 0.899 seconds, Fetched: 2 row(s)

# Output file of the table looks like this.
alex^Araipur^BCG^Apython^C95^Bperl^C90^Akey1^Cdev^Dlead
yoyo^Apune^Apy^C70^Bperl^C80^Bc^C90^Akey1^Cdev^Bkey2^Cdev^Dlead

# one more example:
vi employee.txt

Michael|Montreal,Toronto|Male,30|DB:80|Product:Developer^DLead

Will|Montreal|Male,35|Perl:85|Product:Lead,Test:Lead

Shelley|New York|Female,27|Python:80|Test:Lead,COE:Architect

Lucy|Vancouver|Female,57|Sales:89,HR:94|Sales:Lead

```
CREATE TABLE employee(
      name STRING,
      work_place ARRAY<STRING>,
      gender_age STRUCT<gender:STRING,age:INT>,     
      skills_score MAP<STRING,INT>,     
      depart_title MAP<STRING,ARRAY<STRING>>      
      )
      ROW FORMAT DELIMITED
      FIELDS TERMINATED BY '|'
      COLLECTION ITEMS TERMINATED BY ','
      MAP KEYS TERMINATED BY ':'
      STORED AS TEXTFILE;
```
  
  For nested types, the level of nesting determines the delimiter. Using ARRAY of ARRAY as an example, the delimiters for the outer ARRAY, as expected, are Ctrl + B characters, but the inner ARRAY delimiter becomes Ctrl + C characters, which is the next delimiter in the list. In the preceding example, the depart_title column, which is a MAP of ARRAY, the MAP key delimiter is Ctrl + C, and the ARRAY delimiter is Ctrl + D.
  
# we already know about internal and external table. so moving on to temporary table.
Hive also supports creating temporary tables. A temporary table is only visible to the current user session. It's automatically deleted at the end of the session. The data of the temporary table is stored in the user's scratch directory, such as /tmp/hive-<username>. Therefore, make sure the folder is properly configured or secured when you have sensitive data in temporary tables. Whenever a temporary table has the same name as a permanent table, the temporary table will be chosen rather than the permanent table. A temporary table does not support partitions and indexes. The following are three ways to create temporary tables:
      
```
> CREATE TEMPORARY TABLE IF NOT EXISTS tmp_emp1 (
> name string,
> work_place ARRAY<string>,
> gender_age STRUCT<gender:string,age:int>,
> skills_score MAP<string,int>,
> depart_title MAP<STRING,ARRAY<STRING>>
> ); 
No rows affected (0.122 seconds)
  
```
> CREATE TEMPORARY TABLE tmp_emp2 as SELECT * FROM tmp_emp1;
  
> CREATE TEMPORARY TABLE tmp_emp3 like tmp_emp1;

 
      
# the truncate table statement only removes data from the table. The table still exists, but is empty. Note, truncate table can only apply to an internal table

# To define the proper number of buckets, we should avoid having too much or too little data in each bucket. A better choice is somewhere near two blocks of data, such as 512 MB of data in each bucket. As a best practice, use 2N as the number of buckets.
# Bucketing has a close dependency on the data-loading process. To properly load data into a bucket table, we need to either set the maximum number of reducers to the same number of buckets specified in the table creation (for example, 2), or enable enforce bucketing (recommended), as follows:

> set map.reduce.tasks = 2;
No rows affected (0.026 seconds)
     
 
> set hive.enforce.bucketing = true; -- This is recommended
No rows affected (0.002 seconds)

# To populate the data to a bucket table, we cannot use the LOAD DATA statement, because it does not verify the data against the metadata. Instead, INSERT should be used to populate the bucket table all the time:

> INSERT OVERWRITE TABLE employee_id_buckets SELECT * FROM employee_id;
No rows affected (75.468 seconds)

-- Verify the buckets in the HDFS from shell
$hdfs dfs -ls /user/hive/warehouse/employee_id_buckets
Found 2 items
-rwxrwxrwx   1 hive hive        900 2018-07-02 10:54 
/user/hive/warehouse/employee_id_buckets/000000_0
-rwxrwxrwx   1 hive hive        582 2018-07-02 10:54 
/user/hive/warehouse/employee_id_buckets/000001_0
      
# Views are logical data structures that can be used to simplify queries by hiding the complexities, such as joins, subqueries, and filters. It is called logical because views are only defined in metastore without the footprint in HDFS. Unlike what's in the relational database, views in HQL do not store data or get materialized. Once the view is created, its schema is frozen immediately. Subsequent changes to the underlying tables (for example, adding a column) will not be reflected in the view's schema. If an underlying table is dropped or changed, subsequent attempts to query the invalid view will fail. In addition, views are read-only and may not be used as the target of the LOAD/INSERT/ALTER statements.

The following is an example of a view creation statement:

> CREATE VIEW IF NOT EXISTS employee_skills
> AS
> SELECT 
> name, skills_score['DB'] as DB,
> skills_score['Perl'] as Perl, 
> skills_score['Python'] as Python,
> skills_score['Sales'] as Sales, 
> skills_score['HR'] as HR 
> FROM employee;
No rows affected (0.253 seconds)

# When creating views, there is no yarn job triggered since this is only a metadata change. However, the job will be triggered when querying the view. To check the view definition, we can use the SHOW statement. When modifying the view definition, we can use the ALTER VIEW statement. 
      
# Project data with SELECT
The most common use case for Hive is to query data in Hadoop. To achieve this, we need to write and execute a SELECT statement. The typical work done by the SELECT statement is to project the whole row (with SELECT *) or specified columns (with SELECT column1, column2, ...) from a table, with or without conditions.Most simple SELECT statements will not trigger a Yarn job. Instead, a dump task is created just for dumping the data, such as the hdfs dfs -cat command. The SELECT statement is quite often used with the FROM and DISTINCT keywords. A FROM keyword followed by a table is where SELECT projects data. The DISTINCT keyword used after SELECT ensures only unique rows or combination of columns are returned from the table. In addition, SELECT also supports columns combined with user-defined functions, IF(), or a CASE WHEN THEN ELSE END statement, and regular expressions.
      
 List all columns match java regular expression
> SET hive.support.quoted.identifiers = none; -- Enable this
      
> SELECT `^work.*` FROM employee; -- All columns start with work

 ```
+------------------------+
| employee.work_place    |
+------------------------+
| ["Montreal","Toronto"] |
| ["Montreal"]           |
| ["New York"]           |
| ["Vancouver"]          |
+------------------------+
4 rows selected (0.141 sec
```
 
      
# IN
IN/NOT IN is used as an expression to check whether values belong to a set specified by IN or NOT IN. With effect from Hive v2.1.0, IN and NOT IN statements support more than one column:

> SELECT name FROM employee WHERE gender_age.age in (27, 30);

 ```
+----------+
| name     |
+----------+
| Michael  |
| Shelley  |
+----------+
2 rows selected (0.3 seconds)
 ```
 
With multiple columns support after v2.1.0
> SELECT 
> name, gender_age 
> FROM employee 
> WHERE (gender_age.gender, gender_age.age) IN 
> (('Female', 27), ('Male', 27 + 3)); -- Also support expression

```      
+---------+------------------------------+
| name    | gender_age                   |
+---------+------------------------------+
| Michael | {"gender":"Male","age":30}   |
| Shelley | {"gender":"Female","age":27} |
+---------+------------------------------+
2 rows selected (0.282 seconds)
```

# Join
When JOIN is performed between multiple tables, Yarn/MapReduce jobs are created to process the data in the HDFS. Each of the jobs is called a stage. Usually, it is suggested to put the big table right at the end of the JOIN statement for better performance and to avoid Out Of Memory (OOM) exceptions. This is because the last table in the JOIN sequence is usually streamed through reducers where as the others are buffered in the reducer by default. Also, a hint, /*+STREAMTABLE (table_name)*/, can be specified to advise which table should be streamed over the default decision, as in the following example:

> SELECT /*+ STREAMTABLE(employee_hr) */
> emp.name, empi.employee_id, emph.sin_number
> FROM employee emp
> JOIN employee_hr emph ON emp.name = emph.name
> JOIN employee_id empi ON emph.employee_id = empi.employee_id;
      
 # Cross Join
 The CROSS JOIN statement does not have a join condition. The CROSS JOIN statement can also be written using join without condition or with the always true condition, such as 1 = 1.
      
 > SELECT 
> emp.name, emph.sin_number
> FROM employee emp
> JOIN employee_hr emph on 1=1;

# Although Hive did not support unequal joins explicitly in the earlier version, there are workarounds by using CROSS JOIN and WHERE, as in this example:
> SELECT 
> emp.name, emph.sin_number
> FROM employee emp
> CROSS JOIN employee_hr emph 
> WHERE emp.name <> emph.name;

# Special joins
HQL also supports some special joins that we usually do not see in relational databases, such as MapJoin and Semi-join. MapJoin means doing the join operation only with map, without the reduce job. The MapJoin statement reads all the data from the small table to memory and broadcasts to all maps. During the map phase, the join operation is performed by comparing each row of data in the big table with small tables against the join conditions. Because there is no reduce needed, such kinds of join usually have better performance. In the newer version of Hive, Hive automatically converts join to MapJoin at runtime if possible. However, you can also manually specify the broadcast table by providing a join hint, /*+ MAPJOIN(table_name) */. In addition, MapJoin can be used for unequal joins to improve performance since both MapJoin and WHERE are performed in the map phase. The following is an example of using a MapJoin hint with CROSS JOIN:

> SELECT 
> /*+ MAPJOIN(employee) */ emp.name, emph.sin_number
> FROM employee emp
> CROSS JOIN employee_hr emph 
> WHERE emp.name <> emph.name;
The MapJoin operation does not support the following:

Using MapJoin after UNION ALL, LATERAL VIEW, GROUP BY/JOIN/SORT BY/CLUSTER, and BY/DISTRIBUTE BY
Using MapJoin before UNION, JOIN, and another MapJoin
Bucket MapJoin is a special type of MapJoin that uses bucket columns (the column specified by CLUSTERED BY in the CREATE TABLE statement) as the join condition. Instead of fetching the whole table, as done by the regular MapJoin, bucket MapJoin only fetches the required bucket data. To enable bucket MapJoin, we need to enable some settings and make sure the bucket number is are multiple of each other. If both joined tables are sorted and bucketed with the same number of buckets, a sort-merge join can be performed instead of caching all small tables in the memory:
```
> SET hive.optimize.bucketmapjoin = true;
> SET hive.optimize.bucketmapjoin.sortedmerge = true;
> SET hive.input.format =org.apache.hadoop.hive.ql.io.BucketizedHiveInputFormat; 
 ```
      
In addition, the LEFT SEMI JOIN statement is also a type of MapJoin. It is the same as a subquery with IN/EXISTS after v0.13.0 of Hive. However, it is not recommended for use since it is not part of standard SQL:

> SELECT a.name FROM employee a
> LEFT SEMI JOIN employee_id b ON a.name = b.name;
      
      
# Data exchange with INSERT
Insert data from the CTE statement
```
      > WITH a as (
      > SELECT * FROM ctas_employee 
      > )
      > FROM a 
      > INSERT OVERWRITE TABLE employee
      > SELECT *;
      No rows affected (30.1 seconds)
```

Run multi-insert by only scanning the source table once for better performance:
```
      > FROM ctas_employee
      > INSERT OVERWRITE TABLE employee
      > SELECT *
      > INSERT OVERWRITE TABLE employee_internal
      > SELECT * 
      > INSERT OVERWRITE TABLE employee_partitioned 
      > PARTITION (year=2018, month=9) -- Insert to static partition
      > SELECT *
      > ; 
      No rows affected (27.919 seconds)
```

The INSERT OVERWRITE statement will replace the data in the target table/partition, while INSERT INTO will append data.
When inserting data into the partitions, we need to specify the partition columns. Instead of specifying static partition values, Hive also supports dynamically giving partition values. Dynamic partitions are useful when it is necessary to populate partitions dynamically from data values. Dynamic partitions are disabled by default because a careless dynamic partition insert could create many partitions unexpectedly. We have to set the following properties to enable dynamic partitions:

> SET hive.exec.dynamic.partition=true;

By default, the user must specify at least one static partition column. This is to avoid accidentally overwriting partitions. To disable this restriction, we can set the partition mode to nonstrict from the default strict mode before inserting into dynamic partitions as follows:

> SET hive.exec.dynamic.partition.mode=nonstrict;


Partition year, month are determined from data

> INSERT INTO TABLE employee_partitioned
> PARTITION(year, month)
> SELECT name, array('Toronto') as work_place,
> named_struct("gender","Male","age",30) as gender_age,
> map("Python",90) as skills_score,
> map("R&D",array('Developer')) as depart_title, 
> year(start_date) as year, month(start_date) as month
> FROM employee_hr eh
> WHERE eh.employee_id = 102;
No rows affected (29.024 seconds)
Complex type constructors are used in the preceding example to create a constant value of a complex data type.

INSERT also supports writing data to files, which is the opposite operation compared to LOAD. It is usually used to extract data from SELECT statements to files in the local/HDFS directory. However, it only supports the OVERWRITE keyword, which means we can only overwrite rather than append data to the data files. By default, the columns are separated by Ctrl+A and rows are separated by newlines in the exported file. Column, row, and collection separators can also be overwritten like in the table creation statement. The following are a few examples of exporting data to files using the INSERT OVERWRITE ... directory statement:

We can insert to local files with default row separators

> INSERT OVERWRITE LOCAL DIRECTORY '/tmp/output1'
> SELECT * FROM employee;
No rows affected (30.859 seconds)
	  
Many partial files could be created by reducers when doing an insert into a directory. To merge them into one file, we can use the HDFS merge command: hdfs dfs –getmerge <exported_hdfs_folder> <local_folder>.
Insert into local files with specified row separators

>INSERT OVERWRITE LOCAL DIRECTORY '/tmp/output2'
>ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
>SELECT * FROM employee;
No rows affected (31.937 seconds)
      
```
      -- Verify the separator
      $vi /tmp/output2/000000_0
      Michael,Montreal^BToronto,Male^B30,DB^C80,
      Product^CDeveloper^DLead
      Will,Montreal,Male^B35,Perl^C85,Product^CLead^BTest^CLead
      Shelley,New York,Female^B27,Python^C80,Test^CLead^BCOE^CArchitect
      Lucy,Vancouver,Female^B57,Sales^C89^BHR^C94,Sales^CLead
```

Use multi-insert statements to export data from the same table:
      > FROM employee
      > INSERT OVERWRITE DIRECTORY '/user/dayongd/output3'
      > SELECT *
      > INSERT OVERWRITE DIRECTORY '/user/dayongd/output4'
      > SELECT name ;
      No rows affected (25.4 seconds)
# Combined HQL and HDFS shell commands, we can extract data to local or remote files with both append and overwrite supported. The hive -e quoted_hql_string or hive -f <hql_filename> commands can execute a HQL query or query file. Linux's redirect operators and piping can be used with these commands to redirect result sets. The following are a few examples:
```
Append to local files: $hive -e 'select * from employee' >> test
Overwrite local files: $hive -e 'select * from employee' > test
Append to HDFS files: $hive -e 'select * from employee'|hdfs dfs -appendToFile - /tmp/test1 
Overwrite HDFS files: $hive -e 'select * from employee'|hdfs dfs -put -f - /tmp/test2
```

# Functions
To list all operators, built-in functions, and user-defined functions, we can use the SHOW FUNCTIONS commands. For more details of a specific function, we can use DESC [EXTENDED] function_name as follows:

```
> SHOW FUNCTIONS; -- List all functions
> DESCRIBE FUNCTION <function_name>; -- Detail for the function
> DESCRIBE FUNCTION EXTENDED <function_name>; -- More details 
```

	# Function tips for collections
The size(...) function is used to calculate the collection size for the MAP, ARRAY, or nested MAP/ARRAY. It returns -1 if the collection is NULL and returns 0 if the collection is empty, as follows:
```
> SELECT 
> SIZE(work_place) as array_size,
> SIZE(skills_score) as map_size,
> SIZE(depart_title) as complex_size,
> SIZE(depart_title["Product"]) as nest_size
> FROM employee;
+-------------+-----------+---------------+------------+
| array_size  | map_size  | complex_size  | nest_size  |
+-------------+-----------+---------------+------------+
| 2           | 1         | 1             | 2          |
| 1           | 1         | 2             | 1          |
| 1           | 1         | 2             | -1         |
| 1           | 2         | 1             | -1         |
+-------------+-----------+---------------+------------+
4 rows selected (0.062 seconds)


> SELECT size(null), size(array(null)), size(array());
+-----+-----+-----+
| _c0 | _c1 | _c2 |
+-----+-----+-----+
| -1  |  1  |  0  |
+-----+-----+-----+
1 row selected (11.453 seconds)
```

The array_contains(...) function checks whether an array contains some values or not and returns TRUE or FALSE. The sort_array(...) function sorts the array in ascending order. These can be used as follows:

```
> SELECT 
> array_contains(work_place, 'Toronto') as is_Toronto,
> sort_array(work_place) as sorted_array
> FROM employee;
+-------------+-------------------------+
| is_toronto  |      sorted_array       |
+-------------+-------------------------+
| true        | ["Montreal","Toronto"]  |
| false       | ["Montreal"]            |
| false       | ["New York"]            |
| false       | ["Vancouver"]           |
+-------------+-------------------------+
4 rows selected (0.059 seconds)
```
	
# Virtual column functions
Virtual columns are special functions in HQL. Right now, there are two virtual columns: INPUT__FILE__NAME and BLOCK__OFFSET__INSIDE__FILE. The INPUT__FILE__NAME function shows the input file's name for a mapper task.The BLOCK__OFFSET__INSIDE__FILE function shows the current global file position or the current block's file offset if the file is compressed. The following are examples of using virtual columns to find out where data is physically located in HDFS, especially for bucketed and partitioned tables:

```
> SELECT 
> INPUT__FILE__NAME,BLOCK__OFFSET__INSIDE__FILE as OFFSIDE
> FROM employee;
+-----------------------------------------------------------------------+
| input__file__name                                           | offside |
+-----------------------------------------------------------------------+
| hdfs://localhost:9000/user/hive/warehouse/employee/000000_0 | 0       |
| hdfs://localhost:9000/user/hive/warehouse/employee/000000_0 | 62      |
| hdfs://localhost:9000/user/hive/warehouse/employee/000000_0 | 115     |
| hdfs://localhost:9000/user/hive/warehouse/employee/000000_0 | 176     |
+-------------------------------------------------------------+---------+
4 rows selected (0.47 seconds)
```
# Schema on Read VS Schema on Write
Let say I created a table and write stored as parquet.
then I will try to load a textfile into the table, it will not throw any error but when I will run select * on table it will throw error.

Why this happens?
This happens because of Schema on Read, hive performs the schema checking while reading the data i.e (on select *). On the other hand RDBMS checks schema while writing data into database.

# Basic aggregation 
An aggregate function can be used with other aggregate functions in the same SELECT statement. It can also be used with other functions, such as conditional functions, in a nested way. However, nested aggregate functions are not supported. See the following examples for more details:
```
Multiple aggregate functions in the same SELECT statement:
      > SELECT 
      > gender_age.gender, avg(gender_age.age) as avg_age,
      > count(*) as row_cnt
      > FROM employee GROUP BY gender_age.gender; 
      +--------------------+---------------------+----------+
      | gender_age.gender  |       avg_age       | row_cnt  |
      +--------------------+---------------------+----------+
      | Female             | 42.0                | 2        |
      | Male               | 31.666666666666668  | 3        |
      +--------------------+---------------------+----------+
      2 rows selected (98.857 seconds)
```
Aggregate functions can also be used with CASE WHEN THEN ELSE END, coalesce(...), or if(...):
```
      > SELECT 
      > sum(CASE WHEN gender_age.gender = 'Male'
      > THEN gender_age.age ELSE 0 END)/
      > count(CASE WHEN gender_age.gender = 'Male' THEN 1
      > ELSE NULL END) as male_age_avg 
      > FROM employee;
      +---------------------+
      |    male_age_avg     |
      +---------------------+
      | 31.666666666666668  |
      +---------------------+
      1 row selected (38.415 seconds)
      

      > SELECT
      > sum(coalesce(gender_age.age,0)) as age_sum,
      > sum(if(gender_age.gender = 'Female',gender_age.age,0)) as 
      female_age_sum
      > FROM employee;
      +----------+----------------+
      | age_sum  | female_age_sum |
      +----------+----------------+
      | 179      | 84             |
      +----------+----------------+
      1 row selected (42.137 seconds)
GROUP BY can also apply to expressions:
      > SELECT
      > if(name = 'Will', 1, 0) as name_group, 
      > count(name) as name_cnt 
      > FROM employee 
      > GROUP BY if(name = 'Will', 1, 0);
      +------------+----------+
      | name_group | name_cnt |
      +------------+----------+
      | 0          | 3        |
      | 1          | 1        |
      +------------+----------+
      2 rows selected (23.749 seconds)
```
Verify that nested aggregate functions are not allowed:
```
      > SELECT avg(count(*)) as row_cnt FROM employee;
      Error: Error while compiling statement: FAILED: SemanticException 
      [Error 10128]: Line 1:11 Not yet 
      supported place for UDAF 'count' (state=42000,code=10128)
```

Aggregate functions such as max(...) or min(...) apply to NULL and return NULL. However, functions such as sum() and avg(...) cannot apply to NULL. The count(null) returns 0.
```     
	 > SELECT max(null), min(null), count(null);
      +------+------+-----+
      | _c0  | _c1  | _c2 |
      +------+------+-----+
      | NULL | NULL |  0  |
      +------+------+-----+
      1 row selected (23.54 seconds)
      

      > SELECT sum(null), avg(null);
      Error: Error while compiling statement: FAILED: 
      UDFArgumentTypeException Only numeric or string type 
      arguments are accepted but void is passed. 
      (state=42000,code=40000)
```
# In addition, we may encounter a very special behavior when dealing with aggregation across columns with a NULL value. The entire row (if one column has NULL as a value in the row) will be ignored. To avoid this, we can use coalesce(...) to assign a default value when the column value is NULL. See the following example:

```
      -- Create a table t for testing
      > CREATE TABLE t (val1 int, val2 int);
      > INSERT INTO TABLE t VALUES (1, 2),(null,2),(2,3);
      No rows affected (0.138 seconds) 
      

      -- Check the rows in the table created
      > SELECT * FROM t;
      +---------+---------+
      | t.val1  | t.val2  |
      +---------+---------+
      | 1       | 2       |
      | NULL    | 2       |
      | 2       | 3       |
      +---------+---------+
      3 rows selected (0.069 seconds)
      

#   The 2nd row (NULL, 2) is ignored when doing sum(val1 + val2)
      > SELECT sum(val1), sum(val1 + val2) FROM t; 
      +------+------+
      | _c0  | _c1  |
      +------+------+
      | 3    | 8    |
      +------+------+
      1 row selected (57.775 seconds)
      

      > SELECT 
      > sum(coalesce(val1,0)),
      > sum(coalesce(val1,0) + val2) 
      > FROM t;
      +------+------+
      | _c0  | _c1  |
      +------+------+
      | 3    | 10   |
      +------+------+
      1 row selected (69.967 seconds)
```
Aggregate functions can also be used with the DISTINCT keyword to aggregate on unique values:
```
      > SELECT 
      > count(DISTINCT gender_age.gender) as gender_uni_cnt,
      > count(DISTINCT name) as name_uni_cnt
      > FROM employee;     
      +-----------------+---------------+
      | gender_uni_cnt  | name_uni_cnt  |
      +-----------------+---------------+
      | 2               | 5             |
      +-----------------+---------------+
      1 row selected (35.935 seconds)
```
# When we use COUNT and DISTINCT together, it always ignores the setting (such as mapred.reduce.tasks = 20) for the number of reducers used and may use only one reducer. In this case, the single reducer becomes the bottleneck when processing large volumes of data. The workaround is to use a subquery as follows:

-- May trigger single reducer during the whole processing
> SELECT count(distinct gender_age.gender) as gender_uni_cnt FROM employee;

```
-- Use subquery to select unique value before aggregations
> SELECT 
> count(*) as gender_uni_cnt 
> FROM (
> SELECT DISTINCT gender_age.gender FROM employee
) a;
```

In this case, the first stage of the query implementing DISTINCT can use more than one reducer. In the second stage, the mapper will have less output just for the COUNT purpose, since the data is already unique after implementing DISTINCT. As a result, the reducer will not be overloaded.

Sometimes, we may need to find the max. or min. value of particular columns as well as other columns, for example, to answer this question: who are the oldest males and females with ages in the employee table? To achieve this, we can also use max/min on a struct as follows, instead of using subqueries/window functions:

> SELECT gender_age.gender, 
> max(struct(gender_age.age, name)).col1 as age,
> max(struct(gender_age.age, name)).col2 as name
> FROM employee
> GROUP BY gender_age.gender;
+-------------------+-----+------+
| gender_age.gender | age | name |
+-------------------+-----+------+
| Female            | 57  | Lucy |
| Male              | 35  | Will |
+-------------------+-----+------+
2 rows selected (26.896 seconds)
Although it still needs to use the GROUP BY clause, this job is more efficient than a regular GROUP BY or subquery, as it only triggers one job. 

# The hive.map.aggr property controls aggregations in the map task. The default value for this setting is true, so Hive will do the first-level aggregation directly in the map task for better performance, but consume more memory. Turn it off if you run out of memory in the map phase.

# Enhanced aggregation
Hive offers enhanced aggregation by using the GROUPING SETS, CUBE, and ROLLUP keywords.

# Grouping sets
GROUPING SETS implements advanced multiple GROUP BY operations against the same set of data. Actually, GROUPING SETS are a shorthand way of connecting several GROUP BY result sets with UNION ALL. The GROUPING SETS keyword completes all processes in a single stage of the job, which is more efficient. A blank set () in the GROUPING SETS clause calculates the overall aggregation. The following are a few examples to show the equivalence of GROUPING SETS. For better understanding, we can say that the outer level (brace) of GROUPING SETS defines what data UNION ALL is to be implemented. The inner level (brace) defines what GROUP BY data is to be implemented in each UNION ALL.

Grouping set with one element of column pairs:
```
      SELECT 
      name, start_date, count(sin_number) as sin_cnt 
      FROM employee_hr
      GROUP BY name, start_date 
      GROUPING SETS((name, start_date));
      --||-- equals to
      SELECT
      name, start_date, count(sin_number) as sin_cnt 
      FROM employee_hr
      GROUP BY name, start_date;
      +---------+------------+---------+
      | name    | start_date | sin_cnt |
      +---------+------------+---------+
      | Lucy    | 2010-01-03 | 1       |
      | Michael | 2014-01-29 | 1       |
      | Steven  | 2012-11-03 | 1       |
      | Will    | 2013-10-02 | 1       |
      +---------+------------+---------+
      4 rows selected (26.3 seconds)
```

Grouping set with two elements:
```
      SELECT 
      name, start_date, count(sin_number) as sin_cnt 
      FROM employee_hr
      GROUP BY name, start_date 
      GROUPING SETS(name, start_date);
      --||-- equals to
      SELECT 
      name, null as start_date, count(sin_number) as sin_cnt 
      FROM employee_hr
      GROUP BY name
      UNION ALL
      SELECT 
      null as name, start_date, count(sin_number) as sin_cnt 
      FROM employee_hr
      GROUP BY start_date;
      ----------+------------+---------+
      | name    | start_date | sin_cnt |
      +---------+------------+---------+
      | NULL    | 2010-01-03 | 1       |
      | NULL    | 2012-11-03 | 1       |
      | NULL    | 2013-10-02 | 1       |
      | NULL    | 2014-01-29 | 1       |
      | Lucy    | NULL       | 1       |
      | Michael | NULL       | 1       |
      | Steven  | NULL       | 1       |
      | Will    | NULL       | 1       |
      +---------+------------+---------+
      8 rows selected (22.658 seconds)
```
Grouping set with two elements, a column pair, and a column:
```
      SELECT 
      name, start_date, count(sin_number) as sin_cnt 
      FROM employee_hr
      GROUP BY name, start_date 
      GROUPING SETS((name, start_date), name);
      --||-- equals to
      SELECT 
      name, start_date, count(sin_number) as sin_cnt 
      FROM employee_hr
      GROUP BY name, start_date
      UNION ALL
      SELECT 
      name, null as start_date, count(sin_number) as sin_cnt 
      FROM employee_hr
      GROUP BY name;
      +---------+------------+---------+
      | name    | start_date | sin_cnt |
      +---------+------------+---------+
      | Lucy    | NULL       | 1       |
      | Lucy    | 2010-01-03 | 1       |
      | Michael | NULL       | 1       |
      | Michael | 2014-01-29 | 1       |
      | Steven  | NULL       | 1       |
      | Steven  | 2012-11-03 | 1       |
      | Will    | NULL       | 1       |
      | Will    | 2013-10-02 | 1       |
      +---------+------------+---------+
      8 rows selected (22.503 seconds)
```
Grouping set with four elements, including all combinations of columns:
```
      SELECT 
      name, start_date, count(sin_number) as sin_cnt 
      FROM employee_hr
      GROUP BY name, start_date 
      GROUPING SETS((name, start_date), name, start_date, ());
      --||-- equals to
      SELECT 
      name, start_date, count(sin_number) as sin_cnt 
      FROM employee_hr
      GROUP BY name, start_date
      UNION ALL
      SELECT 
      name, null as start_date, count(sin_number) as sin_cnt 
      FROM employee_hr
      GROUP BY name
      UNION ALL
      SELECT 
      null as name, start_date, count(sin_number) as sin_cnt 
      FROM employee_hr
      GROUP BY start_date
      UNION ALL
      SELECT 
      null as name, null as start_date, count(sin_number) as sin_cnt 
      FROM employee_hr
      +---------+------------+---------+
      | name    | start_date | sin_cnt |
      +---------+------------+---------+
      | NULL    | NULL       | 4       |
      | NULL    | 2010-01-03 | 1       |
      | NULL    | 2012-11-03 | 1       |
      | NULL    | 2013-10-02 | 1       |
      | NULL    | 2014-01-29 | 1       |
      | Lucy    | NULL       | 1       |
      | Lucy    | 2010-01-03 | 1       |
      | Michael | NULL       | 1       |
      | Michael | 2014-01-29 | 1       |
      | Steven  | NULL       | 1       |
      | Steven  | 2012-11-03 | 1       |
      | Will    | NULL       | 1       |
      | Will    | 2013-10-02 | 1       |
      +---------+------------+---------+
      13 rows selected (24.916 seconds)
```

# Rollup and Cube
The ROLLUP statement enables a SELECT statement to calculate multiple levels of aggregations across a specified group of dimensions. The ROLLUP statement is a simple extension of the GROUP BY clause with high efficiency and minimal overhead for a query. Compared to GROUPING SETS, which creates specified levels of aggregations, ROLLUP creates n+1 levels of aggregations, where n is the number of grouping columns. First, it calculates the standard aggregate values specified in the GROUP BY clause. Then, it creates higher-level subtotals, moving from right to left through the list of combinations of grouping columns. For example, GROUP BY a,b,c WITH ROLLUP is equivalent to GROUP BY a,b,c GROUPING SETS ((a,b,c),(a,b),(a),()).

The CUBE statement takes a specified set of grouping columns and creates aggregations for all of their possible combinations. If n columns are specified for CUBE, there will be 2n combinations of aggregations returned. For example, GROUP BY a,b,c WITH CUBE is equivalent to GROUP BY a,b,c GROUPING SETS ((a,b,c),(a,b),(b,c),(a,c),(a),(b),(c),()).

The GROUPING__ID function works as an extension to distinguish entire rows from each other. It returns the decimal equivalent of the BIT vector for each column specified after GROUP BY. The returned decimal number is converted from a binary of ones and zeros, which represents whether the column is aggregated (0) in the row or not (1). On the other hand, the grouping(...) function also indicates whether a column in a GROUP BY clause is aggregated or not by returning the binary of 1 or 0 directly. In the following example, the order of columns starts from counting the nearest column (such as name) from GROUP BY. The first row in the result set indicates that none of the columns are being used in GROUP BY.

Compare the following example with the last example in the GROUPING SETS section for a better understanding of GROUPING_ID and grouping(...):

```
SELECT 
name, start_date, count(employee_id) as emp_id_cnt,
GROUPING__ID,
grouping(name) as gp_name, 
grouping(start_date) as gp_sd
FROM employee_hr 
GROUP BY name, start_date 
WITH CUBE ORDER BY name, start_date;
+---------+------------+------------+-----+---------+-------+
| name    | start_date | emp_id_cnt | gid | gp_name | gp_sd |
+---------+------------+------------+-----+---------+-------+
| NULL    | NULL       | 4          | 3   | 1       | 1     |
| NULL    | 2010-01-03 | 1          | 2   | 1       | 0     |
| NULL    | 2012-11-03 | 1          | 2   | 1       | 0     |
| NULL    | 2013-10-02 | 1          | 2   | 1       | 0     |
| NULL    | 2014-01-29 | 1          | 2   | 1       | 0     |
| Lucy    | NULL       | 1          | 1   | 0       | 1     |
| Lucy    | 2010-01-03 | 1          | 0   | 0       | 0     |
| Michael | NULL       | 1          | 1   | 0       | 1     |
| Michael | 2014-01-29 | 1          | 0   | 0       | 0     |
| Steven  | NULL       | 1          | 1   | 0       | 1     |
| Steven  | 2012-11-03 | 1          | 0   | 0       | 0     |
| Will    | NULL       | 1          | 1   | 0       | 1     |
| Will    | 2013-10-02 | 1          | 0   | 0       | 0     |
+---------+------------+------------+-----+---------+-------+
13 rows selected (55.507 seconds)
```

# Window Functions
Function (arg1,..., argn) OVER ([PARTITION BY <...>] [ORDER BY <....>] [<window_expression>])

Function (arg1,..., argn) can be any function in the following four categories:

Aggregate Functions: Regular aggregate functions, such as sum(...), and max(...)
Sort Functions: Functions for sorting data, such as rank(...), androw_number(...)
Analytics Functions: Functions for statistics and comparisons, such as lead(...), lag(...), and first_value(...)
The OVER [PARTITION BY <...>] clause is similar to the GROUP BY clause. It divides the rows into groups containing identical values in one or more partitions by columns. These logical groups are known as partitions, which is not the same term as used for partition tables. Omitting the PARTITION BY statement applies the operation to all the rows in the table.

The [ORDER BY <....>] clause is the same as the regular ORDER BY clause. It makes sure the rows produced by the PARTITION BY clause are ordered by specifications, such as ascending or descending order.

The regular aggregations are used as window functions:
```
      > SELECT 
      > name, 
      > dept_num as deptno, 
      > salary,
      > count(*) OVER (PARTITION BY dept_num) as cnt,
      > count(distinct dept_num) OVER (PARTITION BY dept_num) as dcnt,
      > sum(salary) OVER(PARTITION BY dept_num ORDER BY dept_num) as 
      sum1,
      > sum(salary) OVER(ORDER BY dept_num) as sum2,
      > sum(salary) OVER(ORDER BY dept_num, name) as sum3
      > FROM employee_contract
      > ORDER BY deptno, name;
      +---------+--------+--------+-----+-----+-------+-------+-------+
      | name    | deptno | salary | cnt | dcnt| sum1  | sum2  | sum3  |
      +---------+--------+--------+-----+-----+-------+-------+-------+
      | Lucy    | 1000   | 5500   | 5   | 1   | 24900 | 24900 | 5500  |
      | Michael | 1000   | 5000   | 5   | 1   | 24900 | 24900 | 10500 |
      | Steven  | 1000   | 6400   | 5   | 1   | 24900 | 24900 | 16900 |
      | Wendy   | 1000   | 4000   | 5   | 1   | 24900 | 24900 | 20900 |
      | Will    | 1000   | 4000   | 5   | 1   | 24900 | 24900 | 24900 |
      | Jess    | 1001   | 6000   | 3   | 1   | 17400 | 42300 | 30900 |
      | Lily    | 1001   | 5000   | 3   | 1   | 17400 | 42300 | 35900 |
      | Mike    | 1001   | 6400   | 3   | 1   | 17400 | 42300 | 42300 |
      | Richard | 1002   | 8000   | 3   | 1   | 20500 | 62800 | 50300 |
      | Wei     | 1002   | 7000   | 3   | 1   | 20500 | 62800 | 57300 |
      | Yun     | 1002   | 5500   | 3   | 1   | 20500 | 62800 | 62800 |
      +---------+--------+--------+-----+-----+-------+-------+-------+
      11 rows selected (111.856 seconds)
```
# Window sort functions
Window sort functions provide the sorting data information, such as row number and rank, within specific groups as part of the data returned. The most commonly used sort functions are as follows:

row_number: Assigns a unique sequence number starting from 1 to each row, according to the partition and order specification.
	
rank: Ranks items in a group, such as finding the top N rows for specific conditions.
	
dense_rank: Similar to rank, but leaves no gaps in the ranking sequence when there are ties. For example, if we rank a match using dense_rank and have two players tied for second place, we would see that the two players were both in second place and that the next person is ranked third. However, the rank function would rank two people in second place, but the next person would be in fourth place.
	
percent_rank: Uses rank values rather than row counts in its numerator as (current rank - 1)/(total number of rows - 1). Therefore, it returns the percentage rank of a value relative to a group of values.
	
ntile: Divides an ordered dataset into a number of buckets and assigns an appropriate bucket number to each row. It can be used to divide rows into equal sets and assign a number to each row.
	
Here are some examples using window sort functions in HQL:
```
> SELECT 
> name, 
> dept_num as deptno, 
> salary,
> row_number() OVER () as rnum, -- sequence in orginal table
> rank() OVER (PARTITION BY dept_num ORDER BY salary) as rk, 
> dense_rank() OVER (PARTITION BY dept_num ORDER BY salary) as drk,
> percent_rank() OVER(PARTITION BY dept_num ORDER BY salary) as prk,
> ntile(4) OVER(PARTITION BY dept_num ORDER BY salary) as ntile
> FROM employee_contract
> ORDER BY deptno, name;
+---------+--------+--------+------+----+-----+------+-------+
| name    | deptno | salary | rnum | rk | drk | prk  | ntile |
+---------+--------+--------+------+----+-----+------+-------+
| Lucy    | 1000   | 5500   | 7    | 4  | 3   | 0.75 | 3     |
| Michael | 1000   | 5000   | 11   | 3  | 2   | 0.5  | 2     |
| Steven  | 1000   | 6400   | 8    | 5  | 4   | 1.0  | 4     |
| Wendy   | 1000   | 4000   | 9    | 1  | 1   | 0.0  | 1     |
| Will    | 1000   | 4000   | 10   | 1  | 1   | 0.0  | 1     |
| Jess    | 1001   | 6000   | 5    | 2  | 2   | 0.5  | 2     |
| Lily    | 1001   | 5000   | 6    | 1  | 1   | 0.0  | 1     |
| Mike    | 1001   | 6400   | 4    | 3  | 3   | 1.0  | 3     |
| Richard | 1002   | 8000   | 1    | 3  | 3   | 1.0  | 3     |
| Wei     | 1002   | 7000   | 3    | 2  | 2   | 0.5  | 2     |
| Yun     | 1002   | 5500   | 2    | 1  | 1   | 0.0  | 1     |
+---------+--------+--------+------+----+-----+------+-------+
11 rows selected (80.052 seconds)
```

# Since Hive v2.1.0, we have been able to use aggregate functions in the OVER clause as follows:
```
> SELECT
> dept_num,
> rank() OVER (PARTITION BY dept_num ORDER BY sum(salary)) as rk
> FROM employee_contract
> GROUP BY dept_num;
+----------+----+
| dept_num | rk |
+----------+----+
| 1000     | 1  |
| 1001     | 1  |
| 1002     | 1  | 
+----------+----+
3 rows selected (54.43 seconds)
```

	# Window analytics functions
Window analytics functions provide extended data analytics, such as getting lag, lead, last, or first rows in the ordered set. The most commonly used analytics functions are as follows:

cume_dist: Computes the number of rows whose value is smaller than or equal to, the value of the total number of rows divided by the current row, such as (number of rows ≤ current row)/(total number of rows).

lead: This function, lead(value_expr[,offset[,default]]), is used to return data from the next row. The number (offset) of rows to lead can optionally be specified, one is by default. The function returns [,default] or NULL when the default is not specified. In addition, the lead for the current row extends beyond the end of the window.

lag: This function, lag(value_expr[,offset[,default]]), is used to access data from a previous row. The number (offset) of rows to lag can optionally be specified, one is by default. The function returns [,default] or NULL when the default is not specified. In addition, the lag for the current row extends beyond the end of the window.

first_value: It returns the first result from an ordered set.

last_value: It returns the last result from an ordered set. 
Here are some examples using window analytics functions in HQL:
```
> SELECT 
> name,
> dept_num as deptno,
> salary,
> cume_dist() OVER (PARTITION BY dept_num ORDER BY salary) as cume,
> lead(salary, 2) OVER (PARTITION BY dept_num ORDER BY salary) as lead,
> lag(salary, 2, 0) OVER (PARTITION BY dept_num ORDER BY salary) as lag,
> first_value(salary) OVER (PARTITION BY dept_num ORDER BY salary) as fval,
> last_value(salary) OVER (PARTITION BY dept_num ORDER BY salary) as lval,
> last_value(salary) OVER (PARTITION BY dept_num ORDER BY salary RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as lval2
> FROM employee_contract 
> ORDER BY deptno, salary;
+--------+------+--------+------+------+-----+------+------+-------+
| name   |deptno| salary | cume | lead | lag | fval |lvalue|lvalue2|
+--------+------+--------+------+------+-----+------+------+-------+
| Will   | 1000 | 4000   | 0.4  | 5500 | 0   | 4000 | 4000 | 6400  |
| Wendy  | 1000 | 4000   | 0.4  | 5000 | 0   | 4000 | 4000 | 6400  |
| Michael| 1000 | 5000   | 0.6  | 6400 | 4000| 4000 | 5000 | 6400  |
| Lucy   | 1000 | 5500   | 0.8  | NULL | 4000| 4000 | 5500 | 6400  |
| Steven | 1000 | 6400   | 1.0  | NULL | 5000| 4000 | 6400 | 6400  |
| Lily   | 1001 | 5000   | 0.33 | 6400 | 0   | 5000 | 5000 | 6400  |
| Jess   | 1001 | 6000   | 0.67 | NULL | 0   | 5000 | 6000 | 6400  |
| Mike   | 1001 | 6400   | 1.0  | NULL | 5000| 5000 | 6400 | 6400  |
| Yun    | 1002 | 5500   | 0.33 | 8000 | 0   | 5500 | 5500 | 8000  |
| Wei    | 1002 | 7000   | 0.67 | NULL | 0   | 5500 | 7000 | 8000  |
| Richard| 1002 | 8000   | 1.0  | NULL | 5500| 5500 | 8000 | 8000  |
+--------+------+--------+------+------+-----+------+------+-------+
11 rows selected (55.203 seconds)
```
# For last_value, the result (the lval column) is a little bit unexpected. This is because the default window clause (introduced in the next section) used is RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW, which, in the example, means the current row will always be the last value. Changing the windowing clause to RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING gives us the expected result (see the lval2 column).
	
# Window expression
[<window_expression>] is used to further sub-partition the result and apply the window functions. There are two types of windows: Row Type and Range Type.

According to the JIRA at https://issues.apache.org/jira/browse/HIVE-4797, the rank(...), ntile(...), dense_rank(...), cume_dist(...), percent_rank(...), lead(...), lag(...), and row_number(...) functions do not support being used with a window expression yet.

For row type windows, the definition is in terms of row numbers before or after the current row. The general syntax of the row window clause is as follows:

ROWS BETWEEN <start_expr> AND <end_expr>

<start_expr> can be any one of the following:

UNBOUNDED PRECEDING
CURRENT ROW
N PRECEDING or FOLLOWING
<end_expr> can be any one of the following:

UNBOUNDED FOLLOWING
CURRENT ROW
N PRECEDING or FOLLOWING
	
![image](https://user-images.githubusercontent.com/101991863/236902896-cdd0f54b-087d-4246-a78b-00725549247f.png)
	
In addition, windows can be defined in a separate window clause or referred to by other windows, as follows:
```
> SELECT 
> name, dept_num, salary,
> max(salary) OVER w1 as win1,
> max(salary) OVER w2 as win2,
> max(salary) OVER w3 as win3
> FROM employee_contract
> WINDOW w1 as (
> PARTITION BY dept_num ORDER BY name
> ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
> ),
> w2 as w3,
> w3 as (
> PARTITION BY dept_num ORDER BY name
> ROWS BETWEEN 1 PRECEDING AND 2 FOLLOWING
> );
+---------+----------+--------+------+------+------+
| name    | dept_num | salary | win1 | win2 | win3 |
+---------+----------+--------+------+------+------+
| Lucy    | 1000     | 5500   | 5500 | 6400 | 6400 |
| Michael | 1000     | 5000   | 5500 | 6400 | 6400 |
| Steven  | 1000     | 6400   | 6400 | 6400 | 6400 |
| Wendy   | 1000     | 4000   | 6400 | 6400 | 6400 |
| Will    | 1000     | 4000   | 6400 | 4000 | 4000 |
| Jess    | 1001     | 6000   | 6000 | 6400 | 6400 |
| Lily    | 1001     | 5000   | 6000 | 6400 | 6400 |
| Mike    | 1001     | 6400   | 6400 | 6400 | 6400 |
| Richard | 1002     | 8000   | 8000 | 8000 | 8000 |
| Wei     | 1002     | 7000   | 8000 | 8000 | 8000 |
| Yun     | 1002     | 5500   | 8000 | 7000 | 7000 |
+---------+----------+--------+------+------+------+
11 rows selected (57.204 seconds)
```

# Compared to row type windows, which are in terms of rows, the range type windows are in terms of values in the window expression's specified range. For example, the max(salary) RANGE BETWEEN 500 PRECEDING AND 1000 FOLLOWING statement will calculate max(salary) within the partition by the distance from the current row’s value of - 500 to + 1000. If the current row's salary is 4,000, this max(salary) will include rows whose salaries range from 3,500 to 5,000 within each dept_num-specified partition:
```
> SELECT
> dept_num, start_date, name, salary,
> max(salary) OVER (PARTITION BY dept_num ORDER BY salary
> RANGE BETWEEN 500 PRECEDING AND 1000 FOLLOWING) win1,
> max(salary) OVER (PARTITION BY dept_num ORDER BY salary
> RANGE BETWEEN 500 PRECEDING AND CURRENT ROW) win2
> FROM employee_contract
> order by dept_num, start_date;
+----------+------------+---------+--------+------+------+
| dept_num | start_date | name    | salary | win1 | win2 |
+----------+------------+---------+--------+------+------+
| 1000     | 2010-01-03 | Lucy    | 5500   | 6400 | 5500 |
| 1000     | 2012-11-03 | Steven  | 6400   | 6400 | 6400 |
| 1000     | 2013-10-02 | Will    | 4000   | 5000 | 4000 |
| 1000     | 2014-01-29 | Michael | 5000   | 5500 | 5000 |
| 1000     | 2014-10-02 | Wendy   | 4000   | 5000 | 4000 |
| 1001     | 2013-11-03 | Mike    | 6400   | 6400 | 6400 |
| 1001     | 2014-11-29 | Lily    | 5000   | 6000 | 5000 |
| 1001     | 2014-12-02 | Jess    | 6000   | 6400 | 6000 |
| 1002     | 2010-04-03 | Wei     | 7000   | 8000 | 7000 |
| 1002     | 2013-09-01 | Richard | 8000   | 8000 | 8000 |
| 1002     | 2014-01-29 | Yun     | 5500   | 5500 | 5500 |
+----------+------------+---------+--------+------+------+
11 rows selected (60.784 seconds)
```
If we omit the window expression clause entirely, the default window specification is RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW. When both ORDER BY and WINDOW expression clauses are missing, the window specification defaults to ROW BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING.
	
# Sampling
When the data volume is extra large, we may need to find a subset of data to speed up data analysis. This is sampling, a technique used to identify and analyze a subset of data in order to discover patterns and trends in the whole dataset. In HQL, there are three ways of sampling data: random sampling, bucket table sampling, and block sampling.

# Random sampling
Random sampling uses the rand() function and LIMIT keyword to get the sampling of data, as shown in the following example. The DISTRIBUTE and SORT keywords are used here to make sure the data is also randomly distributed among mappers and reducers efficiently. The ORDER BY rand() statement can also achieve the same purpose, but the performance is not good:
```
> SELECT name FROM employee_hr 
> DISTRIBUTE BY rand() SORT BY rand() LIMIT 2;
+--------+
| name   |
+--------+
| Will   |
| Steven |
+--------+
2 rows selected (52.399 seconds)
```

# Bucket table sampling
This is a special sampling method, optimized for bucket tables, as shown in the following example. The SELECT clause specifies the columns to sample data from. The rand() function can also be used when sampling entire rows. If the sample column is also the CLUSTERED BY column, the sample will be more efficient:
```
-- Sampling based on the whole row
> SELECT name FROM employee_trans
> TABLESAMPLE(BUCKET 1 OUT OF 2 ON rand()) a;
+--------+
| name   |
+--------+
| Steven |
+--------+
1 row selected (0.129 seconds)

-- Sampling based on the bucket column, which is efficient
> SELECT name FROM employee_trans 
> TABLESAMPLE(BUCKET 1 OUT OF 2 ON emp_id) a;
+---------+
| name    |
+---------+
| Lucy    |
| Steven  |
| Michael |
+---------+
3 rows selected (0.136 seconds)
```

# Block sampling
This type of sampling allows a query to randomly pick up n rows of data, n percentage of the data size, or n bytes of data. The sampling granularity is the HDFS block size. Refer to the following examples:
```
-- Sample by number of rows
> SELECT name
> FROM employee TABLESAMPLE(1 ROWS) a;
+----------+
|   name   |
+----------+
| Michael  |
+----------+
1 rows selected (0.075 seconds)

-- Sample by percentage of data size
> SELECT name
> FROM employee TABLESAMPLE(50 PERCENT) a;
+----------+
|   name   |
+----------+
| Michael  |
| Will     |
+----------+
2 rows selected (0.041 seconds)

-- Sample by data size
-- Support b/B, k/K, m/M, g/G
> SELECT name FROM employee TABLESAMPLE(1B) a;
+----------+
|   name   |
+----------+
| Michael  |
+----------+
1 rows selected (0.075 seconds)
```

# Serde
let say if I have a csv file with data as below:
```
1,kanha,"abkp,#cg"
2,mansi,"r,#cg"
3,anil,"au,#bh"
```

Create table command:
```
create table csv_table(
id int,
name char(10),
addr string)
row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
with serdeproperties(
 "separatorChar" = ",",
   "quoteChar" = "\"",
   "escapeChar" = "#"
) stored as textfile;
```
Here separator means column delimiter. quotechar means which character is used to enquote the values. escapechar means inside quotes which character I want to escape.
In the above data I have # inside the quotes so I am specifying # inside escapeChar="#"
so, if we do select * from csv_table, we dont see # its escaped.
```
1       kanha   abkp,cg
2       mansi   r,cg
3       anil    au,bh
```
# Limitations
This SerDe (csv) treats all columns to be of type String. Even if you create a table with non-string column types using this SerDe, the DESCRIBE TABLE output would show string column type.
The type information is retrieved from the SerDe.

# Loading JSON Data
```
{"name": "Amit", "id": 1, "skills": ["Hadoop", "Python"]}
{"name": "sumit", "id": 2, "skills": ["Hadoop", "Hive"]}
{"name": "Shashank", "id": 3, "skills": ["Airflow", "Python"]}
```

Create table command:
```
> create table t1(
> name string,
> id int,
> skills Array<String>
> )
> row format serde 'org.apache.hive.hcatalog.data.JsonSerDe'
> stored as textfile;
```
Here we dont need to specify and serde properties.
	
# Partitions
Partitioning can be done in one of the following two ways:

Static partitioning
Dynamic partitioning
	
## Static partitioning
In static partitioning, you need to manually insert data in different partitions of a table. Let's use a table partitioned on the states of India. For each state, you need to manually insert the data from the data source to a state partition in the partitioned table. So for 29 states, you need to write the equivalent number of Hive queries to insert data in each partition. Let's understand this using the following example.

First, we create a nonpartitioned table, sales, which is the source of data for our partitioned table, and load data into it:
```
CREATE TABLE sales (id int, fname string, state string, zip string, ip string, pid string) row format delimited fields terminated by '\t';
LOAD DATA LOCAL INPATH '/opt/data/sample_10' INTO TABLE sales;
```
If we query the table sales for a particular state, it would scan the entire data in sales.

Now, let's create a partition table and insert data from sales in to different partitions:
```
CREATE TABLE sales_part(id int, fname string, state string, zip string, ip string) partitioned by (pid string) row format delimited fields terminated by '\t';
In static partitioning, you need to insert data into different partitions of the partitioned table as follows:

Insert into sales_part partition (pid= 'PI_03') select id,fname,state,zip,ip from sales where pid= 'PI_03';
Insert into sales_part partition (pid= 'PI_02') select id,fname,state,zip,ip from sales where pid= 'PI_02';
Insert into sales_part partition (pid= 'PI_05') select id,fname,state,zip,ip from sales where pid= 'PI_05';
```
If we check for the partitions in HDFS, we would find the directory structure as follows:

![image](https://github.com/kj2698/BigData_Bootcamp/assets/101991863/3acbc968-94ae-47c3-9f5f-721d26d9ef66)

## Dynamic partitioning
Let us look at a scenario where we have 50 product IDs and we need to partition data for all the unique product IDs available in the dataset. If we go for static partitioning, we need to run the INSERT INTO command for all 50 distinct product IDs. That is where it is better to go with dynamic partitioning. In this type, partitions would be created for all the unique values in the dataset for a given partition column.

By default, Hive does not allow dynamic partitioning. We need to enable it by setting the following properties on the CLI or in hive-site.xml:
```
hive> set hive.exec.dynamic.partition = true;
hive> set hive.exec.dynamic.partition.mode = nonstrict;
```
Once dynamic partitioning is enabled, we can create partitions for all unique values for any columns, say state of the state table, as follows:
```
hive> create table sales_part_state (id int, fname string, zip string, ip string, pid string) partitioned by (state string) row format delimited fields terminated by '\t';
hive> Insert into sales_part_state partition(state) select id,fname,zip,ip,pid,state from sales;
```
It will create partitions for all unique values of state in the sales table. The HDFS structure for different partitions is as follows:

![image](https://github.com/kj2698/BigData_Bootcamp/assets/101991863/632307bf-f371-4c52-b830-df4d742124b9)

# Creating buckets in Hive
In the scenario where we query on a unique values column of a dataset, partitioning is not a good fit. If we go with a partition on a column with high unique values like ID, it would create a large number of small datasets in HDFS and partition entries in the metastore, thus increasing the load on NameNode and the metastore service.

To optimize queries on such a dataset, we group the data into a particular number of buckets and the data is divided into the maximum number of buckets.

How to do it…
Using the same sales dataset, if we need to optimize queries on a column with high unique column values such as ID, we create buckets on that column as follows:
```
create table sales_buck (id int, fname string, state string, zip string, ip string, pid string) clustered by (id) 
into 50 buckets row format delimited fields terminated by '\t';
```
Here, we have defined 50 buckets for this table, which means that the complete dataset is divided and stored in 50 buckets based on the ID column value.

By default, bucketing is disabled in Hive. You need to enable bucketing before loading data in a bucketed table by setting the following property:
```
set hive.enforce.bucketing=true;
```
Assuming you already have the sales table that we created in the Hive partitioning recipe, we would now load the data in sales_buck from the table sales as follows:
```
insert into table sales_buck select * from sales;
```
If you closely monitor the execution of MapReduce jobs running for this insert statement, you would see that 50 reducers produce 50 output files as buckets for this table, partitioned on ID:

How to do it…
If you have access to HDFS, you can check that 50 files are created in the warehouse directory of the sales_buck table, which would be by default /user/hive/warehouse/sales_buck/. If the location of the table is not known, you can check for the location by executing the describe formatted sales_buck; command on the Hive CLI.

How to do it…
Now, when the user queries the sales_buck table for an ID or a range of IDs, Hive knows which bucket to look in for a particular ID. The query engine would only scan that bucket and return the resultset.

# Using a left semi join
In this recipe, you will learn how to use a left semi join in Hive.

The left semi join is used in place of the IN/EXISTS sub-query in Hive. In a traditional RDBMS, the IN and EXISTS clauses are widely used whereas in Hive, the left semi join is used as a replacement of the same.

In the left semi join, the right-hand side table can only be used in the join clause but not in the WHERE or the SELECT clause.

The general syntax of the left semi join is as follows:

join_condition
  | table_reference LEFT SEMI JOIN table_reference join_condition
Where:

table_reference: Is the table name or the joining table that is used in the join query. table_reference can also be a query alias.
join_condition: join_condition: Is the join clause that will join two or more tables based on an equality condition. The AND keyword is used in case a join is required on more than two tables.
How to do it…
Run the following commands to create a left semi join in Hive:
```
SELECT a.* FROM Sales a LEFT SEMI JOIN Sales_orc b ON a.id = b.id;

SELECT a.*, b.* FROM Sales a LEFT SEMI JOIN Sales_orc b ON a.id = b.id;

SELECT a.* FROM Sales a LEFT SEMI JOIN Sales_orc b ON a.id = b.id WHERE b.id = 1;
```
## How it works…
```
The first statement returns all the rows from the Sales tables. This statement works exactly the same as mentioned next:

SELECT a.* FROM Sales a WHERE a.id IN (SELECT b.id FROM Sales_orc b);
```
The output of both the queries is shown next:
![image](https://github.com/kj2698/BigData_Bootcamp/assets/101991863/f11ea0c5-77c4-42e8-87d8-fe41f3418140)
![image](https://github.com/kj2698/BigData_Bootcamp/assets/101991863/f6650fef-01ec-44df-a506-ef981b7b6a10)

The second statement throws an error as FAILED: SemanticException [Error 10009]: Line 1:12 Invalid table alias 'b'. As mentioned earlier, in a left semi join, the right-hand side table cannot be used in a SELECT clause. The output of the query is shown next:

![image](https://github.com/kj2698/BigData_Bootcamp/assets/101991863/9a6c947e-deea-458b-86a0-b909cb66b97c)

The third statement will also throw an error as FAILED: SemanticException [Error 10009]: Line 1:12 Invalid table alias 'b'. As mentioned earlier, in a left semi join, the right-hand side table cannot be used in a WHERE clause. The output of the query is shown next:

![image](https://github.com/kj2698/BigData_Bootcamp/assets/101991863/db886f29-e765-49d3-a270-9e7c4941d07e)

# Using a cross join
In this recipe, you will learn how to use a cross join in Hive.

Cross join, also known as Cartesian product, is a way of joining multiple tables in which all the rows or tuples from one table are paired with the rows and tuples from another table. For example, if the left-hand side table has 10 rows and the right-hand side table has 13 rows then the result set after joining the two tables will be 130 rows. That means all the rows from the left-hand side table (having 10 rows) are paired with all the tables from the right-hand side table (having 13 rows).

If there is a WHERE clause in the SQL statement that includes a cross join, then first the cross join takes place and then the result set is filtered out with the help of the WHERE clause. This means cross joins are not an efficient and optimized way of joining the tables.

The general syntax of a cross join is as follows:

join_condition
  | table_reference [CROSS] JOIN table_reference join_condition
Where:

table_reference: Is the table name or the joining table that is used in the join query. table_reference can also be a query alias.
join_condition: join_condition: Is the join clause that will join two or more tables based on an equality condition. The AND keyword is used in case a join is required on more than two tables.
How to do it…
Cross joins can be implemented using the JOIN keyword or CROSS JOIN keyword. If the CROSS keyword is not specified then by default a cross join is applied.

The following are examples to use cross joins in tables:
```
SELECT * FROM Sales JOIN Sales_orc;
SELECT * FROM Sales JOIN Sales_orc WHERE Sales.id = 1;
SELECT * FROM Sales CROSS JOIN Sales_orc;
SELECT * FROM Sales a CROSS JOIN Sales_orc b JOIN Location c on a.id = c.id;
```
How it works…
The first statement pairs all rows from one table with the rows of another table. The output of the query is shown next:
![image](https://github.com/kj2698/BigData_Bootcamp/assets/101991863/dbb0fb0d-e14a-4263-abe5-4ef8c3ad0745)


The second statement takes as much time in execution as the one in the first example, even though the result set is filtered out with the help of the WHERE clause. This means that the cross join is processed first, then the WHERE clause. The output of the query is shown next:
![image](https://github.com/kj2698/BigData_Bootcamp/assets/101991863/40fb8edd-e6e5-4a08-85ae-83562de44da5)

We can also use the CROSS keyword for CROSS joins. The third statement gives the same result as the one in the first example. The output of the query is shown next:
![image](https://github.com/kj2698/BigData_Bootcamp/assets/101991863/987b6c9a-749e-4969-90ed-4b4e297a0aec)

We can also club multiple join clauses into a single statement as shown in the fourth statement. In this example, first the cross join is performed between the Sales and Sales_orc table and the result set is then joined with the Location table. The output of the query is shown next:
![image](https://github.com/kj2698/BigData_Bootcamp/assets/101991863/85c3c13c-e104-42c0-825a-ef608c8164f3)
