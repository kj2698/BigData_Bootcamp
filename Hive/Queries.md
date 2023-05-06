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

`
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
      STORED AS TEXTFILE;`
  
  For nested types, the level of nesting determines the delimiter. Using ARRAY of ARRAY as an example, the delimiters for the outer ARRAY, as expected, are Ctrl + B characters, but the inner ARRAY delimiter becomes Ctrl + C characters, which is the next delimiter in the list. In the preceding example, the depart_title column, which is a MAP of ARRAY, the MAP key delimiter is Ctrl + C, and the ARRAY delimiter is Ctrl + D.
  
# we already know about internal and external table. so moving on to temporary table.
Hive also supports creating temporary tables. A temporary table is only visible to the current user session. It's automatically deleted at the end of the session. The data of the temporary table is stored in the user's scratch directory, such as /tmp/hive-<username>. Therefore, make sure the folder is properly configured or secured when you have sensitive data in temporary tables. Whenever a temporary table has the same name as a permanent table, the temporary table will be chosen rather than the permanent table. A temporary table does not support partitions and indexes. The following are three ways to create temporary tables:
      

> CREATE TEMPORARY TABLE IF NOT EXISTS tmp_emp1 (
> name string,
> work_place ARRAY<string>,
> gender_age STRUCT<gender:string,age:int>,
> skills_score MAP<string,int>,
> depart_title MAP<STRING,ARRAY<STRING>>
> ); 
No rows affected (0.122 seconds)
  

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
