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
