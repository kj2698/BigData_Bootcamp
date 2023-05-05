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

CREATE TABLE employee (
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
  
  For nested types, the level of nesting determines the delimiter. Using ARRAY of ARRAY as an example, the delimiters for the outer ARRAY, as expected, are Ctrl + B characters, but the inner ARRAY delimiter becomes Ctrl + C characters, which is the next delimiter in the list. In the preceding example, the depart_title column, which is a MAP of ARRAY, the MAP key delimiter is Ctrl + C, and the ARRAY delimiter is Ctrl + D.
  
