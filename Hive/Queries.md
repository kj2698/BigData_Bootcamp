# creating database with properties and comment.
create database db1
comment 'This is a test db'
with dbproperties("who"="kj","day"="friday");

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
