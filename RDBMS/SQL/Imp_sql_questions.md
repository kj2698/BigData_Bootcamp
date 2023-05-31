# Sql query to print prime numbers between 2 to 1000.
```
with recursive cte1 as
(select 2 as n
UNION all
select n+1 from cte1 where n+1<1000)
select distinct n1 from
(select t1.n as n1,t2.n as n2 from cte1 t1
left join cte1 t2
on t1.n%t2.n=0 and t1.n<>t2.n) q WHERE n2 is NULL;
```
