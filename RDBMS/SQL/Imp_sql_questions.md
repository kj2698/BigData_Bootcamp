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

# write a query to print the pattern
```
*
* *
* * *
* * * *
* * * * *
```

```
with recursive cte1 as
(select 1 as n
UNION
select n+1 from cte1 where n+1<6)
select repeat('* ',n) from cte1;
```

# Q3:
Sometimes, payment transactions are repeated by accident; it could be due to user error, API
failure or a retry error that causes a credit card to be charged twice.
Using the transactions table, identify any payments made at the same merchant with the same credit
card for the same amount within 10 minutes of each other. Count such repeated payments.

![image](https://github.com/kj2698/BigData_Bootcamp/assets/101991863/876797c4-1c8a-41a9-89f3-505616cde9e4)


```
select count(*) from
(select *,(UNIX_TIMESTAMP(next_t1)-UNIX_TIMESTAMP(d1))/60 as diff_min from
(select *,lead(d1) over (partition by merchant_id,credit_card_id order by d1) as next_t1 from
(select *,STR_TO_DATE(transaction_timestamp,'%m/%d/%Y %H:%i:%s') as d1
 from transactions) q) q1)q2 where diff_min<=10;
```
