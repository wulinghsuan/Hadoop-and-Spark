`pyspark`

rdd1 = sc.parallelize([1, 2, 3, 4])
rdd1.collect()
rdd1.take(3)

Run Spark Code
> Spark Shell (e.g. `pyspark` cmd on edge-1)
> Spark Submit (via command or Oozie)
> Notebooks (e.g. Zeppelin)

Creat a new cell: `Ctrl` + `Alt` + `V`

## RDD Exercise

### Shop Revenues Analysis

### 0. Importing files

```
%spark2.pyspark
rdd = sc.wholeTextFiles('/learning/data/city_revenue')
rdd.take(3)
```

[(u'hdfs://hdfs-nn-1.au.adaltas.cloud:8020/learning/data/city_revenue/anger.txt', u'JAN 13\r\nFEB 12\r\nMAR 14\r\nAPR 15\r\nMAY 12\r\nJUN 15\r\nJUL 19\r\nAUG 15\r\nSEP 13\r\nOCT 8\r\nNOV 14\r\nDEC 16')
, (u'hdfs://hdfs-nn-1.au.adaltas.cloud:8020/learning/data/city_revenue/lyon.txt', u'JAN 13\r\nFEB 12\r\nMAR 14\r\nAPR 15\r\nMAY 12\r\nJUN 15\r\nJUL 19\r\nAUG 25\r\nSEP 13\r\nOCT 11\r\nNOV 22\r\nDEC 22')
, (u'hdfs://hdfs-nn-1.au.adaltas.cloud:8020/learning/data/city_revenue/marseilles_1.txt', u'JAN 21\r\nFEB 21\r\nMAR 21\r\nAPR 27\r\nMAY 25\r\nJUN 25\r\nJUL 21\r\nAUG 22\r\nSEP 23\r\nOCT 28\r\nNOV 24\r\nDEC 26')]

We can find a key value with:
* key: HDFS path toward file
* value: file content showed row value as array

### 1. Transform this rdd to get a rdd with format (city, store, month, revenue)

For example:

(“paris”, “paris_2”, “JAN”, 43)

(“paris”, “paris_2”, “FEB”, 42)

and so on.

> Usefull functions: `map`, `flatMap` or `flatMapValues`

```
%spark2.pyspark
rdd_store=rdd.map(lambda v:((v[0].split("/")[-1].split(".")[0].split("_")[0],\
                             v[0].split("/")[-1].split(".")[0]),\
                             v[1].split("\r\n")))                
rdd_store.take(3)
```
```
%spark2.pyspark
rdd_result=rdd_store.flatMapValues(lambda v: v).\
                     map(lambda v: (v[0][0],v[0][1],v[1].split(" ")[0],v[1].split(" ")[1]))
rdd_result.take(3)
```

```
%spark2.pyspark
rdd1 = rdd.map(lambda v: (v[0].split('/')[-1].split('.')[0], v[1]))
rdd2 = rdd1.map(lambda v: ( (v[0].split('_')[0], v[0])  , v[1]))
rdd3 = rdd2.flatMapValues(lambda v: v.split("\r\n"))
rdd3.take(3)
```

```
%spark2.pyspark
rdd_final = rdd3.map(lambda v: (v[0][0], v[0][1], v[1].split()[0], v[1].split()[1]) )
rdd_final.take(3)
```



### 2. Average per month of the the shop (all stores combined)

Sum of all the stores revenue divided by 12.

```
%spark2.pyspark
rdd_franve = =rddfinal.map(lambda v : ("France", v[3]))
```



### 3 Total revenue per city for the year

Sum of all the revenue for each city.





### 4 Average per month per city (on this 1 year data)

Like question 3 but devided by 12.





### 5 Total revenue per store on the year


Sum of all the revenue per store.





### 6 For each month, best store (most revenue)

Get the store with highest revenue each month.

> Hint: this can be done with a reduceByKey.

One way:



Second way:

def max_reduce(a, b):






