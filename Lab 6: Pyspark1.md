## Background Knowledge

rdd1 = sc.parallelize([1, 2, 3, 4])
rdd1.collect()
rdd1.take(3)

Run Spark Code
> Spark Shell (e.g. `pyspark` cmd on edge-1)
> Spark Submit (via command or Oozie)
> Notebooks (e.g. Zeppelin)

|Zeppelin||
| --- | --- |
|Creat a new cell above|`Ctrl` + `Alt` + `A`|
|Creat a new cell below|`Ctrl` + `Alt` + `B`|
|Run current paragraph|`Shift` + `Enter`|

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

For example: (“paris”, “paris_2”, “JAN”, 43), (“paris”, “paris_2”, “FEB”, 42), and so on.

> Usefull functions: `map`, `flatMap` or `flatMapValues`

##### One way:

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
           map(lambda v: (v[0][0], v[0][1], v[1].split(" ")[0], float(v[1].split(" ")[1])))
rdd_result.take(3)
```

##### Second way:

```
%spark2.pyspark
rdd1 = rdd.map(lambda v: (v[0].split('/')[-1].split('.')[0], v[1]))
rdd2 = rdd1.map(lambda v: ( (v[0].split('_')[0], v[0])  , v[1]))
rdd3 = rdd2.flatMapValues(lambda v: v.split("\r\n"))
rdd3.take(3)
```

```
%spark2.pyspark
rdd_final = rdd3.map(lambda v: (v[0][0], v[0][1], v[1].split()[0], float(v[1].split()[1])))
rdd_final.take(30)
```

[(u'anger', u'anger', u'JAN', 13.0), (u'anger', u'anger', u'FEB', 12.0), (u'anger', u'anger', u'MAR', 14.0)]

### 2. Average per month of the the shop (all stores combined)

Sum of all the stores revenue divided by 12.

```
%spark2.pyspark
avg_monthly = rdd_final.map(lambda v: v[3])\
                       .reduce(lambda a,b: a+b) / 12
print avg_monthly
```

301.583333333

### 3. Total revenue per city for the year

Sum of all the revenue for each city.

```
%spark2.pyspark
total_city = rdd_final.map(lambda v: (v[0], v[3]))\
                      .reduceByKey(lambda a, b: a+b)
total_city.collect()
```

[(u'paris', 1568.0), (u'anger', 166.0), (u'lyon', 193.0), (u'troyes', 214.0), (u'toulouse', 177.0), (u'orlean', 196.0), (u'rennes', 180.0), (u'nice', 203.0), (u'nantes', 207.0), (u'marseilles', 515.0)]

```
%spark2.pyspark
df_total_city = spark.createDataFrame(total_city, ('City', 'Revenue'))
df_total_city.show()
```

```
+----------+-------+
|      City|Revenue|
+----------+-------+
|     paris| 1568.0|
|    troyes|  214.0|
|      lyon|  193.0|
|  toulouse|  177.0|
|     anger|  166.0|
|    orlean|  196.0|
|    rennes|  180.0|
|      nice|  203.0|
|    nantes|  207.0|
|marseilles|  515.0|
+----------+-------+
```

### 4. Average per month per city (on this 1 year data)

Like question 3 but devided by 12.

```
%spark2.pyspark
avg_city = total_city.map(lambda v: (v[0], v[1]/12))
avg_city.collect()
```

[(u'paris', 130.66666666666666), (u'troyes', 17.833333333333332), (u'lyon', 16.083333333333332), (u'toulouse', 14.75), (u'anger', 13.833333333333334), (u'orlean', 16.333333333333332), (u'rennes', 15.0), (u'nice', 16.916666666666668), (u'nantes', 17.25), (u'marseilles', 42.916666666666664)]

### 5. Total revenue per store on the year

Sum of all the revenue per store.

##### Old way:

```
%spark2.pyspark
total_store = rdd_final.map(lambda v: (v[1], v[3]))\
                       .reduceByKey(lambda a, b: a+b)
total_store.collect()
```

##### New way using "def":

```
%spark2.pyspark
def store_and_revenue(v):
    return (v[1], v[3])
def reduce_function(a, b):
    return a+b

rdd_final.map(store_and_revenue).reduceByKey(reduce_function).collect()
```

[(u'troyes', 214.0), (u'lyon', 193.0), (u'toulouse', 177.0), (u'marseilles_2', 231.0), (u'anger', 166.0), (u'paris_3', 330.0), (u'paris_1', 596.0), (u'orlean', 196.0), (u'marseilles_1', 284.0), (u'rennes', 180.0), (u'nantes', 207.0), (u'paris_2', 642.0), (u'nice', 203.0)]

### 6. For each month, best store (most revenue)

Get the store with highest revenue each month.

> Hint: this can be done with a reduceByKey.

##### One way:

```
%spark2.pyspark
total_store = rdd_final.map(lambda v: (v[1], v[3]))\
                       .reduceByKey(lambda a, b: a+b)
total_store.collect()
```

##### Second way:

```
%spark2.pyspark
def max_reduce_fuction(a, b):
    if a[1] > b[1]:
        return a
    else:
        return b
        
best_store_monthly2 = rdd_final.map(lambda v: (v[2], (v[1], v[3])))\
                               .reduceByKey(max_reduce_fuction)
best_store_monthly2.collect()
```

[(u'FEB', (u'paris_2', 42.0)), (u'AUG', (u'paris_2', 45.0)), (u'APR', (u'paris_1', 57.0)), (u'JUN', (u'paris_2', 85.0)), (u'JUL', (u'paris_1', 61.0)), (u'JAN', (u'paris_1', 51.0)), (u'MAY', (u'paris_2', 72.0)), (u'NOV', (u'paris_2', 64.0)), (u'MAR', (u'paris_2', 44.0)), (u'DEC', (u'paris_1', 71.0)), (u'OCT', (u'paris_1', 68.0)), (u'SEP', (u'paris_2', 63.0))]

Then print out nicely:

```
%spark2.pyspark
months_name = ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC']

for month in months_name:
    print(month + 
          " best seller store is " + 
          best_store_monthly.lookup(month)[0][0] + 
          " with revenue " + 
          str(best_store_monthly.lookup(month)[0][1]))
```

```
JAN best seller store is paris_1 with revenue 51.0
FEB best seller store is paris_2 with revenue 42.0
MAR best seller store is paris_2 with revenue 44.0
APR best seller store is paris_1 with revenue 57.0
MAY best seller store is paris_2 with revenue 72.0
JUN best seller store is paris_2 with revenue 85.0
JUL best seller store is paris_1 with revenue 61.0
AUG best seller store is paris_2 with revenue 45.0
SEP best seller store is paris_2 with revenue 63.0
OCT best seller store is paris_1 with revenue 68.0
NOV best seller store is paris_2 with revenue 64.0
DEC best seller store is paris_1 with revenue 71.0
```

> How about using sortByKey?
