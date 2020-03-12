https://nosql-database.org/

## 1. Create drivers_raw folder on HDFS

`hdfs dfs -mkdir drivers_raw`

## 2. Put drivers.csv file in it

`scp “drivers.csv” l.wu-dsti@edge-1.au.adaltas.cloud:/home/l.wu-dsti/mapreducer`

`hdfs dfs -copyFromLocal drivers.csv drivers_raw`

## 3. Inspect the file (understand scheme)

hdfs dfs -cat drivers_law/drivers.csv

Create a hql file called **drivers_csv.hql** by `vim frivers_csv.hql`

## Beeline:

```
beeline -u "jdbc:hive2://zoo-1.au.adaltas.cloud:2181,zoo-2.au.adaltas.cloud:2181,zoo-3.au.adaltas.cloud:2181/dsti;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2;"
```

## 4. Create an external table pointing to **divers_csv**

hive

CREATE EXTERNAL TABLE IF NOT EXISTS linghsuan_drivers_csv(driverId INT, name STRING, ssn STRING, location STRING, certified STRING, wage_plan STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
location '/user/l.wu-dsti/drivers_raw'
tblproperties ("skip.header.line.count"="1");

show tables;

Or just simply comment `!run scripts/drivers/drivers_csv.hql`

## 5. Test it (SELECT * FROM LIMIT 20)

select * from linghsuan_drivers_csv
LIMIT 3;

DESCRIBE linghsuan_drivers_csv

## 6. Create the external ORC table and populate it

CREATE TABLE IF NOT EXISTS linghsuan_drivers_orc(driverId INT, name STRING, ssn STRING, location STRING, certified STRING, wage_plan STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS ORC;

Insert the data from the external table to the Hive ORC table:

```
INSERT OVERWRITE TABLE linghsuan_drivers_orc SELECT * FROM linghsuan_drivers_csv;
```

## 7. Test it

select * from linghsuan_drivers_orc
LIMIT 3;

DESCRIBE linghsuan_drivers_orc

> Partition (and buckets)

> user/l.hsuan/products/country=France/type=book/...

> Additional Features

> views

> matweialized views (Hive 3)

> Hive LLAP → Base for Apahe Drvid

(X) 
SELCET * FROM toto, tata
WHERE toto.totoID = tata.totoID;

(Y)
SELECT * FROM toto
JOIN tata ON toto.totoID = tata.totoID;
