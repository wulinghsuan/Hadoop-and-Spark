[https://training.ververica.com/setup/taxiData.html](https://training.ververica.com/setup/taxiData.html)

from pyspark.sql.functions import functions as F

driver_summary = taxi_fares\
                 .groupBy("driver)id")\
                 .agg(F.count("_ride.id")
                 .InegerType())



### 1. Computed the total money earned by each driver for each day



### 2. Compute by hour:

#### (1) The average tip



#### (2) The average duration



#### (3) The average “distance at the crow flies” (“à vol d’oiseau”)



### 3. Find the hour of the day when the tips are the highest



### 4. Find the percentage of each type of payment



### 5. Find the top 10 drivers that:

#### (1) Won the most money



#### (2) Did the greater number of rides
