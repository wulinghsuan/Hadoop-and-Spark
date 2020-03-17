[https://training.ververica.com/setup/taxiData.html](https://training.ververica.com/setup/taxiData.html)

from pyspark.sql.functions import functions as F

driver_summary = taxi_fares\
                 .groupBy("driver)id")\
                 .agg(
                    F.count("_ride.id").InegerType()
                    )
