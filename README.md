# Hadoop-and-Spark

## Hadoop Lab

### Lab 1: HDFS

- Download the most frequently downloaded e-book (in Plain Text UTF-8) from Project Gutenberg.
- Create a directory called **raw** inside your HDFS home directory.
- Put the downloaded e-book inside this directory.
- Create a copy directly inside your HDFS home directory.
- Rename the copy inside your HDFS home directory to **input.txt**.
- Read directly from HDFS the **input.txt** file.
- Remove this file (careful with this command).
- List your HDFS home directory.
- Retrieve the fule from the **raw** directory from HDFS to the local filesystem and rename it **local.txt**.

### Lab 6: Pyspark1.md

Using city_revenue dataset

- Transform this rdd to get a rdd with format (city, store, month, revenue)
- Average per month of the the shop (all stores combined)
- Total revenue per city for the year
- Average per month per city (on this 1 year data)
- Total revenue per store on the year
- For each month, best store (most revenue)

### Lab 7: Pyspark2.md

Using Taxi dataset

- Computed the total money earned by each driver for each day
- Compute by hour:
  - The average tip
  - The average duration
  - The average “distance at the crow flies” (“à vol d’oiseau”)
- Find the hour of the day when the tips are the highest
- Find the percentage of each type of payment
- Find the top 10 drivers that:
  - Won the most money
  - Did the greater number of rides
