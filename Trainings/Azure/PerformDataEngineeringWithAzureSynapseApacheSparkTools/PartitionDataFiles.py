# Partition data files
# Partitioning is an optimization technique that enables spark to maximize performance across the worker nodes. 
# More performance gains can be achieved when filtering data in queries by eliminating unnecessary disk IO.

# Partition the output file
# To save a dataframe as a partitioned set of files, use the partitionBy method when writing the data.
# The following example creates a derived Year field. Then uses it to partition the data.
from pyspark.sql.functions import year, col

# Load source data
df = spark.read.csv('/orders/*.csv', header=True, inferSchema=True)

# Add Year column
dated_df = df.withColumn("Year", year(col("OrderDate")))

# Partition by year
dated_df.write.partitionBy("Year").mode("overwrite").parquet("/data")

# dir: /data/Year=2020
#      /data/Year=2021
#      /data/Year=2022

# Filter parquet files in a query
# you have the ability to pull data from any folder within the hierarchical folders. 
# This filtering process is done with the use of explicit values and wildcards against the partitioned fields.
# In the following example, the following code will pull the sales orders, which were placed in 2020.
orders_2020 = spark.read.parquet('/partitioned_data/Year=2020')
display(orders_2020.limit(5))