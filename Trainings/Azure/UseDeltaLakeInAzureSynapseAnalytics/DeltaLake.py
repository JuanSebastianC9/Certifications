# Creating a Delta Lake table from a dataframe
# One of the easiest ways to create a Delta Lake table is to save a dataframe in the delta format, 
# specifying a path where the data files and related metadata information for the table should be stored.
# For example, the following PySpark code loads a dataframe with data from an existing file, 
# and then saves that dataframe to a new folder location in delta format:
# Load a file into a dataframe
df = spark.read.load('/data/mydata.csv', format='csv', header=True)

# Save the dataframe as a delta table
delta_table_path = "/delta/mydata"
df.write.format("delta").save(delta_table_path)

# After saving the delta table, the path location you specified includes parquet files for the data 
# (regardless of the format of the source file you loaded into the dataframe) 
# and a _delta_log folder containing the transaction log for the table.

# You can replace an existing Delta Lake table with the contents of a dataframe by using the overwrite mode, as shown here:
new_df.write.format("delta").mode("overwrite").save(delta_table_path)

# You can also add rows from a dataframe to an existing table by using the append mode:
new_rows_df.write.format("delta").mode("append").save(delta_table_path)

# Making conditional updates
# While you can make data modifications in a dataframe and then replace a Delta Lake table by overwriting it, 
# a more common pattern in a database is to insert, update or delete rows in an existing table as discrete transactional operations.
# To make such modifications to a Delta Lake table, 
# you can use the DeltaTable object in the Delta Lake API, which supports update, delete, and merge operations. 
# For example, you could use the following code to update the price column for all rows with a category column value of "Accessories":
from delta.tables import *
from pyspark.sql.functions import *

# Create a deltaTable object
deltaTable = DeltaTable.forPath(spark, delta_table_path)

# Update the table (reduce price of accessories by 10%)
deltaTable.update(
    condition = "Category == 'Accessories'",
    set = { "Price": "Price * 0.9" })
# The data modifications are recorded in the transaction log, and new parquet files are created in the table folder as required.

# Querying a previous version of a table
# The transaction log records modifications made to the table, noting the timestamp and version number for each transaction. 
# You can use this logged version data to view previous versions of the table - a feature known as time travel.

# specifying the version required as a versionAsOf option:
df = spark.read.format("delta").option("versionAsOf", 0).load(delta_table_path)

# specify a timestamp by using the timestampAsOf option:
df = spark.read.format("delta").option("timestampAsOf", '2022-01-01').load(delta_table_path)

# Create catalog tables
# External vs managed tables
# Tables in a Spark catalog, including Delta Lake tables, can be managed or external
# 
#     A managed table is defined without a specified location, and the data files are stored within the storage used by the metastore. 
#     Dropping the table not only removes its metadata from the catalog, but also deletes the folder in which its data files are stored.
# 
#     An external table is defined for a custom file location, where the data for the table is stored. 
#     The metadata for the table is defined in the Spark catalog. 
#     Dropping the table deletes the metadata from the catalog, but doesn't affect the data files.

# Creating a catalog table from a dataframe
# You can create managed tables by writing a dataframe using the saveAsTable operation as shown in the following examples:
# Save a dataframe as a managed table
df.write.format("delta").saveAsTable("MyManagedTable")

## specify a path option to save as an external table
df.write.format("delta").option("path", "/mydata").saveAsTable("MyExternalTable")

# Creating a catalog table using SQL
# You can also create a catalog table by using the CREATE TABLE SQL statement with the USING DELTA clause, 
# and an optional LOCATION parameter for external tables. 
# You can run the statement using the SparkSQL API, like the following example:
spark.sql("CREATE TABLE MyExternalTable USING DELTA LOCATION '/mydata'")
# or
%%sql

CREATE TABLE MyExternalTable
USING DELTA
LOCATION '/mydata'

# The CREATE TABLE statement returns an error if a table with the specified name already exists in the catalog. 
# To mitigate this behavior, you can use a CREATE TABLE IF NOT EXISTS statement or the CREATE OR REPLACE TABLE statement.

# Defining the table schema
# when creating a new managed table, or an external table with a currently empty location, 
# you define the table schema by specifying the column names, types, and nullability as part of the CREATE TABLE statement; 
# as shown in the following example:
%%sql

CREATE TABLE ManagedSalesOrders
(
    Orderid INT NOT NULL,
    OrderDate TIMESTAMP NOT NULL,
    CustomerName STRING,
    SalesTotal FLOAT NOT NULL
)
USING DELTA

# Using the DeltaTableBuilder API
# You can use the DeltaTableBuilder API (part of the Delta Lake API) to create a catalog table, as shown in the following example:
from delta.tables import *

DeltaTable.create(spark) \
  .tableName("default.ManagedProducts") \
  .addColumn("Productid", "INT") \
  .addColumn("ProductName", "STRING") \
  .addColumn("Category", "STRING") \
  .addColumn("Price", "FLOAT") \
  .execute()
# Similarly to the CREATE TABLE SQL statement, the create method returns an error if a table with the specified name already exists. 
# You can mitigate this behavior by using the createIfNotExists or createOrReplace method.

# Using catalog tables
# the following code example uses a SELECT statement to query the ManagedSalesOrders table:
%%sql

SELECT orderid, salestotal
FROM ManagedSalesOrders

# Use Delta Lake with streaming data
# All of the data we've explored up to this point has been static data in files. 
# However, many data analytics scenarios involve streaming data that must be processed in near real time. 
# For example, you might need to capture readings emitted by internet-of-things (IoT) devices and store them in a table as they occur.

# Spark Structured Streaming
# A typical stream processing solution involves constantly reading a stream of data from a source, 
# optionally processing it to select specific fields, aggregate and group values, or otherwise manipulate the data, and writing the results to a sink.

# You can use a Delta Lake table as a source or a sink for Spark Structured Streaming

# Using a Delta Lake table as a streaming source

from pyspark.sql.types import *
from pyspark.sql.functions import *

# Load a streaming dataframe from the Delta Table
stream_df = spark.readStream.format("delta") \
    .option("ignoreChanges", "true") \
    .load("/delta/internetorders")

# Now you can process the streaming data in the dataframe
# for example, show it:
stream_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

# Using a Delta Lake table as a streaming sink
# New data is added to the stream whenever a file is added to the folder. The input stream is a boundless dataframe, 
# which is then written in delta format to a folder location for a Delta Lake table.

from pyspark.sql.types import *
from pyspark.sql.functions import *

# Create a stream that reads JSON data from a folder
inputPath = '/streamingdata/'
jsonSchema = StructType([
    StructField("device", StringType(), False),
    StructField("status", StringType(), False)
])
stream_df = spark.readStream.schema(jsonSchema).option("maxFilesPerTrigger", 1).json(inputPath)

# Write the stream to a delta table
# The checkpointLocation option is used to write a checkpoint file that tracks the state of the stream processing. 
# This file enables you to recover from failure at the point where stream processing left off.
table_path = '/delta/devicetable'
checkpoint_path = '/delta/checkpoint'
delta_stream = stream_df.writeStream.format("delta").option("checkpointLocation", checkpoint_path).start(table_path)

# Query the Delta Lake to which the streaming output is being written to
%%sql

CREATE TABLE DeviceTable
USING DELTA
LOCATION '/delta/devicetable';

SELECT device, status
FROM DeviceTable;

# To stop the stream of data being written to the Delta Lake table, 
# you can use the stop method of the streaming query
delta_stream.stop()

