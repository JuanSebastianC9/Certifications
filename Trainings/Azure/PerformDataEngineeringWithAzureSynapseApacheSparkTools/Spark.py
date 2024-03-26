# To load data into a dataframe, you use the spark.read function, specifying the file format, path, and optionally the schema of the data to be read
# the following code loads data from all .csv files in the orders folder into a dataframe named order_details and then displays the first five records.
order_details = spark.read.csv('/orders/*.csv', header=True, inferSchema=True)
display(order_details.limit(5))

# Transform the data structure
# Typical operations on a dataframe include:

#     Filtering rows and columns
#     Renaming columns
#     Creating new columns, often derived from existing ones
#     Replacing null or other values

# the following example, the code uses the split function to separate the values in the CustomerName column 
# into two new columns named FirstName and LastName. 
# Then it uses the drop method to delete the original CustomerName column.
from pyspark.sql.functions import split, col

# Create the new FirstName and LastName fields
transformed_df = order_details.withColumn("FirstName", split(col("CustomerName"), " ").getItem(0)).withColumn("LastName", split(col("CustomerName"), " ").getItem(1))

# Remove the CustomerName field
transformed_df = transformed_df.drop("CustomerName")

display(transformed_df.limit(5))

# Save the transformed data
# you can save the results to a supported format in your data lake.

# The following code example saves the dataFrame into a parquet file in the data lake, 
# replacing any existing file of the same name.

# The Parquet format is typically preferred for data files that you will use 
# for further analysis or ingestion into an analytical store. 
# Parquet is a very efficient format that is supported by most large scale data analytics systems. 
transformed_df.write.mode("overwrite").parquet('/transformed_data/orders.parquet')
print ("Transformed data saved!")