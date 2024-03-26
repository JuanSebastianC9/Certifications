# The following code example saves a dataframe (loaded from CSV files) as an external table name sales_orders. 
# The files are stored in the /sales_orders_table folder in the data lake.
order_details.write.saveAsTable('sales_orders', format='parquet', mode='overwrite', path='/sales_orders_table')

# Use SQL to query and transform the data
# The following code creates two new derived columns named Year and Month, 
# then creates a new table transformed_orders with the new derived columns added.
# Create derived columns
sql_transform = spark.sql("SELECT *, YEAR(OrderDate) AS Year, MONTH(OrderDate) AS Month FROM sales_orders")

# Save the results
sql_transform.write.partitionBy("Year","Month").saveAsTable('transformed_orders', format='parquet', mode='overwrite', path='/transformed_orders_table')

# The data files for the new table are stored in a hierarchy of folders 
# with the format of Year=*NNNN* / Month=*N*, with each folder containing a parquet file for the corresponding orders by year and month.

# Query the metastore
# Because this new table was created in the metastore, 
# you can use SQL to query it directly with the %%sql magic key in the first line 
# to indicate that the SQL syntax will be used as shown in the following script:
%%sql

SELECT * FROM transformed_orders
WHERE Year = 2021
    AND Month = 1

# Drop tables
# you can use the DROP command to delete the table definitions from the metastore without affecting the files in the data lake
%%sql

DROP TABLE transformed_orders;
DROP TABLE sales_orders;