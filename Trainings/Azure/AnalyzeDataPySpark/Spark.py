## Analyze data with Spark
    ## One of the benefits of using Spark is that you can write and run code in various programming languages
    ## PySpark # a Spark#optimized version of Python
    ## Scala (a Java#derived language that can be used interactively)
    ## SQL (a variant of the commonly used SQL language included in the Spark SQL library to work with relational data structures). 
    ## Software engineers can also create compiled solutions that run on Spark using frameworks such as Java and Microsoft .NET.

## Loading data into a dataframe
####################################################################
## ProductID, ProductName,               Category,       ListPrice #
## 771,       "Mountain#100 Silver, 38", Mountain Bikes, 3399.9900 #
## 772,       "Mountain#100 Silver, 42", Mountain Bikes, 3399.9900 #
## 773,       "Mountain#100 Silver, 44", Mountain Bikes, 3399.9900 #
####################################################################

# The %%pyspark line at the beginning is called a magic, and tells Spark that the language used in this cell is PySpark.
%%pyspark
df = spark.read.load('abfss://container@store.dfs.core.windows.net/products.csv',
    format='csv',
    header=True
)
display(df.limit(10))

# PySpark example shows how to specify a schema for the dataframe to be loaded from a file named product-data.csv in this format: 
from pyspark.sql.types import *
from pyspark.sql.functions import *

productSchema = StructType([
    StructField("ProductID", IntegerType()),
    StructField("ProductName", StringType()),
    StructField("Category", StringType()),
    StructField("ListPrice", FloatType())
    ])

df = spark.read.load('abfss://container@store.dfs.core.windows.net/product-data.csv',
    format='csv',
    schema=productSchema,
    header=False)
display(df.limit(10))

# Filtering and grouping dataframes
pricelist_df = df.select("ProductID", "ListPrice")
# OR
pricelist_df = df["ProductID", "ListPrice"]

# To group and aggregate data, you can use the groupBy method and aggregate functions
counts_df = df.select("ProductID", "Category").groupBy("Category").count()
display(counts_df)

# Using SQL expressions in Spark
# Creating database objects in the Spark catalog
# One of the simplest ways to make data in a dataframe available for querying in the Spark catalog is to create a temporary view,
df.createOrReplaceTempView("products")

# create an empty table
df.spark.catalog.createTable()

# save a dataframe as a table
df.saveAsTable()

# create an external table (External tables define metadata in the catalog but get their underlying data from an external storage location)
# Deleting an external table does not delete the underlying data.
df.spark.catalog.createExternalTable()

# Using the Spark SQL API to query data
bikes_df = spark.sql("SELECT ProductID, ProductName, ListPrice \
                      FROM products \
                      WHERE Category IN ('Mountain Bikes', 'Road Bikes')")
display(bikes_df)

# Using graphics packages in code
# built on the base Matplotlib library.
from matplotlib import pyplot as plt

# Get the data as a Pandas dataframe
data = spark.sql("SELECT Category, COUNT(ProductID) AS ProductCount \
                  FROM products \
                  GROUP BY Category \
                  ORDER BY Category").toPandas()

# Clear the plot area
plt.clf()

# Create a Figure
fig = plt.figure(figsize=(12,8))

# Create a bar plot of product counts by category
plt.bar(x=data['Category'], height=data['ProductCount'], color='orange')

# Customize the chart
plt.title('Product Counts by Category')
plt.xlabel('Category')
plt.ylabel('Products')
plt.grid(color='#95a5a6', linestyle='--', linewidth=2, axis='y', alpha=0.7)
plt.xticks(rotation=70)

# Show the plot area
plt.show()