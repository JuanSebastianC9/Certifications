// Analyze data with Spark
    // One of the benefits of using Spark is that you can write and run code in various programming languages
    // PySpark / a Spark/optimized version of Python
    // Scala (a Java/derived language that can be used interactively)
    // SQL (a variant of the commonly used SQL language included in the Spark SQL library to work with relational data structures). 
    // Software engineers can also create compiled solutions that run on Spark using frameworks such as Java and Microsoft .NET.

// Loading data into a dataframe
////////////////////////////////////////////////////////////////////
// ProductID, ProductName,               Category,       ListPrice /
// 771,       "Mountain/100 Silver, 38", Mountain Bikes, 3399.9900 /
// 772,       "Mountain/100 Silver, 42", Mountain Bikes, 3399.9900 /
// 773,       "Mountain/100 Silver, 44", Mountain Bikes, 3399.9900 /
////////////////////////////////////////////////////////////////////

// The magic %%spark is used to specify Scala.
%%spark
val df = spark.read.format("csv").option("header", "true").load("abfss://container@store.dfs.core.windows.net/products.csv")
display(df.limit(10))