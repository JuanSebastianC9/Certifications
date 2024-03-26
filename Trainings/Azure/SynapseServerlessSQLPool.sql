
-- Built on the OPENROWSET SQL function; which generates a tabular rowset from data in one or more files.
-- For example, the following query could be used to extract data from CSV files.
SELECT TOP 100 *
FROM OPENROWSET(
    BULK 'https://mydatalake.blob.core.windows.net/data/files/*.csv',
    FORMAT = 'csv') AS rows;

-- https://mydatalake.blob.core.windows.net/data/files/file1.csv: Only include file1.csv in the files folder.
-- https://mydatalake.blob.core.windows.net/data/files/file*.csv: All .csv files in the files folder with names that start with "file".
-- https://mydatalake.blob.core.windows.net/data/files/*: All files in the files folder.
-- https://mydatalake.blob.core.windows.net/data/files/**: All files in the files folder, and recursively its subfolders.

-- Delimited text files are a common file format within many businesses. The specific formatting used in delimited files can vary, for example:
-- With and without a header row.
-- Comma and tab-delimited values.
-- Windows and Unix style line endings.
-- Non-quoted and quoted values, and escaping characters.
SELECT TOP 100 *
FROM OPENROWSET(
    BULK 'https://mydatalake.blob.core.windows.net/data/files/*.csv',
    FORMAT = 'csv',
    PARSER_VERSION = '2.0',
    FIRSTROW = 2) AS rows;

-- PARSER_VERSION:
--  Version 1.0 is the default and supports a wide range of file encodings 
--  Version 2.0 supports fewer encodings but offers better performance.
-- FIELDTERMINATOR - the character used to separate field values in each row.
-- ROWTERMINATOR - the character used to signify the end of a row of data. 
-- FIELDQUOTE - the character used to enclose quoted string values. 

-- product_id, product_name,list_price
--        123,       Widget,     12.99
--        124,       Gadget,      3.99

-- HEADER_ROW - (which is only available when using parser version 2.0) instructs the query engine to use the first row of data in each file as the column names
SELECT TOP 100 *
FROM OPENROWSET(
    BULK 'https://mydatalake.blob.core.windows.net/data/files/*.csv',
    FORMAT = 'csv',
    PARSER_VERSION = '2.0',
    HEADER_ROW = TRUE) AS rows

-- You can override the default column names and inferred data types by providing a schema definition in a WITH clause
-- When working with text files, you may encounter some incompatibility with UTF-8 encoded data and 
-- the collation used in the master database for the serverless SQL pool. 
-- To overcome this, you can specify a compatible collation for individual VARCHAR columns in the schema. 
-- See the troubleshooting guidance for more details.
SELECT TOP 100 *
FROM OPENROWSET(
    BULK 'https://mydatalake.blob.core.windows.net/data/files/*.csv',
    FORMAT = 'csv',
    PARSER_VERSION = '2.0')
WITH (
    product_id INT,
    product_name VARCHAR(20) COLLATE Latin1_General_100_BIN2_UTF8,
    list_price DECIMAL(5,2)
) AS rows

-- JSON is a popular format for web applications that exchange data through REST interfaces or use NoSQL data stores such as Azure Cosmos DB.
-- {
--     "product_id": 123,
--     "product_name": "Widget",
--     "list_price": 12.99
-- }
-- OPENROWSET has no specific format for JSON files, 
-- so you must use csv format with FIELDTERMINATOR, FIELDQUOTE, and ROWTERMINATOR set to 0x0b, 
-- and a schema that includes a single NVARCHAR(MAX) column. 
-- The result of this query is a rowset containing a single column of JSON documents, like this:
   -----------------------------------------------------------------
-- | doc                                                           |
   -----------------------------------------------------------------
-- | {"product_id":123,"product_name":"Widget","list_price": 12.99}|
-- | {"product_id":124,"product_name":"Gadget","list_price": 3.99} |
   -----------------------------------------------------------------
SELECT doc
FROM
    OPENROWSET(
        BULK 'https://mydatalake.blob.core.windows.net/data/files/*.json',
        FORMAT = 'csv',
        FIELDTERMINATOR ='0x0b',
        FIELDQUOTE = '0x0b',
        ROWTERMINATOR = '0x0b'
    ) WITH (doc NVARCHAR(MAX)) as rows

-- To extract individual values from the JSON, you can use the JSON_VALUE function in the SELECT statement
SELECT JSON_VALUE(doc, '$.product_name') AS product,
           JSON_VALUE(doc, '$.list_price') AS price
FROM
    OPENROWSET(
        BULK 'https://mydatalake.blob.core.windows.net/data/files/*.json',
        FORMAT = 'csv',
        FIELDTERMINATOR ='0x0b',
        FIELDQUOTE = '0x0b',
        ROWTERMINATOR = '0x0b'
    ) WITH (doc NVARCHAR(MAX)) as rows

-- Parquet is a commonly used format for big data processing on distributed file storage. 
-- the schema of the data is embedded within the Parquet file, 
-- so you only need to specify:
-- BULK with a path to the file(s) you want to read
-- FORMAT parameter = parquet
SELECT TOP 100 *
FROM OPENROWSET(
    BULK 'https://mydatalake.blob.core.windows.net/data/files/*.*',
    FORMAT = 'parquet') AS rows

-- It's common in a data lake to partition data by splitting across multiple files in subfolders that reflect partitioning criteria. 
-- This enables distributed processing systems to work in parallel on multiple partitions of the data, 
-- or to easily eliminate data reads from specific folders based on filtering criteria
-- /orders
--     /year=2020
--         /month=1
--             /01012020.parquet
--             /02012020.parquet
--             ...
--         /month=2
--             /01022020.parquet
--             /02022020.parquet
--             ...
--         ...
--     /year=2021
--         /month=1
--             /01012021.parquet
--             /02012021.parquet
--             ...
--         ...
-- To create a query that filters the results to include only the orders for January and February 2020, you could use the following code:
SELECT *
FROM OPENROWSET(
    BULK 'https://mydatalake.blob.core.windows.net/data/orders/year=*/month=*/*.*',
    FORMAT = 'parquet') AS orders
WHERE orders.filepath(1) = '2020'
    AND orders.filepath(2) IN ('1','2');

---------------------------------------------------------------------------------------------------------------------------------------------------------------

-- CREATING A DATABASE
-- You can create a database in a serverless SQL pool just as you would in a SQL Server instance. 
-- One consideration is to set the collation of your database so that it supports conversion of text data in files to appropriate Transact-SQL data types.
CREATE DATABASE SalesDB
    COLLATE Latin1_General_100_BIN2_UTF8

--If you plan to query data in the same location frequently, it's more efficient to define an external data source that references that location.  
-- For example, the following code creates a data source named files for the hypothetical https://mydatalake.blob.core.windows.net/data/files/ folder:
CREATE EXTERNAL DATA SOURCE files
WITH (
    LOCATION = 'https://mydatalake.blob.core.windows.net/data/files/'
)

-- Then query, in this example, the BULK parameter is used to specify the relative path for all .csv files in the orders folder, 
-- which is a subfolder of the files folder referenced by the data source.
SELECT *
FROM
    OPENROWSET(
        BULK 'orders/*.csv',
        DATA_SOURCE = 'files',
        FORMAT = 'csv',
        PARSER_VERSION = '2.0'
    ) AS orders 

-- Another benefit of using a data source is that you can assign a credential for the data source,
-- enabling you to provide access to data through SQL without permitting users to access the data directly.
-- For example, the following code creates a credential that uses a shared access signature (SAS) to authenticate
-- against the underlying Azure storage account hosting the data lake.
CREATE DATABASE SCOPED CREDENTIAL sqlcred
WITH
    IDENTITY='SHARED ACCESS SIGNATURE',  
    SECRET = 'sv=xxx...';
GO

CREATE EXTERNAL DATA SOURCE secureFiles
WITH (
    LOCATION = 'https://mydatalake.blob.core.windows.net/data/secureFiles/'
    CREDENTIAL = sqlcred
);
GO

-- CREATING AN EXTERNAL FILE FORMAT
-- While an external data source simplifies the code needed to access files with the OPENROWSET function, 
-- you still need to provide format details for the file being access
CREATE EXTERNAL FILE FORMAT CsvFormat
    WITH (
        FORMAT_TYPE = DELIMITEDTEXT,
        FORMAT_OPTIONS(
            FIELD_TERMINATOR = ',',
            STRING_DELIMITER = '"'
        )
    );
GO

CREATE EXTERNAL FILE FORMAT ParquetFormat
WITH (
        FORMAT_TYPE = PARQUET,
        DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
    );

-- CREATING AN EXTERNAL TABLE
-- When you need to perform a lot of analysis from files in the data lake, using the OPENROWSET function can result in complex code that includes data sources and file paths. 
-- To simplify access to the data, you can encapsulate the files in an external table; 
-- which one can query using a standard SQL SELECT statement just like any other database table. 
-- To create an external table, use the CREATE EXTERNAL TABLE statement, specifying the column schema as for a standard table, 
-- and including a WITH clause specifying the external data source, relative path, and external file format for your data.
CREATE EXTERNAL TABLE dbo.products
(
    product_id INT,
    product_name VARCHAR(20),
    list_price DECIMAL(5,2)
)
WITH
(
    DATA_SOURCE = files,
    LOCATION = 'products/*.csv',
    FILE_FORMAT = CsvFormat
);
GO

-- query the table
SELECT * FROM dbo.products;

-- You can use a CREATE EXTERNAL TABLE AS SELECT (CETAS) statement 
-- in a dedicated SQL pool or serverless SQL pool to persist the results of a query in an external table, 
-- which stores its data in a file in the data lake.
-- Using the CETAS statement, and noting that the LOCATION and BULK parameters in the following example are relative paths for the results and source files respectively. 
-- The paths are relative to the file system location referenced by the files external data source.
CREATE EXTERNAL TABLE SpecialOrders
    WITH (
        -- details for storing results
        LOCATION = 'special_orders/',
        DATA_SOURCE = files,
        FILE_FORMAT = ParquetFormat
    )
AS
SELECT OrderID, CustomerName, OrderTotal
FROM
    OPENROWSET(
        -- details for reading source files
        BULK 'sales_orders/*.csv',
        DATA_SOURCE = 'files',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0',
        HEADER_ROW = TRUE
    ) AS source_data
WHERE OrderType = 'Special Order';

-- You can also use a second data source to define a connection to the source data or use the fully qualified path
CREATE EXTERNAL TABLE SpecialOrders
    WITH (
        -- details for storing results
        LOCATION = 'special_orders/',
        DATA_SOURCE = files,
        FILE_FORMAT = ParquetFormat
    )
AS
SELECT OrderID, CustomerName, OrderTotal
FROM
    OPENROWSET(
        -- details for reading source files
        BULK 'https://mystorage.blob.core.windows.net/data/sales_orders/*.csv',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0',
        HEADER_ROW = TRUE
    ) AS source_data
WHERE OrderType = 'Special Order';

-- Dropping external tables
DROP EXTERNAL TABLE SpecialOrders;

-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------

-- It's good practice to encapsulate the transformation operation in stored procedure. 
-- This approach can make it easier to operationalize data transformations 
-- by enabling you to supply parameters, retrieve outputs, and include additional logic in a single procedure call.
-- As an example, we create a stored procedure that drops the external table if it already exists before recreating it with order data for the specified year.
-- Also, dropping an existing external table does not delete the folder containing its data files. 
-- You must explicitly delete the target folder if it exists before running the stored procedure, or an error will occur.
CREATE PROCEDURE usp_special_orders_by_year @order_year INT
AS
BEGIN

	-- Drop the table if it already exists
	IF EXISTS (
                SELECT * FROM sys.external_tables
                WHERE name = 'SpecialOrders'
            )
        DROP EXTERNAL TABLE SpecialOrders

	-- Create external table with special orders
	-- from the specified year
	CREATE EXTERNAL TABLE SpecialOrders
		WITH (
			LOCATION = 'special_orders/',
			DATA_SOURCE = files,
			FILE_FORMAT = ParquetFormat
		)
	AS
	SELECT OrderID, CustomerName, OrderTotal
	FROM
		OPENROWSET(
			BULK 'sales_orders/*.csv',
			DATA_SOURCE = 'files',
			FORMAT = 'CSV',
			PARSER_VERSION = '2.0',
			HEADER_ROW = TRUE
		) AS source_data
	WHERE OrderType = 'Special Order'
	AND YEAR(OrderDate) = @order_year
END

-- Reduces client to server network traffic:
-- The commands in a procedure are executed as a single batch of code; 
-- which can significantly reduce network traffic between the server and client because only the call to execute the procedure is sent across the network.

-- Provides a security boundary:
-- Multiple users and client programs can perform operations on underlying database objects through a procedure, 
-- even if the users and programs don't have direct permissions on those underlying objects. 
-- The procedure controls what processes and activities are performed and protects the underlying database objects; 
-- eliminating the requirement to grant permissions at the individual object level and simplifies the security layers.

-- Eases maintenance:
-- Any changes in the logic or file system locations involved in the data transformation can be applied only to the stored procedure; 
-- without requiring updates to client applications or other calling functions.

-- Improved performance:
-- Stored procedures are compiled the first time they're executed, 
-- and the resulting execution plan is held in the cache and reused on subsequent runs of the same stored procedure. 
-- As a result, it takes less time to process the procedure.

-- Encapsulating a CREATE EXTERNAL TABLE AS SELECT (CETAS) statement in a stored procedure makes it easier for you to operationalize data transformations that you may need to perform repeatedly. 
-- In Azure Synapse Analytics and Azure Data Factory, you can create pipelines that connect to linked services, including Azure Data Lake Store Gen2 storage accounts that host data lake files, 
-- and serverless SQL pools; enabling you to call your stored procedures as part of an overall data extract, transform, and load (ETL) pipeline.
-- For example, you can create a pipeline that includes the following activities:
--     A Delete activity that deletes the target folder for the transformed data in the data lake if it already exists.
--     A Stored procedure activity that connects to your serverless SQL pool and runs the stored procedure that encapsulates your CETAS operation.
-- https://learn.microsoft.com/en-us/training/wwl-data-ai/use-azure-synapse-serverless-sql-pools-for-transforming-data-lake/media/synapse-pipeline.png


------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

-- USE A LAKE DATABASE
-- Using a serverless SQL pool there is no need to use an OPENROWSET function or include any additional code to access the data from the underlying file storage.
-- The serverless SQL pool handles the mapping to the files for you.
USE RetailDB;
GO

SELECT CustomerID, FirstName, LastName
FROM Customer
ORDER BY LastName;

-- Using an Apache Spark pool
-- You can work with lake database tables using Spark SQL in an Apache Spark pool.
-- Insert a new customer record into the Customer table:
%%sql
INSERT INTO `RetailDB`.`Customer` VALUES (123, 'John', 'Yang')
-- Then query the table:
%%sql
SELECT * FROM `RetailDB`.`Customer` WHERE CustomerID = 123

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

-- AUTHENTICATION METHODS 
-- Two types of authentication are supported:
--     SQL Authentication:
--      This authentication method uses a username and password.
--     Microsoft Entra authentication:
--      This authentication method uses identities managed by Microsoft Entra ID. 
--      For Microsoft Entra users, multi-factor authentication can be enabled. 
--      Use Active Directory authentication (integrated security) whenever possible.

-- A user that is logged into the serverless SQL pool service must be authorized to access and query the files in Azure Storage. Serverless SQL pool supports the following authorization types:

--     Anonymous access
--      To access publicly available files placed on Azure storage accounts that allow anonymous access.

--     Shared access signature (SAS)
--      Provides delegated access to resources in storage account. 
--      With a SAS, you can grant clients access to resources in storage account, without sharing account keys. 
--      A SAS gives you granular control over the type of access you grant to clients who have the SAS: validity interval, granted permissions, acceptable IP address range, acceptable protocol (https/http).

--     Managed Identity.
--      Is a feature of Microsoft Entra ID that provides Azure services for serverless SQL pool. Also, it deploys an automatically managed identity in Microsoft Entra ID. 
--      This identity can be used to authorize the request for data access in Azure Storage. 
--      Before accessing the data, the Azure Storage administrator must grant permissions to Managed Identity for accessing the data. 
--      Granting permissions to Managed Identity is done the same way as granting permission to any other Microsoft Entra user.

--     User Identity
--      Also known as "pass-through", is an authorization type where the identity of the Microsoft Entra user that logged into serverless SQL pool is used to authorize access to the data. 
--      Before accessing the data, Azure Storage administrator must grant permissions to Microsoft Entra user for accessing the data. 
--      This authorization type uses the Microsoft Entra user that logged into serverless SQL pool, therefore it's not supported for SQL user types.

--  (Access Control Lists) ACLs
-- There are two kinds of access control lists:

--     Access ACLs
--      Controls access to an object. Files and directories both have access ACLs.

--     Default ACLs
--      Are templates of ACLs associated with a directory that determine the access ACLs for any child items that are created under that directory. 
--      Files do not have default ACLs.
   ------------------------------------------------------------------------------------------------------------------------------------------------------
-- |Permission 	    File 	                                                            Directory                                                       |  
   ------------------------------------------------------------------------------------------------------------------------------------------------------                                                                       
-- |Read (R) 	    Can read the contents of a file 	                                Requires Read and Execute to list the contents of the directory | 
-- |Write (W) 	    Can write or append to a file 	                                    Requires Write and Execute to create child items in a directory |
-- |Execute (X) 	Does not mean anything in the context of Data Lake Storage Gen2 	Required to traverse the child items of a directory             |
   ------------------------------------------------------------------------------------------------------------------------------------------------------

-- Guidelines in setting up ACLs

-- Always use Microsoft Entra security groups as the assigned principal in an ACL entry. 
-- Resist the opportunity to directly assign individual users or service principals. 
-- Using this structure will allow you to add and remove users or service principals without the need to reapply ACLs to an entire directory structure. 
-- Instead, you can just add or remove users and service principals from the appropriate Microsoft Entra security group.

-- There are many ways to set up groups. 
-- For example, imagine that you have a directory named /LogData which holds log data that is generated by your server. 
-- Azure Data Factory (ADF) ingests data into that folder. 
-- Specific users from the service engineering team will upload logs and manage other users of this folder, and various Databricks clusters will analyze logs from that folder.

-- To enable these activities, you could create a LogsWriter group and a LogsReader group. Then, you could assign permissions as follows:
--     Add the LogsWriter group to the ACL of the /LogData directory with rwx permissions.
--     Add the LogsReader group to the ACL of the /LogData directory with r-x permissions.
--     Add the service principal object or Managed Service Identity (MSI) for ADF to the LogsWriters group.
--     Add users in the service engineering team to the LogsWriter group.
--     Add the service principal object or MSI for Databricks to the LogsReader group.

-- If a user in the service engineering team leaves the company, you could just remove them from the LogsWriter group. 
-- If you did not add that user to a group, but instead, you added a dedicated ACL entry for that user, you would have to remove that ACL entry from the /LogData directory. 
-- You would also have to remove the entry from all subdirectories and files in the entire directory hierarchy of the /LogData directory.

-- Database level permission

-- To provide more granular access to the user, you should use Transact-SQL syntax to create logins and users.
-- To grant access to a user to a single serverless SQL pool database, follow the steps in this example:

-- 1. Create LOGIN
use master
CREATE LOGIN [alias@domain.com] FROM EXTERNAL PROVIDER;

-- 2. Create USER
use yourdb -- Use your DB name
CREATE USER alias FROM LOGIN [alias@domain.com];

-- 3. Add USER to members of the specified role
use yourdb -- Use your DB name
alter role db_datareader 
Add member alias -- Type USER name from step 2
-- You can use any Database Role which exists 
-- (examples: db_owner, db_datareader, db_datawriter)
-- Replace alias with alias of the user you would like to give access and domain with the company domain you are using.

-- Server level permission

-- 1. To grant full access to a user to all serverless SQL pool databases, follow the step in this example: 
CREATE LOGIN [alias@domain.com] FROM EXTERNAL PROVIDER;
ALTER SERVER ROLE sysadmin ADD MEMBER [alias@domain.com];