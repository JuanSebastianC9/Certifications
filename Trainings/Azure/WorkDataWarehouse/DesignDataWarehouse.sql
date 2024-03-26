-- a data warehouse contains tables in which the data you want to analyze is stored. Most commonly, 
-- these tables are organized in a schema that is optimized for multidimensional modeling

-- FACTS: numerical measures associated with events   
-- DIMENSIONS: the attributes of associated entities across multiple 
-- Facts can be aggregated by Dimensions

-- Dimension Tables
-- They describe business entities, such as products, people, places, and dates.
-- Dimension tables contain columns for attributes of an entity.

-- A dimension table contains a unique key column that uniquely identifies each row in the table
-- Surrogate key: specific to the data warehouse and uniquely identifies each row in the dimension table in the data warehouse 
--  - usually an incrementing integer number.
-- Alternate key: a natural or business key used to identify a specific instance of an entity in the transactional source system from which the entity record originated 
--  - such as a product code or a customer ID

-- Why have two keys? There are a few good reasons:

--     The data warehouse may be populated with data from multiple source systems, 
--     which can lead to the risk of duplicate or incompatible business keys.
--     Simple numeric keys generally perform better in queries that join lots of tables  
--       - a common pattern in data warehouses.
--     Attributes of entities may change over time - for example, a customer might change their address. Since the data warehouse is used to support historic reporting, you may want to retain a record for each instance of an entity at multiple points in time; so that, for example, sales orders for a specific customer are counted for the city where they lived at the time the order was placed. In this case, multiple customer records would have the same business key associated with the customer, but different surrogate keys for each discrete address where the customer lived at various times.

-- CustomerKey 	CustomerAltKey 	Name 	        Email 	                Street 	        City 	    PostalCode 	 CountryRegion
-- ----------------------------------------------------------------------------------------------------------------------------
-- 123 	        I-543 	        Navin Jones 	navin1@contoso.com 	    1 Main St. 	    Seattle 	90000 	     United States
-- 124 	        R-589 	        Mary Smith 	    mary2@contoso.com 	    234 190th Ave 	Buffalo 	50001 	     United States
-- 125 	        I-321 	        Antoine Dubois 	antoine1@contoso.com 	2 Rue Jolie 	Paris 	    20098 	     France
-- 126 	        I-543 	        Navin Jones 	navin1@contoso.com 	    24 125th Ave. 	New York 	50000 	     United States
-- ... 	        ... 	        ... 	        ... 	                ... 	        ... 	    ... 	     ...         

-- Observe that the table contains two records for Navin Jones. 
-- Both records use the same alternate key to identify this person (I-543), 
-- but each record has a different surrogate key. 
-- From this, you can surmise that the customer moved from Seattle to New York. 
-- Sales made to the customer while living in Seattle are associated with the key 123, 
-- while purchases made after moving to New York are recorded against record 126.

-- In addition to dimension tables that represent business entities, 
-- it's common for a data warehouse to include a dimension table that represents time. 
-- This table enables data analysts to aggregate data over temporal intervals. 
-- Depending on the type of data you need to analyze, the lowest granularity 
-- (referred to as the grain) of a time dimension could represent times 
-- (to the hour, second, millisecond, nanosecond, or even lower), or dates.

-- Data warehouse schema designs
-- Usually data is normalized to reduce duplication. In a data warehouse however, 
-- the dimension data is generally de-normalized to reduce the number of joins required to query the data.
-- In this case, the DimProduct table has been normalized to create separate dimension tables for product categories and suppliers, 
-- and a DimGeography table has been added to represent geographical attributes for both customers and stores. 

-- Creating a dedicated SQL pool
-- To create a relational data warehouse in Azure Synapse Analytics, you must create a dedicated SQL Pool. 
-- When provisioning a dedicated SQL pool, you can specify the following configuration settings:

--     A unique name for the dedicated SQL pool.
--     A performance level for the SQL pool, which can range from DW100c to DW30000c and which determines the cost per hour for the pool when it's running.
--     Whether to start with an empty pool or restore an existing database from a backup.
--     The collation of the SQL pool, which determines sort order and string comparison rules for the database. (You can't change the collation after creation).

-- Considerations for creating tables

-- The data warehouse is composed of fact and dimension tables as discussed previously. 
-- Staging tables are often used as part of the data warehousing loading process to ingest data from source systems.
-- To create tables in the dedicated SQL pool, you use the CREATE TABLE (or sometimes the CREATE EXTERNAL TABLE) Transact-SQL statement. 
-- The specific options used in the statement depend on the type of table you're creating, which can include:

--     Fact tables
--     Dimension tables
--     Staging tables

-- When designing a star schema model for small or medium sized datasets you can use your preferred database, such as Azure SQL. 
-- For larger data sets you may benefit from implementing your data warehouse in Azure Synapse Analytics instead of SQL Server. 
-- It's important to understand some key differences when creating tables in Synapse Analytics.

-- Data integrity constraints

-- Dedicated SQL pools in Synapse Analytics don't support foreign key and unique constraints as found in other relational database systems like SQL Server.
-- This means that jobs used to load data must maintain uniqueness and referential integrity for keys, 
-- without relying on the table definitions in the database to do so.

-- Indexes

-- While Synapse Analytics dedicated SQL pools support clustered indexes as found in SQL Server, the default index type is clustered columnstore. 
-- This index type offers a significant performance advantage when querying large quantities of data in a typical data warehouse schema and should be used where possible. 
-- However, some tables may include data types that can't be included in a clustered columnstore index (for example, VARBINARY(MAX)), in which case a clustered index can be used instead.

-- Distribution

-- Azure Synapse Analytics dedicated SQL pools use a massively parallel processing (MPP) architecture, 
-- as opposed to the symmetric multiprocessing (SMP) architecture used in most OLTP database systems. 
-- In an MPP system, the data in a table is distributed for processing across a pool of nodes. 
-- Synapse Analytics supports the following kinds of distribution:

--     Hash: A deterministic hash value is calculated for the specified column and used to assign the row to a compute node.
--     Round-robin: Rows are distributed evenly across all compute nodes.
--     Replicated: A copy of the table is stored on each compute node.

-- Table type 	Recommended distribution option
-- Dimension 	Use replicated distribution for smaller tables to avoid data shuffling when joining to distributed fact tables. If tables are too large to store on each compute node, use hash distribution.
-- Fact 	    Use hash distribution with clustered columnstore index to distribute fact tables across compute nodes.
-- Staging 	    Use round-robin distribution for staging tables to evenly distribute data across compute nodes.

-- Creating dimension tables
-- ensure that the table definition includes surrogate and alternate keys as well as columns for the attributes of the dimension that you want to use to group aggregations. 
-- use an IDENTITY column to auto-generate an incrementing surrogate key (otherwise you need to generate unique keys every time you load data).

CREATE TABLE dbo.DimCustomer
(
    CustomerKey INT IDENTITY NOT NULL,
    CustomerAlternateKey NVARCHAR(15) NULL,
    CustomerName NVARCHAR(80) NOT NULL,
    EmailAddress NVARCHAR(50) NULL,
    Phone NVARCHAR(25) NULL,
    StreetAddress NVARCHAR(100),
    City NVARCHAR(20),
    PostalCode NVARCHAR(10),
    CountryRegion NVARCHAR(20)
)
WITH
(
    DISTRIBUTION = REPLICATE,
    CLUSTERED COLUMNSTORE INDEX
);

-- If you intend to use a snowflake schema in which dimension tables are related to one another, 
-- you should include the key for the parent dimension in the definition of the child dimension table. 
-- For example, the following SQL code could be used to move the geographical address details from the DimCustomer table to a separate DimGeography dimension table:

CREATE TABLE dbo.DimGeography
(
    GeographyKey INT IDENTITY NOT NULL,
    GeographyAlternateKey NVARCHAR(10) NULL,
    StreetAddress NVARCHAR(100),
    City NVARCHAR(20),
    PostalCode NVARCHAR(10),
    CountryRegion NVARCHAR(20)
)
WITH
(
    DISTRIBUTION = REPLICATE,
    CLUSTERED COLUMNSTORE INDEX
);

CREATE TABLE dbo.DimCustomer
(
    CustomerKey INT IDENTITY NOT NULL,
    CustomerAlternateKey NVARCHAR(15) NULL,
    GeographyKey INT NULL,
    CustomerName NVARCHAR(80) NOT NULL,
    EmailAddress NVARCHAR(50) NULL,
    Phone NVARCHAR(25) NULL
)
WITH
(
    DISTRIBUTION = REPLICATE,
    CLUSTERED COLUMNSTORE INDEX
);

-- Time dimension tables
-- Most data warehouses include a time dimension table that enables you to aggregate data by multiple hierarchical levels of time interval.

CREATE TABLE dbo.DimDate
( 
    DateKey INT NOT NULL,
    DateAltKey DATETIME NOT NULL,
    DayOfMonth INT NOT NULL,
    DayOfWeek INT NOT NULL,
    DayName NVARCHAR(15) NOT NULL,
    MonthOfYear INT NOT NULL,
    MonthName NVARCHAR(15) NOT NULL,
    CalendarQuarter INT  NOT NULL,
    CalendarYear INT NOT NULL,
    FiscalQuarter INT NOT NULL,
    FiscalYear INT NOT NULL
)
WITH
(
    DISTRIBUTION = REPLICATE,
    CLUSTERED COLUMNSTORE INDEX
);

-- Creating fact tables
-- Fact tables include the keys for each dimension to which they're related, 
-- and the attributes and numeric measures for specific events or observations that you want to analyze.
-- Example: a fact table named FactSales that is related to multiple dimensions through key columns (date, customer, product, and store)

CREATE TABLE dbo.FactSales
(
    OrderDateKey INT NOT NULL,
    CustomerKey INT NOT NULL,
    ProductKey INT NOT NULL,
    StoreKey INT NOT NULL,
    OrderNumber NVARCHAR(10) NOT NULL,
    OrderLineItem INT NOT NULL,
    OrderQuantity SMALLINT NOT NULL,
    UnitPrice DECIMAL NOT NULL,
    Discount DECIMAL NOT NULL,
    Tax DECIMAL NOT NULL,
    SalesAmount DECIMAL NOT NULL
)
WITH
(
    DISTRIBUTION = HASH(OrderNumber),
    CLUSTERED COLUMNSTORE INDEX
);

-- Creating staging tables
-- Staging tables are used as temporary storage for data as it's being loaded into the data warehouse.
-- A typical pattern is to structure the table to make it as efficient as possible to ingest the data from its external source 
-- (often files in a data lake) into the relational database, 
-- and then use SQL statements to load the data from the staging tables into the dimension and fact tables.
-- A staging table for product data that will ultimately be loaded into a dimension table:

CREATE TABLE dbo.StageProduct
(
    ProductID NVARCHAR(10) NOT NULL,
    ProductName NVARCHAR(200) NOT NULL,
    ProductCategory NVARCHAR(200) NOT NULL,
    Color NVARCHAR(10),
    Size NVARCHAR(10),
    ListPrice DECIMAL NOT NULL,
    Discontinued BIT NOT NULL
)
WITH
(
    DISTRIBUTION = ROUND_ROBIN,
    CLUSTERED COLUMNSTORE INDEX
);

-- Using external tables
-- if the data to be loaded is in files with an appropriate structure, 
-- it can be more effective to create external tables that reference the file location.
-- the data can be read directly from the source files instead of being loaded into the relational store.
-- how to create an external table that references files in the data lake associated with the Synapse workspace:

-- External data source links to data lake location
CREATE EXTERNAL DATA SOURCE StagedFiles
WITH (
    LOCATION = 'https://mydatalake.blob.core.windows.net/data/stagedfiles/'
);
GO

-- External format specifies file format
CREATE EXTERNAL FILE FORMAT ParquetFormat
WITH (
    FORMAT_TYPE = PARQUET,
    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
);
GO

-- External table references files in external data source
CREATE EXTERNAL TABLE dbo.ExternalStageProduct
(
    ProductID NVARCHAR(10) NOT NULL,
    ProductName NVARCHAR(200) NOT NULL,
    ProductCategory NVARCHAR(200) NOT NULL,
    Color NVARCHAR(10),
    Size NVARCHAR(10),
    ListPrice DECIMAL NOT NULL,
    Discontinued BIT NOT NULL
)
WITH
(
    DATA_SOURCE = StagedFiles,
    LOCATION = 'products/*.parquet',
    FILE_FORMAT = ParquetFormat
);
GO

-- Load data warehouse tables
-- Adding new data from files in a data lake into tables in the data warehouse. 
-- The COPY statement is an effective way to accomplish this task, as shown in the following example:
COPY INTO dbo.StageProducts
    (ProductID, ProductName, ProductCategory, Color, Size, ListPrice, Discontinued)
FROM 'https://mydatalake.blob.core.windows.net/data/stagedfiles/products/*.parquet'
WITH
(
    FILE_TYPE = 'PARQUET',
    MAXERRORS = 0,
    IDENTITY_INSERT = 'OFF'
);