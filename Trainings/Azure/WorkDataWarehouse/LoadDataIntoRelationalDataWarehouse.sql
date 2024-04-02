-- Many enterprise analytical solutions include a relational data warehouse. 
-- Data engineers are responsible for implementing ingestion solutions that load data into the data warehouse tables, usually on a regular schedule.

-- As a data engineer, you need to be familiar with the considerations and techniques that apply to loading a data warehouse. 
-- In this module, we'll focus on ways that you can use SQL to load data into tables in a dedicated SQL pool in Azure Synapse Analytics.

-- Load staging tables
-- common patterns for loading a data warehouse is to transfer data from source systems to files in a data lake, 
-- ingest the file data into staging tables, and then use SQL statements to load the data from the staging tables into the dimension and fact tables.
-- Usually data loading is performed as a periodic batch process in which inserts and updates to the data warehouse are coordinated to occur at a regular interval (for example, daily, weekly, or monthly).

-- Creating staging tables
-- Many organized warehouses have standard structures for staging the database and might even use a specific schema for staging the data.
-- This example creates a staging table in the default dbo schema. 
-- You can also create separate schemas for staging tables with a meaningful name, such as stage so architects and users understand the purpose of the schema.

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

-- Using the COPY command
-- This is generally the recommended approach to load staging tables due to its high performance throughput.

COPY INTO dbo.StageProduct
    (ProductID, ProductName, ...)
FROM 'https://mydatalake.../data/products*.parquet'
WITH
(
    FILE_TYPE = 'PARQUET',
    MAXERRORS = 0,
    IDENTITY_INSERT = 'OFF'
);

-- Using external tables
-- if the data to be loaded is stored in files with an appropriate structure, 
-- it can be more effective to create external tables that reference the file location.
-- the data can be read directly from the source files instead of being loaded into the relational store. 
-- create an external table that references files in the data lake associated with the Azure Synapse Analytics workspace:

CREATE EXTERNAL TABLE dbo.ExternalStageProduct
 (
     ProductID NVARCHAR(10) NOT NULL,
     ProductName NVARCHAR(10) NOT NULL,
 ...
 )
WITH
 (
    DATE_SOURCE = StagedFiles,
    LOCATION = 'folder_name/*.parquet',
    FILE_FORMAT = ParquetFormat
 );
GO

-- After staging dimension data, you can load it into dimension tables using SQL.
-- Using a CREATE TABLE AS (CTAS) statement
-- This statement creates a new table based on the results of a SELECT statement.

-- You can't use IDENTITY to generate a unique integer value for the surrogate key when using a CTAS statement, 
-- so this example uses the ROW_NUMBER function to generate an incrementing row number for each row in the results ordered by the ProductID business key in the staged data.

CREATE TABLE dbo.DimProduct
WITH
(
    DISTRIBUTION = REPLICATE,
    CLUSTERED COLUMNSTORE INDEX
)
AS
SELECT ROW_NUMBER() OVER(ORDER BY ProdID) AS ProdKey,
    ProdID as ProdAltKey,
    ProductName,
    ProductCategory,
    Color,
    Size,
    ListPrice,
    Discontinued
FROM dbo.StageProduct;

-- You can also load a combination of new and updated data into a dimension table 
-- by using a CREATE TABLE AS (CTAS) statement to create a new table that UNIONs the existing rows from the dimension table 
-- with the new and updated records from the staging table. 
-- After creating the new table, you can delete or rename the current dimension table, and rename the new table to replace it.

-- While this technique is effective in merging new and existing dimension data, 
-- lack of support for IDENTITY columns means that it's difficult to generate a surrogate key.

CREATE TABLE dbo.DimProductUpsert
WITH
(
    DISTRIBUTION = REPLICATE,
    CLUSTERED COLUMNSTORE INDEX
)
AS
-- New or updated rows
SELECT  stg.ProductID AS ProductBusinessKey,
        stg.ProductName,
        stg.ProductCategory,
        stg.Color,
        stg.Size,
        stg.ListPrice,
        stg.Discontinued
FROM    dbo.StageProduct AS stg
UNION ALL  
-- Existing rows
SELECT  dim.ProductBusinessKey,
        dim.ProductName,
        dim.ProductCategory,
        dim.Color,
        dim.Size,
        dim.ListPrice,
        dim.Discontinued
FROM    dbo.DimProduct AS dim
WHERE NOT EXISTS
(   SELECT  *
    FROM dbo.StageProduct AS stg
    WHERE stg.ProductId = dim.ProductBusinessKey
);

RENAME OBJECT dbo.DimProduct TO DimProductArchive;
RENAME OBJECT dbo.DimProductUpsert TO DimProduct;

-- Using an INSERT statement
-- When you need to load staged data into an existing dimension table, you can use an INSERT statement. 
-- This approach works if the staged data contains only records for new dimension entities (not updates to existing entities). 
-- This approach is much less complicated than the technique in the last section, which required a UNION ALL and then renaming table objects.

-- Assuming the DimCustomer dimension table is defined with an IDENTITY CustomerKey column for the surrogate key (as described in the previous unit), 
-- the key will be generated automatically and the remaining columns will be populated using the values retrieved from the staging table by the SELECT query.

INSERT INTO dbo.DimCustomer
SELECT CustomerNo AS CustAltKey,
    CustomerName,
    EmailAddress,
    Phone,
    StreetAddress,
    City,
    PostalCode,
    CountryRegion
FROM dbo.StageCustomers

-- Load time dimension tables
-- Time dimension tables store a record for each time interval based on the grain by which you want to aggregate data over time.
-- For example, a time dimension table at the date grain contains a record for each date between the earliest and latest dates referenced by the data in related fact tables.
-- Create a temporary table for the dates we need

-- As the data warehouse is populated in the future with new fact data, you periodically need to extend the range of dates related time dimension tables.
-- Scripting this in SQL may be time-consuming in a dedicated SQL pool – it may be more efficient to prepare the data 
-- in Microsoft Excel or an external script and import it using the COPY statement.

CREATE TABLE #TmpStageDate (DateVal DATE NOT NULL)

-- Populate the temp table with a range of dates
DECLARE @StartDate DATE
DECLARE @EndDate DATE
SET @StartDate = '2019-01-01'
SET @EndDate = '2023-12-31'
DECLARE @LoopDate = @StartDate
WHILE @LoopDate <= @EndDate
BEGIN
    INSERT INTO #TmpStageDate VALUES
    (
        @LoopDate
    )
    SET @LoopDate = DATEADD(dd, 1, @LoopDate)
END

-- Insert the dates and calculated attributes into the dimension table
INSERT INTO dbo.DimDate
SELECT CAST(CONVERT(VARCHAR(8), DateVal, 112) as INT), -- date key
    DateVal, --date alt key
    Day(DateVal) -- day number of month
    --,  other derived temporal fields as required
FROM #TmpStageDate
GO

--Drop temporary table
DROP TABLE #TmpStageDate