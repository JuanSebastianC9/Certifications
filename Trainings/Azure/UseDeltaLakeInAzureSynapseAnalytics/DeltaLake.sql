-- Use Delta Lake in a SQL pool
-- The serverless SQL pool in Azure Synapse Analytics includes support for reading delta format files; 
-- enabling you to use the SQL pool to query Delta Lake tables

SELECT *
FROM
    OPENROWSET(
        BULK 'https://mystore.dfs.core.windows.net/files/delta/mytable/',
        FORMAT = 'DELTA'
    ) AS deltadata

-- You could also create a database and add a data source that encapsulates the location of your Delta Lake data files,
CREATE DATABASE MyDB
      COLLATE Latin1_General_100_BIN2_UTF8;
GO;

USE MyDB;
GO

CREATE EXTERNAL DATA SOURCE DeltaLakeStore
WITH
(
    LOCATION = 'https://mystore.dfs.core.windows.net/files/delta/'
);
GO

SELECT TOP 10 *
FROM OPENROWSET(
        BULK 'mytable',
        DATA_SOURCE = 'DeltaLakeStore',
        FORMAT = 'DELTA'
    ) as deltadata;

-- When working with Delta Lake data, which is stored in Parquet format, it's generally best to create a database with a UTF-8 based collation in order to ensure string compatibility. 

-- Querying catalog tables
-- The serverless SQL pool in Azure Synapse Analytics has shared access to databases in the Spark metastore, 
-- so you can query catalog tables that were created using Spark SQL

-- By default, Spark catalog tables are created in a database named "default"
-- If you created another database using Spark SQL, you can use it here
USE default;

SELECT * FROM MyDeltaTable;