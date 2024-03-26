-- Using SQL code
-- In a notebook, you can also use the %%sql magic to run SQL code 
%%sql

SELECT Category, COUNT(ProductID) AS ProductCount
FROM products
GROUP BY Category
ORDER BY Category