
-- =============================================
-- Author:		AI Generated SQL Query
-- Create date: 27/06/2025
-- Description:	This query selects the product with the highest total quantity sold by joining the `Product` and `SalesOrderDetail` tables, grouping by product, summing the order quantities, and then ordering the results by total quantity sold in descending order. The `TOP 1` keyword limits the results to only the product that sold the most.
-- =============================================

SELECT TOP 1 
    P.Name AS ProductName, 
    SUM(OD.OrderQty) AS TotalQuantitySold
FROM 
    SalesLT.Product AS P
JOIN 
    SalesLT.SalesOrderDetail AS OD ON P.ProductID = OD.ProductID
GROUP BY 
    P.ProductID, P.Name
ORDER BY 
    SUM(OD.OrderQty) DESC;