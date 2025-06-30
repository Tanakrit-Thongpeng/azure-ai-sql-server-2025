-- =============================================
        -- Author:		AI Generated SQL Query
        -- Create date: 30/06/2025
        -- Description:	This query joins the `Customer`, `SalesOrderHeader`, and `SalesOrderDetail` tables and calculates the total amount spent by each customer by summing up the `LineTotal` from the `SalesOrderDetail` table. The results are then grouped by customer and ordered in descending order of the total amount spent, with only the top 10 customers displayed.
        -- =============================================
        
SELECT TOP 10
    c.CustomerID,
    c.FirstName,
    c.LastName,
    SUM(sod.LineTotal) AS TotalSpent
FROM dbo.Customer c
JOIN dbo.SalesOrderHeader soh ON c.CustomerID = soh.CustomerID
JOIN dbo.SalesOrderDetail sod ON soh.SalesOrderID = sod.SalesOrderID
GROUP BY c.CustomerID, c.FirstName, c.LastName
ORDER BY TotalSpent DESC;