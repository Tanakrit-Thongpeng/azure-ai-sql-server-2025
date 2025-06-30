-- =============================================
-- Author:		AI Generated SQL Query
-- Create date: 27/06/2025
-- Description:	This query joins the Customer, SalesOrderHeader, and SalesOrderDetail tables to calculate the total amount spent by each customer and then lists the top 10 customers based on their total spending amount.
-- =============================================

SELECT TOP 10 c.CustomerID, c.FirstName, c.LastName, SUM(od.LineTotal) AS TotalSpent
FROM SalesLT.Customer c
JOIN SalesLT.SalesOrderHeader oh ON c.CustomerID = oh.CustomerID
JOIN SalesLT.SalesOrderDetail od ON oh.SalesOrderID = od.SalesOrderID
GROUP BY c.CustomerID, c.FirstName, c.LastName
ORDER BY TotalSpent DESC;