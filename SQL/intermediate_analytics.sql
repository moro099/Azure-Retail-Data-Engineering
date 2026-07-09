--top csutomers by revenue--

SELECT TOP 10
    c.CustomerID,
    c.AccountNumber,
    SUM(f.TotalDue) AS Revenue
FROM gold.vw_FactSales f
JOIN gold.vw_Customers c
    ON f.CustomerID = c.CustomerID
GROUP BY
    c.CustomerID,
    c.AccountNumber
ORDER BY Revenue DESC;

--average order value--

SELECT
    AVG(TotalDue) AS AverageOrderValue
FROM gold.vw_FactSales;

--orders per customers--

SELECT
    CustomerID,
    COUNT(DISTINCT SalesOrderID) AS TotalOrders
FROM gold.vw_FactSales
GROUP BY CustomerID
ORDER BY TotalOrders DESC;


--revenue contribution by category--

 SELECT
    p.CategoryName,
    SUM(f.LineTotal) AS Revenue,
    ROUND(
        SUM(f.LineTotal) * 100.0 /
        SUM(SUM(f.LineTotal)) OVER (),
        2
    ) AS RevenuePercentage
FROM gold.vw_FactSales f
JOIN gold.vw_Products p
    ON f.ProductID = p.ProductID
GROUP BY p.CategoryName
ORDER BY Revenue DESC;

--  monthly sales growth--

SELECT
    YEAR(OrderDate) AS SalesYear,
    MONTH(OrderDate) AS SalesMonth,
    SUM(TotalDue) AS TotalSales
FROM gold.vw_FactSales
GROUP BY
    YEAR(OrderDate),
    MONTH(OrderDate)
ORDER BY
    SalesYear,
    SalesMonth;