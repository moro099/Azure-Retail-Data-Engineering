--total sales by year --

SELECT
    YEAR(OrderDate) AS SalesYear,
    SUM(TotalDue) AS TotalSales
FROM gold.vw_FactSales
GROUP BY YEAR(OrderDate)
ORDER BY SalesYear;

--total sales by month--

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

--top 10 products by revenue--

SELECT TOP 10
    p.ProductName,
    SUM(f.LineTotal) AS Revenue
FROM gold.vw_FactSales f
JOIN gold.vw_Products p
    ON f.ProductID = p.ProductID
GROUP BY
    p.ProductName
ORDER BY Revenue DESC;

---sales by category--

SELECT
    p.CategoryName,
    SUM(f.LineTotal) AS TotalSales
FROM gold.vw_FactSales f
JOIN gold.vw_Products p
    ON f.ProductID = p.ProductID
GROUP BY
    p.CategoryName
ORDER BY TotalSales DESC;

---sales by territory--

SELECT
    t.TerritoryName,
    SUM(f.TotalDue) AS TotalSales
FROM gold.vw_FactSales f
JOIN gold.vw_Territory t
    ON f.TerritoryID = t.TerritoryID
GROUP BY
    t.TerritoryName
ORDER BY TotalSales DESC;

