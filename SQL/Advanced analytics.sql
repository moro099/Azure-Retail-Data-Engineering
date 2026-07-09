--Running total Sales--

SELECT
    OrderDate,
    SUM(TotalDue) AS DailySales,
    SUM(SUM(TotalDue)) OVER(
        ORDER BY OrderDate
    ) AS RunningTotal
FROM gold.vw_FactSales
GROUP BY OrderDate
ORDER BY OrderDate;

--customer Ranking--

SELECT
    CustomerID,
    SUM(TotalDue) AS Revenue,
    RANK() OVER(
        ORDER BY SUM(TotalDue) DESC
    ) AS CustomerRank
FROM gold.vw_FactSales
GROUP BY CustomerID;

-- Territory ranking--

SELECT
    t.TerritoryName,
    SUM(f.TotalDue) AS Revenue,
    DENSE_RANK() OVER(
        ORDER BY SUM(f.TotalDue) DESC
    ) AS TerritoryRank
FROM gold.vw_FactSales f
JOIN gold.vw_Territory t
ON f.TerritoryID=t.TerritoryID
GROUP BY t.TerritoryName;

-- top product in each category--

WITH ProductSales AS
(
SELECT
    p.CategoryName,
    p.ProductName,
    SUM(f.LineTotal) AS Revenue,
    ROW_NUMBER() OVER(
        PARTITION BY p.CategoryName
        ORDER BY SUM(f.LineTotal) DESC
    ) AS rn
FROM gold.vw_FactSales f
JOIN gold.vw_Products p
ON f.ProductID=p.ProductID
GROUP BY
    p.CategoryName,
    p.ProductName
)

SELECT *
FROM ProductSales
WHERE rn=1;

-- YOY sales growth--

WITH YearlySales AS
(
SELECT
    YEAR(OrderDate) AS SalesYear,
    SUM(TotalDue) AS TotalSales
FROM gold.vw_FactSales
GROUP BY YEAR(OrderDate)
)

SELECT
    SalesYear,
    TotalSales,
    LAG(TotalSales) OVER(
        ORDER BY SalesYear
    ) AS PreviousYearSales,
    ROUND(
        (TotalSales-LAG(TotalSales) OVER(ORDER BY SalesYear))
        *100.0/
        LAG(TotalSales) OVER(ORDER BY SalesYear),
        2
    ) AS YoYGrowth
FROM YearlySales;