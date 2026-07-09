------------------------
--create view fact Sales
------------------------

CREATE VIEW gold.vw_FactSales
AS
SELECT *
FROM OPENROWSET(
    BULK 'https://stsaleslakehouse.dfs.core.windows.net/gold/factsales',
    FORMAT = 'DELTA'
) AS rows;

--updated views factsales--

ALTER VIEW gold.vw_FactSales
AS
SELECT *
FROM OPENROWSET(
    BULK 'factsales',
    DATA_SOURCE = 'source_gold',
    FORMAT = 'DELTA'
) AS rows;

--------------------------
--create view dim product
--------------------------

CREATE VIEW gold.vw_Products
AS
SELECT *
FROM OPENROWSET(
    BULK 'https://stsaleslakehouse.dfs.core.windows.net/gold/dimproduct',
    FORMAT = 'DELTA'
) AS rows;

--updated views products--

ALTER VIEW gold.vw_Products
AS
SELECT *
FROM OPENROWSET(
    BULK 'dimproduct',
    DATA_SOURCE = 'source_gold',
    FORMAT = 'DELTA'
) AS rows;

--------------------------
--create view dim customer
--------------------------

CREATE VIEW gold.vw_Customers
AS
SELECT *
FROM OPENROWSET(
    BULK 'https://stsaleslakehouse.dfs.core.windows.net/gold/dimcustomer',
    FORMAT = 'DELTA'
) AS rows;

--updated views customers--

ALTER VIEW gold.vw_Customers
AS
SELECT *
FROM OPENROWSET(
    BULK 'dimcustomer',
    DATA_SOURCE = 'source_gold',
    FORMAT = 'DELTA'
) AS rows;

----------------------------
--create view dim territory
----------------------------

CREATE VIEW gold.vw_Territory
AS
SELECT *
FROM OPENROWSET(
    BULK 'https://stsaleslakehouse.dfs.core.windows.net/gold/dimterritory',
    FORMAT = 'DELTA'
) AS rows;





--updateTerritory--

ALTER VIEW gold.vw_Territory
AS
SELECT *
FROM OPENROWSET(
    BULK 'dimterritory',
    DATA_SOURCE = 'source_gold',
    FORMAT = 'DELTA'
) AS rows;