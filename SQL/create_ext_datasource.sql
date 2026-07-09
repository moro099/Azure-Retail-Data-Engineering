
CREATE DATABASE SCOPED CREDENTIAL cred_ketan
WITH IDENTITY = 'Managed Identity'
    
CREATE EXTERNAL DATA SOURCE source_gold
WITH
(
    LOCATION = 'https://stsaleslakehouse.dfs.core.windows.net/gold',
    CREDENTIAL = cred_ketan
)

CREATE EXTERNAL FILE FORMAT format_delta
WITH
(
    FORMAT_TYPE = DELTA
)


 