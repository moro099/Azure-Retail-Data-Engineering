# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.stsaleslakehouse.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.stsaleslakehouse.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.stsaleslakehouse.dfs.core.windows.net", "client id")
spark.conf.set("fs.azure.account.oauth2.client.secret.stsaleslakehouse.dfs.core.windows.net", "client secret")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.stsaleslakehouse.dfs.core.windows.net", "clint endpoint")

# COMMAND ----------

#Read silver tables
# Product
df_product = spark.read.format("delta").load(
    "abfss://silver@stsaleslakehouse.dfs.core.windows.net/products"
)

# Product Category
df_category = spark.read.format("delta").load(
    "abfss://silver@stsaleslakehouse.dfs.core.windows.net/product_category"
)

# Product Subcategory
df_subcategory = spark.read.format("delta").load(
    "abfss://silver@stsaleslakehouse.dfs.core.windows.net/product_Subcategory"
)

# Customer
df_customer = spark.read.format("delta").load(
    "abfss://silver@stsaleslakehouse.dfs.core.windows.net/customers"
)

# Sales Order Header
df_header = spark.read.format("delta").load(
    "abfss://silver@stsaleslakehouse.dfs.core.windows.net/salesOrderHeader"
)

# Sales Order Detail
df_detail = spark.read.format("delta").load(
    "abfss://silver@stsaleslakehouse.dfs.core.windows.net/salesOrderDetail"
)

# Sales Territory
df_territory = spark.read.format("delta").load(
    "abfss://silver@stsaleslakehouse.dfs.core.windows.net/salesTerritory"
)

# COMMAND ----------

df_product.display()

# COMMAND ----------

df_subcategory.display()

# COMMAND ----------

df_dim_product = (
    df_product.alias("p")
    .join(
        df_subcategory.alias("ps"),
        col("p.ProductSubcategoryID") == col("ps.ProductSubcategoryID"),
        "left"
    )
)


# COMMAND ----------

display(
    df_dim_product.select(
        col("p.ProductId"),
        col("p.ProductName"),
        col("ps.Product_Subcategory_Name")
    )
)

# COMMAND ----------

df_dim_product = (
    df_dim_product.alias("dp")
    .join(
        df_category.alias("pc"),
        col("dp.ProductCategoryID") == col("pc.ProductCategoryID"),
        "left"
    )
)

# COMMAND ----------

display(
    df_dim_product.select(
        col("dp.ProductId"),
        col("dp.ProductName"),
        col("dp.Product_Subcategory_Name"),
        col("pc.CategoryName")
    )
)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

df_dim_product = df_dim_product.select(
    col("dp.ProductId").alias("ProductID"),
    col("dp.ProductName"),
    col("dp.ProductNumber"),
    col("pc.CategoryName"),
    col("dp.Product_Subcategory_Name").alias("SubCategoryName"),
    col("dp.Color"),
    col("dp.Size"),
    col("dp.ProductLine"),
    col("dp.Class"),
    col("dp.Style"),
    col("dp.StandardCost"),
    col("dp.ListPrice"),
    col("dp.SellStartDate")
)
df_dim_product = df_dim_product.withColumn(
    "LoadTimestamp",
    current_timestamp()
)

# COMMAND ----------

display(df_dim_product)

# COMMAND ----------

df_dim_product.printSchema()

# COMMAND ----------

(df_dim_product.write
    .format("delta")
    .mode("overwrite")
    .save("abfss://gold@stsaleslakehouse.dfs.core.windows.net/dimproduct"))

# COMMAND ----------

# MAGIC %md
# MAGIC ######dim_customer

# COMMAND ----------

from pyspark.sql.functions import col

df_dim_customer = df_customer.select(
    col("CustomerID"),
    col("PersonID"),
    col("StoreID"),
    col("TerritoryID"),
    col("AccountNumber")
)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

df_dim_customer = df_dim_customer.withColumn(
    "LoadTimestamp",
    current_timestamp()
)

# COMMAND ----------

display(df_dim_customer)

df_dim_customer.printSchema()

# COMMAND ----------

#write to silver
(df_dim_customer.write
    .format("delta")
    .mode("overwrite")
    .save("abfss://gold@stsaleslakehouse.dfs.core.windows.net/dimcustomer"))

# COMMAND ----------

# MAGIC %md
# MAGIC ######dim_territory
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp

df_dim_territory = df_territory.select(
    col("TerritoryID"),
    col("Name").alias("TerritoryName"),
    col("CountryRegionCode"),
    col("Group"),
    col("SalesYTD"),
    col("SalesLastYear"),
    col("CostYTD"),
    col("CostLastYear")
)

# COMMAND ----------

#adding audit col
df_dim_territory = df_dim_territory.withColumn(
    "LoadTimestamp",
    current_timestamp()
)

# COMMAND ----------

display(df_dim_territory)
df_dim_territory.printSchema()

# COMMAND ----------

#write to gold
(df_dim_territory.write
    .format("delta")
    .mode("overwrite")
    .save("abfss://gold@stsaleslakehouse.dfs.core.windows.net/dimterritory"))

# COMMAND ----------

# MAGIC %md
# MAGIC ######fact_sales

# COMMAND ----------

from pyspark.sql.functions import col

df_fact_sales = (
    df_detail.alias("d")
    .join(
        df_header.alias("h"),
        col("d.SalesOrderID") == col("h.SalesOrderID"),
        "inner"
    )
)

# COMMAND ----------

display(df_fact_sales)

print(df_fact_sales.count())

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp

df_fact_sales = df_fact_sales.select(
    col("d.SalesOrderDetailID"),
    col("d.SalesOrderID"),
    col("h.CustomerID"),
    col("h.TerritoryID"),
    col("d.ProductID"),
    col("h.OrderDate"),
    col("h.DueDate"),
    col("h.ShipDate"),
    col("d.OrderQty"),
    col("d.UnitPrice"),
    col("d.UnitPriceDiscount"),
    col("d.LineTotal"),
    col("h.SubTotal"),
    col("h.TaxAmt"),
    col("h.Freight"),
    col("h.TotalDue")
)

# COMMAND ----------

df_fact_sales = df_fact_sales.withColumn(
    "LoadTimestamp",
    current_timestamp()
)

# COMMAND ----------

display(df_fact_sales)

df_fact_sales.printSchema()

# COMMAND ----------

#write fact_sales to gold
(df_fact_sales.write
    .format("delta")
    .mode("overwrite")
    .save("abfss://gold@stsaleslakehouse.dfs.core.windows.net/factsales"))