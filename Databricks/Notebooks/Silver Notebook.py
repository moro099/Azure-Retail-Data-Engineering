# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC #####Data Access using Application

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.stsaleslakehouse.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.stsaleslakehouse.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.stsaleslakehouse.dfs.core.windows.net", "client id")
spark.conf.set("fs.azure.account.oauth2.client.secret.stsaleslakehouse.dfs.core.windows.net", "client secret")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.stsaleslakehouse.dfs.core.windows.net", "client End Point")

# COMMAND ----------

# MAGIC %md
# MAGIC ######Reading Data

# COMMAND ----------

df_cust = spark.read.format("Parquet").option("header" , True).option("inferschema",True).load('abfss://bronze@stsaleslakehouse.dfs.core.windows.net/customers')

# COMMAND ----------

df_cust.display()

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

df_proSub = spark.read.format("Parquet").option("header" , True).option("inferschema",True).load('abfss://bronze@stsaleslakehouse.dfs.core.windows.net/product_Subcategory')

# COMMAND ----------

df_proCat = spark.read.format("Parquet").option("header" , True).option("inferschema",True).load('abfss://bronze@stsaleslakehouse.dfs.core.windows.net/product_category')

# COMMAND ----------

df_pro = spark.read.format("Parquet").option("header" , True).option("inferschema",True).load('abfss://bronze@stsaleslakehouse.dfs.core.windows.net/products')

# COMMAND ----------

df_salesOrderDetail = spark.read.format("Parquet").option("header" , True).option("inferschema",True).load('abfss://bronze@stsaleslakehouse.dfs.core.windows.net/salesOrderDetail')

# COMMAND ----------

df_salesOrderHeader = spark.read.format("Parquet").option("header" , True).option("inferschema",True).load('abfss://bronze@stsaleslakehouse.dfs.core.windows.net/salesOrderHeader')

# COMMAND ----------

df_sales_Terr = spark.read.format("Parquet").option("header" , True).option("inferschema",True).load('abfss://bronze@stsaleslakehouse.dfs.core.windows.net/sales_territory')

# COMMAND ----------

# MAGIC %md
# MAGIC ######Transformations

# COMMAND ----------

# MAGIC %md
# MAGIC ###### For_Customers

# COMMAND ----------

#Rename columns
df_cust = (df_cust
      .withColumnRenamed("accountNumber", "AccountNumber")
      .withColumnRenamed("rowguid", "RowGuid")
)

# COMMAND ----------

#Remove duplicates customers
df_cust = df_cust.dropDuplicates(["CustomerID"])

# COMMAND ----------

#datatype conversion
df_cust= df_cust.withColumn("CustomerID",col("CustomerID").cast(IntegerType()))
df_cust = (
    df_cust
    .withColumn("TerritoryID", col("TerritoryID").cast(IntegerType()))
)

# COMMAND ----------

# Remove records where CustomerID is NULL
df_cust = df_cust.filter(col("CustomerID").isNotNull())

# COMMAND ----------

# adding audit col to track silver records
df_cust = df_cust.withColumn("LoadTimestamp", current_timestamp())

# COMMAND ----------

# Write to Silver
df_cust.write.format("delta")\
       .mode("overwrite")\
       .option("path","abfss://silver@stsaleslakehouse.dfs.core.windows.net/customers")\
       .save()


# COMMAND ----------

# MAGIC %md
# MAGIC ######Product Category

# COMMAND ----------

df_proCat.display()

# COMMAND ----------

# Rename columns
df_proCat = (df_proCat.withColumnRenamed("name", "CategoryName")
      .withColumnRenamed("rowguid", "RowGuid")
)

# COMMAND ----------

df_proCat = (df_proCat.withColumn("ProductCategoryID", col("ProductCategoryID").cast(IntegerType()))
)

# COMMAND ----------

#remove duplicates
df_proCat = df_proCat.dropDuplicates(["ProductCategoryID"])

# COMMAND ----------

#remove invalid records
df_proCat = df_proCat.filter(col("ProductCategoryID").isNotNull())

# COMMAND ----------

#write to silver
df_proCat.write.format("delta")\
               .mode("overwrite")\
               .option("path","abfss://silver@stsaleslakehouse.dfs.core.windows.net/product_category").save()

# COMMAND ----------

# MAGIC %md
# MAGIC ######ProductSubcategory

# COMMAND ----------

df_proSub.display()

# COMMAND ----------

#Renaming columns
df_proSub = df_proSub.withColumnRenamed("name", "Product_Subcategory_Name").withColumnRenamed("rowguid", "RowGuid")

# COMMAND ----------

#remove duplicates
df_proSub = df_proSub.dropDuplicates(["ProductSubcategoryID"])

# COMMAND ----------

#datatype conversion
df_proSub = (df_proSub.withColumn("ProductSubcategoryID", col("ProductSubcategoryID").cast(IntegerType())))
             

# COMMAND ----------

# remove invalid records
df_proSub = df_proSub.filter(col("ProductSubcategoryID").isNotNull()&
                             col("ProductCategoryID").isNotNull())

# COMMAND ----------

df_proSub.display()

# COMMAND ----------

#write to silver
df_proSub.write.format("Delta").mode("overwrite").option("path","abfss://silver@stsaleslakehouse.dfs.core.windows.net/product_Subcategory").save()

# COMMAND ----------

# MAGIC %md
# MAGIC ######Products

# COMMAND ----------

df_pro.display()

# COMMAND ----------

df_pro.groupBy("Batch").count().show()

# COMMAND ----------

df1 = spark.read.parquet("abfss://bronze@stsaleslakehouse.dfs.core.windows.net/products/Product_Batch_001.parquet")
df1.groupBy("Batch").count().show()

df2 = spark.read.parquet("abfss://bronze@stsaleslakehouse.dfs.core.windows.net/products/Product_Batch_002.parquet")
df2.groupBy("Batch").count().show()

df3 = spark.read.parquet("abfss://bronze@stsaleslakehouse.dfs.core.windows.net/products/Product_Batch_003.parquet")
df3.groupBy("Batch").count().show()

# COMMAND ----------

#renmaing columns
df_pro = (df_pro
      .withColumnRenamed("Name", "ProductName")
      .withColumnRenamed("rowguid", "RowGuid")
)

# COMMAND ----------

#remove duplicates
df_pro = df_pro.dropDuplicates(["ProductId"])
 

# COMMAND ----------

# Drop Temporary Column
df_pro = df_pro.drop("Batch")

# COMMAND ----------

#remove invalid records
df_pro = df_pro.filter(col("ProductId").isNotNull() &
                       col("ProductName").isNotNull() &
                       col("ProductNumber").isNotNull())

# COMMAND ----------

# Trim String Columns
string_cols = [
    "ProductName",
    "ProductNumber",
    "Color",
    "Size",
    "SizeUnitMeasureCode",
    "WeightUnitMeasureCode",
    "ProductLine",
    "Class",
    "Style",
    "RowGuid"
]

for c in string_cols:
    df_pro = df_pro.withColumn(c, trim(col(c)))

# COMMAND ----------

# Data Type Conversion
df_pro = (df_pro
      .withColumn("ProductID", col("ProductID").cast(IntegerType()))
      .withColumn("SafetyStockLevel", col("SafetyStockLevel").cast(IntegerType()))
      .withColumn("ReorderPoint", col("ReorderPoint").cast(IntegerType()))
      .withColumn("StandardCost", col("StandardCost").cast(DecimalType(18,2)))
      .withColumn("ListPrice", col("ListPrice").cast(DecimalType(18,2)))
      .withColumn("Weight", col("Weight").cast(DoubleType()))
      .withColumn("DaysToManufacture", col("DaysToManufacture").cast(IntegerType()))
      .withColumn("ProductSubcategoryID", col("ProductSubcategoryID").cast(IntegerType()))
      .withColumn("ProductModelID", col("ProductModelID").cast(IntegerType()))
      .withColumn("SellStartDate", col("SellStartDate").cast(DateType()))
      .withColumn("SellEndDate", col("SellEndDate").cast(DateType()))
      .withColumn("DiscontinuedDate", col("DiscontinuedDate").cast(DateType()))
)

# COMMAND ----------

df_pro.printSchema()

# COMMAND ----------

#check timestamp
df_pro = df_pro.withColumn("LoadTimestamp", current_timestamp()
)

# COMMAND ----------

#write to silver
df_pro.write.format("delta").mode("overwrite").option("path","abfss://silver@stsaleslakehouse.dfs.core.windows.net/products").save()

# COMMAND ----------

# MAGIC %md
# MAGIC ######Sales Territory

# COMMAND ----------

df_sales_Terr.display()

# COMMAND ----------

# drop Duplicates
df_sales_Terr = df_sales_Terr.dropDuplicates(["TerritoryID"])

# COMMAND ----------

#renaming columns
df_sales_Terr = df_sales_Terr.withColumnRenamed("TerritoryID","TerritoryId").withColumnRenamed("rowguid","RowGuid")
                                                                                

# COMMAND ----------

#correct dataType
df_sales_Terr = (df_sales_Terr
                 .withColumn("SalesYTD", col("SalesYTD").cast(DecimalType(18,4)))
                 .withColumn("SalesLastYear", col("SalesLastYear").cast(DecimalType(18,4)))
                 .withColumn("TerritoryID", col("TerritoryID").cast(IntegerType()))
                 .withColumn("CostYTD", col("CostYTD").cast(DecimalType(18,2)))
                 .withColumn("CostLastYear", col("CostLastYear").cast(DecimalType(18,2)))
                 .withColumn("rowguid", col("rowguid").cast(StringType()))
)
                                                            

# COMMAND ----------

#Trim string columns
df_sales_Terr = (df_sales_Terr
                  .withColumn("Name", trim(col("Name")))
    .withColumn("CountryRegionCode", upper(trim(col("CountryRegionCode"))))
    .withColumn("Group", trim(col("Group")))
)
                  

# COMMAND ----------

#null handling
df_sales_Terr = df_sales_Terr.filter(col("TerritoryId").isNotNull())

# COMMAND ----------

# write to silver
df_sales_Terr.write.format("delta").mode("append").option("path" ,"abfss://silver@stsaleslakehouse.dfs.core.windows.net/salesTerritory").save()

# COMMAND ----------

# MAGIC %md
# MAGIC ######SalesOrderHeader

# COMMAND ----------

df_salesOrderHeader.display()

# COMMAND ----------

df_salesOrderHeader.filter(col("OrderDate").startswith("2022")).display()

# COMMAND ----------

files = df_salesOrderHeader.inputFiles()

for f in files:
    cnt = spark.read.parquet(f).count()
    print(f, "->", cnt)

# COMMAND ----------

df_salesOrderHeader.count()

# COMMAND ----------

#renaming columns
df_salesOrderHeader = df_salesOrderHeader.withColumnRenamed("rowguid","Rowguid")

# COMMAND ----------

   # Remove duplicate records
df_salesOrderHeader= df_salesOrderHeader.dropDuplicates(["SalesOrderID"])

    # Remove rows with null primary key
df_salesOrderHeader= df_salesOrderHeader.filter(col("SalesOrderID").isNotNull())

 # Trim string columns
df_salesOrderHeader = (df_salesOrderHeader.withColumn("SalesOrderNumber", trim(col("SalesOrderNumber")))
                                         .withColumn("PurchaseOrderNumber", trim(col("PurchaseOrderNumber")))
                                         .withColumn("AccountNumber", upper(trim(col("AccountNumber"))))
                                         .withColumn("CreditCardApprovalCode", trim(col("CreditCardApprovalCode"))))

# COMMAND ----------

 # Cast integer columns
df_salesOrderHeader= ( df_salesOrderHeader.withColumn("SalesOrderID", col("SalesOrderID").cast(IntegerType()))
     .withColumn("TerritoryID", col("TerritoryID").cast(IntegerType()))
    .withColumn("BillToAddressID", col("BillToAddressID").cast(IntegerType()))
    .withColumn("ShipToAddressID", col("ShipToAddressID").cast(IntegerType()))
    .withColumn("ShipMethodID", col("ShipMethodID").cast(IntegerType()))
    .withColumn("CreditCardID", col("CreditCardID").cast(IntegerType()))
    .withColumn("CurrencyRateID", col("CurrencyRateID").cast(IntegerType())))

    # Cast small integer columns
 df_salesOrderHeader= (df_salesOrderHeader.withColumn("RevisionNumber", col("RevisionNumber").cast(ShortType()))
    .withColumn("Status", col("Status").cast(ShortType())))

    # Boolean
 df_salesOrderHeader= (df_salesOrderHeader.withColumn("OnlineOrderFlag", col("OnlineOrderFlag").cast(BooleanType())))

    # Decimal columns
df_salesOrderHeader= (df_salesOrderHeader.withColumn("SubTotal", col("SubTotal").cast(DecimalType(18,2)))
    .withColumn("TaxAmt", col("TaxAmt").cast(DecimalType(18,2)))
    .withColumn("Freight", col("Freight").cast(DecimalType(18,2)))
    .withColumn("TotalDue", col("TotalDue").cast(DecimalType(18,2)))

    # Timestamp columns
    .withColumn("OrderDate", col("OrderDate").cast(TimestampType()))
    .withColumn("DueDate", col("DueDate").cast(TimestampType()))
    .withColumn("ShipDate", col("ShipDate").cast(TimestampType()))
    .withColumn("ModifiedDate", col("ModifiedDate").cast(TimestampType())))

     


# COMMAND ----------

 # Audit Column
df_salesOrderHeader =df_salesOrderHeader.withColumn("LoadTimestamp", current_timestamp())


# COMMAND ----------

# negative amount validation
df_salesOrderHeader = df_salesOrderHeader.filter(
    (col("SubTotal") >= 0) &
    (col("TaxAmt") >= 0) &
    (col("Freight") >= 0) &
    (col("TotalDue") >= 0)
)

# COMMAND ----------

from pyspark.sql.functions import year, col
from delta.tables import DeltaTable

df_2022_23 = df_salesOrderHeader.filter(year(col("OrderDate")) <= 2023)

df_2024 = df_salesOrderHeader.filter(year(col("OrderDate")) == 2024)

df_2025 = df_salesOrderHeader.filter(year(col("OrderDate")) == 2025)
 

# COMMAND ----------


silver_path = "abfss://silver@stsaleslakehouse.dfs.core.windows.net/salesOrderHeader"
DeltaTable.isDeltaTable(spark, silver_path)
df_2022_23.write \
    .format("delta") \
    .mode("overwrite") \
    .save(silver_path)

# COMMAND ----------

#merge 2024 salesOrderHeader
deltaTable = DeltaTable.forPath(spark, silver_path)

(
    deltaTable.alias("target")
    .merge(
        df_2024.alias("source"),
        "target.SalesOrderID = source.SalesOrderID"
    )
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)

print("2024 Merge Completed")

# COMMAND ----------

df = spark.read.format("delta").load(silver_path)

print(df.count())

# COMMAND ----------

print("2022-23:", df_2022_23.count())
print("2024:", df_2024.count())
print("2025:", df_2025.count())

# COMMAND ----------

#merge 2025 salesOrderHeader
silver_path = "abfss://silver@stsaleslakehouse.dfs.core.windows.net/salesOrderHeader"
deltaTable = DeltaTable.forPath(spark,silver_path)
(
    deltaTable.alias("target").
    merge(
        df_2025.alias("source"),
        "target.SalesOrderID = source.SalesOrderID"
    )
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
    )
print("2025 Merge Completed")


# COMMAND ----------

# MAGIC %md
# MAGIC ######SalesOrderDetail

# COMMAND ----------

df_salesOrderDetail.display()

# COMMAND ----------

#renaming columns
df_salesOrderDetail = df_salesOrderDetail.withColumnRenamed("rowguid","RowGuid")

# COMMAND ----------

#remove nulls
df_salesOrderDetail = (df_salesOrderDetail.filter(
        col("SalesOrderID").isNotNull() &
        col("SalesOrderDetailID").isNotNull()
    )

#trim stringcolumns
    .withColumn(
        "CarrierTrackingNumber",
        trim(col("CarrierTrackingNumber"))
    )
)

# COMMAND ----------

 # Data Type Casting
df_salesOrderDetail = (df_salesOrderDetail.withColumn("SalesOrderID", col("SalesOrderID").cast(IntegerType()))
                                         .withColumn("SalesOrderDetailID", col("SalesOrderDetailID").cast(IntegerType()))
                                         .withColumn("OrderQty", col("OrderQty").cast(IntegerType()))
                                         .withColumn("ProductID", col("ProductID").cast(IntegerType()))
                                         .withColumn("SpecialOfferID", col("SpecialOfferID").cast(IntegerType()))
                                         .withColumn("UnitPrice", col("UnitPrice").cast(DecimalType(18,2)))
                                         .withColumn("UnitPriceDiscount", col("UnitPriceDiscount").cast(DecimalType(18,2)))
                                         .withColumn("LineTotal", col("LineTotal").cast(DecimalType(18,2)))
                                         .withColumn("ModifiedDate", col("ModifiedDate").cast(TimestampType()))
)

# COMMAND ----------

  # Data Quality Checks
df_salesOrderDetail= (df_salesOrderDetail.filter(col("OrderQty") > 0)
                                        .filter(col("UnitPrice") >= 0)
                                        .filter(col("UnitPriceDiscount") >= 0)
                                        .filter(col("LineTotal") >= 0))

# COMMAND ----------

# write to silver
df_salesOrderDetail.write.format("delta").mode("overwrite").save("abfss://silver@stsaleslakehouse.dfs.core.windows.net/salesOrderDetail")

# COMMAND ----------

df = spark.read.format("parquet").load("abfss://bronze@stsaleslakehouse.dfs.core.windows.net/salesOrderDetail")

print(df.count())

# COMMAND ----------

df = spark.read.format("parquet").load("abfss://bronze@stsaleslakehouse.dfs.core.windows.net/salesOrderDetail/increm_")
print(df.count())  
df.display() 

# COMMAND ----------

#transformations for increm_folder of salesOrderDetail
df = (df.withColumnRenamed("rowguid","RowGuid")
       .withColumn("SalesOrderID", col("SalesOrderID").cast(IntegerType()))
       .withColumn("SalesOrderDetailID", col("SalesOrderDetailID").cast(IntegerType()))
       .withColumn("OrderQty", col("OrderQty").cast(IntegerType()))
       .withColumn("ProductID", col("ProductID").cast(IntegerType()))
       .withColumn("SpecialOfferID", col("SpecialOfferID").cast(IntegerType()))
       .withColumn("UnitPrice" , col("UnitPrice").cast(DecimalType(18,2)))
       .withColumn("UnitPriceDiscount", col("UnitPriceDiscount").cast(DecimalType(18,2)))
       .withColumn("ModifiedDate", col("ModifiedDate").cast(TimestampType()))
       .filter(col("salesOrderID").isNotNull() & col("SalesOrderDetailID").isNotNull())
       .withColumn("CarrierTrackingNumber", trim(col("CarrierTrackingNumber")))
       )

# COMMAND ----------

#write this incremfolder data to silver
from delta.tables import DeltaTable
silver_path = "abfss://silver@stsaleslakehouse.dfs.core.windows.net/salesOrderDetail"
deltaTable = DeltaTable.forPath(spark, silver_path)
(
    deltaTable.alias("target").
    merge(
        df.alias("source"),
        "target.SalesOrderDetailID = source.SalesOrderDetailID"
    )
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
    )


# COMMAND ----------

print(df.count())

# COMMAND ----------

spark.read.format("delta").load(silver_path) \
.filter("SalesOrderDetailID >= 121338") \
.show()

# COMMAND ----------

spark.read.format("delta").load(silver_path) \
.filter("SalesOrderDetailID IN (13,14,15,16,17)") \
.show()