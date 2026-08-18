# Databricks notebook source
# Parameters received from Azure Data Factory

dbutils.widgets.text("file_name", "")
dbutils.widgets.text("entity_name", "")
dbutils.widgets.text("primary_key", "")
dbutils.widgets.text("bronze_path", "")
dbutils.widgets.text("silver_path", "")
dbutils.widgets.text("load_date", "")

print("ADF parameters created.")

# COMMAND ----------

# Read parameter values

file_name = dbutils.widgets.get("file_name")
entity_name = dbutils.widgets.get("entity_name")
primary_key = dbutils.widgets.get("primary_key")
bronze_path = dbutils.widgets.get("bronze_path")
silver_path = dbutils.widgets.get("silver_path")
load_date = dbutils.widgets.get("load_date")

print("File:", file_name)
print("Entity:", entity_name)
print("Primary Key:", primary_key)
print("Bronze Path:", bronze_path)
print("Silver Path:", silver_path)
print("Load Date:", load_date)

# COMMAND ----------

storage_account = "stadventureworksthowfee"

storage_key = dbutils.secrets.get(
    scope="kv-adworks",
    key="storage-key"
)

spark.conf.set(
    f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
    storage_key
)

print("ADLS authentication configured.")

# COMMAND ----------

# Read CSV using the path received from the parameter

df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(bronze_path)
)

print("File loaded:", file_name)
print("Row count:", df.count())

display(df.limit(10))

# COMMAND ----------

from pyspark.sql.functions import col, trim, current_timestamp, lit
from pyspark.sql.types import StringType

# Remove exact duplicate rows
clean_df = df.dropDuplicates()

# Remove rows where the primary key is NULL
clean_df = clean_df.filter(col(primary_key).isNotNull())

# Trim whitespace from all text columns
for field in clean_df.schema.fields:
    if isinstance(field.dataType, StringType):
        clean_df = clean_df.withColumn(
            field.name,
            trim(col(field.name))
        )

# Add lineage columns
clean_df = (
    clean_df
    .withColumn("ingestion_timestamp", current_timestamp())
    .withColumn("load_date", lit(load_date).cast("date"))
)

print("Entity:", entity_name)
print("Rows after cleaning:", clean_df.count())

display(clean_df.limit(10))

# COMMAND ----------

from pyspark.sql.functions import col, to_date

# Apply data types based on the entity

if entity_name == "customer":
    clean_df = (
        clean_df
        .withColumn("CustomerID", col("CustomerID").cast("int"))
        .withColumn("TerritoryID", col("TerritoryID").cast("int"))
    )

elif entity_name == "product":
    clean_df = (
        clean_df
        .withColumn("ProductID", col("ProductID").cast("int"))
        .withColumn("StandardCost", col("StandardCost").cast("double"))
        .withColumn("ListPrice", col("ListPrice").cast("double"))
        .withColumn("SafetyStockLevel", col("SafetyStockLevel").cast("int"))
    )

elif entity_name == "sales_order_detail":
    clean_df = (
        clean_df
        .withColumn("SalesOrderID", col("SalesOrderID").cast("int"))
        .withColumn("SalesOrderDetailID", col("SalesOrderDetailID").cast("int"))
        .withColumn("ProductID", col("ProductID").cast("int"))
        .withColumn("OrderQty", col("OrderQty").cast("int"))
        .withColumn("UnitPrice", col("UnitPrice").cast("double"))
        .withColumn("UnitPriceDiscount", col("UnitPriceDiscount").cast("double"))
        .withColumn("LineTotal", col("LineTotal").cast("double"))
    )

elif entity_name == "sales_order_header":
    clean_df = (
        clean_df
        .withColumn("SalesOrderID", col("SalesOrderID").cast("int"))
        .withColumn("OrderDate", to_date(col("OrderDate")))
        .withColumn("CustomerID", col("CustomerID").cast("int"))
        .withColumn("TerritoryID", col("TerritoryID").cast("int"))
        .withColumn("SubTotal", col("SubTotal").cast("double"))
        .withColumn("TaxAmt", col("TaxAmt").cast("double"))
        .withColumn("Freight", col("Freight").cast("double"))
        .withColumn("TotalDue", col("TotalDue").cast("double"))
        .withColumn("Status", col("Status").cast("int"))
    )

print("Data types applied for:", entity_name)
clean_df.printSchema()

# COMMAND ----------

# Final validation before writing to Silver

print("Entity:", entity_name)
print("Primary key:", primary_key)
print("Rows ready for Silver:", clean_df.count())

null_keys = clean_df.filter(col(primary_key).isNull()).count()

print("NULL primary keys:", null_keys)

# COMMAND ----------

from delta.tables import DeltaTable

# Check whether a Delta table already exists in Silver
if DeltaTable.isDeltaTable(spark, silver_path):

    print("Existing Silver Delta table found. Running MERGE...")

    silver_table = DeltaTable.forPath(spark, silver_path)

    (
        silver_table.alias("target")
        .merge(
            clean_df.alias("source"),
            f"target.{primary_key} = source.{primary_key}"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    print("MERGE completed.")

else:

    print("No existing Silver Delta table found. Creating it...")

    (
        clean_df.write
        .format("delta")
        .mode("overwrite")
        .save(silver_path)
    )

    print("Silver Delta table created.")