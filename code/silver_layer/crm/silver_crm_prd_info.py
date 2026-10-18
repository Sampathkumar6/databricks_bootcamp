# Databricks notebook source
# MAGIC %md
# MAGIC ##Initialization

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.types import StringType, DateType
from pyspark.sql.functions import col, trim
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ##Read data from Bronze

# COMMAND ----------

df = spark.table("proj01_bootcamp.bronze.crm_prd_info")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Silver transformations

# COMMAND ----------

# MAGIC %md
# MAGIC ####trimming

# COMMAND ----------

#trim the names

for field in df.schema.fields:
  if isinstance(field.dataType, StringType):
      df = df.withColumn(field.name, trim(col(field.name)))

# COMMAND ----------

# MAGIC %md
# MAGIC ###Parsing
# MAGIC ####creating new column cat_id, and store substring of prd_key from 1 to 5 position, also replacing - with _
# MAGIC ####the original prd_key retaining only the non catid info, the leftover part and overwiting to same column

# COMMAND ----------

df = df.withColumn("cat_id", F.regexp_replace(F.substring(col("prd_key"), 1, 5), "-", "_"))
df = df.withColumn("prd_key", F.substring(col("prd_key"), 7, F.length(col("prd_key"))))


# COMMAND ----------

# MAGIC %md
# MAGIC ###prd_cost fill na

# COMMAND ----------

df = df.fillna(0,"prd_cost")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Product Line Normalization

# COMMAND ----------

#normalization for prd_line

df = (
    df
    .withColumn(
        "prd_line",
        F.when(F.upper(col("prd_line")) == "M", "Mountain")
         .when(F.upper(col("prd_line")) == "R", "Road")
         .when(F.upper(col("prd_line")) == "S", "Other Sales")
         .when(F.upper(col("prd_line")) == "T", "Touring")
         .otherwise("n/a")
    )
)
         

# COMMAND ----------

# MAGIC %md
# MAGIC ###Date Casting

# COMMAND ----------

df = df.withColumn("prd_start_dt", col("prd_start_dt").cast(DateType()))

# COMMAND ----------

# MAGIC %md
# MAGIC ###Remove Records with Missing end_date

# COMMAND ----------

# id not null only records

df = df.filter(col("prd_end_dt").isNotNull())

# COMMAND ----------

# MAGIC %md
# MAGIC ###Renaming Columns

# COMMAND ----------

RENAME_MAP = {
    "prd_id": "product_id",
    "cat_id": "category_id",
    "prd_key": "product_number",
    "prd_nm": "product_name",
    "prd_cost": "product_cost",
    "prd_line": "product_line",
    "prd_start_dt": "start_date",
    "prd_end_dt": "end_date"
}
for old_name, new_name in RENAME_MAP.items():
    df = df.withColumnRenamed(old_name, new_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Sense check final dataframe before writing

# COMMAND ----------

df.limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Write into silver layer

# COMMAND ----------

(
    df.write
    .mode("overwrite")
    .format("delta")
    .saveAsTable("proj01_bootcamp.silver.crm_products")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Sense check final table

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from proj01_bootcamp.silver.crm_products limit 50;