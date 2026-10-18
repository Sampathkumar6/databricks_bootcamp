# Databricks notebook source
# MAGIC %md
# MAGIC ##Initialization

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.types import StringType
from pyspark.sql.functions import trim,col

# COMMAND ----------

# MAGIC %md
# MAGIC ##Read data from Bronze

# COMMAND ----------

df = spark.table("proj01_bootcamp.bronze.crm_cust_info")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Silver transformations

# COMMAND ----------

# MAGIC %md
# MAGIC ####trimming firstname and lastname

# COMMAND ----------

#trim the names

for field in df.schema.fields:
  if isinstance(field.dataType, StringType):
      df = df.withColumn(field.name, trim(col(field.name)))

# COMMAND ----------

# MAGIC %md
# MAGIC ####normalize gender and marital status

# COMMAND ----------

#normalization for gndr,marital_status

df = (
    df
    .withColumn(
        "cst_marital_status",
        F.when(F.upper(F.col("cst_marital_status")) == "S","Single")
        .when(F.upper(F.col("cst_marital_status")) == "M","Married")
        .otherwise("n/a")
    )
    .withColumn(
        "cst_gndr",
        F.when(F.upper(F.col("cst_gndr")) == "M","Male")
        .when(F.upper(F.col("cst_gndr")) == "F","Female")
        .otherwise("n/a")
    )
)
         

# COMMAND ----------

# MAGIC %md
# MAGIC ###Remove Records with Missing Customer ID

# COMMAND ----------

# id not null only records

df = df.filter(col("cst_id").isNotNull())

# COMMAND ----------

# MAGIC %md
# MAGIC ####Renaming column names

# COMMAND ----------

#headers normalize

RENAME_MAP = {
    "cst_id": "customer_id",
    "cst_key": "customer_key",
    "cst_firstname": "customer_firstname",
    "cst_lastname": "customer_lastname",
    "cst_marital_status": "customer_marital_status",
    "cst_gndr": "customer_gender",
    "cst_create_date": "customer_create_date",
}
for old_name, new_name in RENAME_MAP.items():
    df = df.withColumnRenamed(old_name, new_name)


# COMMAND ----------

# MAGIC %md
# MAGIC ###Write into silver layer

# COMMAND ----------

(
    df.write
    .mode("overwrite")
    .format("delta")
    .saveAsTable("proj01_bootcamp.silver.crm_customers")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Sense check final table

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from proj01_bootcamp.silver.crm_customers limit 50;