# Databricks notebook source
# MAGIC %md
# MAGIC #Reading csv from volume to bronzelayer

# COMMAND ----------

# MAGIC %md
# MAGIC #Write csv to Bronze Layer

# COMMAND ----------

# MAGIC %md
# MAGIC ###crm_cust_info.csv

# COMMAND ----------

df = (
    spark.read.option("header","true")
    .option("inferSchema","true")
    .csv(["/Volumes/workspace/bronze/souce_data/source_crm/cust_info.csv"]
        )
    )
df.write.mode("overwrite").saveAsTable("bronze.crm_cust_info")

# COMMAND ----------

# MAGIC %md
# MAGIC ###crm_prod_info.csv

# COMMAND ----------

df = (
    spark.read.option("header","true")
    .option("inferSchema","true")
    .csv(["/Volumes/workspace/bronze/souce_data/source_crm/prd_info.csv"])
    )
df.write.mode("overwrite").saveAsTable("bronze.crm_prd_info")

# COMMAND ----------

# MAGIC %md
# MAGIC ###crm_sales_details.csv

# COMMAND ----------

df = (
    spark.read.option("header","true")
    .option("inferSchema","true")
    .csv(["/Volumes/workspace/bronze/souce_data/source_crm/sales_details.csv"])
    )
df.write.mode("overwrite").saveAsTable("bronze.crm_sales_details")

# COMMAND ----------

# MAGIC %md
# MAGIC ###erp_CUST_AZ12.csv

# COMMAND ----------

df = (
    spark.read.option("header","true")
    .option("inferSchema","true")
    .csv(["/Volumes/workspace/bronze/souce_data/source_erp/CUST_AZ12.csv"])
    )
df.write.mode("overwrite").saveAsTable("bronze.erp_CUST_AZ12")

# COMMAND ----------

# MAGIC %md
# MAGIC ###erp_LOC_A101.csv

# COMMAND ----------

df = (
    spark.read.option("header","true")
    .option("inferSchema","true")
    .csv(["/Volumes/workspace/bronze/souce_data/source_erp/LOC_A101.csv"])
    )
df.write.mode("overwrite").saveAsTable("bronze.erp_LOC_A101")

# COMMAND ----------

# MAGIC %md
# MAGIC ###erp_PX_CAT_G1V2.csv

# COMMAND ----------

df = (
    spark.read.option("header","true")
    .option("inferSchema","true")
    .csv(["/Volumes/workspace/bronze/souce_data/source_erp/PX_CAT_G1V2.csv"])
    )
df.write.mode("overwrite").saveAsTable("bronze.erp_PX_CAT_G1V2")