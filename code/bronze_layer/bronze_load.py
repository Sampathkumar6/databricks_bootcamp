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
    .csv(["/Volumes/proj01_bootcamp/bronze/source_data/cust_info.csv"]
        )
    )
df.write.mode("overwrite").saveAsTable("proj01_bootcamp.bronze.crm_cust_info")

# COMMAND ----------

# MAGIC %md
# MAGIC ###crm_prod_info.csv

# COMMAND ----------

df = (
    spark.read.option("header","true")
    .option("inferSchema","true")
    .csv(["/Volumes/proj01_bootcamp/bronze/source_data/prd_info.csv"])
    )
df.write.mode("overwrite").saveAsTable("proj01_bootcamp.bronze.crm_prd_info")

# COMMAND ----------

# MAGIC %md
# MAGIC ###crm_sales_details.csv

# COMMAND ----------

df = (
    spark.read.option("header","true")
    .option("inferSchema","true")
    .csv(["/Volumes/proj01_bootcamp/bronze/source_data/sales_details.csv"])
    )
df.write.mode("overwrite").saveAsTable("proj01_bootcamp.bronze.crm_sales_details")

# COMMAND ----------

# MAGIC %md
# MAGIC ###erp_CUST_AZ12.csv

# COMMAND ----------

df = (
    spark.read.option("header","true")
    .option("inferSchema","true")
    .csv(["/Volumes/proj01_bootcamp/bronze/source_data/CUST_AZ12.csv"])
    )
df.write.mode("overwrite").saveAsTable("proj01_bootcamp.bronze.erp_cust_az12")

# COMMAND ----------

# MAGIC %md
# MAGIC ###erp_LOC_A101.csv

# COMMAND ----------

df = (
    spark.read.option("header","true")
    .option("inferSchema","true")
    .csv(["/Volumes/proj01_bootcamp/bronze/source_data/LOC_A101.csv"])
    )
df.write.mode("overwrite").saveAsTable("proj01_bootcamp.bronze.erp_loc_a101")

# COMMAND ----------

# MAGIC %md
# MAGIC ###erp_PX_CAT_G1V2.csv

# COMMAND ----------

df = (
    spark.read.option("header","true")
    .option("inferSchema","true")
    .csv(["/Volumes/proj01_bootcamp/bronze/source_data/PX_CAT_G1V2.csv"])
    )
df.write.mode("overwrite").saveAsTable("proj01_bootcamp.bronze.erp_px_cat_g1v2")