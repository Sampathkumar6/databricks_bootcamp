# Databricks notebook source
# MAGIC %md
# MAGIC ##The Transformation Logic

# COMMAND ----------

query = """
SELECT
    sd.order_number,
    pr.product_key,
    cu.customer_key,
    sd.order_date,
    sd.ship_date,
    sd.due_date,
    sd.sales_amount,
    sd.quantity,
    sd.price
FROM proj01_bootcamp.silver.crm_sales sd
LEFT JOIN proj01_bootcamp.gold.dim_products pr
    ON sd.product_number = pr.product_number
LEFT JOIN proj01_bootcamp.gold.dim_customers cu
    ON sd.customer_id = cu.customer_id;
"""
df = spark.sql(query)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Writing Gold Table

# COMMAND ----------

df.write.mode("overwrite").format("delta").saveAsTable("proj01_bootcamp.gold.fact_sales")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Sanity checks of Gold table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM proj01_bootcamp.gold.fact_sales LIMIT 10