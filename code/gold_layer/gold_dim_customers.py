# Databricks notebook source
# MAGIC %md
# MAGIC ##The Transformation Logic

# COMMAND ----------

query = """
SELECT
    ROW_NUMBER() OVER (ORDER BY ci.customer_id) AS customer_key,
    ci.customer_id,
    ca.customer_number,
    ci.customer_firstname AS first_name,
    ci.customer_lastname AS last_name,
    la.country,
    ci.customer_marital_status AS marital_status,
    CASE
        WHEN ci.customer_gender <> 'n/a' THEN ci.customer_gender
        ELSE COALESCE(ca.gender, 'n/a')
    END AS gender,
    ca.birth_date AS birthdate,
    ci.customer_create_date AS create_date
FROM proj01_bootcamp.silver.crm_customers ci
LEFT JOIN proj01_bootcamp.silver.erp_customers ca
    ON ci.customer_key = ca.customer_number
LEFT JOIN proj01_bootcamp.silver.erp_customer_location la
    ON ci.customer_key = la.customer_number
"""
df = spark.sql(query)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Writing Gold Table

# COMMAND ----------

df.write.mode("overwrite").format("delta").saveAsTable("proj01_bootcamp.gold.dim_customers")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Sanity checks of Gold table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM proj01_bootcamp.gold.dim_customers LIMIT 10