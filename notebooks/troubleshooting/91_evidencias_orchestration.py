# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT * FROM control.ingestion_log

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM silver.quality_log

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gold.vw_pedidos_analytics LIMIT 20

# COMMAND ----------

