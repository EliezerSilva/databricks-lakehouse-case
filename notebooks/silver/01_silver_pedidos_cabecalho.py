# Databricks notebook source
# DBTITLE 1,No CE pode ser necessário executar manualmente antes do `Run all`.
# MAGIC %run ../00_setup

# COMMAND ----------

import datetime
from pyspark.sql import functions as F

# COMMAND ----------

SILVER_TABLE = "silver_pedidos_cabecalho"
t0 = datetime.datetime.utcnow()

df_raw = spark.read.table("bronze.bronze_erp_pedidos_cabecalho")

# COMMAND ----------

# DBTITLE 1,quarantine - order_id nulo
df_invalidos = df_raw.filter(F.col("order_id").isNull())
if df_invalidos.count() > 0:
    write_invalidos(df_invalidos, SILVER_TABLE, "order_id nulo")
    log_quality(SILVER_TABLE, "order_id_nulo", df_invalidos.count(), "registros sem order_id descartados")

df = df_raw.filter(F.col("order_id").isNotNull())

# COMMAND ----------

# DBTITLE 1,cleansing
def parse_date_col(col_name):
    return F.coalesce(
        F.try_to_timestamp(F.col(col_name), F.lit("yyyy-MM-dd")).cast("date"),
        F.try_to_timestamp(F.col(col_name), F.lit("yyyy/MM/dd")).cast("date"),
        F.try_to_timestamp(F.col(col_name), F.lit("dd/MM/yyyy")).cast("date"),
    )

df_silver = (
    df
    .withColumn(
        "status_order",
        F.when(F.col("status_order").isNull(), "DESCONHECIDO")
        .otherwise(
            F.regexp_replace(
                F.upper(F.trim(F.col("status_order"))),
                " ",
                "_"
            )
        )
    )
    .withColumn("order_date", parse_date_col("order_date"))
    .withColumn("promised_date", parse_date_col("promised_date"))
    .withColumn("last_update", F.to_timestamp(F.col("last_update"), "yyyy-MM-dd HH:mm:ss"))
    .withColumn("gross_amount",
        F.when(F.col("gross_amount").isNull(), F.col("net_amount") + F.col("discount_amount"))
         .otherwise(F.col("gross_amount"))
    )
    .withColumn("order_source", F.get_json_object(F.col("payment_details"), "$.source"))
    .withColumn("order_priority", F.get_json_object(F.col("payment_details"), "$.priority"))
    .drop("payment_details", "source_file", "processing_timestamp", "ingestion_date")
    .withColumn("processing_timestamp", F.current_timestamp())
)

# COMMAND ----------

# DBTITLE 1,quality log
n_sem_status = df_silver.filter(F.col("status_order") == "DESCONHECIDO").count()
n_sem_data = df_silver.filter(F.col("order_date").isNull()).count()
n_gross_nulo = df_raw.filter(F.col("gross_amount").isNull()).count()

log_quality(SILVER_TABLE, "status_order_desconhecido", n_sem_status, f"{n_sem_status} pedidos com status_order nulo")
log_quality(SILVER_TABLE, "order_date_invalida", n_sem_data,   f"{n_sem_data} pedidos sem order_date após parse em 3 formatos")
log_quality(SILVER_TABLE, "gross_amount_derivado", n_gross_nulo, f"{n_gross_nulo} registros com gross_amount recalculado")

# COMMAND ----------

(
    df_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("status_order")
    .saveAsTable(f"silver.{SILVER_TABLE}")
)

elapsed = round((datetime.datetime.utcnow() - t0).total_seconds(), 1)
print(f"silver.{SILVER_TABLE}: {df_silver.count()} rows | {elapsed}s")