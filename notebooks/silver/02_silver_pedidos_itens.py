# Databricks notebook source
# DBTITLE 1,No CE pode ser necessário executar manualmente antes do `Run all`.
# MAGIC %run ../00_setup

# COMMAND ----------

import datetime
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

SILVER_TABLE = "silver_pedidos_itens"
t0 = datetime.datetime.utcnow()

df_raw = spark.read.table("bronze.bronze_erp_pedidos_itens")

# COMMAND ----------


dedup_window = Window.partitionBy("order_id", "item_seq").orderBy(F.col("processing_timestamp").desc())

df_dedup = (
    df_raw
    .withColumn("rn", F.row_number().over(dedup_window))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

n_dupl = df_raw.count() - df_dedup.count()
log_quality(SILVER_TABLE, "duplicatas_removidas", n_dupl, f"{n_dupl} duplicatas removidas por (order_id, item_seq)")

# COMMAND ----------

df_invalidos = df_dedup.filter(F.col("quantity").isNull())
n_invalidos = df_invalidos.count()

if n_invalidos > 0:
    write_invalidos(df_invalidos, SILVER_TABLE, "quantity nula")
    log_quality(SILVER_TABLE, "quantity_nula", n_invalidos, "itens sem quantity descartados")

df = df_dedup.filter(F.col("quantity").isNotNull())

# COMMAND ----------

df_silver = (
    df
    .withColumn(
        "item_status",
        F.when(F.col("item_status").isNull(), "NAO_INFORMADO")
        .otherwise(
            F.regexp_replace(
                F.upper(F.trim(F.col("item_status"))),
                " ",
                "_"
            )
        )
    )
    .withColumn("total_item",
        F.when(
            F.abs(F.col("total_item") - (F.col("quantity") * F.col("unit_price"))) > 0.01,
            F.round(F.col("quantity") * F.col("unit_price"), 2)
        ).otherwise(F.col("total_item"))
    )
    .drop("source_file", "processing_timestamp", "ingestion_date")
    .withColumn("processing_timestamp", F.current_timestamp())
)

n_status_nulo = df_silver.filter(F.col("item_status") == "NAO_INFORMADO").count()
n_total_recomp = df.filter(
    F.abs(F.col("total_item") - (F.col("quantity") * F.col("unit_price"))) > 0.01
).count()

log_quality(SILVER_TABLE, "item_status_nao_informado", n_status_nulo,  f"{n_status_nulo} itens com item_status nulo")
log_quality(SILVER_TABLE, "total_item_recomputado",    n_total_recomp, f"{n_total_recomp} itens com total_item divergente — recomputados")

# COMMAND ----------

(
    df_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"silver.{SILVER_TABLE}")
)

elapsed = round((datetime.datetime.utcnow() - t0).total_seconds(), 1)
print(f"silver.{SILVER_TABLE}: {df_silver.count()} rows | {elapsed}s")