# Databricks notebook source
# DBTITLE 1,No CE é necessário executar manualmente antes do `Run all`.
# %run ../00_setup

# COMMAND ----------

import datetime
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

_SOURCE_FILE        = "erp_pedidos_cabecalho_2025.csv"
_TARGET_TABLE       = "bronze_erp_pedidos_cabecalho"
_EXPECTED_SEP       = ";"
_EXPECTED_COL_COUNT = 11

pipeline_execution_timestamp = datetime.datetime.utcnow()

# COMMAND ----------

check_already_loaded(_SOURCE_FILE, f"bronze.{_TARGET_TABLE}")

# COMMAND ----------

val_status, val_msg, detected_schema, expected_schema = validate_csv_layout(
    _SOURCE_FILE, _EXPECTED_SEP, _EXPECTED_COL_COUNT
)

if val_status != "OK":
    log_ingestion(
        source_file                  = _SOURCE_FILE,
        target_table                 = f"bronze.{_TARGET_TABLE}",
        records_loaded               = 0,
        pipeline_execution_timestamp = pipeline_execution_timestamp,
        bronze_load_timestamp        = datetime.datetime.utcnow(),
        status                       = val_status,
        validation_message           = val_msg,
        detected_schema              = detected_schema,
        expected_schema              = expected_schema,
    )
    raise ValueError(f"Layout inválido em {_SOURCE_FILE}: {val_msg}")

# COMMAND ----------

schema = StructType([
    StructField("order_id",         StringType(), True),
    StructField("customer_code",    StringType(), True),
    StructField("seller_id",        StringType(), True),
    StructField("order_date",       StringType(), True),  # múltiplos formatos — parse na Silver
    StructField("promised_date",    StringType(), True),
    StructField("status_order",     StringType(), True),
    StructField("gross_amount",     DoubleType(), True),
    StructField("discount_amount",  DoubleType(), True),
    StructField("net_amount",       DoubleType(), True),
    StructField("payment_details",  StringType(), True),
    StructField("last_update",      StringType(), True),
])

df_raw = (
    spark.read
    .option("header", "true")
    .option("sep", _EXPECTED_SEP)
    .option("multiLine", "true")  # necessário pelo JSON embutido em payment_details
    .option("escape", '"')
    .schema(schema)
    .csv(f"{SOURCE_ROOT}/{_SOURCE_FILE}")
)

df_bronze = add_bronze_cols(df_raw, _SOURCE_FILE, pipeline_execution_timestamp)

(
    df_bronze.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("ingestion_date")
    .saveAsTable(f"bronze.{_TARGET_TABLE}")
)

bronze_load_timestamp = datetime.datetime.utcnow()
elapsed = round((bronze_load_timestamp - pipeline_execution_timestamp).total_seconds(), 1)

# COMMAND ----------

records_loaded = df_bronze.count()

log_ingestion(
    source_file                  = _SOURCE_FILE,
    target_table                 = f"bronze.{_TARGET_TABLE}",
    records_loaded               = records_loaded,
    pipeline_execution_timestamp = pipeline_execution_timestamp,
    bronze_load_timestamp        = bronze_load_timestamp,
    status                       = val_status,
    validation_message           = val_msg,
    detected_schema              = detected_schema,
    expected_schema              = expected_schema,
)

print(f"{_TARGET_TABLE}: {records_loaded} rows | {elapsed}s | status={val_status}")