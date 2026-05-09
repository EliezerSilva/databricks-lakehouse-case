# Databricks notebook source
# MAGIC %md
# MAGIC ## Shared setup
# MAGIC
# MAGIC Configuração compartilhada de paths, databases, schemas e funções auxiliares.
# MAGIC Executar via `%run` antes das camadas bronze, silver e gold.

# COMMAND ----------

# DBTITLE 1,Imports and shared dependencies
import re
import datetime
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, TimestampType, DateType
)

# COMMAND ----------

# DBTITLE 1,Storage path
SOURCE_ROOT = "/Volumes/workspace/lakehouse_case/sources"

# COMMAND ----------

# DBTITLE 1,Create schemas
spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")
spark.sql("CREATE SCHEMA IF NOT EXISTS silver")
spark.sql("CREATE SCHEMA IF NOT EXISTS gold")
spark.sql("CREATE SCHEMA IF NOT EXISTS control")

# COMMAND ----------

# DBTITLE 1,Control and quality tables
spark.sql("""
    CREATE TABLE IF NOT EXISTS control.ingestion_log (
        source_file                   STRING    COMMENT 'arquivo de origem',
        target_table                  STRING    COMMENT 'tabela bronze de destino',
        records_loaded                LONG      COMMENT '0 quando status=NOK',
        pipeline_execution_timestamp  TIMESTAMP COMMENT 'início do notebook',
        source_file_last_modified     TIMESTAMP COMMENT 'mtime do arquivo no volume',
        source_reference_date         DATE      COMMENT 'data derivada do nome do arquivo',
        bronze_load_timestamp         TIMESTAMP COMMENT 'conclusão da escrita',
        status                        STRING    COMMENT 'OK ou NOK',
        validation_message            STRING,
        detected_schema               STRING,
        expected_schema               STRING
    )
    USING DELTA
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS silver.quality_log (
        processing_timestamp  TIMESTAMP,
        source_table          STRING,
        check_name            STRING,
        records_affected      LONG,
        message               STRING
    )
    USING DELTA
""")

# payload serializado — esquema genérico para não proliferar tabelas por domínio
spark.sql("""
    CREATE TABLE IF NOT EXISTS silver.tabelas_invalidas (
        processing_timestamp  TIMESTAMP,
        source_table          STRING,
        motivo                STRING,
        payload               STRING
    )
    USING DELTA
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS silver.tabelas_orfas (
        processing_timestamp  TIMESTAMP,
        source_table          STRING,
        join_key_name         STRING,
        join_key_value        STRING,
        payload               STRING
    )
    USING DELTA
""")

print("schemas: bronze | silver | gold | control")

# COMMAND ----------

# DBTITLE 1,Auxiliary schemas
_LOG_SCHEMA = StructType([
    StructField("source_file",                   StringType(),    True),
    StructField("target_table",                  StringType(),    True),
    StructField("records_loaded",                LongType(),      True),
    StructField("pipeline_execution_timestamp",  TimestampType(), True),
    StructField("source_file_last_modified",     TimestampType(), True),
    StructField("source_reference_date",         DateType(),      True),
    StructField("bronze_load_timestamp",         TimestampType(), True),
    StructField("status",                        StringType(),    True),
    StructField("validation_message",            StringType(),    True),
    StructField("detected_schema",               StringType(),    True),
    StructField("expected_schema",               StringType(),    True),
])

_QUALITY_LOG_SCHEMA = StructType([
    StructField("processing_timestamp", TimestampType(), True),
    StructField("source_table",         StringType(),    True),
    StructField("check_name",           StringType(),    True),
    StructField("records_affected",     LongType(),      True),
    StructField("message",              StringType(),    True),
])

# COMMAND ----------

# DBTITLE 1,Helper functions
def get_file_last_modified(filename):
    try:
        info = dbutils.fs.ls(f"{SOURCE_ROOT}/{filename}")
        return datetime.datetime.utcfromtimestamp(info[0].modificationTime / 1000)
    except Exception:
        return None

def derive_reference_date(filename):
    m = re.search(r"_(\d{4})(?:[_.]|$)", filename)
    if m:
        return datetime.date(int(m.group(1)), 1, 1)
    return None

def check_already_loaded(source_file, target_table):
    """Returns True if a successful load already exists for this source/target pair."""
    n = spark.sql(f"""
        SELECT COUNT(1) AS n FROM control.ingestion_log
        WHERE source_file  = '{source_file}'
          AND target_table = '{target_table}'
          AND status = 'OK'
    """).first()["n"]
    if n > 0:
        print(f"[RERUN] {source_file} → {target_table}")
    return n > 0

def add_bronze_cols(df, source_name, pipeline_ts):
    """Adds standard lineage columns to a Bronze DataFrame."""
    return (
        df
        .withColumn("ingestion_date",       F.current_date())
        .withColumn("source_file",          F.lit(source_name))
        .withColumn("processing_timestamp", F.lit(pipeline_ts).cast("timestamp"))
    )

def write_bronze_table(df, table_name):
    """Overwrites a Bronze Delta managed table. Returns row count."""
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"bronze.{table_name}")
    )
    n = df.count()
    print(f"bronze.{table_name}: {n} rows")
    return n

def validate_csv_layout(filename, expected_delimiter, expected_col_count):
    """CSV layout check — delimiter and column count only. Pure function, no side effects."""
    try:
        raw    = dbutils.fs.head(f"{SOURCE_ROOT}/{filename}", 2048)
        header = raw.split("\n")[0].strip()

        detected_delimiter = ";" if header.count(";") > header.count(",") else ","
        detected_col_count = len(header.split(detected_delimiter))

        detected_schema = f"sep={detected_delimiter} cols={detected_col_count}"
        expected_schema = f"sep={expected_delimiter} cols={expected_col_count}"

        issues = []
        if detected_delimiter != expected_delimiter:
            issues.append(f"delimiter: expected '{expected_delimiter}' found '{detected_delimiter}'")
        if detected_col_count != expected_col_count:
            issues.append(f"columns: expected {expected_col_count} found {detected_col_count}")

        if issues:
            msg = " | ".join(issues)
            print(f"  [NOK] {filename}: {msg}")
            return "NOK", msg, detected_schema, expected_schema

        return "OK", None, detected_schema, expected_schema

    except Exception as e:
        msg = f"layout check failed: {e}"
        print(f"  [NOK] {filename}: {msg}")
        return "NOK", msg, None, f"sep={expected_delimiter} cols={expected_col_count}"

def log_ingestion(source_file, target_table, records_loaded,
                  pipeline_execution_timestamp,
                  bronze_load_timestamp,
                  status="OK",
                  validation_message=None,
                  detected_schema=None,
                  expected_schema=None):
    """Append one row to control.ingestion_log."""
    row = [(
        source_file,
        target_table,
        int(records_loaded),
        pipeline_execution_timestamp,
        get_file_last_modified(source_file),
        derive_reference_date(source_file),
        bronze_load_timestamp,
        status,
        validation_message,
        detected_schema,
        expected_schema,
    )]
    (
        spark.createDataFrame(row, _LOG_SCHEMA)
        .write.format("delta").mode("append")
        .saveAsTable("control.ingestion_log")
    )

def log_quality(source_table, check_name, records_affected, message):
    """Append to silver.quality_log. Skips when records_affected == 0."""
    if records_affected == 0:
        return
    row = [(datetime.datetime.utcnow(), source_table, check_name, int(records_affected), message)]
    (
        spark.createDataFrame(row, _QUALITY_LOG_SCHEMA)
        .write.format("delta").mode("append")
        .saveAsTable("silver.quality_log")
    )

def write_invalidos(df, source_table, motivo):
    """Persiste registros descartados em silver.tabelas_invalidas."""
    (
        df.withColumn("processing_timestamp", F.current_timestamp())
          .withColumn("source_table",         F.lit(source_table))
          .withColumn("motivo",               F.lit(motivo))
          .withColumn("payload",              F.to_json(F.struct([F.col(c) for c in df.columns])))
          .select("processing_timestamp", "source_table", "motivo", "payload")
          .write.format("delta").mode("append")
          .saveAsTable("silver.tabelas_invalidas")
    )

def write_orfaos(df, source_table, join_key_name, join_key_col):
    """Persiste registros órfãos em silver.tabelas_orfas."""
    (
        df.withColumn("processing_timestamp", F.current_timestamp())
          .withColumn("source_table",         F.lit(source_table))
          .withColumn("join_key_name",        F.lit(join_key_name))
          .withColumn("join_key_value",       F.col(join_key_col).cast("string"))
          .withColumn("payload",              F.to_json(F.struct([F.col(c) for c in df.columns])))
          .select("processing_timestamp", "source_table", "join_key_name", "join_key_value", "payload")
          .write.format("delta").mode("append")
          .saveAsTable("silver.tabelas_orfas")
    )

# COMMAND ----------

print("shared setup loaded")