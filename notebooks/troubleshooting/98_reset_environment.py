# Databricks notebook source
# Databricks notebook source

tables = [
    # bronze
    "bronze.bronze_erp_pedidos_cabecalho",
    "bronze.bronze_erp_pedidos_itens",
    "bronze.bronze_crm_clientes",
    "bronze.bronze_comercial_canais",
    "bronze.bronze_comercial_vendedores",
    "bronze.bronze_legado_regioes",

    # silver
    "silver.quality_log",
    "silver.tabelas_invalidas",
    "silver.tabelas_orfas",

    # control
    "control.ingestion_log"
]

for table in tables:
    try:
        spark.sql(f"DROP TABLE IF EXISTS {table}")
        print(f"[OK] dropped {table}")
    except Exception as e:
        print(f"[NOK] {table}: {e}")

print("\nEnvironment reset completed")