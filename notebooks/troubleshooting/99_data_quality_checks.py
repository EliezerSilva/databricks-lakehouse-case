# Databricks notebook source
# DBTITLE 1,Validation — catalogs, schemas and storage objects
print("Available catalogs:")
spark.sql("SHOW CATALOGS").show()

print("\nSchemas in workspace catalog:")
spark.sql("SHOW SCHEMAS IN workspace").show()

print("\nAvailable volumes in lakehouse_case schema:")
spark.sql("SHOW VOLUMES IN workspace.lakehouse_case").show()

print("\nBronze tables:")
spark.sql("SHOW TABLES IN bronze").show(truncate=False)

print("\nSilver tables:")
spark.sql("SHOW TABLES IN silver").show(truncate=False)

print("\nGold tables:")
spark.sql("SHOW TABLES IN gold").show(truncate=False)

print("\nControl tables:")
spark.sql("SHOW TABLES IN control").show(truncate=False)

# COMMAND ----------

# DBTITLE 1,table ingestion_log
# MAGIC %sql
# MAGIC SELECT COUNT(*) 
# MAGIC FROM bronze.bronze_erp_pedidos_cabecalho;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     source_file,
# MAGIC     target_table,
# MAGIC     records_loaded,
# MAGIC     status,
# MAGIC     bronze_load_timestamp
# MAGIC FROM control.ingestion_log
# MAGIC ORDER BY bronze_load_timestamp DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN silver;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) 
# MAGIC FROM silver.silver_pedidos_cabecalho;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     order_id,
# MAGIC     order_date,
# MAGIC     promised_date
# MAGIC FROM silver.silver_pedidos_cabecalho
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     status_order,
# MAGIC     COUNT(*) AS total
# MAGIC FROM silver.silver_pedidos_cabecalho
# MAGIC GROUP BY status_order
# MAGIC ORDER BY total DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     source_table AS table_name,
# MAGIC     check_name AS quality_rule,
# MAGIC     records_affected,
# MAGIC     message AS details
# MAGIC FROM workspace.silver.quality_log
# MAGIC ORDER BY processing_timestamp DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     order_source,
# MAGIC     order_priority
# MAGIC FROM silver.silver_pedidos_cabecalho
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*)
# MAGIC FROM silver.silver_pedidos_itens;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     order_id,
# MAGIC     item_seq,
# MAGIC     COUNT(*) AS total
# MAGIC FROM silver.silver_pedidos_itens
# MAGIC GROUP BY order_id, item_seq
# MAGIC HAVING COUNT(*) > 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     item_status,
# MAGIC     COUNT(*)
# MAGIC FROM silver.silver_pedidos_itens
# MAGIC GROUP BY item_status
# MAGIC ORDER BY 2 DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     source_table,
# MAGIC     check_name,
# MAGIC     records_affected,
# MAGIC     message
# MAGIC FROM silver.quality_log
# MAGIC WHERE source_table = 'silver_pedidos_itens'
# MAGIC ORDER BY processing_timestamp DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN silver;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM silver.silver_regioes;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM silver.silver_canais;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM silver.silver_vendedores;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM silver.silver_clientes;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     estado,
# MAGIC     COUNT(*)
# MAGIC FROM silver.silver_clientes
# MAGIC GROUP BY estado
# MAGIC ORDER BY 2 DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     delivery_status,
# MAGIC     COUNT(*)
# MAGIC FROM silver.silver_logistica
# MAGIC GROUP BY delivery_status
# MAGIC ORDER BY 2 DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     flag_atrasado,
# MAGIC     COUNT(*)
# MAGIC FROM silver.silver_logistica
# MAGIC GROUP BY flag_atrasado;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     severity,
# MAGIC     COUNT(*)
# MAGIC FROM silver.silver_atendimento_ocorrencias
# MAGIC GROUP BY severity
# MAGIC ORDER BY 2 DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     source_table,
# MAGIC     check_name,
# MAGIC     records_affected,
# MAGIC     message
# MAGIC FROM silver.quality_log
# MAGIC ORDER BY processing_timestamp DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     COUNT(*) total,
# MAGIC     COUNT(DISTINCT sk_cliente) sk_distintas
# MAGIC FROM gold.dim_cliente;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     COUNT(*) total,
# MAGIC     COUNT(DISTINCT sk_produto) sk_distintas
# MAGIC FROM gold.dim_produto;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     COUNT(*) total,
# MAGIC     COUNT(DISTINCT sk_data) sk_distintas
# MAGIC FROM gold.dim_data;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     data,
# MAGIC     nome_mes,
# MAGIC     nome_dia_semana,
# MAGIC     flag_fim_semana
# MAGIC FROM gold.dim_data
# MAGIC ORDER BY data
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     MIN(data),
# MAGIC     MAX(data)
# MAGIC FROM gold.dim_data;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*)
# MAGIC FROM gold.fato_pedidos;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     COUNT(*) total,
# MAGIC     COUNT(DISTINCT concat(order_id, '-', item_seq)) granularidade
# MAGIC FROM gold.fato_pedidos;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     flag_atrasado,
# MAGIC     COUNT(*)
# MAGIC FROM gold.fato_pedidos
# MAGIC GROUP BY flag_atrasado;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     flag_cancelado,
# MAGIC     COUNT(*)
# MAGIC FROM gold.fato_pedidos
# MAGIC GROUP BY flag_cancelado;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     valor_item,
# MAGIC     valor_desconto_pedido,
# MAGIC     receita_liquida_item
# MAGIC FROM gold.fato_pedidos
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     order_id,
# MAGIC     item_seq,
# MAGIC     COUNT(*) qtd
# MAGIC FROM gold.fato_pedidos
# MAGIC GROUP BY order_id, item_seq
# MAGIC HAVING COUNT(*) > 1
# MAGIC ORDER BY qtd DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     seller_id,
# MAGIC     COUNT(*)
# MAGIC FROM gold.dim_vendedor
# MAGIC GROUP BY seller_id
# MAGIC HAVING COUNT(*) > 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     order_ref,
# MAGIC     COUNT(*)
# MAGIC FROM silver.silver_logistica
# MAGIC WHERE order_ref IN ('O00081', 'O00011', 'O00121')
# MAGIC GROUP BY order_ref;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM silver.silver_logistica
# MAGIC WHERE order_ref IN ('O00081', 'O00011', 'O00121')
# MAGIC ORDER BY order_ref, delivered_at;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     order_id,
# MAGIC     item_seq,
# MAGIC     COUNT(*)
# MAGIC FROM silver.silver_pedidos_itens
# MAGIC WHERE order_id IN ('O00081', 'O00011', 'O00121')
# MAGIC GROUP BY order_id, item_seq
# MAGIC HAVING COUNT(*) > 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     COUNT(*) total,
# MAGIC     COUNT(DISTINCT concat(order_id, '-', item_seq)) granularidade
# MAGIC FROM gold.fato_pedidos;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     order_id,
# MAGIC     COUNT(*)
# MAGIC FROM silver.silver_pedidos_cabecalho
# MAGIC GROUP BY order_id
# MAGIC HAVING COUNT(*) > 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     COUNT(*) total,
# MAGIC     COUNT(DISTINCT concat(order_id, '-', item_seq)) granularidade
# MAGIC FROM gold.fato_pedidos;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM gold.metricas_por_periodo;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM gold.metricas_por_regiao;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM gold.metricas_por_canal;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM gold.metricas_por_categoria;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM gold.metricas_operacionais;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     ano_mes,
# MAGIC     receita_bruta,
# MAGIC     receita_liquida,
# MAGIC     total_descontos,
# MAGIC     ticket_medio
# MAGIC FROM gold.metricas_por_periodo
# MAGIC ORDER BY ano_mes;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     ano_mes,
# MAGIC     nome_regiao,
# MAGIC     nome_canal,
# MAGIC     taxa_cancelamento_pct,
# MAGIC     taxa_atraso_pct
# MAGIC FROM gold.metricas_operacionais
# MAGIC ORDER BY ano_mes
# MAGIC LIMIT 30;

# COMMAND ----------

