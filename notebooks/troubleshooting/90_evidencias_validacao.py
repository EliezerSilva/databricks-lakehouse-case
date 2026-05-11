# Databricks notebook source
# MAGIC %sql
# MAGIC --01 — TABELAS GOLD
# MAGIC --Evidência da camada analítica final
# MAGIC
# MAGIC SHOW TABLES IN gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 02 — QUALITY LOG
# MAGIC -- Evidência principal de governança e tratamento
# MAGIC SELECT
# MAGIC     source_table,
# MAGIC     check_name,
# MAGIC     records_affected,
# MAGIC     message,
# MAGIC     processing_timestamp
# MAGIC FROM silver.quality_log
# MAGIC ORDER BY processing_timestamp DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 03 — RESUMO DO QUALITY LOG
# MAGIC -- Quantidade total de problemas tratados
# MAGIC SELECT
# MAGIC     source_table,
# MAGIC     COUNT(*) AS total_checks,
# MAGIC     SUM(records_affected) AS total_registros_afetados
# MAGIC FROM silver.quality_log
# MAGIC GROUP BY source_table
# MAGIC ORDER BY total_registros_afetados DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 04 — DETECÇÃO DE REPLAY / DUPLICIDADE
# MAGIC -- Evidência do troubleshooting realizado
# MAGIC SELECT
# MAGIC     order_id,
# MAGIC     COUNT(*) AS qtd
# MAGIC FROM silver.silver_pedidos_cabecalho
# MAGIC GROUP BY order_id
# MAGIC HAVING COUNT(*) > 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 05 — GRANULARIDADE DA FATO
# MAGIC -- Validação de grain
# MAGIC SELECT
# MAGIC     COUNT(*) total,
# MAGIC     COUNT(DISTINCT concat(order_id, '-', item_seq)) granularidade
# MAGIC FROM gold.fato_pedidos;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 06 — VOLUME DA FATO
# MAGIC
# MAGIC SELECT COUNT(*) AS total_fato
# MAGIC FROM gold.fato_pedidos;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 07 — INTEGRIDADE DAS DIMENSÕES
# MAGIC -- Surrogate keys únicas
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
# MAGIC -- 08 — RECEITA POR PERÍODO
# MAGIC -- Evidência da camada executiva
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
# MAGIC -- 09 — MÉTRICAS OPERACIONAIS
# MAGIC -- Cancelamento e atraso
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

# MAGIC %sql
# MAGIC -- 10 — SEMANTIC LAYER
# MAGIC -- Evidência da view analítica
# MAGIC SELECT
# MAGIC     order_id,
# MAGIC     nome_cliente,
# MAGIC     nome_produto,
# MAGIC     nome_canal,
# MAGIC     nome_regiao,
# MAGIC     receita_liquida_item,
# MAGIC     status_order,
# MAGIC     status_entrega
# MAGIC FROM gold.vw_pedidos_analytics
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 11 — INTEGRIDADE DA SEMANTIC LAYER
# MAGIC SELECT
# MAGIC     COUNT(CASE WHEN nome_cliente IS NULL THEN 1 END) AS sem_cliente,
# MAGIC     COUNT(CASE WHEN nome_produto IS NULL THEN 1 END) AS sem_produto,
# MAGIC     COUNT(CASE WHEN nome_canal IS NULL THEN 1 END) AS sem_canal,
# MAGIC     COUNT(CASE WHEN nome_regiao IS NULL THEN 1 END) AS sem_regiao,
# MAGIC     COUNT(CASE WHEN ano IS NULL THEN 1 END) AS sem_data
# MAGIC FROM gold.vw_pedidos_analytics;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 12 — DIMENSÃO CALENDÁRIO
# MAGIC -- Cobertura temporal
# MAGIC SELECT
# MAGIC     MIN(data) AS data_min,
# MAGIC     MAX(data) AS data_max
# MAGIC FROM gold.dim_data;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 13 — TOP PRODUTOS
# MAGIC -- Exemplo analítico final
# MAGIC SELECT
# MAGIC     nome_produto,
# MAGIC     categoria,
# MAGIC     SUM(receita_liquida_item) AS receita
# MAGIC FROM gold.vw_pedidos_analytics
# MAGIC GROUP BY nome_produto, categoria
# MAGIC ORDER BY receita DESC
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 14 - RESUMO EXECUTIVO DO PIPELINE
# MAGIC
# MAGIC WITH fato AS (
# MAGIC     SELECT COUNT(*) AS total_fato
# MAGIC     FROM gold.fato_pedidos
# MAGIC ),
# MAGIC
# MAGIC granularidade AS (
# MAGIC     SELECT
# MAGIC         COUNT(DISTINCT concat(order_id, '-', item_seq)) AS granularidade
# MAGIC     FROM gold.fato_pedidos
# MAGIC ),
# MAGIC
# MAGIC clientes AS (
# MAGIC     SELECT COUNT(*) AS total_clientes
# MAGIC     FROM gold.dim_cliente
# MAGIC ),
# MAGIC
# MAGIC produtos AS (
# MAGIC     SELECT COUNT(*) AS total_produtos
# MAGIC     FROM gold.dim_produto
# MAGIC ),
# MAGIC
# MAGIC receita AS (
# MAGIC     SELECT
# MAGIC         ROUND(SUM(receita_liquida), 2) AS receita_total
# MAGIC     FROM gold.metricas_por_periodo
# MAGIC ),
# MAGIC
# MAGIC quality AS (
# MAGIC     SELECT
# MAGIC         COUNT(*) AS total_checks,
# MAGIC         SUM(records_affected) AS registros_tratados
# MAGIC     FROM silver.quality_log
# MAGIC ),
# MAGIC
# MAGIC semantic AS (
# MAGIC     SELECT COUNT(*) AS registros_analytics
# MAGIC     FROM gold.vw_pedidos_analytics
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     f.total_fato,
# MAGIC     g.granularidade,
# MAGIC     c.total_clientes,
# MAGIC     p.total_produtos,
# MAGIC     r.receita_total,
# MAGIC     q.total_checks,
# MAGIC     q.registros_tratados,
# MAGIC     s.registros_analytics
# MAGIC FROM fato f
# MAGIC CROSS JOIN granularidade g
# MAGIC CROSS JOIN clientes c
# MAGIC CROSS JOIN produtos p
# MAGIC CROSS JOIN receita r
# MAGIC CROSS JOIN quality q
# MAGIC CROSS JOIN semantic s;

# COMMAND ----------

