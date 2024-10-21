# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "23cc9a3d-2cf7-4620-b0e4-e7dc5d339b52",
# META       "default_lakehouse_name": "LH_Bronze",
# META       "default_lakehouse_workspace_id": "0b451587-eab4-4658-a425-3e76245475b4"
# META     }
# META   }
# META }

# MARKDOWN ********************

# ## Leitura arquivos parquet
# E transformação para temp table

# CELL ********************


df = spark.read.parquet('Files/data/contents/parquet')
df.createOrReplaceTempView('tab_news')


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Manipulação (ETL) e Criação de tabela com SparkSQL
# https://docs.delta.io/latest/delta-batch.html

# CELL ********************

# MAGIC %%sql
# MAGIC -- contar a quantidade de publicações por dia
# MAGIC WITH tab_news_published as (
# MAGIC SELECT DISTINCT id,
# MAGIC        CAST(published_at AS DATE) as published_at
# MAGIC 
# MAGIC FROM tab_news
# MAGIC 
# MAGIC WHERE status = 'published' 
# MAGIC AND type = 'content' 
# MAGIC 
# MAGIC ),
# MAGIC tab_news_publication as (
# MAGIC -- fazer a media de qtde de publicações / qtde de dias
# MAGIC SELECT published_at,
# MAGIC         COUNT(DISTINCT id) as count_publication
# MAGIC 
# MAGIC FROM tab_news_published
# MAGIC 
# MAGIC GROUP BY published_at
# MAGIC -- da pra fazer a diferença entre quantidade de publicações feitas em um mês / no mes anterior
# MAGIC )
# MAGIC 
# MAGIC SELECT SUM(count_publication)/COUNT(*)
# MAGIC 
# MAGIC FROM tab_news_publication

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Manipulação (ETL) e Criação tabela com PySpark

# CELL ********************

# MAGIC %%sql
# MAGIC WITH tab_news_sum_tabcoins as (
# MAGIC SELECT DISTINCT owner_username as user,
# MAGIC        SUM(tabcoins) sum_tabcoins 
# MAGIC 
# MAGIC FROM tab_news
# MAGIC 
# MAGIC WHERE status = 'published' AND type = 'content' 
# MAGIC 
# MAGIC GROUP BY owner_username,
# MAGIC         tabcoins
# MAGIC )
# MAGIC 
# MAGIC SELECT *,  
# MAGIC         DENSE_RANK() OVER (ORDER BY sum_tabcoins) as rank
# MAGIC 
# MAGIC FROM tab_news_sum_tabcoins
# MAGIC 
# MAGIC GROUP BY user,
# MAGIC         sum_tabcoins
# MAGIC 
# MAGIC ORDER BY sum_tabcoins DESC
# MAGIC 
# MAGIC LIMIT 10

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
