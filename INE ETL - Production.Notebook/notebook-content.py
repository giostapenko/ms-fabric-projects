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

# CELL ********************

df = spark.read.option("multiline", "true").json("Files/data/ine_data/json/production/*.json")
# df now is a Spark DataFrame containing JSON data from "Files/data/ine_data/json/production/2024-10-28 14_18_30_766088_1986.json".
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, lit

# Define the list of years dynamically
years = [str(year) for year in range(1986, 2025)]

# Explode each year in `dados` if it exists and union them into one DataFrame
exploded_dfs = []
for year in years:
    # Check if the year exists in the `dados` struct and explode if present
    if year in df.schema["Dados"].dataType.fieldNames():
        exploded_df = df.select(explode(col(f"Dados.`{year}`")).alias("record")).withColumn("year", lit(year))
        exploded_dfs.append(
            exploded_df.select(
                col("record.dim_3").alias("dim_3"),
                col("record.geocod").alias("geocod"),
                col("record.ind_string").alias("ind_string"),
                col("record.dim_3_t").alias("dim_3_t"),
                col("record.sinal_conv").alias("sinal_conv"),
                col("record.geodsg").alias("geodsg"),
                col("record.sinal_conv_desc").alias("sinal_conv_desc"),
                col("year")
            )
        )

# Union all exploded DataFrames into one final DataFrame
final_df = exploded_dfs[0]
for exploded_df in exploded_dfs[1:]:
    final_df = final_df.union(exploded_df)

# Show the final result
final_df.show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#df = spark.read.json('Files/data/ine_data/json/production')
final_df.createOrReplaceTempView('tab_ine_production')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT DISTINCT *
# MAGIC 
# MAGIC FROM tab_ine_production


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE tb_agricultural_prod AS
# MAGIC SELECT DISTINCT
# MAGIC         year,
# MAGIC         geodsg as location,
# MAGIC         dim_3_t as specie,
# MAGIC         ind_string as production
# MAGIC 
# MAGIC FROM tab_ine_production
# MAGIC 
# MAGIC WHERE ind_string <> "x"
# MAGIC 
# MAGIC ORDER BY year, location

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
