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

df = spark.read.option("multiline", "true").json("Files/data/ine_data/json/workforce/*.json")
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
years = [f"{year}" for year in [1989, 1993, 1995, 1997, 1999, 2003, 2005, 2007, 2009, 2013, 2016, 2019]]

# Explode each year in `dados` if it exists and union them into one DataFrame
exploded_dfs = []
for year in years:
    # Check if the year exists in the `dados` struct and explode if present
    if year in df.schema["Dados"].dataType.fieldNames():
        exploded_df = df.select(explode(col(f"Dados.`{year}`")).alias("record")).withColumn("year", lit(year))
        exploded_dfs.append(
            exploded_df.select(
                col("year"),
                col("record.geodsg").alias("location"),
                col("record.dim_3_t").alias("labour_labor_type"),
                col("record.ind_string").alias("labour_force")
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

final_df.createOrReplaceTempView("tab_ine_workforce")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE tb_workforce AS
# MAGIC SELECT *
# MAGIC 
# MAGIC FROM tab_ine_workforce

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
