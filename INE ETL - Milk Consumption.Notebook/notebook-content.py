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

# Welcome to your new notebook
# Type here in the cell editor to add code!
df = spark.read.option("multiline", "true").json("Files/data/ine_data/json/milk_consumption/*.json")
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
years = [f"{year}" for year in range (1983, 2021)]

# Explode each year in `dados` if it exists and union them into one DataFrame
exploded_dfs = []
for year in years:
    # Check if the year exists in the `dados` struct and explode if present
    if year in df.schema["Dados"].dataType.fieldNames():
        exploded_df = df.select(explode(col(f"Dados.`{year}`")).alias("record")).withColumn("year", lit(year))
        exploded_dfs.append(
            exploded_df.select(
                col("year"),
                col("record.valor").alias("consumption")
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

final_df.createOrReplaceTempView("tb_ine_milk_consumption")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE tb_milk_consumption AS
# MAGIC SELECT *
# MAGIC 
# MAGIC FROM tb_ine_milk_consumption

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
