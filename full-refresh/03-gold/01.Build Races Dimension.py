# Databricks notebook source
# MAGIC %md
# MAGIC ## Build Constructors Dimenion
# MAGIC 1. Read silver `constructors` table
# MAGIC 2. Read silver `circuits` table
# MAGIC 3. Join data from `races` with `circuits` using `circuit_id`
# MAGIC 4. Select required columns - `races.season`,`races.round`,`races.race_name`,`races.race_date`,`circuits.circuit_name`,`circuits.locality`,`circuits.country`
# MAGIC 5. Write the tranformed data to gold `dim_races` table

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

target_table=f'{catalog_name}.{schema}.dim_races'

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 to 4 - Read tables, join data and select columns

# COMMAND ----------

circuits_df=spark.read.table(f'{catalog_name}.{schema}.silver_circuits')
races_df=spark.read.table(f'{catalog_name}.{schema}.silver_races')

# COMMAND ----------

dim_races=(
    races_df.join(circuits_df, 'circuit_id', 'inner')
    .select(races_df.season,
            races_df.round,
            races_df.race_name,
            races_df.race_date,
            circuits_df.circuit_name,
            circuits_df.locality,
            circuits_df.country
        )
) #on condition - races_df.circuit_id == circuits_df.circuit_id

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5 - Write the tranformed data to gold `dim_races` table

# COMMAND ----------

(
    dim_races
    .write
    .format('delta')
    .mode('overwrite')
    .saveAsTable(target_table)
)