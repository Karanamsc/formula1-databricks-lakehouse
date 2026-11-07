# Databricks notebook source
# MAGIC %md
# MAGIC ## Build Results Fact
# MAGIC 1. Read silver `results` table
# MAGIC 2. Read silver `sprints` table
# MAGIC 3. Add new column `session_type` with values `RACE` or `SPRINT`
# MAGIC 4. Union data from `results` with `sprints`
# MAGIC 5. Derive additional columns  
# MAGIC     - `is win`-> indicated if driver won the race
# MAGIC     - `is_podium` -> indicates that the driver scored a podium result(1,2,3)
# MAGIC     - `has_points` -> indicates that the driver has scored points
# MAGIC 6. Write the tranformed data to gold `fact_session_results` table

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

target_table=f'{catalog_name}.{schema}.fact_session_results'

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 to 4 - Read tables, union data and select columns

# COMMAND ----------

results_df=(
    spark.read.table(f'{catalog_name}.{schema}.silver_results')
    .withColumn('session_type',F.lit('RACE'))
    .drop('race_name','race_date','ingestion_timestamp','souce_file')
)

sprints_df=(
    spark.read.table(f'{catalog_name}.{schema}.silver_sprints')
    .withColumn('session_type',F.lit('SPRINT'))
    .drop('race_name','race_date','ingestion_timestamp','souce_file')
)

# COMMAND ----------

results_sprints_df=results_df.unionByName(sprints_df,allowMissingColumns=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5 - Derive additional columns

# COMMAND ----------

fact_session_results_df=(
    results_sprints_df
    .withColumn('is_win', F.col('finish_position')==1)
    .withColumn('is_podium',F.col('finish_position').between(1,3))
    .withColumn('has_points',F.col('points')>0)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5 - Write the tranformed data to gold `dim_constructors` table

# COMMAND ----------

(
    fact_session_results_df
    .write
    .format('delta')
    .mode('overwrite')
    .saveAsTable(target_table)
)