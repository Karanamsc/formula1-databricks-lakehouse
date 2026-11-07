# Databricks notebook source
# MAGIC %md
# MAGIC ## Build Constructors Dimenion
# MAGIC 1. Read silver `constructors` table
# MAGIC 2. Read silver `circuits` table
# MAGIC 3. Join data from `races` with `circuits` using `circuit_id`
# MAGIC 4. Select required columns - `races.season`,`races.round`,`races.race_name`,`races.race_date`,`circuits.circuit_name`,`circuits.locality`,`circuits.country`
# MAGIC 5. Write the tranformed data to gold `dim_races` table
# MAGIC
# MAGIC For incremental load processing
# MAGIC
# MAGIC 1. Accept batch_id as a parameter to notebook
# MAGIC 2. Process data only for the batch_id being passed in (i.e. filter reading from silver using batch_id)
# MAGIC 3. Add created_timestamp,updated_timestamp and batch_id to the gold table
# MAGIC 4. Merge the processed data to gold table
# MAGIC     - created_timestamp should only be populated at the time of inserting/creating the record. It should not be updated during the merge update

# COMMAND ----------

dbutils.widgets.text('p_batch_id','')
v_batch_id = dbutils.widgets.get('p_batch_id')

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00-common/04.gold-helpers

# COMMAND ----------

target_table=f'{catalog_name}.{schema}.dim_races'

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 to 4 - Read tables, join data and select columns

# COMMAND ----------

circuits_df=(
    spark.read.table(f'{catalog_name}.{schema}.silver_circuits')
        .filter(F.col('batch_id') == v_batch_id)
)
races_df=(
    spark.read.table(f'{catalog_name}.{schema}.silver_races')
    .filter(F.col('batch_id') == v_batch_id)
)

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

write_to_gold(
    input_df=dim_races,
    target_table=target_table,
    merge_condition='t.season = s.season AND t.round = s.round',
    columns_to_update=[
        'race_name',
        'race_date',
        'circuit_name',
        'locality',
        'country'
    ]
)