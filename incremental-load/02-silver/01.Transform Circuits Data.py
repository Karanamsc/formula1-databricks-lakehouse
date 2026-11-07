# Databricks notebook source
# MAGIC %md
# MAGIC ## Transform Circuits Data
# MAGIC 1. Read bronze `circuits` table
# MAGIC 2. Keep only the columns required for analytics (Drop `url` column)
# MAGIC 3. Standardise column names using snake_case (`circuitId` → `circuit_id`, `circuitName` → `circuit_name`)
# MAGIC 4. Rename columns to make them more meaningful (`lat` → `latitude`, `long` → `longitude`)
# MAGIC 5. Filter out rows where `circuit_id` is null (business key validation)
# MAGIC 6. Remove duplicate records
# MAGIC 7. Transform values of columns `circuit_name` and `locality` to Title Case
# MAGIC 8. Write the transformed data to silver `circuits` table
# MAGIC
# MAGIC For incremental load processing
# MAGIC
# MAGIC 1. Accept batch_id as a parameter to notebook
# MAGIC 2. Process data only for the batch_id being passed in (i.e. filter reading from bronze using batch_id)
# MAGIC 3. Add created_timestamp,updated_timestamp and batch_id to the silver table
# MAGIC 4. Merge the processed data to silver table
# MAGIC     - created_timestamp should only be populated at the time of inserting/creating the record. It should not be updated during the merge update
# MAGIC     - Ensure that we are not overwriting the data in silver table by older bronze data (re-run scenario)      

# COMMAND ----------

dbutils.widgets.text('p_batch_id','')
v_batch_id = dbutils.widgets.get('p_batch_id')

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00-common/03.silver-helpers

# COMMAND ----------

bronze_table = f'{catalog_name}.{schema}.bronze_circuits'
silver_table = f'{catalog_name}.{schema}.silver_circuits'

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 to 4 - Read and transform the data

# COMMAND ----------

circuits_df= (
    spark.read.table(bronze_table)
    .filter(F.col('batch_id')==v_batch_id)
    .select(F.col('circuitId'),
            F.col('circuitName'),
            F.col('lat'),
            F.col('long'),
            F.col('locality'),
            F.col('country'),
            F.col('injestion_timestamp'),
            F.col('souce_file'),
            F.col('batch_id'))
    .withColumnsRenamed({'circuitId':'circuit_id',
                         'circuitName':'circuit_name',
                         'lat':'latitude',
                         'long':'longitude'})
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5 to 7 - Filter rows, remove duplicates and transform column values

# COMMAND ----------

circuits_final_df=(
    circuits_df
    .filter(F.col('circuit_id').isNotNull())
    .dropDuplicates(['circuit_id'])
    .withColumn('circuit_name',F.initcap(F.col('circuit_name')))
    .withColumn('locality',F.initcap(F.col('locality')))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 8 - Write the transformed data to silver circuits table

# COMMAND ----------

write_to_silver(
        input_df=circuits_final_df,
        target_table=silver_table,
        merge_condition="t.circuit_id = s.circuit_id",
        columns_to_update=[
            'circuit_name',
            'latitude',
            'longitude',
            'locality',
            'country',
            'injestion_timestamp',
            'souce_file',
            'batch_id'
        ]
)