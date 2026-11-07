# Databricks notebook source
# MAGIC %md
# MAGIC ## Transform Constructors Data
# MAGIC 1. Read bronze `constructors` table
# MAGIC 2. Keep only the columns required for analytics (Drop `url` column)
# MAGIC 3. Standardise column names using snake_case (`constructorId` → `constructor_id`)
# MAGIC 4. Rename columns to make them more meaningful (`name` → `constructor_name`)
# MAGIC 5. Remove duplicate records
# MAGIC 6. Transform values of columns `nationality` to Title Case
# MAGIC 7. Write the transformed data to silver `constructors` table
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

dbutils.widgets.text('v_batch_id','')
batch_id=dbutils.widgets.get('v_batch_id')

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00-common/03.silver-helpers

# COMMAND ----------

bronze_table = f'{catalog_name}.{schema}.bronze_constructors'
silver_table = f'{catalog_name}.{schema}.silver_constructors'

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 to 4 - Read and transform the data

# COMMAND ----------

constructors_df=(
    spark.read.table(bronze_table)
    .filter(F.col('batch_id')==batch_id)
    .drop(F.col('url'))
    .withColumnsRenamed({'constructorId':'constructor_id',
                        'name':'constructor_name'})
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5 to 7 - Filter rows, remove duplicates and transform column values

# COMMAND ----------

constructors_final_df=(
    constructors_df
    .filter(F.col('constructor_id').isNotNull())
    .dropDuplicates(['constructor_id'])
    .withColumn('nationality',F.initcap(F.col('nationality')))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 8 - Write the transformed data to silver constructors table

# COMMAND ----------

write_to_silver(
        input_df=constructors_final_df,
        target_table=silver_table,
        merge_condition="t.constructor_id = s.constructor_id",
        columns_to_update=[
            'constructor_name',
            'nationality',
            'injestion_timestamp',
            'souce_file',
            'batch_id'
        ]
)