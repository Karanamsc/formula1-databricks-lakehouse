# Databricks notebook source
# MAGIC %md
# MAGIC ## Transform Results Data
# MAGIC 1. Read bronze `results` table
# MAGIC 2. Keep only the columns required for analytics (Drop `url` column)
# MAGIC 3. Standardise column names using snake_case (`constructorId` → `constructor_id`,`driverId` → `driver_id`, `raceName` → `race_name`,`positionText` → `finish_position_text`)
# MAGIC 4. Rename columns to make them more meaningful (`date` → `race_date`,`grid` → `grid_position`, `laps` → `completed_laps`,`number` → `car_number`,`position`→ `finish_position`)
# MAGIC 5. Filter out rows where `season`,`round`,`constructor_id` or `driver_id` is null (business key validation)
# MAGIC 6. Remove duplicate records
# MAGIC 7. Transform values of columns `race_name` to Title Case
# MAGIC 8. Write the transformed data to silver `results` table
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
batch_id = dbutils.widgets.get('p_batch_id')

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00-common/03.silver-helpers

# COMMAND ----------

bronze_table = f'{catalog_name}.{schema}.bronze_results'
silver_table = f'{catalog_name}.{schema}.silver_results'

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 to 4 - Read and transform the data

# COMMAND ----------

results_df= (
    spark.read.table(bronze_table)
    .filter(F.col('batch_id')==batch_id)
    .drop(F.col('url'))
    .withColumnsRenamed({'constructorId':'constructor_id',
                         'driverId':'driver_id',
                         'raceName':'race_name',
                         'positionText':'final_position_text',
                         'date':'race_date',
                         'grid':'grid_position',
                         'laps':'completed_laps',
                         'number':'car_number',
                         'position':'finish_position'
                    })
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5 & 6 - Apply Data Quality Checks

# COMMAND ----------

results_final_df=(
    results_df
    .filter(F.col('season').isNotNull() & F.col('round').isNotNull() & F.col('constructor_id').isNotNull() & F.col('driver_id').isNotNull())
    .dropDuplicates(['season','round','constructor_id','driver_id'])
    .withColumn('race_name',F.initcap(F.col('race_name')))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 8 - Write the transformed data to silver drivers table

# COMMAND ----------

write_to_silver(
        input_df=results_final_df,
        target_table=silver_table,
        merge_condition="t.season = s.season AND t.round = s.round AND t.constructor_id = s.constructor_id AND t.driver_id = s.driver_id",
        columns_to_update=[
            'race_date',
            'race_name',
            'grid_position',
            'completed_laps',
            'car_number',
            'points',
            'finish_position',
            'final_position_text',
            'status',
            'injestion_timestamp',
            'souce_file',
            'batch_id'
        ]
)