# Databricks notebook source
# MAGIC %md
# MAGIC ## Transform Drivers Data
# MAGIC 1. Read bronze `drivers` table
# MAGIC 2. Keep only the columns required for analytics (Drop `url` column)
# MAGIC 3. Standardise column names using snake_case (`driverId` → `driver_id`, `dateOfbirth` → `date_of_birth`)
# MAGIC 4. Concatenate `name.givenName` and `name.familyName` to create new column called `driver_name` and transform value to Title Case
# MAGIC 5. Filter out rows where `driver_id` is null (business key validation)
# MAGIC 6. Remove duplicate records
# MAGIC 7. Transform values of columns `nationality` to Title Case
# MAGIC 8. Write the transformed data to silver `drivers` table
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

bronze_table = f'{catalog_name}.{schema}.bronze_drivers'
silver_table = f'{catalog_name}.{schema}.silver_drivers'

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 to 4 - Read and transform the data

# COMMAND ----------

drivers_df=(
    spark.read.table(bronze_table)
    .filter(F.col('batch_id')==batch_id)
    .drop(F.col('url'))
    .withColumnsRenamed({'driverId':'driver_id',
                         'dateOfBirth':'date_of_birth'})
    .withColumn('driver_name',F.initcap(F.concat_ws(' ',F.col('name.givenName'),F.col('name.familyName'))))
    .drop(F.col('name'))
) 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5 - 6 - Apply Data Quality Checks

# COMMAND ----------

drivers_valid_df=(
    drivers_df
    .filter(F.col('driver_id').isNotNull())
    .dropDuplicates(['driver_id'])
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 7 - Transform values of columns nationality to Title Case

# COMMAND ----------

drivers_final_df=(
    drivers_valid_df
    .withColumn('nationality',F.initcap(F.col('nationality')))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 8 - Write the transformed data to silver drivers table

# COMMAND ----------

write_to_silver(
        input_df=drivers_final_df,
        target_table=silver_table,
        merge_condition="t.driver_id = s.driver_id",
        columns_to_update=[
            'driver_name',
            'date_of_birth',
            'nationality',
            'injestion_timestamp',
            'souce_file',
            'batch_id'
        ]
)