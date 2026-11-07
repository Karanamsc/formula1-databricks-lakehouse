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

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

bronze_table = f'{catalog_name}.{schema}.bronze_drivers'
silver_table = f'{catalog_name}.{schema}.silver_drivers'

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 to 4 - Read and transform the data

# COMMAND ----------

drivers_df=(
    spark.read.table(bronze_table)
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

(
    drivers_final_df
    .write
    .format('delta')
    .mode('overwrite')
    .saveAsTable(silver_table)
)