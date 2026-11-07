# Databricks notebook source
# MAGIC %md
# MAGIC ## Transform Sprints Data
# MAGIC 1. Read bronze `sprints` table
# MAGIC 2. Keep only the columns required for analytics (Drop `url` column)
# MAGIC 3. Standardise column names using snake_case (`constructorId` → `constructor_id`,`driverId` → `driver_id`, `raceName` → `race_name`,`positionText` → `finish_position_text`)
# MAGIC 4. Rename columns to make them more meaningful (`date` → `race_date`,`grid` → `grid_position`, `laps` → `completed_laps`,`number` → `car_number`,`position`→ `finish_position`)
# MAGIC 5. Filter out rows where `season`,`round`,`constructor_id` or `driver_id` is null (business key validation)
# MAGIC 6. Remove duplicate records
# MAGIC 7. Transform values of columns `race_name` to Title Case
# MAGIC 8. Write the transformed data to silver `sprints` table

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

bronze_table=f'{catalog_name}.{schema}.bronze_sprints'
silver_table=f'{catalog_name}.{schema}.silver_sprints'

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 to 4 - Read and transform the data

# COMMAND ----------

sprints_df=(
    spark.read.table(bronze_table)
    .drop(F.col('url'))
    .withColumnsRenamed({'constructorId':'constructor_id',
                         'driverId':'driver_id',
                         'raceName':'race_name',
                         'positionText':'finish_position_text',
                         'date':'race_date',
                         'grid':'grid_position',
                         'laps':'completed_laps',
                         'number':'car_number',
                         'position':'finish_position'})
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5 & 6 - Apply data quality checks

# COMMAND ----------

sprints_valid_df=(
    sprints_df
    .filter(F.col('season').isNotNull() & F.col('round').isNotNull() &
            F.col('constructor_id').isNotNull() & F.col('driver_id').isNotNull())
    .dropDuplicates(['season','round','constructor_id','driver_id'])
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 7 - Transform values of columns `race_name` to Title Case

# COMMAND ----------

sprints_final_df=(
    sprints_valid_df
    .withColumn('race_name',F.initcap(F.col('race_name')))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 8 - Write the transformed data to silver sprints table

# COMMAND ----------

(
    sprints_final_df
    .write
    .format('delta')
    .mode('overwrite')
    .saveAsTable(silver_table)
)