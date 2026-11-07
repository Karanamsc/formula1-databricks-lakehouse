# Databricks notebook source
# MAGIC %md
# MAGIC ## Transform Races Data
# MAGIC 1. Read bronze `races` table
# MAGIC 2. Keep only the columns required for analytics (Drop `url` column)
# MAGIC 3. Standardise column names using snake_case (`raceName` → `race_name`, `circuitId` → `circuit_id`)
# MAGIC 4. Rename columns to make them more meaningful (`date` → `race_date`)
# MAGIC 5. Filter out rows where `season` and `round` is null (business key validation)
# MAGIC 6. Remove duplicate records
# MAGIC 7. Transform values of columns `circuit_id` and `race_name` to Title Case
# MAGIC 8. Write the transformed data to silver `races` table

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

bronze_table = f'{catalog_name}.{schema}.bronze_races'
silver_table = f'{catalog_name}.{schema}.silver_races'

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 to 4 - Read and transform the data

# COMMAND ----------

races_df= (
    spark.read.table(bronze_table)
    .select(F.col('season'),
            F.col('round'),
            F.col('raceName'),
            F.col('date'),
            F.col('circuitId'),
            F.col('injestion_timestamp'),
            F.col('souce_file'))
    .withColumnsRenamed({'circuitId':'circuit_id',
                         'raceName':'race_name',
                         'date':'race_date'})
) #spark.table()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5 to 7 - Filter rows, remove duplicates and transform column values

# COMMAND ----------

races_final_df=(
    races_df
    .filter(F.col('season').isNotNull() & F.col('round').isNotNull())
    .dropDuplicates(['season','round'])
    .withColumn('race_name',F.initcap(F.col('race_name')))
)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 8 - Write the transformed data to silver races table

# COMMAND ----------

(
    races_final_df
    .write
    .format('delta')
    .mode('overwrite')
    .saveAsTable(silver_table)
)