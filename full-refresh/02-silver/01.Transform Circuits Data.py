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

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

bronze_table = f'{catalog_name}.{schema}.bronze_circuits'
silver_table = f'{catalog_name}.{schema}.silver_circuits'

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 to 4 - Read and transform the data

# COMMAND ----------

circuits_df= (
    spark.read.table(bronze_table)
    .select(F.col('circuitId'),
            F.col('circuitName'),
            F.col('lat'),
            F.col('long'),
            F.col('locality'),
            F.col('country'),
            F.col('injestion_timestamp'),
            F.col('souce_file'))
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

(
    circuits_final_df
    .write
    .format('delta')
    .mode('overwrite')
    .saveAsTable(silver_table)
)