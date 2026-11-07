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

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

bronze_table = f'{catalog_name}.{schema}.bronze_constructors'
silver_table = f'{catalog_name}.{schema}.silver_constructors'

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 to 4 - Read and transform the data

# COMMAND ----------

constructors_df=(
    spark.read.table(bronze_table)
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

(
    constructors_final_df
    .write
    .format('delta')
    .mode('overwrite')
    .saveAsTable(silver_table)
)