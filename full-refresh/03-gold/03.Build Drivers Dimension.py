# Databricks notebook source
# MAGIC %md
# MAGIC ## Build Drivers Dimension
# MAGIC 1. Read silver `drivers` table
# MAGIC 2. Read silver `ref_nationality_region` table
# MAGIC 3. Join data from `drivers` with `ref_nationality_region` using `nationality`
# MAGIC 4. Select required columns - `drivers.driver_id`,`drivers.driver_name`,`drivers.date_of_birth`,`drivers.nationality`,`ref_nationality_region.region`
# MAGIC 5. Write the tranformed data to gold `dim_drivers` table

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

target_table=f'{catalog_name}.{schema}.dim_drivers'

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 to 4 - Read tables, join data and select columns

# COMMAND ----------

drivers_df=spark.read.table(f'{catalog_name}.{schema}.silver_drivers')
ref_nationality_region_df=spark.read.table(f'{catalog_name}.{schema}.ref_nationality_region')

# COMMAND ----------

dim_drivers=(
    drivers_df.join(ref_nationality_region_df, 'nationality', 'left')
    .select(drivers_df.driver_id,
            drivers_df.driver_name,
            drivers_df.date_of_birth,
            drivers_df.nationality,
            ref_nationality_region_df.region.alias('nationality_region')
        )
) 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5 - Write the tranformed data to gold `dim_constructors` table

# COMMAND ----------

(
    dim_drivers
    .write
    .format('delta')
    .mode('overwrite')
    .saveAsTable(target_table)
)