# Databricks notebook source
# MAGIC %md
# MAGIC ## Build Drivers Dimension
# MAGIC 1. Read silver `drivers` table
# MAGIC 2. Read silver `ref_nationality_region` table
# MAGIC 3. Join data from `drivers` with `ref_nationality_region` using `nationality`
# MAGIC 4. Select required columns - `drivers.driver_id`,`drivers.driver_name`,`drivers.date_of_birth`,`drivers.nationality`,`ref_nationality_region.region`
# MAGIC 5. Write the tranformed data to gold `dim_drivers` table

# COMMAND ----------

dbutils.widgets.text('p_batch_id','')
v_batch_id = dbutils.widgets.get('p_batch_id')

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00-common/04.gold-helpers

# COMMAND ----------

target_table=f'{catalog_name}.{schema}.dim_drivers'

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 to 4 - Read tables, join data and select columns

# COMMAND ----------

drivers_df=(
    spark.read.table(f'{catalog_name}.{schema}.silver_drivers')
    .filter(F.col('batch_id') == v_batch_id)
)
ref_nationality_region_df=(
    spark.read.table(f'{catalog_name}.{schema}.ref_nationality_region')
)

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

write_to_gold(
    input_df=dim_drivers,
    target_table=target_table,
    merge_condition='t.driver_id = s.driver_id',
    columns_to_update=[
        'driver_name',
        'date_of_birth',
        'nationality',
        'nationality_region'
    ]
)