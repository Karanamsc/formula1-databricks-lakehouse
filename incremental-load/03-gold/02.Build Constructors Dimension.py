# Databricks notebook source
# MAGIC %md
# MAGIC ## Build Constructors Dimension
# MAGIC 1. Read silver `constructors` table
# MAGIC 2. Read silver `ref_nationality_region` table
# MAGIC 3. Join data from `constructors` with `ref_nationality_region` using `nationality`
# MAGIC 4. Select required columns - `constructors.constructor_id`,`constructors.constructor_name`,`constructors.constructor.nationality`,`ref_nationality_region.region`
# MAGIC 5. Write the tranformed data to gold `dim_constructors` table

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

target_table=f'{catalog_name}.{schema}.dim_constructors'

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 to 4 - Read tables, join data and select columns

# COMMAND ----------

constructors_df=(
    spark.read.table(f'{catalog_name}.{schema}.silver_constructors')
    .filter(F.col('batch_id') == v_batch_id)
)
ref_nationality_region_df=(
    spark.read.table(f'{catalog_name}.{schema}.ref_nationality_region')
)

# COMMAND ----------

dim_constructors=(
    constructors_df.join(ref_nationality_region_df, 'nationality', 'left')
    .select(constructors_df.constructor_id,
            constructors_df.constructor_name,
            constructors_df.nationality,
            ref_nationality_region_df.region.alias('nationality_region')
        )
) 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5 - Write the tranformed data to gold `dim_constructors` table

# COMMAND ----------

write_to_gold(
    input_df=dim_constructors,
    target_table=target_table,
    merge_condition='t.constructor_id = s.constructor_id',
    columns_to_update=[
        'constructor_name',
        'nationality',
        'nationality_region'
    ]
)

# COMMAND ----------

display(spark.table(target_table))