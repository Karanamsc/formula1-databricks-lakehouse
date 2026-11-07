# Databricks notebook source
# MAGIC %md
# MAGIC ## Injest circuits.csv file
# MAGIC 1. Read the file using spark dataframe reader API
# MAGIC 2. Add Metadata Columns
# MAGIC     - Souce File
# MAGIC     - Injestion Timestamp
# MAGIC 3. Write bronze delta layer

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00-common/02.bronze-helpers

# COMMAND ----------

landing_folder_path

# COMMAND ----------

races_source_file= f'{landing_folder_path}/races.csv'
circuits_source_file= f'{landing_folder_path}/circuits.csv'
constructors_source_file= f'{landing_folder_path}/constructors.json'
drivers_source_file= f'{landing_folder_path}/drivers.json'
results_source_folder= f'{landing_folder_path}/results'
sprints_source_folder= f'{landing_folder_path}/sprints'
races_table_name=f'{catalog_name}.{schema}.bronze_races'
circuits_table_name=f'{catalog_name}.{schema}.bronze_circuits'
constructors_table_name=f'{catalog_name}.{schema}.bronze_constructors'
drivers_table_name=f'{catalog_name}.{schema}.bronze_drivers'
results_table_name=f'{catalog_name}.{schema}.bronze_results'
sprints_table_name=f'{catalog_name}.{schema}.bronze_sprints'

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 - Read the csv file using dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, DateType

races_schema=StructType([
    StructField('season',IntegerType()),
    StructField('round',IntegerType()),
    StructField('url',StringType()),
    StructField('raceName',StringType()),
    StructField('date',DateType()),
    StructField('circuitId',StringType()),
])


circuits_schema=StructType([
    StructField('circuitId', StringType()),
    StructField('url', StringType()),
    StructField('circuitName', StringType()),
    StructField('lat', DoubleType()),
    StructField('long', DoubleType()),
    StructField('locality', StringType()),
    StructField('country', StringType())
])

constructors_schema ="""constructorId STRING, 
                        name STRING, 
                        nationality STRING,
                        url STRING"""

drivers_name_schema= StructType([
    StructField('givenName',StringType()),
    StructField('familyName',StringType()),
])

drivers_schema=StructType([
    StructField('driverId',StringType()),
    StructField('name',drivers_name_schema),
    StructField('dateOfBirth',DateType()),
    StructField('nationality',StringType()),
    StructField('url',StringType()),
])

results_schema=StructType([
    StructField('date',DateType()),
    StructField('raceName',StringType()),
    StructField('round',IntegerType()),
    StructField('season',IntegerType()),
    StructField('url',StringType()),
    StructField('constructorId',StringType()),
    StructField('driverId',StringType()),
    StructField('grid',IntegerType()),
    StructField('laps',IntegerType()),
    StructField('number',IntegerType()),
    StructField('points', DoubleType()),
    StructField('position',IntegerType()),
    StructField('positionText',StringType()),
    StructField('status',StringType()),
])

sprints_schema=StructType([
    StructField('date',DateType()),
    StructField('raceName',StringType()),
    StructField('round',IntegerType()),
    StructField('season',IntegerType()),
    StructField('url',StringType()),
    StructField('constructorId',StringType()),
    StructField('driverId',StringType()),
    StructField('grid',IntegerType()),
    StructField('laps',IntegerType()),
    StructField('number',IntegerType()),
    StructField('points', DoubleType()),
    StructField('position',IntegerType()),
    StructField('positionText',StringType()),
    StructField('status',StringType()),
])


"""drivers_schema = '''driverId STRING,
                    name STRUCT<givenName: STRING, familyName: STRING>,
                    dateOfBirth DATE,
                    nationality STRING,
                    url STRING''' """

# COMMAND ----------

races_df=(
    spark.read
    .format('csv')
    .option('header',True)
    .option('mode','FAILFAST')
    .schema(races_schema)
    .load(races_source_file)
)

# COMMAND ----------

circuits_df=(
    spark.read
    .format('csv')
    .option('header',True)
#    .option('inferSchema', True)
    .option('mode','FAILFAST')
    .schema(circuits_schema)
    .load(circuits_source_file)
)


# COMMAND ----------

constructors_df=(
    spark.read
    .format('json')
    .option('mode','FAILFAST')
    .schema(constructors_schema)
    .load(constructors_source_file)
)

# COMMAND ----------

drivers_df=(
    spark.read
    .format('json')
    .option('mode','FAILFAST')
    .schema(drivers_schema)
    .load(drivers_source_file)
)

# COMMAND ----------

results_df=(
    spark.read
    .format('json')
    .option('mode','FAILFAST')
    .schema(results_schema)
    .load(results_source_folder)
)

# COMMAND ----------

sprints_df=(
    spark.read
    .format('json')
    .option('mode','FAILFAST')
    .schema(sprints_schema)
    .option('multiLine',True)
    .load(sprints_source_folder)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 - Add Metadata columns

# COMMAND ----------

from pyspark.sql import functions as F
races_final_df=add_ingestion_metadata(races_df)
circuits_final_df=add_ingestion_metadata(circuits_df)
constructors_final_df=add_ingestion_metadata(constructors_df)
drivers_final_df=add_ingestion_metadata(drivers_df)
results_final_df=add_ingestion_metadata(results_df)
sprints_final_df=add_ingestion_metadata(sprints_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 - Write to bronze delta table

# COMMAND ----------

(
    races_final_df
    .write
    .mode('overwrite')
    .format('delta')
    .saveAsTable(races_table_name)
)

(
    circuits_final_df
    .write
    .mode('overwrite')
    .format('delta')
    .saveAsTable(circuits_table_name)
)

(
    constructors_final_df
    .write
    .mode('overwrite')
    .format('delta')
    .saveAsTable(constructors_table_name)
)

(
    drivers_final_df
    .write
    .mode('overwrite')
    .format('delta')
    .saveAsTable(drivers_table_name)
)

(
    results_final_df
    .write
    .mode('overwrite')
    .format('delta')
    .saveAsTable(results_table_name)
)

(
    sprints_final_df
    .write
    .mode('overwrite')
    .format('delta')
    .saveAsTable(sprints_table_name)
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM databricks_dev.skaranam_test.bronze_circuits;

# COMMAND ----------

display(spark.table(circuits_table_name))