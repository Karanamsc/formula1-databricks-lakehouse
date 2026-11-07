# Databricks notebook source
from pyspark.sql import functions as F

def add_ingestion_metadata(df):
    return (
        df.withColumn('injestion_timestamp',F.current_timestamp())
         .withColumn('souce_file',F.col('_metadata.file_path'))
    )


# COMMAND ----------

def write_to_bronze(input_df,table_name,batch_id):
    final_df=input_df.withColumn('batch_id',F.lit(batch_id))
    (
            final_df
                .write
                .mode('overwrite')
                .format('delta')
                .partitionBy('batch_id')
                .option('replaceWhere',f"batch_id='{batch_id}'")
                .saveAsTable(table_name)
    )