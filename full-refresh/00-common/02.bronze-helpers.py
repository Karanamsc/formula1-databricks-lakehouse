# Databricks notebook source
from pyspark.sql import functions as F

def add_ingestion_metadata(df):
    return (
        df.withColumn('injestion_timestamp',F.current_timestamp())
         .withColumn('souce_file',F.col('_metadata.file_path'))
    )
