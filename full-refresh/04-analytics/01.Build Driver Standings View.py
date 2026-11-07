# Databricks notebook source
# MAGIC %md
# MAGIC ## Build Constructor Standings
# MAGIC
# MAGIC **Sources**
# MAGIC 1. fact_session_results
# MAGIC 2. dim_constructors
# MAGIC
# MAGIC **Output Columns**
# MAGIC
# MAGIC 1. season
# MAGIC 2. constructor id
# MAGIC 3. constructor name
# MAGIC 4. nationality
# MAGIC 5. race starts
# MAGIC 6. total points
# MAGIC 7. number of wins
# MAGIC 8. number of podiums
# MAGIC 9. standing position

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config
# MAGIC

# COMMAND ----------

fact_session_results=f'{catalog_name}.{schema}.fact_session_results'
dim_drivers=f'{catalog_name}.{schema}.dim_constructors'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW databricks_dev.skaranam_test.v_constructor_standings AS
# MAGIC WITH constructor_session_summary AS(SELECT r.season, 
# MAGIC        c.constructor_id, 
# MAGIC        c.constructor_name,
# MAGIC        c.nationality,
# MAGIC        COUNT(*) AS race_starts,
# MAGIC        SUM(r.points) AS total_points,
# MAGIC        COUNT_IF(r.is_win) AS number_of_wins,
# MAGIC        COUNT_IF(r.is_podium) AS number_of_podiums
# MAGIC     FROM databricks_dev.skaranam_test.fact_session_results r
# MAGIC     JOIN databricks_dev.skaranam_test.dim_constructors c
# MAGIC     ON r.constructor_id = c.constructor_id
# MAGIC     GROUP BY r.season, 
# MAGIC        c.constructor_id, 
# MAGIC        c.constructor_name,
# MAGIC        c.nationality)
# MAGIC
# MAGIC SELECT *,
# MAGIC     RANK() OVER(PARTITION BY season ORDER BY total_points DESC, number_of_wins DESC) AS standing
# MAGIC     FROM constructor_session_summary

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM  databricks_dev.skaranam_test.v_constructor_standings where season=2025;

# COMMAND ----------

