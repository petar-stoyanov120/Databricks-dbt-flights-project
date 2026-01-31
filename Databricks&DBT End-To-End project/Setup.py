# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE VOlUME databricksdbt_flyight_project.raw.rawvolume

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME databricksdbt_flyight_project.gold.goldvolume

# COMMAND ----------

dbutils.fs.mkdirs("/Volumes/databricksdbt_flyight_project/raw/rawvolume/rawdata/airports")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta.`/Volumes/databricksdbt_flyight_project/bronze/bronzevolume/flights/data/`

# COMMAND ----------

