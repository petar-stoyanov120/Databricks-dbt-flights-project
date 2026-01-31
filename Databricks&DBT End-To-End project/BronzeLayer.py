# Databricks notebook source
# MAGIC %md
# MAGIC ### **INCREMENTAL DATA INGESTION**

# COMMAND ----------

dbutils.widgets.text("src","")

# COMMAND ----------

src_value = dbutils.widgets.get("src")
print(src_value)

# COMMAND ----------

df = spark.readStream.format("cloudFiles")\
        .option("cloudFiles.format","csv")\
        .option("cloudFiles.schemalocation", f"/Volumes/databricksdbt_flyight_project/bronze/bronzevolume/{src_value}/checkpoint")\
        .option("cloudFiles.schemaEvolutionMode", "rescue")\
        .load(f"/Volumes/databricksdbt_flyight_project/raw/rawvolume/rawdata/{src_value}/")



# COMMAND ----------

df.writeStream.format("delta")\
    .outputMode("append")\
    .trigger(once=True)\
    .option("checkpointLocation", f"/Volumes/databricksdbt_flyight_project/bronze/bronzevolume/{src_value}/checkpoint")\
    .option("path", f"/Volumes/databricksdbt_flyight_project/bronze/bronzevolume/{src_value}/data")\
        .start()

# COMMAND ----------

