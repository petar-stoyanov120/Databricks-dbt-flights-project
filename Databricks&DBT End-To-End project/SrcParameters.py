# Databricks notebook source
src_array = [

    {"src":"bookings"},
    {"src":"customers"},
    {"src":"airports"},
    {"src":"flights"}
    ]

# COMMAND ----------

dbutils.jobs.taskValues.set(key = "output_key", value = src_array)

# COMMAND ----------

