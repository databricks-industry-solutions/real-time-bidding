# Databricks notebook source
# MAGIC %md # setup table for dashboard and interactive DLT demo.

# COMMAND ----------

# MAGIC %sql
# MAGIC create database if not exists field_demos_media;
# MAGIC use field_demos_media;

# COMMAND ----------

# MAGIC %python
# MAGIC for folder in dbutils.fs.ls('/mnt/field-demos/media/rtb'):
# MAGIC   for file in dbutils.fs.ls(folder.path):
# MAGIC     if "raw_incoming" not in file.path:
# MAGIC       #sql = f"drop table field_demos_media.{folder.name[:-1]}_{file.name[:-1]}"
# MAGIC       sql = f"create table if not exists field_demos_media.rtb_{folder.name[:-1]}_{file.name[:-1]} location '{file.path}'"
# MAGIC       print(sql)
# MAGIC       spark.sql(sql)

# COMMAND ----------

# MAGIC %sql show tables
