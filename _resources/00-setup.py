# Databricks notebook source
# MAGIC %pip install category_encoders

# COMMAND ----------

dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")

# COMMAND ----------

import re
import mlflow
import pandas as pd
from category_encoders.hashing import HashingEncoder
from sklearn.model_selection import cross_val_score
from sklearn.model_selection import RepeatedStratifiedKFold
from sklearn.model_selection import train_test_split
from sklearn.metrics import average_precision_score
from hyperopt import fmin, tpe, rand, hp, Trials, STATUS_OK, SparkTrials, space_eval
from mlflow.models.signature import infer_signature
from xgboost import XGBClassifier
from pyspark.sql.functions import col
from pyspark.sql.types import DoubleType
from hyperopt.pyll.base import scope

# COMMAND ----------

dbutils.fs.rm("/tmp/raw_incoming_bid_stream", recurse=True)
dbutils.fs.mkdirs("/tmp/raw_incoming_bid_stream")

# COMMAND ----------

# MAGIC %sh
# MAGIC cp /dbfs/databricks-datasets/media/rtb/raw_incoming_bid_stream/bidRequestSample.txt /dbfs/tmp/raw_incoming_bid_stream/bidRequestSample.txt

# COMMAND ----------

useremail = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
experiment_name = f"/Users/{useremail}/rtb_lite"
mlflow.set_experiment(experiment_name) 

# COMMAND ----------

#db_prefix = "media"
#current_user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
#current_user_no_at = current_user[:current_user.rfind('@')]
#current_user_no_at = re.sub(r'\W+', '_', current_user_no_at)

#dbName = db_prefix+"_"+current_user_no_at
#cloud_storage_path = f"/Users/{current_user}/field_demos/{db_prefix}/rtb/"
#reset_all = dbutils.widgets.get("reset_all_data") == "true"

#if reset_all:
#  spark.sql(f"DROP DATABASE IF EXISTS {dbName} CASCADE")
#  dbutils.fs.rm(cloud_storage_path, True)

#spark.sql(f"""create database if not exists {dbName} LOCATION '{cloud_storage_path}/tables' """)
#spark.sql(f"""USE {dbName}""")

#print("using cloud_storage_path {}".format(cloud_storage_path))
