# Databricks notebook source
# MAGIC %md
# MAGIC ## Real-Time Bidding - ML model to predict viewability
# MAGIC 
# MAGIC We previously saw how to create a DLT pipeline to ingest and prepare the data required for our model. This notebook will show how we can take this one take further and leverage this data to predict ad viewability, improving ads performance and ultimately customer aqcuisition.
# MAGIC 
# MAGIC ## Step 5: Train in-view classification model (Using XG Boost)
# MAGIC 
# MAGIC <img style="float: right; padding-left: 10px" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/media/resources/images/rtb-pipeline-dlt-4.png" width="600"/>
# MAGIC 
# MAGIC Now that our data is analyzed, we can start building a ML model to predict viewability (the user likely to click on the ad `in_view`) using XGBoost.
# MAGIC 
# MAGIC We'll be leveraging hyperopt to do hyperparameter tuning and find the best set of hyperparameters for our model.
# MAGIC 
# MAGIC <!-- do not remove -->
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Fmedia%2Frtb%2Fnotebook_ml&dt=MEDIA_USE_CASE">
# MAGIC <!-- [metadata={"description":"Build ML model to predict viewability.</i>", "authors":["layla.yang@databricks.com"]}] -->

# COMMAND ----------

# MAGIC %run ./_resources/00-setup $reset_all_data=false

# COMMAND ----------

# DBTITLE 1,Create train and test data sets
features =['device_w','device_connectiontype','device_devicetype','device_lat','imp_bidfloor']

df = spark.table('field_demos_media.rtb_dlt_bids_gold').toPandas()
X_train, X_test, y_train, y_test = train_test_split(df[features], df['in_view'], test_size=0.33, random_state=55)

# COMMAND ----------

# DBTITLE 1,Define model evaluation for hyperopt
def evaluate_model(params):
  #instantiate model
  model = XGBClassifier(learning_rate=params["learning_rate"],
                            gamma=int(params["gamma"]),
                            reg_alpha=int(params["reg_alpha"]),
                            reg_lambda=int(params["reg_lambda"]),
                            max_depth=int(params["max_depth"]),
                            n_estimators=int(params["n_estimators"]),
                            min_child_weight = params["min_child_weight"], objective='reg:linear', early_stopping_rounds=50)
  
  #train
  model.fit(X_train, y_train)
  
  #predict
  y_prob = model.predict_proba(X_test)
  
  #score
  precision = average_precision_score(y_test, y_prob[:,1])
  mlflow.log_metric('avg_precision', precision)  # record actual metric with mlflow run
  
  # return results (negative precision as we minimize the function)
  return {'loss': -precision, 'status': STATUS_OK, 'model': model}

# COMMAND ----------

# DBTITLE 1,Define search space for hyperopt
# define hyperopt search space
search_space = {'max_depth': scope.int(hp.quniform('max_depth', 2, 8, 1)),
                'learning_rate': hp.loguniform('learning_rate', -3, 0),
                'gamma': hp.uniform('gamma', 0, 5),
                'reg_alpha': hp.loguniform('reg_alpha', -5, -1),
                'reg_lambda': hp.loguniform('reg_lambda', -6, -1),
                'min_child_weight': scope.int(hp.loguniform('min_child_weight', -1, 3)),
                'n_estimators':  scope.int(hp.quniform('n_estimators', 50, 200, 1))}

# COMMAND ----------

# DBTITLE 1,Perform evaluation to find optimal hyperparameters
# perform evaluation
with mlflow.start_run(run_name='XGBClassifier') as run:
  trials = SparkTrials(parallelism=4)
  argmin = fmin(fn=evaluate_model, space=search_space, algo=tpe.suggest, max_evals=20, trials=trials)
  #log the best model information
  model = trials.best_trial['result']['model']
  signature = infer_signature(X_test, model.predict_proba(X_test))
  mlflow.sklearn.log_model(trials.best_trial['result']['model'], 'model', signature=signature, input_example=X_test.iloc[0].to_dict())
  #add hyperopt model params
  for p in argmin:
    mlflow.log_param(p, argmin[p])
  mlflow.log_metric("avg_precision", trials.best_trial['result']['loss'])
  run_id = run.info.run_id

# COMMAND ----------

# MAGIC %md-sandbox 
# MAGIC 
# MAGIC **Why is it so great?**
# MAGIC 
# MAGIC <div style="float:right"><img src="https://quentin-demo-resources.s3.eu-west-3.amazonaws.com/images/tuning-2.gif" style="height: 280px; margin-left: 20px"/></div>
# MAGIC 
# MAGIC - Trials are automatically logged in MLFlow! It's then easy to compare all the runs and understand how each parameter play a role in the model
# MAGIC - Job by providing a `SparkTrial` instead of the standard `Trial`, the training and tuning is automatically paralellized in your cluster
# MAGIC - Training can easily be launched as a job and model deployment automatized based on the best model performance

# COMMAND ----------

# DBTITLE 1,Save our new model to the registry as a new version
model_registered = mlflow.register_model("runs:/"+run_id+"/model", "field_demos_rtb")

# COMMAND ----------

# DBTITLE 1,Flag this version as production ready
client = mlflow.tracking.MlflowClient()
print("registering model version "+model_registered.version+" as production model")
client.transition_model_version_stage(name = "field_demos_rtb", version = model_registered.version, stage = "Production", archive_existing_versions=True)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Step 6: Use model to predict viewability
# MAGIC 
# MAGIC <img style="float: right; padding-left: 10px" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/media/resources/images/rtb-pipeline-dlt-5.png" width="600"/>
# MAGIC 
# MAGIC Now that our model is built and saved in MLFlow registry, we can load it to run our inferences at scale.
# MAGIC 
# MAGIC This can be done:
# MAGIC 
# MAGIC * In batch or streaming (ex: refresh every night)
# MAGIC   * Using a standard notebook job
# MAGIC   * Or as part of the DLT pipeline we built
# MAGIC * In real-time over a REST API, deploying Databricks serving capabilities
# MAGIC 
# MAGIC In the following cell, we'll focus on deploying the model in this notebook directly

# COMMAND ----------

# DBTITLE 1,load the model from registry as UDF
#                                 Stage/version
#                       Model name       |
#                           |            |
model_path = 'models:/field_demos_rtb/Production'
predict_in_view = mlflow.pyfunc.spark_udf(spark, model_path, result_type = DoubleType())


# COMMAND ----------

# DBTITLE 1,Perform Inference on Streaming Data
model_features = predict_in_view.metadata.get_input_schema().input_names()
new_df = spark.table('field_demos_media.rtb_dlt_bids_gold').select(*model_features)
display(
  new_df.withColumn('in_view_prediction', predict_in_view(*model_features)).filter(col('in_view_prediction') == 1)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## What's next?
# MAGIC We now have or viewability prediction, and we'll be able to leverage this information to improve our customer aqcuisition efficiency.
# MAGIC 
# MAGIC As Summary, we demonstrate the Lakehouse benefits:
# MAGIC 
# MAGIC * How Delta Live Table can simplify data ingestion
# MAGIC * How Databricks can use DBSQL to run BI workload on top of ingesting data 
# MAGIC * How the same data can be used by the Data Science team to not only analyze existing data, but react and make prediction on this information

# COMMAND ----------

# MAGIC %md
# MAGIC Copyright Databricks, Inc. [2021]. The source in this notebook is provided subject to the [Databricks License](https://databricks.com/db-license-source).  All included or referenced third party libraries are subject to the licenses set forth below.
# MAGIC 
# MAGIC |Library Name|Library license | Library License URL | Library Source URL |
# MAGIC |---|---|---|---|
# MAGIC |pandas|BSD 3-Clause License|https://github.com/pandas-dev/pandas/blob/main/LICENSE|https://github.com/pandas-dev/pandas|
# MAGIC |hyperopt|BSD License (BSD)|https://github.com/hyperopt/hyperopt/blob/master/LICENSE.txt|https://github.com/hyperopt/hyperopt|
# MAGIC |xgboost|Apache License 2.0|https://github.com/dmlc/xgboost/blob/master/LICENSE|https://github.com/dmlc/xgboost|
# MAGIC |scikit-learn|BSD 3-Clause "New" or "Revised" License|https://github.com/scikit-learn/scikit-learn/blob/main/COPYING|https://github.com/scikit-learn/scikit-learn|
# MAGIC |mlflow|Apache-2.0 License |https://github.com/mlflow/mlflow/blob/master/LICENSE.txt|https://github.com/mlflow/mlflow|
# MAGIC |Python|Python Software Foundation (PSF) |https://github.com/python/cpython/blob/master/LICENSE|https://github.com/python/cpython|
# MAGIC |Spark|Apache-2.0 License |https://github.com/apache/spark/blob/master/LICENSE|https://github.com/apache/spark|
