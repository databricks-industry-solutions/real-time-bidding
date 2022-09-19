## Real-Time Bidding

**Use Case Overview**

Real-time bidding (**RTB**): is a subcategory of programmatic media buying. RTB firms established the technology of buying and selling ads in real time (~ 10ms ) in an instant auction, on a per-impression basis.

* The selling-buying cycle includes: publishers, a supply-side platform (SSP) or an ad exchange, a demand-side platform (DSP), and advertisers
* The value of RTB is that it creates greater transparency for both publishers and advertisers in the the ad market: 
  * Publishers can better control their inventory and CPMs (cost per 1000 ad impressions) 
  * Advertisers that leverage RTB can boost advertising effectiveness by only bidding on impressions that are likely to be **viewed** by a given user.

**Viewability** is a metric that measures whether or not an ad was actually seen by a user. This gives marketers a more precise measurement about whether or not their message appeared to users in a visible way.
* In this Databricks demo, we demonstrate a process to predict viewability using BidRequest Data. Keep in mind, the more likely users are to see an ad, the higher the price a DSPs will want to place on a bid for that ad, because it is ultimately more valueable to the advertiser.
* By building a reliable, scalable, and efficient pipeline to predict viewability, advertisers can more accurately identify where to spend their marketing budgets to fine-tune media spend, improve ROI, and enhance campaign effectiveness.


We'll implement the following data pipeline for RTB:

<img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/media/resources/images/rtb-pipeline-dlt.png" width="1000"/>


___

&copy; 2022 Databricks, Inc. All rights reserved. The source in this notebook is provided subject to the Databricks License [https://databricks.com/db-license-source].  All included or referenced third party libraries are subject to the licenses set forth below.

|Library Name|Library license | Library License URL | Library Source URL |
|---|---|---|---|
|pandas|BSD 3-Clause License|https://github.com/pandas-dev/pandas/blob/main/LICENSE|https://github.com/pandas-dev/pandas|
|hyperopt|BSD License (BSD)|https://github.com/hyperopt/hyperopt/blob/master/LICENSE.txt|https://github.com/hyperopt/hyperopt|
|xgboost|Apache License 2.0|https://github.com/dmlc/xgboost/blob/master/LICENSE|https://github.com/dmlc/xgboost|
|scikit-learn|BSD 3-Clause "New" or "Revised" License|https://github.com/scikit-learn/scikit-learn/blob/main/COPYING|https://github.com/scikit-learn/scikit-learn|
|mlflow|Apache-2.0 License |https://github.com/mlflow/mlflow/blob/master/LICENSE.txt|https://github.com/mlflow/mlflow|
|Python|Python Software Foundation (PSF) |https://github.com/python/cpython/blob/master/LICENSE|https://github.com/python/cpython|
|Spark|Apache-2.0 License |https://github.com/apache/spark/blob/master/LICENSE|https://github.com/apache/spark|

To run this accelerator, clone this repo into a Databricks workspace. Attach the RUNME notebook to any cluster running a DBR 11.0 or later runtime, and execute the notebook via Run-All. A multi-step-job describing the accelerator pipeline will be created, and the link will be provided. Execute the multi-step-job to see how the pipeline runs.

The job configuration is written in the RUNME notebook in json format. The cost associated with running the accelerator is the user's responsibility.
