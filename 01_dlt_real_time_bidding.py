# Databricks notebook source
# MAGIC %md
# MAGIC ## Real-Time Bidding
# MAGIC 
# MAGIC **Use Case Overview**
# MAGIC 
# MAGIC Real-time bidding (**RTB**): is a subcategory of programmatic media buying. RTB firms established the technology of buying and selling ads in real time (~ 10ms ) in an instant auction, on a per-impression basis.
# MAGIC 
# MAGIC * The selling-buying cycle includes: publishers, a supply-side platform (SSP) or an ad exchange, a demand-side platform (DSP), and advertisers
# MAGIC * The value of RTB is that it creates greater transparency for both publishers and advertisers in the the ad market: 
# MAGIC   * Publishers can better control their inventory and CPMs (cost per 1000 ad impressions) 
# MAGIC   * Advertisers that leverage RTB can boost advertising effectiveness by only bidding on impressions that are likely to be **viewed** by a given user.
# MAGIC 
# MAGIC **Viewability** is a metric that measures whether or not an ad was actually seen by a user. This gives marketers a more precise measurement about whether or not their message appeared to users in a visible way.
# MAGIC * In this Databricks demo, we demonstrate a process to predict viewability using BidRequest Data. Keep in mind, the more likely users are to see an ad, the higher the price a DSPs will want to place on a bid for that ad, because it is ultimately more valueable to the advertiser.
# MAGIC * By building a reliable, scalable, and efficient pipeline to predict viewability, advertisers can more accurately identify where to spend their marketing budgets to fine-tune media spend, improve ROI, and enhance campaign effectiveness.
# MAGIC 
# MAGIC 
# MAGIC We'll implement the following data pipeline for RTB:
# MAGIC 
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/media/resources/images/rtb-pipeline-dlt.png" width="1000"/>
# MAGIC 
# MAGIC <!-- do not remove -->
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Fmedia%2Frtb%2Fnotebook_de&dt=MEDIA_USE_CASE">
# MAGIC <!-- [metadata={"description":"Build DE pipeline to prep data for Data Analysis and ML.</i>", "authors":["layla.yang@databricks.com"]}] -->

# COMMAND ----------

# MAGIC %run ./_resources/00-setup $reset_all_data=false

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Data Ingestion with Delta Live Table
# MAGIC 
# MAGIC To simplify the ingestion process and accelerate our developments, we'll leverage Delta Live Table (DLT).
# MAGIC 
# MAGIC DLT let you declare your transformations and will handle the Data Engineering complexity for you:
# MAGIC 
# MAGIC - Data quality tracking with expectations
# MAGIC - Continuous or scheduled ingestion, orchestrated as pipeline
# MAGIC - Build lineage and manage data dependencies
# MAGIC - Automating scaling and fault tolerance
# MAGIC 
# MAGIC ### Step 1: Stream real-time bidding data into Delta Lake
# MAGIC 
# MAGIC <img style="float: right; padding-left: 10px" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/media/resources/images/rtb-pipeline-dlt-1.png" width="600"/>
# MAGIC 
# MAGIC Our raw data is being sent to a blob storage. We'll use Databricks autoloader to ingest this information.
# MAGIC 
# MAGIC Autoloader simplify ingestion, including schema inference, schema evolution while being able to scale to millions of incoming files.

# COMMAND ----------

# DBTITLE 1,Create bids_bronze table
import dlt
from pyspark.sql.functions import explode, col
@dlt.table
def bids_bronze():
  # Since Autoloader is a streaming source, this table is incremental (readStream)
  return (
      spark.readStream.format("cloudFiles") ## can change to readStream kinesis, kafka...
       .option("cloudFiles.format", "json")
       .option("cloudFiles.inferColumnTypes", "true")
       .load("/tmp/raw_incoming_bid_stream/"))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from SOLACC_rtb_lite.bids_bronze

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Step 2: Parse nested JSON and create silver tables
# MAGIC 
# MAGIC <img style="float: right; padding-left: 10px" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/media/resources/images/rtb-pipeline-dlt-2.png" width="600"/>
# MAGIC 
# MAGIC The next step is to extract the json from the incoming bronze table and build the 3 silver tables

# COMMAND ----------

# DBTITLE 1,Create bids_device_silver table
@dlt.table
def bids_device_silver():
    # Since we read the bronze table as a stream, this silver table is also updated incrementally
    df = dlt.read_stream("bids_bronze").select(col("id").alias("auction_id") , "device.*" ).select("*", "geo.*").drop("geo")
    return df.select([col(c).alias("device_"+c) for c in df.columns])

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from SOLACC_rtb_lite.bids_device_silver

# COMMAND ----------

# DBTITLE 1,Create bids_imp_silver table
@dlt.table
def bids_imp_silver():
  df = dlt.read_stream("bids_bronze").withColumn("imp", explode("imp")).select(col("id").alias("auction_id") , "imp.*" )\
                  .select("*", col("banner.h").alias("banner_h"), col("banner.pos").alias("banner_pos"), col("banner.w").alias("banner_w"))\
                  .drop("banner")
  return df.select([col(c).alias("imp_"+c) for c in df.columns])

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from SOLACC_rtb_lite.bids_imp_silver

# COMMAND ----------

# DBTITLE 1,Create bids_site_silver table
@dlt.table
def bids_site_silver():
    df = dlt.read_stream("bids_bronze").select(col("id").alias("auction_id"), col("regs.ext.gdpr").alias("gdpr_status"), "site.*" )\
                  .select("*", col("publisher.id").alias("publisher_id"))\
                  .drop("publisher", "site_content")
    return df.select([col(c).alias("site_"+c) for c in df.columns])

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from SOLACC_rtb_lite.bids_site_silver

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Step 3: Create Gold Table
# MAGIC 
# MAGIC <img style="float: right; padding-left: 10px" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/media/resources/images/rtb-pipeline-dlt-3.png" width="600"/>
# MAGIC 
# MAGIC Once our Silver tables are ready, we'll merge the information they contain into a final Gold table, ready for data analysis and data science.

# COMMAND ----------

# DBTITLE 1,Create bids_gold table
@dlt.table(name="bids_gold", comment="filter out non-gdpr compliance bid requests")
@dlt.expect_or_drop("valid_gdpr_status", "site_gdpr_status IS NOT NULL AND site_gdpr_status >0")
def bids_gold():
  df = spark.sql(""" SELECT *, round(rand()+0.1) as in_view
                    FROM (
                      SELECT * FROM  LIVE.bids_device_silver A
                      LEFT JOIN LIVE.bids_imp_silver B ON A.device_auction_id = B.imp_auction_id ) D
                      LEFT JOIN LIVE.bids_site_silver C ON D.device_auction_id = C.site_auction_id """)
  return df.drop("imp_pmp","device_dpidmd5", "device_dpidsha1", "device_ipv6", "site_keywords", "site_cat", "site_content", "site_pagecat")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from SOLACC_rtb_lite.bids_gold

# COMMAND ----------

# MAGIC %md
# MAGIC Our DLT pipeline is now completed!
# MAGIC 
# MAGIC Open the [DLT graph](https://e2-demo-field-eng.cloud.databricks.com/?o=1444828305810485&owned-by-me=false&name-order=ascend#joblist/pipelines/979c8405-61a7-4e77-a491-7938c4019c1e) and start the execution to process new files

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ## Step 4: Perform exploratory data analysis using Databricks SQL
# MAGIC 
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/media/resources/images/rtb-dashboard.png" width="600" style="float: right"/>
# MAGIC 
# MAGIC Now that our gold layer is built, we can start running custom analysis on the data, using noteboks or creating DBSQL dashboard to track and analyse our audience globally 
# MAGIC 
# MAGIC You could also use a DBSQL endpoint and leverage any BI tools such as PowerBI or Tableau to build custom dashboards.
# MAGIC 
# MAGIC [Open the DBSQL dashboard](https://e2-demo-field-eng.cloud.databricks.com/sql/dashboards/070b6c4f-eac2-45c2-b85e-1cc0d5dc95bb-rtb-audience-and-bidstream-insight?o=1444828305810485&p_state=%5B%22NY%22%2C%22SC%22%2C%22TX%22%2C%22AL%22%2C%22PA%22%2C%22IA%22%5D)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Our DLT pipeline is now ready. You can use this notebook to create and start your pipeline.
# MAGIC 
# MAGIC ## Next step: 
# MAGIC 
# MAGIC [Implement ML Model]($./02_dlt_real_time_bidding_ml) to predict viewability and improve ad performances
