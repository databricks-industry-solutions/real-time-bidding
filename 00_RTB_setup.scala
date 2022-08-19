// Databricks notebook source
// MAGIC %md
// MAGIC ## Real-Time Bidding
// MAGIC 
// MAGIC **Use Case Overview**
// MAGIC 
// MAGIC Real-time bidding (**RTB**): is a subcategory of programmatic media buying. RTB firms established the technology of buying and selling ads in real time (~ 10ms ) in an instant auction, on a per-impression basis.
// MAGIC 
// MAGIC * The selling-buying cycle includes: publishers, a supply-side platform (SSP) or an ad exchange, a demand-side platform (DSP), and advertisers
// MAGIC * The value of RTB is that it creates greater transparency for both publishers and advertisers in the the ad market: 
// MAGIC   * Publishers can better control their inventory and CPMs (cost per 1000 ad impressions) 
// MAGIC   * Advertisers that leverage RTB can boost advertising effectiveness by only bidding on impressions that are likely to be **viewed** by a given user.
// MAGIC 
// MAGIC **Viewability** is a metric that measures whether or not an ad was actually seen by a user. This gives marketers a more precise measurement about whether or not their message appeared to users in a visible way.
// MAGIC * In this Databricks demo, we demonstrate a process to predict viewability using BidRequest Data. Keep in mind, the more likely users are to see an ad, the higher the price a DSPs will want to place on a bid for that ad, because it is ultimately more valueable to the advertiser.
// MAGIC * By building a reliable, scalable, and efficient pipeline to predict viewability, advertisers can more accurately identify where to spend their marketing budgets to fine-tune media spend, improve ROI, and enhance campaign effectiveness.
// MAGIC 
// MAGIC 
// MAGIC We'll implement the following data pipeline for RTB:
// MAGIC 
// MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/media/resources/images/rtb-pipeline-dlt.png" width="1000"/>
// MAGIC 
// MAGIC <!-- do not remove -->
// MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Fmedia%2Frtb%2Fnotebook_de&dt=MEDIA_USE_CASE">
// MAGIC <!-- [metadata={"description":"Build DE pipeline to prep data for Data Analysis and ML.</i>", "authors":["layla.yang@databricks.com"]}] -->

// COMMAND ----------

// MAGIC %run ./_resources/00-setup $reset_all_data=false
