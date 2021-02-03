# Databricks notebook source
from pyspark.sql.functions import date_format, col, count, lit
from pyspark.sql.types import StringType
from datetime import datetime, date

# COMMAND ----------

# DBTITLE 1,Set Spark Configs
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled","true")
spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.autoCompact", "false")
spark.conf.set("spark.databricks.optimizer.dynamicPartitionPruning","true")

# COMMAND ----------

# DBTITLE 1,Data Loader Helper
# MAGIC %run "/Helpers/DataLoader"

# COMMAND ----------

# DBTITLE 1,Parameter & Variables
dbutils.widgets.text("DateToProcess","")
DateToProcess = dbutils.widgets.get("DateToProcess")
DateToProcess = datetime.strptime(DateToProcess, "%Y-%m-%d").date()
DateToProcess_path = DateToProcess.strftime('%Y/%m/%d')
DateToProcess_str = DateToProcess.strftime('%Y%m%d')

# COMMAND ----------

# DBTITLE 1,Datasets
raw_transaction = "/mnt/ADLS/Example/Raw/Transaction/"+DateToProcess_path+"/Transaction_"+DateToProcess_str+".csv"
delta_transaction = "/mnt/ADLS/Example/Prepared/Transaction/DELTA/"

# COMMAND ----------

# DBTITLE 1,Load Raw Transaction Data
df_transaction = data_loader(raw_transaction,'csv')
df_transaction = df_transaction.withColumn('year', date_format(col('date'),'yyyy').cast(StringType()))
df_transaction = df_transaction.withColumn('month', date_format(col('date'),'MM').cast(StringType()))
df_transaction = df_transaction.withColumn('day', date_format(col('date'),'dd').cast(StringType()))

df_transaction.createOrReplaceTempView('vw_Transaction')

# COMMAND ----------

# DBTITLE 1,Create DELTA Table
# MAGIC %sql
# MAGIC 
# MAGIC DROP TABLE IF EXISTS Example_Prepared_Transaction_DELTA;
# MAGIC 
# MAGIC CREATE TABLE Example_Prepared_Transaction_DELTA (
# MAGIC   `date` STRING,
# MAGIC   `transaction_id` STRING,
# MAGIC   `product_id` STRING,
# MAGIC   `product_name` STRING,
# MAGIC   `customer_id` STRING,
# MAGIC   `cost` DOUBLE,
# MAGIC   `currency` STRING,
# MAGIC   `quantity` INTEGER,
# MAGIC   `year` STRING,
# MAGIC   `month` STRING,
# MAGIC   `day` STRING
# MAGIC )
# MAGIC USING delta
# MAGIC PARTITIONED BY (year, month, day)
# MAGIC LOCATION '/mnt/ADLS/Example/Prepared/Transaction/DELTA/'

# COMMAND ----------

# DBTITLE 1,Merge Into Transaction DELTA
InsertPO = spark.sql("""
  MERGE INTO Example_Prepared_Transaction_DELTA AS target 
  USING vw_Transaction AS source 
    ON target.`date` = source.`date`
    AND target.`transaction_id` = source.`transaction_id` 
    AND target.`year` = source.`year` 
    AND target.`month` = source.`month` 
    AND target.`day` = source.`day`
  WHEN MATCHED THEN 
    UPDATE SET 
      target.`product_id` = source.`product_id`,
      target.`product_name` = source.`product_name`,
      target.`customer_id` = source.`customer_id`,
      target.`cost` = source.`cost`,
      target.`currency` = source.`currency`,
      target.`quantity` = source.`quantity`
  WHEN NOT MATCHED THEN 
    INSERT (
      `date`,
      `transaction_id`,
      `product_id`, 
      `product_name`, 
      `customer_id`, 
      `cost`, 
      `currency`, 
      `quantity`, 
      `year`, 
      `month`, 
      `day`
    ) 
    VALUES ( 
      source.`date`, 
      source.`transaction_id`, 
      source.`product_id`, 
      source.`product_name`, 
      source.`customer_id`, 
      source.`cost`, 
      source.`currency`, 
      source.`quantity`, 
      source.`year`, 
      source.`month`, 
      source.`day`
    )
""")
