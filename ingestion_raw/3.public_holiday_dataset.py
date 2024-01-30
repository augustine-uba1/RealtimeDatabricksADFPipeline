# Databricks notebook source
# MAGIC %md
# MAGIC ### Steps
# MAGIC ##### Source open dataset from Microsoft Open Datasets
# MAGIC ##### Read and access data from the openset blob storage
# MAGIC ##### write data into mount point with audit columns (Raw Layer)
# MAGIC ##### raw notebook to be ran one daily
# MAGIC
# MAGIC https://learn.microsoft.com/en-us/azure/open-datasets/dataset-public-holidays?tabs=pyspark

# COMMAND ----------

mnt_path = "/mnt/blobstorageaccountuba/publicholidays/"
raw_path = f"{mnt_path}/raw/"

# COMMAND ----------

# Azure storage access info
blob_account_name = "azureopendatastorage"
blob_container_name = "holidaydatacontainer"
blob_relative_path = "Processed"
blob_sas_token = r""

# COMMAND ----------

# Allow SPARK to read from Blob remotely
wasbs_path = 'wasbs://%s@%s.blob.core.windows.net/%s' % (blob_container_name, blob_account_name, blob_relative_path)
spark.conf.set(
  'fs.azure.sas.%s.%s.blob.core.windows.net' % (blob_container_name, blob_account_name),
  blob_sas_token)
print('Remote blob path: ' + wasbs_path)

# COMMAND ----------

# SPARK read parquet, note that it won't load any data yet by now


from pyspark.sql.functions import current_timestamp

df = spark.read.parquet(wasbs_path)

# Add ingestion_date column to the dataframe
df = df.withColumn('ingestion_date', current_timestamp())

#this step is not necessary, only shows how to easily convert a dataframe into a temporary view. If ommited raw data is still processed and copied into raw container
print('Register the DataFrame as a SQL temporary view: source')
df.createOrReplaceTempView('source') 

# COMMAND ----------

# Display top 10 rows
print('Displaying top 1000 rows: ')
display(spark.sql('SELECT * FROM source  ORDER BY DATE DESC LIMIT 1000'))

# COMMAND ----------

# write the data into raw container

from datetime import datetime

# define file name with current date at the start of the file name
current_date_str = datetime.now().strftime("%Y%m%d")
file_name = f"{current_date_str}_public_holiday.parquet"

# concatenate mount point and file name as full file path
full_file_path = f"{raw_path}/{file_name}"

#write into raw container
df.write.parquet(full_file_path)
