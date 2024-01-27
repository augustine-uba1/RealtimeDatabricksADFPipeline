# Databricks notebook source
# MAGIC %md
# MAGIC ### Steps
# MAGIC ##### Create service Principal
# MAGIC ##### Assign blob account contributor role to service principal
# MAGIC ##### Create Secret Vault set access mode to 'vault access policy' and add the service principal to a policy Azure RBAC might cause more complexity with access. 
# MAGIC
# MAGIC https://learn.microsoft.com/en-us/azure/databricks/connect/storage/azure-storage

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

service_principal_secret = dbutils.secrets.get(scope='publicholiday2', key='principal-secret')
service_principal_tenant_id = dbutils.secrets.get(scope='publicholiday2', key='tenant-id')
service_principal_client_id= dbutils.secrets.get(scope='publicholiday2', key='principal-client-id')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.blobstorageaccountuba.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.blobstorageaccountuba.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.blobstorageaccountuba.dfs.core.windows.net", service_principal_client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.blobstorageaccountuba.dfs.core.windows.net", service_principal_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.blobstorageaccountuba.dfs.core.windows.net", f"https://login.microsoftonline.com/{service_principal_tenant_id}/oauth2/token")

# COMMAND ----------

dbutils.fs.ls("abfss://publicholidays@blobstorageaccountuba.dfs.core.windows.net")

# COMMAND ----------

display(dbutils.fs.ls('/'))

# COMMAND ----------


