# Databricks notebook source
# MAGIC %md
# MAGIC ### Steps
# MAGIC ##### Create service Principal
# MAGIC ##### Assign blob account contributor role to service principal
# MAGIC ##### Create Secret Vault set access mode to 'vault access policy' and add the service principal to a policy Azure RBAC might cause more complexity with access.
# MAGIC ##### use assigned service principal to create mount point
# MAGIC https://learn.microsoft.com/en-us/azure/databricks/dbfs/mounts 

# COMMAND ----------

service_principal_secret = dbutils.secrets.get(scope='publicholiday2', key='principal-secret')
service_principal_tenant_id = dbutils.secrets.get(scope='publicholiday2', key='tenant-id')
service_principal_client_id= dbutils.secrets.get(scope='publicholiday2', key='principal-client-id')
container_name = "publicholidays"
storage_account_name = "blobstorageaccountuba"

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": service_principal_client_id,
          "fs.azure.account.oauth2.client.secret": service_principal_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{service_principal_tenant_id}/oauth2/token"}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
  mount_point = f"/mnt/{storage_account_name}/{container_name}",
  extra_configs = configs)

# COMMAND ----------

display(dbutils.fs.ls('/mnt/blobstorageaccountuba/publicholidays/'))

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------


