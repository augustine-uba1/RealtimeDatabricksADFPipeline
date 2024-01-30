-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Steps
-- MAGIC ##### create Delta table (apply unity catalog)
-- MAGIC ###### creating unity catalog
-- MAGIC ####### 1. create an Azure Data Lake
-- MAGIC ####### 2. create Azure Access connector (this is used to assign permission for storage container to a databricks workspace)
-- MAGIC ####### 3. add necessary role to Azure Data Lake blob storage
-- MAGIC ####### 4. create unity metastore and enable databricks workspace to use unity catalog
-- MAGIC ####### 5. create an external location using access connector, service principal, and storage credential. Access connector and service principal steps are done outside the notebook.
-- MAGIC ####### 6. create catalog (database)
-- MAGIC ##### change all column names to pascal_casing
-- MAGIC ##### add audit columns (created date, updated date)
-- MAGIC ##### update changes, insert new records
-- MAGIC
-- MAGIC ##### useful links https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/sql-ref-syntax-ddl-create-location

-- COMMAND ----------

-- Create a location accessed using the abfss_remote_cred credential
CREATE EXTERNAL LOCATION holiday_external_raw URL 'abfss://publicholiday@austinedatabricks.dfs.core.windows.net/'
    WITH (STORAGE CREDENTIAL holiday_external)
    COMMENT 'Default source for Azure exernal data';

-- COMMAND ----------

-- Create a location accessed using the abfss_remote_cred credential
CREATE EXTERNAL LOCATION holiday_external_transformed URL 'abfss://publicholidaytrans@austinedatabricks.dfs.core.windows.net/'
    WITH (STORAGE CREDENTIAL holiday_external)
    COMMENT 'Default source for Azure exernal data';

-- COMMAND ----------

DESC EXTERNAL LOCATION holiday_external_raw

-- COMMAND ----------

DESC EXTERNAL LOCATION holiday_external_transformed

-- COMMAND ----------

-- Drop the public_holiday_raw catalog
DROP CATALOG IF EXISTS public_holiday CASCADE;
-- Create a public_holiday_raw catalog
CREATE CATALOG IF NOT EXISTS public_holiday COMMENT 'This is the catalog for all processing for the public holiday pipeline';


-- COMMAND ----------

-- Select the catalog named public_holiday
USE CATALOG public_holiday
--SHOW CATALOGS LIKE 'public_holiday';

-- COMMAND ----------

-- create raw managed location
CREATE SCHEMA IF NOT EXISTS public_holiday_raw 
MANAGED LOCATION "abfss://publicholiday@austinedatabricks.dfs.core.windows.net/";

-- COMMAND ----------

-- create transformed managed location
CREATE SCHEMA IF NOT EXISTS public_holiday_transformed 
MANAGED LOCATION "abfss://publicholidaytrans@austinedatabricks.dfs.core.windows.net/";

-- COMMAND ----------

SHOW SCHEMAS

-- COMMAND ----------


