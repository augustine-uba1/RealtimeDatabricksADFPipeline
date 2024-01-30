-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Steps
-- MAGIC ##### ingest data from raw container into transformed table in real-time (pipeline trigger configured using Azure Data Factory)

-- COMMAND ----------

-- FIX THE BELOW CODE
CREATE TABLE IF NOT EXISTS public_holiday.public_holiday_transformed.public_holidays
(
    holiday_id BIGINT,
    country_or_region STRING,
    holiday_name STRING,
    is_paid_time_off BOOLEAN,
    holiday_date DATE,
    ingestion_date TIMESTAMP,
    updated_date TIMESTAMP,
    data_source STRING
)


-- COMMAND ----------

SELECT * FROM public_holiday.public_holiday_transformed.public_holidays

-- COMMAND ----------


