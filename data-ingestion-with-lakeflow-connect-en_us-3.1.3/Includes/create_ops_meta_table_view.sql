-- Databricks notebook source
CREATE CATALOG IF NOT EXISTS dbacademy;
CREATE SCHEMA IF NOT EXISTS dbacademy.ops;
DROP TABLE IF EXISTS dbacademy.ops.meta_source;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC spark.sql('''
-- MAGIC   CREATE TABLE IF NOT EXISTS dbacademy.ops.meta_source (
-- MAGIC     owner VARCHAR(255) NOT NULL,
-- MAGIC     object VARCHAR(255),
-- MAGIC     key VARCHAR(255),
-- MAGIC     value VARCHAR(255)
-- MAGIC )
-- MAGIC ''')
-- MAGIC
-- MAGIC def insert_into_meta_source(user_email:str, 
-- MAGIC                             user_name_no_email:str, 
-- MAGIC                             catalog_name: str, 
-- MAGIC                             schema_name: str):
-- MAGIC   spark.sql(f'''
-- MAGIC     INSERT INTO dbacademy.ops.meta_source (owner, object, key, value) VALUES
-- MAGIC     ("{user_email}", NULL, "username", "{user_email}"),
-- MAGIC     ("{user_email}", NULL, "catalog_name", "{catalog_name}"),
-- MAGIC     ("{user_email}", NULL, "schema_name", "default"),
-- MAGIC     ("{user_email}", NULL, "paths.working_dir", "/Volumes/dbacademy/ops/dbacademy_{user_name_no_email}"),
-- MAGIC     ("{user_email}", NULL, "cluster_name", "abc"),
-- MAGIC     ("{user_email}", NULL, "warehouse_name", "shared_warehouse"),
-- MAGIC     ("{user_email}", NULL, "pseudonym", "abc")
-- MAGIC   ''')
-- MAGIC
-- MAGIC
-- MAGIC insert_into_meta_source(user_email='peter.styliadis@databricks.com', 
-- MAGIC                         user_name_no_email='peter_styliadis', 
-- MAGIC                         catalog_name='dbacademy_peter_styliadis', 
-- MAGIC                         schema_name='default')
-- MAGIC
-- MAGIC insert_into_meta_source(user_email='mark.ott@databricks.com', 
-- MAGIC                         user_name_no_email='mark_ott', 
-- MAGIC                         catalog_name='dbacademy_mark_ott', 
-- MAGIC                         schema_name='default')

-- COMMAND ----------

SELECT * FROM dbacademy.ops.meta_source

-- COMMAND ----------

CREATE OR REPLACE VIEW dbacademy.ops.meta AS
SELECT
  owner,
  object,
  key,
  value
FROM dbacademy.ops.meta_source
WHERE 
  CASE
    -- When a member of the admins group (true), you can see all rows
    WHEN owner = current_user() THEN TRUE
    -- Otherwise not part of the admins group (false), you can only see the IT rows
    ELSE FALSE
  END;

-- COMMAND ----------

SELECT * FROM dbacademy.ops.meta;

-- COMMAND ----------

-- CREATE CATALOG IF NOT EXISTS dbacademy_retail;
-- CREATE SCHEMA IF NOT EXISTS dbacademy_retail.v01;
-- CREATE VOLUME IF NOT EXISTS dbacademy_retail.v01.`retail-pipeline`;

-- COMMAND ----------

-- %python

-- dbutils.fs.mkdirs('/Volumes/dbacademy_retail/v01/retail-pipeline/customers/stream_json')
-- dbutils.fs.mkdirs('/Volumes/dbacademy_retail/v01/retail-pipeline/orders/stream_json')
-- dbutils.fs.mkdirs('/Volumes/dbacademy_retail/v01/retail-pipeline/status/stream_json')