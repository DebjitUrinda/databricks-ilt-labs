# Databricks notebook source
# MAGIC %sql
# MAGIC -- Create safe function for catalog name
# MAGIC CREATE OR REPLACE TEMPORARY FUNCTION safe_uc_name(value STRING)
# MAGIC RETURNS STRING
# MAGIC RETURN
# MAGIC   COALESCE(
# MAGIC     NULLIF(
# MAGIC       REGEXP_REPLACE(
# MAGIC         REGEXP_REPLACE(
# MAGIC           LOWER(TRIM(value)),
# MAGIC           '[^a-z0-9_]',
# MAGIC           '_'
# MAGIC         ),
# MAGIC         '_+',
# MAGIC         '_'
# MAGIC       ),
# MAGIC       ''
# MAGIC     ),
# MAGIC     'user'
# MAGIC   );
# MAGIC
# MAGIC -- Create temp view of all available catalogs
# MAGIC CREATE OR REPLACE TEMPORARY VIEW list_of_catalogs AS
# MAGIC SELECT LOWER(catalog_name) AS catalog_names
# MAGIC FROM system.information_schema.catalogs;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Start SQL scripting block to select the catalog for this workspace.
# MAGIC -- This restored Azure workspace uses the existing databricks_ilt catalog.
# MAGIC
# MAGIC BEGIN
# MAGIC
# MAGIC -- =========================================
# MAGIC -- 1. Declare variables
# MAGIC -- =========================================
# MAGIC
# MAGIC   DECLARE catalog_forced STRING DEFAULT 'databricks_ilt';
# MAGIC
# MAGIC   DECLARE user_email STRING;
# MAGIC   DECLARE user_name STRING;
# MAGIC   DECLARE safe_user_name STRING;
# MAGIC   DECLARE vocareum_catalog_name STRING DEFAULT 'NA';
# MAGIC   DECLARE catalog_name STRING DEFAULT 'NA';
# MAGIC   DECLARE catalog_already_exists BOOLEAN DEFAULT FALSE;
# MAGIC
# MAGIC -- =========================================
# MAGIC -- 2. Capture current user
# MAGIC -- =========================================
# MAGIC
# MAGIC   SET user_email = current_user();
# MAGIC   SET user_name = SPLIT(user_email, '@')[0];
# MAGIC   SET safe_user_name = safe_uc_name(user_name);
# MAGIC
# MAGIC -- =========================================
# MAGIC -- 3. Select and validate the catalog
# MAGIC -- =========================================
# MAGIC
# MAGIC   SET catalog_name = catalog_forced;
# MAGIC
# MAGIC   IF EXISTS (
# MAGIC     SELECT 1
# MAGIC     FROM list_of_catalogs
# MAGIC     WHERE catalog_names = LOWER(catalog_name)
# MAGIC   ) THEN
# MAGIC     SET catalog_already_exists = TRUE;
# MAGIC   ELSE
# MAGIC     SIGNAL SQLSTATE '45000'
# MAGIC       SET MESSAGE_TEXT =
# MAGIC         'Catalog databricks_ilt does not exist or is not visible.';
# MAGIC   END IF;
# MAGIC
# MAGIC   -- Create the view required by the next notebook cell.
# MAGIC   CREATE OR REPLACE TEMP VIEW catalog_name_vw AS
# MAGIC   SELECT 'databricks_ilt' AS catalog_name;
# MAGIC
# MAGIC -- =========================================
# MAGIC -- 4. Return setup results
# MAGIC -- =========================================
# MAGIC
# MAGIC   SELECT
# MAGIC     'Restored Azure Databricks Workspace' AS Workspace,
# MAGIC     catalog_name AS `User Catalog Name`,
# MAGIC     'Catalog Already Exists' AS `Catalog Status`;
# MAGIC
# MAGIC END;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create the global SQL variable with the catalog name using the temp view
# MAGIC DECLARE OR REPLACE my_catalog STRING;
# MAGIC SET VAR my_catalog = (SELECT catalog_name FROM catalog_name_vw);
# MAGIC
# MAGIC
# MAGIC -- Set default catalog to user's labuser catalog
# MAGIC USE CATALOG IDENTIFIER(my_catalog);
# MAGIC
# MAGIC
# MAGIC -- Create lab schema for learner
# MAGIC
# MAGIC DECLARE OR REPLACE my_schema STRING;
# MAGIC SET VAR my_schema = 'data_ingestion';
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS IDENTIFIER(my_schema);
# MAGIC USE SCHEMA IDENTIFIER(my_schema);

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP VIEW IF EXISTS catalog_name_vw;
# MAGIC DROP VIEW IF EXISTS list_of_catalogs;
# MAGIC DROP TABLE IF EXISTS sql_csv_autoloader;