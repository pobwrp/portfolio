# Databricks notebook source
# Common
dbutils.widgets.removeAll()
dbutils.widgets.dropdown("env", "dev", ["dev", "sit", "uat", "prd"])
dbutils.widgets.text("data_dt", "")
dbutils.widgets.dropdown("is_dryrun", "false", ["false", "true"])

# Source (ADLS)
dbutils.widgets.text("source_connection_name", "")
dbutils.widgets.text("sql_file_path", "")

# Target (Database)
dbutils.widgets.text("target_connection_name", "")
dbutils.widgets.text("target_schema_name", "")
dbutils.widgets.text("target_table_name", "")
dbutils.widgets.text("pre_ins_sql_file_path", "")
dbutils.widgets.dropdown("write_mode", "overwrite", ["overwrite", "append"])
dbutils.widgets.text("batchsize", "10000")

# COMMAND ----------

from common.utils import *
from common.config import DatabricksConfig
from typing import Optional

kwargs = dict(dbutils.notebook.entry_point.getCurrentBindings())
logger.info(f"Notebook arguments received: {kwargs}")

# Common
# Common parameters
env: str = kwargs.get("env")
data_dt: str = kwargs.get("data_dt")
is_dryrun: bool = kwargs.get("is_dryrun", "false").lower() == "true"

# Source (ADLS) parameters
source_connection_name: str = kwargs.get("source_connection_name")
sql_file_path: str = kwargs.get("sql_file_path")

# Target (Database) parameters
target_connection_name: str = kwargs.get("target_connection_name")
target_schema_name: str = kwargs.get("target_schema_name")
target_table_name: str = kwargs.get("target_table_name")
pre_ins_sql_file_path: Optional[str] = kwargs.get("pre_ins_sql_file_path")
write_mode: str = kwargs.get("write_mode", "append")
batchsize: str = kwargs.get("batchsize", "10000")

# COMMAND ----------

# MAGIC %md
# MAGIC # Configuration Setup
# MAGIC Load and validate connection configurations for ADLS and Database

# COMMAND ----------

try:
    config = DatabricksConfig("../../configs/connection.ini")

    adls_cfg = config.get_section(source_connection_name)
    logger.info(f"ADLS configuration loaded for: {adls_cfg.get('storage_account_name')}")

    db_cfg = config.get_section(target_connection_name)
    logger.info(f"Database configuration loaded for: {db_cfg.get('jdbc_url').format(**db_cfg)}")

    jdbc_cfg = {
        "url": db_cfg["jdbc_url"].format(**db_cfg),
        "driver": db_cfg["driver"],
        "user": db_cfg["user"],
        "password": db_cfg["password"],
    }

    # Configure Spark for Azure Blob Storage access
    spark.conf.set(
        f"fs.azure.account.key.{adls_cfg['storage_account_name']}.dfs.core.windows.net",
        adls_cfg["access_key"],
    )

except Exception as e:
    logger.error(f"Error setting up configurations: {e}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC # Extract Data from ADLS
# MAGIC Load data from Azure Data Lake Storage using SQL query

# COMMAND ----------
try:
    spark.conf.set("spark.sql.parquet.mergeSchema", "true")
    logger.info(f"Loading SQL from: {sql_file_path}")

    sql = read_sql(path=sql_file_path, **adls_cfg, **kwargs)
    logger.info("Executing SQL query...")

    df = spark.sql(sql)
    df.persist()

    row_count = df.count()
    logger.info(f"Data loaded successfully. Row count: {row_count:,}")

    display(df)

except Exception as e:
    logger.error(f"Error loading data from ADLS: {e}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC # Load Data to Database
# MAGIC Execute pre-insert SQL (if provided) and write data to target database table

# COMMAND ----------

if pre_ins_sql_file_path:
    try:
        logger.info(f"Executing pre-insert SQL from: {pre_ins_sql_file_path}")
        pre_ins_sql = read_sql(path=pre_ins_sql_file_path, **kwargs)
        execute_sql_on_database(spark, jdbc_cfg, pre_ins_sql, is_dryrun)
        logger.info("Pre-insert SQL executed successfully")
    except Exception as e:
        logger.error(f"Error executing pre-insert SQL: {e}")
        raise

# COMMAND ----------

# Write data to database table
dbtable = f"{target_schema_name}.{target_table_name}"

try:
    if is_dryrun:
        logger.info(f"DRY RUN: Would write {row_count:,} rows to '{dbtable}' with mode '{write_mode}'")
    else:
        logger.info(f"Writing {row_count:,} rows to {dbtable} with mode '{write_mode}'...")

        (
            df.write.format("jdbc")
            .options(**jdbc_cfg)
            .option("dbtable", dbtable)
            .option("truncate", "true")
            .option("ิbatchsize", batchsize)
            .mode(write_mode)
            .save()
        )

        logger.info(f"Successfully wrote data to {dbtable}")

except Exception as e:
    logger.error(f"Error writing data to database table '{dbtable}': {e}")
    raise

# COMMAND ----------

try:
    df.unpersist()
except Exception as e:
    logger.warning(f"Error unpersisting DataFrame: {e}")
