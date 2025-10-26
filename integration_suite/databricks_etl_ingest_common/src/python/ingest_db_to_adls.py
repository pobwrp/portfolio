# Databricks notebook source
# Common
dbutils.widgets.removeAll()
dbutils.widgets.dropdown("env", "dev", ["dev", "sit", "uat", "prd"])
dbutils.widgets.text("data_dt", "")
dbutils.widgets.dropdown("is_dryrun", "false", ["false", "true"])

# Source (Database)
dbutils.widgets.text("source_connection_name", "")
dbutils.widgets.text("sql_file_path", "")

# Target (ADLS)
dbutils.widgets.text("target_connection_name", "")
dbutils.widgets.text("target_file_path", "")
dbutils.widgets.dropdown("write_mode", "overwrite", ["overwrite", "append"])

# COMMAND ----------

from common.utils import *
from common.config import DatabricksConfig

kwargs = dict(dbutils.notebook.entry_point.getCurrentBindings())
logger.info(f"Notebook arguments received: {kwargs}")

# Common
# Common parameters
env: str = kwargs.get("env")
data_dt: str = kwargs.get("data_dt")
is_dryrun: bool = kwargs.get("is_dryrun", "false").lower() == "true"

# Source (Database) parameters
source_connection_name: str = kwargs.get("source_connection_name")
sql_file_path: str = kwargs.get("sql_file_path")

# Target (ADLS) parameters
target_connection_name: str = kwargs.get("target_connection_name")
target_file_path: str = kwargs.get("target_file_path")
write_mode: str = kwargs.get("write_mode", "overwrite")

# COMMAND ----------

# MAGIC %md
# MAGIC # Configuration Setup
# MAGIC Load and validate connection configurations for Database and ADLS

# COMMAND ----------

try:
    config = DatabricksConfig("../../configs/connection.ini")

    db_cfg = config.get_section(source_connection_name)
    logger.info(f"Database configuration loaded for: {db_cfg.get('jdbc_url').format(**db_cfg)}")

    adls_cfg = config.get_section(target_connection_name)
    logger.info(f"ADLS configuration loaded for: {adls_cfg.get('storage_account_name')}")

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
    logger.info(f"Spark configured for ADLS access: {adls_cfg['storage_account_name']}")

except Exception as e:
    logger.error(f"Error setting up configurations: {e}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC # Extract Data from Database
# MAGIC Load data from database using JDBC connection and SQL query

# COMMAND ----------
try:
    spark.conf.set("spark.sql.parquet.mergeSchema", "true")
    logger.info(f"Loading SQL from: {sql_file_path}")

    sql = read_sql(path=sql_file_path, **kwargs)
    logger.info("Reading data from database using JDBC...")

    df = spark.read.format("jdbc").options(**jdbc_cfg).option("query", sql).load()
    df.persist()

    row_count = df.count()
    logger.info(f"Data loaded successfully. Row count: {row_count:,}")

    display(df)

except Exception as e:
    logger.error(f"Error loading data from database: {e}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC # Load Data to ADLS
# MAGIC Write data to Azure Data Lake Storage as Parquet files with partitioning

# COMMAND ----------

# Build ADLS path with DATA_DT partition
adls_path = f"abfss://{adls_cfg['container_name']}@{adls_cfg['storage_account_name']}.dfs.core.windows.net/{target_file_path}/DATA_DT={data_dt}"

try:
    if is_dryrun:
        logger.info(f"DRY RUN: Would write {row_count:,} rows to '{adls_path}' with mode '{write_mode}'")
    else:
        logger.info(f"Writing {row_count:,} rows to {adls_path} as parquet with mode '{write_mode}'...")

        (df.write.format("parquet").mode(write_mode).save(adls_path))

        logger.info(f"Successfully wrote data to {adls_path}")

except Exception as e:
    logger.error(f"Error writing data to ADLS path '{adls_path}': {e}")
    raise

# COMMAND ----------

try:
    df.unpersist()
except Exception as e:
    logger.warning(f"Error unpersisting DataFrame: {e}")
