# Databricks notebook source
# Common
dbutils.widgets.removeAll()
dbutils.widgets.dropdown("env", "dev", ["dev", "sit", "uat", "prd"])
dbutils.widgets.text("data_dt", "")
dbutils.widgets.dropdown("is_dryrun", "false", ["false", "true"])

# API Configuration
dbutils.widgets.text("api_connection_name", "api.cps")
dbutils.widgets.text("db_connection_name", "postgresql.cpsdb")

# COMMAND ----------

from common.utils import *
from common.config import DatabricksConfig
import requests
from datetime import datetime
import time

kwargs = dict(dbutils.notebook.entry_point.getCurrentBindings())
logger.info(f"Notebook arguments received: {kwargs}")

# Parameters
env: str = kwargs.get("env")
data_dt: str = kwargs.get("data_dt")
is_dryrun: bool = kwargs.get("is_dryrun", "false").lower() == "true"
api_connection_name: str = kwargs.get("api_connection_name")
db_connection_name: str = kwargs.get("db_connection_name")

# COMMAND ----------

# MAGIC %md
# MAGIC # Configuration Setup
# MAGIC Load API and Database configurations

# COMMAND ----------

try:
    config = DatabricksConfig("../../configs/connection.ini")

    # API configuration
    api_cfg = config.get_section(api_connection_name)
    logger.info(f"API configuration loaded: {api_cfg.get('base_url')}")

    # Database configuration
    db_cfg = config.get_section(db_connection_name)
    logger.info(f"Database configuration loaded for: {db_cfg.get('jdbc_url').format(**db_cfg)}")

    jdbc_cfg = {
        "url": db_cfg["jdbc_url"].format(**db_cfg),
        "driver": db_cfg["driver"],
        "user": db_cfg["user"],
        "password": db_cfg["password"],
    }

except Exception as e:
    logger.error(f"Error setting up configurations: {e}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC # API Helper Functions

# COMMAND ----------

def trigger_api(api_cfg, endpoint, data_date, is_dryrun=False):
    """Trigger API endpoint with date parameter"""
    try:
        # Format date from YYYYMMDD to YYYY-MM-DD
        formatted_date = datetime.strptime(str(data_date), "%Y%m%d").strftime("%Y-%m-%d")
        url = api_cfg["base_url"] + api_cfg[endpoint] + formatted_date

        headers = {"Accept": "application/json"}

        if is_dryrun:
            logger.info(f"DRY RUN: Would call API - {url}")
            return {
                "status": "dry_run",
                "status_code": 200,
                "response": {"Header": {"ResponseCode": 200, "ResponseMessage": "OK", "Language": "TH"}},
            }
        else:
            logger.info(f"Calling API: {url}")
            response = requests.get(url, headers=headers, timeout=60, verify=False)
            response.raise_for_status()

            logger.info(f"API response status: {response.status_code}")
            return {
                "status": "success",
                "status_code": response.status_code,
                "response": response.json() if response.content else {},
            }

    except Exception as e:
        logger.error(f"Error calling API: {e}")
        return {"status": "error", "error": str(e)}


def check_activity_completion(spark, jdbc_cfg, activity_names, is_dryrun=False):
    """Check if activities are completed by looking for start/finish pairs today"""
    try:
        if is_dryrun:
            logger.info(f"DRY RUN: Would check completion for activities: {activity_names}")
            return {"completed": activity_names, "pending": [], "all_completed": True}

        activity_names_str = "', '".join(activity_names)
        # Build individual activity completion checks
        activity_checks = []
        for activity in activity_names:
            activity_checks.append(f"""
            SELECT
                '{activity}' as activity_name,
                CASE
                    WHEN latest_start IS NOT NULL AND latest_finish IS NOT NULL
                    AND latest_finish > latest_start
                    THEN 1
                    ELSE 0
                END as is_completed
            FROM (
                SELECT
                    MAX(CASE WHEN LOWER(activity_name) = LOWER('{activity} start') THEN created_date END) as latest_start,
                    MAX(CASE WHEN LOWER(activity_name) = LOWER('{activity} finish') THEN created_date END) as latest_finish
                FROM cps.activity_log
                WHERE (LOWER(activity_name) = LOWER('{activity} start') OR LOWER(activity_name) = LOWER('{activity} finish'))
                    AND DATE(created_date) = CURRENT_DATE
            ) activity_times
            """)

        check_sql = f"""
        WITH activity_completion AS (
            {' UNION ALL '.join(activity_checks)}
        )
        SELECT
            COUNT(*) as total_activities,
            SUM(is_completed) as completed_activities,
            CASE WHEN COUNT(*) = SUM(is_completed) THEN 1 ELSE 0 END as all_completed
        FROM activity_completion
        """

        df = spark.read.format("jdbc").options(**jdbc_cfg).option("query", check_sql).load()
        result = df.collect()[0]

        total_activities = result.total_activities
        completed_activities_count = result.completed_activities
        all_completed = result.all_completed == 1

        logger.info(
            f"Activity completion check - Total: {total_activities}, Completed: {completed_activities_count}, All completed: {all_completed}"
        )

        if all_completed:
            completed_activities = activity_names
            pending_activities = []
        else:
            completed_activities = []
            pending_activities = activity_names

        return {"completed": completed_activities, "pending": pending_activities, "all_completed": all_completed}

    except Exception as e:
        logger.error(f"Error checking activity completion: {e}")
        return {"completed": [], "pending": activity_names, "all_completed": False}


# COMMAND ----------

# MAGIC %md
# MAGIC # Trigger Customer API
# MAGIC This triggers immediately after CUSTOMER_OUT table ingestion

# COMMAND ----------

try:
    # Trigger Customer API
    logger.info("Triggering Customer API...")
    result = trigger_api(api_cfg, "customer_endpoint", data_dt, is_dryrun)

    if result["status"] == "success" or result["status"] == "dry_run":
        logger.info("Customer API triggered successfully")
    else:
        raise Exception(f"Customer API call failed: {result.get('error')}")

except Exception as e:
    logger.error(f"Error in Customer API trigger: {e}")
    raise

# COMMAND ----------

logger.info("Customer API trigger completed successfully")
