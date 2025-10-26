"""Databricks notebook that exports Delta data to SharePoint as Excel."""

import configparser
import json
import logging
from datetime import datetime, timedelta
from io import BytesIO

import pandas as pd
import pyspark.sql.functions as F
import pytz
from office365.runtime.auth.client_credential import ClientCredential
from office365.sharepoint.client_context import ClientContext

logger = logging.getLogger("sharepoint_export")

# Widgets ---------------------------------------------------------------

dbutils.widgets.removeAll()
dbutils.widgets.dropdown("env", "dev", ["dev", "sit", "uat", "prd"])
dbutils.widgets.text("job_datetime", "")

# Parameters -----------------------------------------------------------

env = dbutils.widgets.get("env")
job_datetime = dbutils.widgets.get("job_datetime")

trigger_dttm = datetime.fromisoformat(job_datetime.split(".")[0])
trigger_dttm = pytz.utc.localize(trigger_dttm).astimezone(pytz.timezone("Asia/Bangkok"))

from_dttm = trigger_dttm.replace(second=0, microsecond=0, minute=0, hour=0) - timedelta(days=1)
to_dttm = trigger_dttm.replace(second=0, microsecond=0, minute=0, hour=0)
data_dt = from_dttm.strftime("%Y%m%d")

print("job_datetime:", job_datetime)
print("from_dttm:", from_dttm)
print("to_dttm:", to_dttm)
print("data_dt:", data_dt)

# Configuration -------------------------------------------------------

conn_cfg = configparser.ConfigParser()
conn_cfg.read("../configs/connection.ini")

sp_conn_cfg = conn_cfg["sharepoint.portfolio"]
safe_config = {key: ("***" if "secret" in key else value) for key, value in sp_conn_cfg.items()}
print("SharePoint config:", json.dumps(safe_config, indent=4))

# Load data -----------------------------------------------------------

response_df = (
    spark.read.format("delta")
    .table(f"{env}_ods_portfolio.vw_restapi_response")
    .filter((F.col("load_dt") >= from_dttm) & (F.col("load_dt") < to_dttm))
)
response_df.persist()
display(response_df)

# SharePoint upload ---------------------------------------------------

credentials = ClientCredential(sp_conn_cfg["client_id"], sp_conn_cfg["client_secret"])
ctx = ClientContext(sp_conn_cfg["site_url"]).with_credentials(credentials)

def upload_excel(ctx: ClientContext, df: pd.DataFrame, folder: str, filename: str) -> None:
    target_folder = ctx.web.folders.add(folder)
    ctx.load(target_folder)
    ctx.execute_query()

    buffer = BytesIO()
    df.to_excel(buffer, index=False)
    buffer.seek(0)

    target_folder.upload_file(filename, buffer).execute_query()

sharepoint_filename = f"RESTAPI_RESPONSE_{data_dt}.xlsx"
logger.info(
    "Uploading to SharePoint: %s/%s",
    sp_conn_cfg["site_url"].split("/sites/")[0] + sp_conn_cfg["sharepoint_path"],
    sharepoint_filename,
)

upload_excel(
    ctx,
    response_df.toPandas(),
    sp_conn_cfg["sharepoint_path"],
    sharepoint_filename,
)

response_df.unpersist()
