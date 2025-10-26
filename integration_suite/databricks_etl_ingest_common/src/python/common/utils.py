from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StringType,
    DecimalType,
    DoubleType,
    FloatType,
    DateType,
    TimestampType,
)
import pyspark.sql.functions as F

import os
import sys
import logging


LOG_FORMAT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
logging.basicConfig(format=LOG_FORMAT, stream=sys.stdout, level=logging.INFO)
logging.getLogger("py4j").setLevel(logging.ERROR)
logger = logging.getLogger("ETL")


def convert_columns_to_string(
    df: DataFrame,
    date_fmt: str = "yyyy-MM-dd",
    timestamp_fmt: str = "yyyy-MM-dd HH:mm:ss",
    number_fmt: str = "#.####################",
) -> DataFrame:
    """
    Convert all columns in the DataFrame to string type with specific formatting.

    Parameters:
    df (DataFrame): Input Spark DataFrame.
    date_fmt (str): Date format string.
    timestamp_fmt (str): Timestamp format string.
    number_fmt (str): Number format string.

    Returns:
    DataFrame: Spark DataFrame with all columns converted to string type.
    """
    for field in df.schema:
        name = field.name
        dtype = field.dataType
        if isinstance(dtype, StringType):
            continue
        elif isinstance(dtype, (DecimalType, DoubleType, FloatType)):
            df = df.withColumn(name, F.expr(f"format_number({name}, '{number_fmt}')"))
        elif isinstance(dtype, DateType):
            df = df.withColumn(name, F.date_format(F.col(name), date_fmt))
        elif isinstance(dtype, TimestampType):
            df = df.withColumn(name, F.date_format(F.col(name), timestamp_fmt))
        else:
            df = df.withColumn(name, F.col(name).cast("string"))
    return df


def replace_placeholders(txt: str, **placeholders) -> str:
    for k, v in placeholders.items():
        txt = txt.replace(f"{{{k.upper()}}}", str(v))
        txt = txt.replace(f"{{{k.lower()}}}", str(v))

    if txt.find("{") >= 0:
        logger.warning(f"Cannot replace all placeholders in '{txt}'")

    return txt


def read_sql(path: str, **placeholders):
    abspath = os.path.abspath(path)
    logger.info(f"Reading SQL file from path: '{abspath}'")
    with open(abspath) as f:
        sql = replace_placeholders(txt=f.read(), **placeholders)
    logger.info(f"SQL: \n{sql}")
    return sql


def execute_sql_on_database(spark: SparkSession, jdbc_cfg: dict, sql_statement: str, is_dryrun=False):
    # To build spark driver wrapper for Oracle
    if jdbc_cfg["driver"] in [
        "oracle.jdbc.driver.OracleDriver",
        "oracle.jdbc.OracleDriver",
    ]:
        (spark.read.format("jdbc").options(**jdbc_cfg).option("query", "SELECT 1 FROM dual").load())

    # Create a connection
    driver_manager = spark._sc._gateway.jvm.java.sql.DriverManager
    conn = driver_manager.getConnection(jdbc_cfg["url"], jdbc_cfg["user"], jdbc_cfg["password"])
    stmt = conn.createStatement()
    # Execute SQL statements
    for statement in sql_statement.split(";"):
        if statement.strip():
            if is_dryrun:
                logger.info(f"Dry run mode, Skipping execute SQL statement: {statement.strip()}")
            else:
                logger.info(f"Executing SQL statement: {statement.strip()}")
                stmt.execute(statement.strip())

    stmt.close()
    conn.close()
