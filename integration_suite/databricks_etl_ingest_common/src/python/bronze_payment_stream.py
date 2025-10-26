"""Structured Streaming job that ingests payment transactions from Kafka to Delta."""

from pyspark.sql import functions as F
from common.config import DatabricksConfig
from common.utils import logger

# Widgets ---------------------------------------------------------------

dbutils.widgets.removeAll()
dbutils.widgets.dropdown("env", "dev", ["dev", "sit", "uat", "prd"])
dbutils.widgets.combobox("starting_offsets", "earliest", ["earliest", "latest"], "Kafka - Starting Offsets")

env = dbutils.widgets.get("env")
starting_offsets = dbutils.widgets.get("starting_offsets")

topic = "payments.realtime.txn"
target_table = f"{env}_raw_payments.tbl_payment_txn"
checkpoint_location = f"/mnt/{env}/raw/payments/tbl_payment_txn/_checkpoint"

# Configuration ---------------------------------------------------------

config = DatabricksConfig("../configs/connection.ini")
kafka_cfg = config.get_section("kafka.realtime")
logger.info("Kafka configuration loaded for bootstrap servers: %s", kafka_cfg.get("bootstrap_servers"))

spark_kafka_options = {
    "kafka.bootstrap.servers": kafka_cfg["bootstrap_servers"],
    "kafka.security.protocol": kafka_cfg["security_protocol"],
    "kafka.sasl.mechanism": kafka_cfg["sasl_mechanism"],
    "kafka.sasl.jaas.config": (
        "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required "
        f"username=\"{kafka_cfg['user']}\" password=\"{kafka_cfg['password']}\";"
    ),
}

# Stream ingestion ------------------------------------------------------

logger.info("Starting structured stream from topic %s", topic)
stream = (
    spark.readStream.format("kafka")
    .options(**spark_kafka_options)
    .option("subscribe", topic)
    .option("startingOffsets", starting_offsets)
    .option("maxOffsetsPerTrigger", "20000")
    .option("skipChangeCommits", "true")
    .load()
    .withColumn("key", F.col("key").cast("string"))
    .withColumn("value", F.col("value").cast("string"))
)

query = (
    stream.writeStream.format("delta")
    .trigger(processingTime="1 seconds")
    .option("checkpointLocation", checkpoint_location)
    .outputMode("append")
    .table(target_table)
)

logger.info("Streaming query started with id %s", query.id)
