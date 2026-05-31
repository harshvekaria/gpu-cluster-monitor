import os
import time
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp,
    avg, stddev, max as spark_max,
    when, round as spark_round, lit, count
)
from pyspark.sql.types import (
    StructType, StructField, StringType,
    DoubleType, IntegerType
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC             = os.getenv("KAFKA_TOPIC", "gpu-telemetry")
POSTGRES_URL            = "jdbc:postgresql://postgres:5432/gpu_monitor"
POSTGRES_PROPS          = {
    "user"    : os.getenv("POSTGRES_USER", "gpuadmin"),
    "password": os.getenv("POSTGRES_PASSWORD", "gpupass123"),
    "driver"  : "org.postgresql.Driver",
}

TELEMETRY_SCHEMA = StructType([
    StructField("event_time",         StringType(),  True),
    StructField("gpu_id",             StringType(),  True),
    StructField("node",               StringType(),  True),
    StructField("gpu_model",          StringType(),  True),
    StructField("utilization_pct",    DoubleType(),  True),
    StructField("temperature_c",      DoubleType(),  True),
    StructField("power_draw_w",       DoubleType(),  True),
    StructField("mem_bandwidth_gbs",  DoubleType(),  True),
    StructField("mem_used_pct",       DoubleType(),  True),
    StructField("sm_clock_mhz",       IntegerType(), True),
    StructField("mem_clock_mhz",      IntegerType(), True),
    StructField("error_code",         StringType(),  True),
])

def wait_for_kafka(retries=20, delay=6):
    from kafka import KafkaConsumer
    from kafka.errors import NoBrokersAvailable
    for attempt in range(1, retries + 1):
        try:
            c = KafkaConsumer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
            topics = c.topics()
            c.close()
            if KAFKA_TOPIC in topics:
                log.info("Topic '%s' is ready.", KAFKA_TOPIC)
                return
            log.info("Topic not yet visible (attempt %d/%d) — waiting %ds...", attempt, retries, delay)
        except NoBrokersAvailable:
            log.info("Kafka not reachable (attempt %d/%d) — waiting %ds...", attempt, retries, delay)
        time.sleep(delay)
    raise RuntimeError("Kafka topic '%s' never appeared." % KAFKA_TOPIC)

def build_spark():
    return (
        SparkSession.builder
        .appName("GPU-Cluster-Health-Monitor")
        .master("local[*]")
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
                "org.postgresql:postgresql:42.6.0")
        .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoints")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )

def read_kafka(spark):
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
        .selectExpr("CAST(value AS STRING) as json_str")
        .select(from_json(col("json_str"), TELEMETRY_SCHEMA).alias("d"))
        .select("d.*")
        .withColumn("event_time", to_timestamp(col("event_time")))
    )

def process_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        log.info("Batch %d is empty, skipping.", batch_id)
        return

    row_count = batch_df.count()
    log.info("Processing batch %d  rows=%d", batch_id, row_count)

    # 1. Raw snapshot
    batch_df.select(
        "event_time", "gpu_id", "node", "gpu_model",
        "utilization_pct", "temperature_c", "power_draw_w",
        "mem_bandwidth_gbs", "mem_used_pct",
        "sm_clock_mhz", "mem_clock_mhz", "error_code"
    ).write.jdbc(POSTGRES_URL, "gpu_raw_telemetry", mode="append", properties=POSTGRES_PROPS)

    # 2. Per-GPU aggregates
    agg = batch_df.groupBy("gpu_id", "node", "gpu_model").agg(
        spark_round(avg("utilization_pct"),    2).alias("avg_util"),
        spark_round(avg("temperature_c"),      2).alias("avg_temp"),
        spark_round(spark_max("temperature_c"),2).alias("max_temp"),
        spark_round(avg("power_draw_w"),       2).alias("avg_power"),
        spark_round(avg("mem_bandwidth_gbs"),  2).alias("avg_mem_bw"),
        spark_round(avg("mem_used_pct"),       2).alias("avg_mem_util"),
        spark_round(stddev("utilization_pct"), 2).alias("util_stddev"),
        spark_round(stddev("temperature_c"),   2).alias("temp_stddev"),
        count("*").alias("reading_count"),
    )

    # 3. Efficiency score
    scored = agg.withColumn(
        "efficiency_score",
        spark_round(
            (col("avg_util") * 0.50)
            + ((col("avg_mem_bw") / 900) * 100 * 0.50),
            2
        )
    )

    # 4. Health flags
    flagged = scored \
        .withColumn("thermal_throttle",
            when((col("max_temp") > 83) & (col("avg_util") < 60), lit(True))
            .otherwise(lit(False))) \
        .withColumn("underutilized",
            when(col("avg_util") < 25, lit(True))
            .otherwise(lit(False)))

    # 5. Anomaly detection
    cluster_mean = flagged.agg(avg("avg_util")).collect()[0][0] or 0.0
    cluster_std  = flagged.agg({"avg_util": "stddev"}).collect()[0][0] or 1.0

    analytics = flagged \
        .withColumn("anomaly_flag",
            when(
                ((col("avg_util") - lit(cluster_mean)) / lit(cluster_std)) > lit(2.5),
                lit(True)
            ).otherwise(lit(False))) \
        .withColumn("health_status",
            when(col("thermal_throttle"), lit("CRITICAL"))
            .when(col("anomaly_flag"),    lit("WARNING"))
            .when(col("underutilized"),   lit("LOW_UTIL"))
            .otherwise(lit("HEALTHY")))

    analytics.write.jdbc(
        POSTGRES_URL, "gpu_analytics", mode="append", properties=POSTGRES_PROPS
    )

    # 6. Alerts
    alerts = analytics.filter(col("health_status") != "HEALTHY")
    alert_count = alerts.count()
    if alert_count > 0:
        alerts.select(
            "gpu_id", "node",
            col("health_status").alias("alert_type"),
            "avg_temp", "avg_util", "max_temp"
        ).write.jdbc(
            POSTGRES_URL, "gpu_alerts", mode="append", properties=POSTGRES_PROPS
        )
        log.info("Wrote %d alert(s) to postgres.", alert_count)

def main():
    wait_for_kafka()
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    stream_df = read_kafka(spark)

    query = (
        stream_df.writeStream
        .foreachBatch(process_batch)
        .option("checkpointLocation", "/tmp/spark-checkpoints/main")
        .trigger(processingTime="10 seconds")
        .start()
    )

    log.info("Streaming query started. Awaiting termination...")
    query.awaitTermination()

if __name__ == "__main__":
    main()