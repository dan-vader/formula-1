from pyspark import pipelines as dp
from pyspark.sql import functions as F

from formula_1.bronze.openf1_stream import STREAM_ENDPOINTS

NAMESPACE = spark.conf.get("formula_1.eventhub_namespace")
EVENTHUB = spark.conf.get("formula_1.eventhub_name")
SECRET_SCOPE = spark.conf.get("formula_1.secret_scope")
LISTEN_SECRET = "formula-1-eventhub-listen-conn-str"

ENVELOPE_SCHEMA = "replay_id STRING, endpoint STRING, session_key BIGINT, event_time STRING"


def jaas_config():
    connection_string = dbutils.secrets.get(SECRET_SCOPE, LISTEN_SECRET)
    return (
        "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required "
        f'username="$ConnectionString" password="{connection_string}";'
    )


@dp.table(
    name="openf1_stream_raw",
    comment="Raw OpenF1 replay messages from Event Hub; rows with endpoint IS NULL are invalid messages",
    # Append-only history: a full refresh would wipe it and the hub keeps data only for a short time
    table_properties={"pipelines.reset.allowed": "false"},
)
def openf1_stream_raw():
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", f"{NAMESPACE}.servicebus.windows.net:9093")
        .option("subscribe", EVENTHUB)
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.mechanism", "PLAIN")
        .option("kafka.sasl.jaas.config", jaas_config())
        .option("startingOffsets", "earliest")
        # Retention is short, so expired offsets must not fail the update
        .option("failOnDataLoss", "false")
        .load()
    )
    message = F.col("value").cast("string")
    envelope = F.from_json(message, ENVELOPE_SCHEMA)
    payload = F.get_json_object(message, "$.payload")
    is_valid = (
        envelope["endpoint"].isin(*STREAM_ENDPOINTS)
        & envelope["session_key"].isNotNull()
        & envelope["replay_id"].isNotNull()
        & payload.isNotNull()
    )
    return raw.select(
        F.when(is_valid, envelope["endpoint"]).alias("endpoint"),
        F.when(is_valid, envelope["session_key"]).alias("session_key"),
        F.when(is_valid, envelope["replay_id"]).alias("replay_id"),
        F.when(is_valid, F.expr("try_to_timestamp(get_json_object(cast(value as string), '$.event_time'))")).alias(
            "event_time"
        ),
        F.when(is_valid, payload).alias("payload"),
        message.alias("_raw_message"),
        F.col("partition").alias("_kafka_partition"),
        F.col("offset").alias("_kafka_offset"),
        F.col("timestamp").alias("_kafka_timestamp"),
        F.current_timestamp().alias("_ingested_at"),
    )
