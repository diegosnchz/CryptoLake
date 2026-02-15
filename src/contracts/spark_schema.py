from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

TRADE_EVENT_V1_SPARK_SCHEMA = StructType(
    [
        StructField("event_version", StringType(), False),
        StructField("source", StringType(), False),
        StructField("symbol", StringType(), False),
        StructField("event_time_ms", LongType(), False),
        StructField("trade_time_ms", LongType(), True),
        StructField("price", DoubleType(), False),
        StructField("quantity", DoubleType(), False),
        StructField("trade_id", LongType(), True),
        StructField("is_buyer_maker", BooleanType(), True),
    ]
)
