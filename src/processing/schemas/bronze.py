from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

futures_trade_schema = StructType(
    [
        StructField("symbol", StringType(), False),
        StructField("event_time", TimestampType(), False),
        StructField("price", DoubleType(), False),
        StructField("qty", DoubleType(), False),
        StructField("side", StringType(), False),
        StructField("trade_id", LongType(), False),
        StructField("exchange", StringType(), False),
        StructField("ingest_ts", TimestampType(), False),
    ]
)
