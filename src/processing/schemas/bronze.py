from pyspark.sql.types import StructType, StructField, StringType, LongType, BooleanType

# Binance Futures AggTrade Schema
# https://binance-docs.github.io/apidocs/futures/en/#aggregate-trade-streams
# Payload:
# {
#   "e": "aggTrade",  // Event type
#   "E": 123456789,   // Event time
#   "s": "BTCUSDT",   // Symbol
#   "a": 12345,       // Aggregate trade ID
#   "p": "0.001",     // Price
#   "q": "100",       // Quantity
#   "f": 100,         // First trade ID
#   "l": 105,         // Last trade ID
#   "T": 123456785,   // Trade time
#   "m": true,        // Is the buyer the market maker?
# }
binance_futures_aggtrade_schema = StructType([
    StructField("e", StringType(), True),  # Event type
    StructField("E", LongType(), True),    # Event time
    StructField("s", StringType(), True),  # Symbol
    StructField("a", LongType(), True),    # Aggregate trade ID
    StructField("p", StringType(), True),  # Price
    StructField("q", StringType(), True),  # Quantity
    StructField("f", LongType(), True),    # First trade ID
    StructField("l", LongType(), True),    # Last trade ID
    StructField("T", LongType(), True),    # Trade time
    StructField("m", BooleanType(), True)  # Is the buyer the market maker?
])
