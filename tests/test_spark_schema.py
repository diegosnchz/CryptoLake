from pyspark.sql.types import BooleanType, DoubleType, LongType, StringType

from src.contracts.spark_schema import TRADE_EVENT_V1_SPARK_SCHEMA


def test_trade_event_v1_spark_schema_fields_and_types() -> None:
    fields = {field.name: field.dataType for field in TRADE_EVENT_V1_SPARK_SCHEMA.fields}

    assert isinstance(fields["event_version"], StringType)
    assert isinstance(fields["source"], StringType)
    assert isinstance(fields["symbol"], StringType)
    assert isinstance(fields["event_time_ms"], LongType)
    assert isinstance(fields["trade_time_ms"], LongType)
    assert isinstance(fields["price"], DoubleType)
    assert isinstance(fields["quantity"], DoubleType)
    assert isinstance(fields["trade_id"], LongType)
    assert isinstance(fields["is_buyer_maker"], BooleanType)
