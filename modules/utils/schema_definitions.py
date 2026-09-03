import pyspark.sql.types as T

TIMESERIES_VALUE_TYPE = T.FloatType()
TIMESERIES_VALUE_SQL_TYPE = "float"

TIMESERIES_TIME_TYPE = T.DoubleType()
TIMESERIES_TIME_SQL_TYPE = "double"

BRONZE_META_OUTPUT_COLUMNS = [
    "container_id",
    "channel_id",
    "group_idx",
    "channel_idx",
    "channel_name",
    "unit",
    "filename",
    "header_datetime",
    "md_comment",
]

TAGS_SCHEMA = T.StructType([
    T.StructField("container_id", T.LongType(), nullable=False),
    T.StructField("channel_id", T.IntegerType(), nullable=True),
    T.StructField("key", T.StringType()),
    T.StructField("value", T.StringType()),
])

CONTAINER_TAGS_SCHEMA = T.StructType([
    T.StructField("container_id", T.LongType(), nullable=False),
    T.StructField("key", T.StringType()),
    T.StructField("value", T.StringType()),
])

METRICS_SCHEMA = T.StructType([
    T.StructField("container_id", T.LongType(), nullable=False),
    T.StructField("vehicle_key", T.StringType()),
    T.StructField("start_ts", TIMESERIES_TIME_TYPE),
    T.StructField("stop_ts", TIMESERIES_TIME_TYPE),
    T.StructField("start_dt", T.TimestampType()),
    T.StructField("stop_dt", T.TimestampType()),
    T.StructField("duration_s", TIMESERIES_TIME_TYPE),
    T.StructField("num_channels", T.IntegerType()),
])

CHANNELS_SCHEMA = T.StructType([
    T.StructField("container_id", T.LongType(), nullable=False),
    T.StructField("channel_id", T.IntegerType(), nullable=False),
    T.StructField("tstart", TIMESERIES_TIME_TYPE, nullable=False),
    T.StructField("tend", TIMESERIES_TIME_TYPE, nullable=False),
    T.StructField("value", TIMESERIES_VALUE_TYPE, nullable=True),
])

CHANNEL_METRICS_SCHEMA = T.StructType([
    T.StructField("container_id", T.LongType(), nullable=False),
    T.StructField("channel_id", T.IntegerType(), nullable=False),
    T.StructField("channel_name", T.StringType(), nullable=False),
    T.StructField("sample_count", T.IntegerType(), nullable=False),
    T.StructField("min", TIMESERIES_VALUE_TYPE, nullable=True),
    T.StructField("max", TIMESERIES_VALUE_TYPE, nullable=True),
    T.StructField("mean", TIMESERIES_VALUE_TYPE, nullable=True),
    T.StructField("begin_ts", TIMESERIES_TIME_TYPE, nullable=True),
    T.StructField("end_ts", TIMESERIES_TIME_TYPE, nullable=True),
    T.StructField("duration_s", TIMESERIES_TIME_TYPE, nullable=True),
    T.StructField("sample_rate", TIMESERIES_VALUE_TYPE, nullable=True),
    T.StructField("value_type", T.StringType(), nullable=True),
])

BRONZE_META_SCHEMA = T.StructType([
    T.StructField("container_id", T.LongType(), nullable=False),
    T.StructField("channel_id", T.IntegerType(), nullable=False),
    T.StructField("group_idx", T.IntegerType(), nullable=False),
    T.StructField("channel_idx", T.IntegerType(), nullable=False),
    T.StructField("channel_name", T.StringType(), nullable=False),
    T.StructField("unit", T.StringType(), nullable=True),
    T.StructField("filename", T.StringType(), nullable=False),
    T.StructField("header_datetime", T.TimestampType(), nullable=True),
    T.StructField("md_comment", T.StringType(), nullable=True),
])

MASTERS_SCHEMA = T.StructType([
    T.StructField("container_id", T.LongType(), nullable=False),
    T.StructField("group_idx", T.IntegerType(), nullable=False),
    T.StructField("timestamp", TIMESERIES_TIME_TYPE, nullable=False),
])
