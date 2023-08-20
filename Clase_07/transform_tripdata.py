from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql import HiveContext


SPARK_CONTEXT = SparkContext('local')
SPARK_SESSION = SparkSession(SPARK_CONTEXT)
HIVE_CONTEXT = HiveContext(SPARK_CONTEXT)

BASE_URL = "hdfs://172.17.0.2:9000/ingest"
TRIPDATA_2021_01_PARQUET_FILE = f"{BASE_URL}/yellow_tripdata_2021-01.parquet"
TRIPDATA_2021_02_PARQUET_FILE = f"{BASE_URL}/yellow_tripdata_2021-02.parquet"

# Read tripdata parquet files
df_tripdata_2021_01 = SPARK_SESSION.read.option('header', 'true').parquet(
    TRIPDATA_2021_01_PARQUET_FILE
)
df_tripdata_2021_02 = SPARK_SESSION.read.option('header', 'true').parquet(
    TRIPDATA_2021_02_PARQUET_FILE
)

# Print dataframes shape
# print((df_tripdata_2021_01.count(), len(df_tripdata_2021_01.columns)))
# print((df_tripdata_2021_02.count(), len(df_tripdata_2021_02.columns)))

# Join dataframes
df_tripdata_2021 = df_tripdata_2021_01.union(df_tripdata_2021_02)

# Sort dataframe
df_tripdata_2021 = df_tripdata_2021.sort(
    df_tripdata_2021.tpep_pickup_datetime.asc(),
    df_tripdata_2021.tpep_dropoff_datetime.asc(),
)

# Filter travels paid with cash that started or ended at airports
# Airport_fee: $1.25 for pick up only at LaGuardia and John F. Kennedy Airports
df_tripdata_2021_filter = df_tripdata_2021.filter(
    (df_tripdata_2021.payment_type == 2)
    & (df_tripdata_2021.airport_fee == 1.25)
)

# Select columns
df_tripdata_2021_filter = df_tripdata_2021_filter.select(
    df_tripdata_2021_filter.tpep_pickup_datetime,
    df_tripdata_2021_filter.airport_fee,
    df_tripdata_2021_filter.payment_type,
    df_tripdata_2021_filter.tolls_amount,
)

# Insert dataframe data into Hive table
df_tripdata_2021_filter.write.insertInto("tripdata.airport_trips")
