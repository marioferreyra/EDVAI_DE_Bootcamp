from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql import HiveContext
from pyspark.sql.functions import to_date


SPARK_CONTEXT = SparkContext('local')
SPARK_SESSION = SparkSession(SPARK_CONTEXT)
HIVE_CONTEXT = HiveContext(SPARK_CONTEXT)

BASE_URL_ENVIOS = "hdfs://172.17.0.2:9000/sqoop/ingest/envios"
BASE_URL_ORDER_DETAILS = "hdfs://172.17.0.2:9000/sqoop/ingest/order_details"

# Read envios and order_details parquet files
df_envios = SPARK_SESSION.read.option('header', 'true').parquet(
    f"{BASE_URL_ENVIOS}/*.parquet"
)
df_order_details = SPARK_SESSION.read.option('header', 'true').parquet(
    f"{BASE_URL_ORDER_DETAILS}/*.parquet"
)

# Cast some envios dataframe columns
df_envios = df_envios.withColumn(
    'order_id', df_envios.order_id.cast('int')
)
df_envios = df_envios.withColumn(
    'shipped_date', to_date(df_envios.shipped_date, 'yyyy-MM-dd')
)
df_envios = df_envios.withColumn(
    'company_name', df_envios.company_name.cast('string')
)
df_envios = df_envios.withColumn(
    'phone', df_envios.phone.cast('string')
)

# Cast some order_details dataframe columns
df_order_details = df_order_details.withColumn(
    'order_id', df_order_details.order_id.cast('int')
)
df_order_details = df_order_details.withColumn(
    'unit_price', df_order_details.unit_price.cast('double')
)
df_order_details = df_order_details.withColumn(
    'quantity', df_order_details.quantity.cast('int')
)
df_order_details = df_order_details.withColumn(
    'discount', df_order_details.discount.cast('double')
)

# Join envio and order_details
print("Step 1 - Join envio and order_details")
df_envios_od = df_envios.join(
    df_order_details,
    df_envios.order_id == df_order_details.order_id,
    'inner',
).drop(df_order_details.order_id)

print(f"DF Envios OD Rows    = {df_envios_od.count()}")
print(f"DF Envios OD Columns = {len(df_envios_od.columns)}")

# Filter orders that have had a discount
print("Step 2 - Filter orders that have had a discount")
df_envios_od = df_envios_od.filter(
    df_envios_od.discount > 0
)

print(f"DF Envios OD Rows    = {df_envios_od.count()}")
print(f"DF Envios OD Columns = {len(df_envios_od.columns)}")

# Added new column unit_price_discount
print("Step 3 - Added new column unit_price_discount")
df_envios_od = df_envios_od.withColumn(
    'unit_price_discount',
    (df_envios_od['unit_price'] - df_envios_od['discount']),
)

# Added new column total_price
print("Step 4 - Added new column total_price")
df_envios_od = df_envios_od.withColumn(
    'total_price',
    (df_envios_od['unit_price_discount'] * df_envios_od['quantity']),
)

# Select columns
df_envios_od = df_envios_od.select(
    df_envios_od.order_id,
    df_envios_od.shipped_date,
    df_envios_od.company_name,
    df_envios_od.phone,
    df_envios_od.unit_price_discount,
    df_envios_od.quantity,
    df_envios_od.total_price,
)

print(f"DF Envios OD Rows    = {df_envios_od.count()}")
print(f"DF Envios OD Columns = {len(df_envios_od.columns)}")

# Insert dataframe data into Hive table
print("Step 5 - Insert dataframe data into Hive table")
df_envios_od.write.mode('overwrite') \
    .insertInto('northwind_analytics.products_sent')
