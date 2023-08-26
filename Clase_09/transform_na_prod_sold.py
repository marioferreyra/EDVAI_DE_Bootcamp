from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql import HiveContext
from pyspark.sql.functions import avg


SPARK_CONTEXT = SparkContext('local')
SPARK_SESSION = SparkSession(SPARK_CONTEXT)
HIVE_CONTEXT = HiveContext(SPARK_CONTEXT)

BASE_URL = "hdfs://172.17.0.2:9000/sqoop/ingest/clientes"
CLIENTES_PARQUET_FILE = f"{BASE_URL}/*.parquet"

# Read clientes parquet file
df_clientes = SPARK_SESSION.read.option('header', 'true').parquet(
    CLIENTES_PARQUET_FILE
)

# Cast some results dataframe columns
df_clientes = df_clientes.withColumn(
    'customer_id', df_clientes.customer_id.cast('string')
)
df_clientes = df_clientes.withColumn(
    'company_name', df_clientes.company_name.cast('string')
)
df_clientes = df_clientes.withColumn(
    'productos_vendidos', df_clientes.productos_vendidos.cast('int')
)

# Get productos_vendidos average
print("Step 1 - Get productos_vendidos average")
cavg_prod_vend_column = df_clientes.select(avg(df_clientes.productos_vendidos))
rows_cavg_prod_vend = cavg_prod_vend_column.collect()
avg_productos_vendidos = rows_cavg_prod_vend[0]['avg(productos_vendidos)']
print(f"AVG productos_vendidos = {avg_productos_vendidos}")

# Filter clientes where productos_vendidos > average
print("Step 2 - Filter clientes where productos_vendidos > average")
df_clientes_filter = df_clientes.filter(
    df_clientes.productos_vendidos > avg_productos_vendidos
)

print(f"DF Clientes Filter Rows    = {df_clientes_filter.count()}")
print(f"DF Clientes Filter Columns = {len(df_clientes_filter.columns)}")

# Insert dataframe data into Hive table
print("Step 3 - Insert dataframe data into Hive table")
df_clientes_filter.write.mode('overwrite') \
    .insertInto("northwind_analytics.products_sold")
