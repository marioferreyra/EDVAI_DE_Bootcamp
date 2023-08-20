from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql import HiveContext
from pyspark.sql.functions import (
    max,
    to_date,
    # lit,
)


SPARK_CONTEXT = SparkContext('local')
SPARK_SESSION = SparkSession(SPARK_CONTEXT)
HIVE_CONTEXT = HiveContext(SPARK_CONTEXT)

BASE_URL = "hdfs://172.17.0.2:9000/ingest"
F1_RESULTS_CSV_FILE = f"{BASE_URL}/results.csv"
F1_DRIVERS_CSV_FILE = f"{BASE_URL}/drivers.csv"

# Read f1 csv files
df_results = SPARK_SESSION.read.option('header', 'true').csv(
    F1_RESULTS_CSV_FILE
)
df_drivers = SPARK_SESSION.read.option('header', 'true').csv(
    F1_DRIVERS_CSV_FILE
)

# Print dataframes schema
# df_results.printSchema()
# df_drivers.printSchema()

# Show first 5 rows of dataframes
# df_results.show(5)
# df_drivers.show(5)

# Cast some results dataframe columns
df_results = df_results.withColumn(
    'resultId', df_results.resultId.cast('int')
)
df_results = df_results.withColumn(
    'raceId', df_results.raceId.cast('int')
)
df_results = df_results.withColumn(
    'driverId', df_results.driverId.cast('int')
)
df_results = df_results.withColumn(
    'constructorId', df_results.constructorId.cast('int')
)
df_results = df_results.withColumn(
    'points', df_results.points.cast('double')
)

# Cast some drivers dataframe columns
df_drivers = df_drivers.withColumn(
    'driverId', df_drivers.driverId.cast('int')
)
df_drivers = df_drivers.withColumn(
    'dob', to_date(df_drivers.dob, 'yyyy-MM-dd')
)

# Get results with max points
print("Step 1 - Get results with max points")
results_max_points_column = df_results.select(max(df_results.points))
rows_results_max_points = results_max_points_column.collect()
max_points = rows_results_max_points[0]['max(points)']
print(f"Max Points = {max_points}")

# Filter results with the most points
print("Step 2 - Filter results with the most points")
df_results_filter = df_results.filter(df_results.points == max_points)
df_results_filter = df_results_filter.select(
    df_results_filter.driverId,
    df_results_filter.points,
)

print(f"DF Results Filter Rows    = {df_results_filter.count()}")
print(f"DF Results Filter Columns = {len(df_results_filter.columns)}")

# Get Drivers with the most points and select columns

# unique_drivers_id = df_results_filter.select('driverId') \
#   .distinct().rdd.flatMap(lambda x: x).collect()
# df_drivers_filter = df_drivers.filter(
#     df_drivers.driverId.isin(unique_drivers_id)
# )
# df_drivers_filter.withColumn('points', lit(max_points))

print("Step 3 - Get Drivers with the most points and select columns")
df_drivers_filter = df_drivers.join(
    df_results_filter,
    df_drivers.driverId == df_results_filter.driverId,
    'inner',
).drop(df_results_filter.driverId)

df_drivers_filter = df_drivers_filter.select(
    df_drivers_filter.forename,
    df_drivers_filter.surname,
    df_drivers_filter.nationality,
    df_drivers_filter.points,
)

# Rename some columns
df_drivers_filter = df_drivers_filter \
    .withColumnRenamed('forename', 'driver_forename') \
    .withColumnRenamed('surname', 'driver_surname') \
    .withColumnRenamed('nationality', 'driver_nationality')

print(f"DF Drivers Filter Rows    = {df_drivers_filter.count()}")
print(f"DF Drivers Filter Columns = {len(df_drivers_filter.columns)}")


# Insert dataframe data into Hive table
print("Step 4 - Insert dataframe data into Hive table")
df_drivers_filter.write.mode('overwrite') \
    .insertInto('f1.driver_results')
