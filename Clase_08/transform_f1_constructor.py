from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql import HiveContext


SPARK_CONTEXT = SparkContext('local')
SPARK_SESSION = SparkSession(SPARK_CONTEXT)
HIVE_CONTEXT = HiveContext(SPARK_CONTEXT)

BASE_URL = "hdfs://172.17.0.2:9000/ingest"
F1_RESULTS_CSV_FILE = f"{BASE_URL}/results.csv"
F1_CONSTRUCTORS_CSV_FILE = f"{BASE_URL}/constructors.csv"
F1_RACES_CSV_FILE = f"{BASE_URL}/races.csv"

# Read f1 csv files
df_results = SPARK_SESSION.read.option('header', 'true').csv(
    F1_RESULTS_CSV_FILE
)
df_constructors = SPARK_SESSION.read.option('header', 'true').csv(
    F1_CONSTRUCTORS_CSV_FILE
)
df_races = SPARK_SESSION.read.option('header', 'true').csv(
    F1_RACES_CSV_FILE
)

# Print dataframes schema
# df_results.printSchema()
# df_constructors.printSchema()
# df_races.printSchema()

# Show first 5 rows of dataframes
# df_results.show(5)
# df_constructors.show(5)
# df_races.show(5)

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

# Cast some constructors dataframe columns
df_constructors = df_constructors.withColumn(
    'constructorId', df_constructors.constructorId.cast('int')
)

# Cast some races dataframe columns
df_races = df_races.withColumn(
    'raceId', df_races.raceId.cast('int')
)
df_races = df_races.withColumn(
    'year', df_races.year.cast('int')
)
df_races = df_races.withColumn(
    'round', df_races.round.cast('int')
)
df_races = df_races.withColumn(
    'circuitId', df_races.circuitId.cast('int')
)

# Filter 'Spanish Grand Prix' races in 1991
print("Step 1 - Filter 'Spanish Grand Prix' races in 1991")
df_races_filter = df_races.filter(
    (df_races.name == 'Spanish Grand Prix')
    & (df_races.year == 1991)
)

# Select columns
df_races_filter = df_races_filter.select(df_races_filter.raceId)

print(f"DF Races Filter Rows    = {df_races_filter.count()}")
print(f"DF Races Filter Columns = {len(df_races_filter.columns)}")

# Get Results from 'Spanish Grand Prix' races in 1991
print("Step 2 - Get Results from 'Spanish Grand Prix' races in 1991")
df_results_filter = df_results.join(
    df_races_filter,
    df_results.raceId == df_races_filter.raceId,
    'inner',
).drop(df_races_filter.raceId)

# Select columns
df_results_filter = df_results_filter.select(
    df_results_filter.constructorId,
    df_results_filter.points,
)

print(f"DF Results Filter Rows    = {df_results_filter.count()}")
print(f"DF Results Filter Columns = {len(df_results_filter.columns)}")

# Group by constructorId and sum points
df_results_grouped = df_results_filter.groupBy('constructorId') \
    .sum('points') \
    .withColumnRenamed('sum(points)', 'points')

print(f"DF Results Grouped Rows    = {df_results_grouped.count()}")
print(f"DF Results Grouped Columns = {len(df_results_grouped.columns)}")

# Get Constructors by constructors results grouped
print("Step 3 - Get Constructors by constructors results grouped")
df_constructors_filter = df_constructors.join(
    df_results_grouped,
    df_constructors.constructorId == df_results_grouped.constructorId,
    'inner',
).drop(df_results_grouped.constructorId)

# Select columns
df_constructors_filter = df_constructors_filter.select(
    df_constructors_filter.constructorRef,
    df_constructors_filter.name,
    df_constructors_filter.nationality,
    df_constructors_filter.url,
    df_constructors_filter.points,
)

# Rename some columns
df_constructors_filter = df_constructors_filter \
    .withColumnRenamed('name', 'cons_name') \
    .withColumnRenamed('nationality', 'cons_nationality')

print(f"DF Constructor Filter Rows    = {df_constructors_filter.count()}")
print(f"DF Constructor Filter Columns = {len(df_constructors_filter.columns)}")

# Insert dataframe data into Hive table
print("Step 4 - Insert dataframe data into Hive table")
df_constructors_filter.write.mode('overwrite') \
    .insertInto('f1.constructor_results')
