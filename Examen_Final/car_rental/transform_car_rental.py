from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql import HiveContext
from pyspark.sql.functions import round, lower


SPARK_CONTEXT = SparkContext('local')
SPARK_SESSION = SparkSession(SPARK_CONTEXT)
HIVE_CONTEXT = HiveContext(SPARK_CONTEXT)

BASE_URL = "hdfs://172.17.0.2:9000/car_rental"
CRD_01_CSV_FILE = f"{BASE_URL}/CarRentalData.csv"
CRD_02_CSV_FILE = f"{BASE_URL}/CarRentalData02.csv"

# Read f1 csv files
df_crd_01 = SPARK_SESSION.read \
    .option('header', 'true') \
    .option('delimiter', ',').csv(CRD_01_CSV_FILE)

df_crd_02 = SPARK_SESSION.read \
    .option('header', 'true') \
    .option('delimiter', ';').csv(CRD_02_CSV_FILE)

# Print dataframes schema
# df_crd_01.printSchema()
# df_crd_02.printSchema()

# Show first 5 rows of dataframes
# df_crd_01.show(5)
# df_crd_02.show(5)

# Rename some columns
df_crd_01 = df_crd_01 \
    .withColumnRenamed('location.city', 'city') \
    .withColumnRenamed('location.country', 'country') \
    .withColumnRenamed('location.latitude', 'latitude') \
    .withColumnRenamed('location.longitude', 'longitude') \
    .withColumnRenamed('location.state', 'state') \
    .withColumnRenamed('owner.id', 'owner_id') \
    .withColumnRenamed('rate.daily', 'rate_daily') \
    .withColumnRenamed('vehicle.make', 'make') \
    .withColumnRenamed('vehicle.model', 'model') \
    .withColumnRenamed('vehicle.type', 'type') \
    .withColumnRenamed('vehicle.year', 'year')

# Cast some dataframe columns
df_crd_01 = df_crd_01.withColumn(
    'fuelType', df_crd_01.fuelType.cast('string')
)
df_crd_01 = df_crd_01.withColumn(
    'rating', df_crd_01.rating.cast('double')
)
df_crd_01 = df_crd_01.withColumn(
    'renterTripsTaken', df_crd_01.renterTripsTaken.cast('int')
)
df_crd_01 = df_crd_01.withColumn(
    'reviewCount', df_crd_01.reviewCount.cast('int')
)
df_crd_01 = df_crd_01.withColumn(
    'city', df_crd_01.city.cast('string')
)
df_crd_01 = df_crd_01.withColumn(
    'country', df_crd_01.country.cast('string')
)
df_crd_01 = df_crd_01.withColumn(
    'latitude', df_crd_01.latitude.cast('double')
)
df_crd_01 = df_crd_01.withColumn(
    'longitude', df_crd_01.longitude.cast('double')
)
df_crd_01 = df_crd_01.withColumn(
    'state', df_crd_01.state.cast('string')
)
df_crd_01 = df_crd_01.withColumn(
    'owner_id', df_crd_01.owner_id.cast('int')
)
df_crd_01 = df_crd_01.withColumn(
    'rate_daily', df_crd_01.rate_daily.cast('int')
)
df_crd_01 = df_crd_01.withColumn(
    'make', df_crd_01.make.cast('string')
)
df_crd_01 = df_crd_01.withColumn(
    'model', df_crd_01.model.cast('string')
)
df_crd_01 = df_crd_01.withColumn(
    'type', df_crd_01.type.cast('string')
)
df_crd_01 = df_crd_01.withColumn(
    'year', df_crd_01.year.cast('int')
)

# Round rating column
df_crd_01 = df_crd_01.select("*", round('rating'))

# Drop rating column (double type)
df_crd_01 = df_crd_01.drop(df_crd_01.rating)

# Rename round(rating, 0) column to rating
df_crd_01 = df_crd_01.withColumnRenamed('round(rating, 0)', 'rating')

# Cast rating column to int
df_crd_01 = df_crd_01.withColumn(
    'rating', df_crd_01.rating.cast('int')
)

# Cast some dataframe columns
df_crd_02 = df_crd_02.withColumn(
    'geo_point_2d', df_crd_02.geo_point_2d.cast('string')
)
df_crd_02 = df_crd_02.withColumn(
    'geo_shape', df_crd_02.geo_shape.cast('string')
)
df_crd_02 = df_crd_02.withColumn(
    'year', df_crd_02.year.cast('int')
)
df_crd_02 = df_crd_02.withColumn(
    'ste_code', df_crd_02.ste_code.cast('int')
)
df_crd_02 = df_crd_02.withColumn(
    'ste_name', df_crd_02.ste_name.cast('string')
)
df_crd_02 = df_crd_02.withColumn(
    'ste_area_code', df_crd_02.ste_area_code.cast('string')
)
df_crd_02 = df_crd_02.withColumn(
    'ste_type', df_crd_02.ste_type.cast('string')
)
df_crd_02 = df_crd_02.withColumn(
    'ste_stusps_code', df_crd_02.ste_stusps_code.cast('string')
)
df_crd_02 = df_crd_02.withColumn(
    'ste_fp_code', df_crd_02.ste_fp_code.cast('string')
)
df_crd_02 = df_crd_02.withColumn(
    'ste_gnis_code', df_crd_02.ste_gnis_code.cast('string')
)

# Select columns
df_crd_02 = df_crd_02.select(
    df_crd_02.ste_stusps_code,
    df_crd_02.ste_name,
)

# Rename column
df_crd_02 = df_crd_02 \
    .withColumnRenamed('ste_name', 'state_name')

# Join dataframes
df_crd = df_crd_01.join(
    df_crd_02,
    df_crd_01.state == df_crd_02.ste_stusps_code,
    'left',
).drop(df_crd_02.ste_stusps_code)

# Select columns
df_crd = df_crd.select(
    df_crd.fuelType,
    df_crd.rating,
    df_crd.renterTripsTaken,
    df_crd.reviewCount,
    df_crd.city,
    df_crd.state_name,
    df_crd.owner_id,
    df_crd.rate_daily,
    df_crd.make,
    df_crd.model,
    df_crd.year,
)

# Drop Nan rows in rating column
df_crd = df_crd.na.drop(subset=['rating'])

# Convert fuelType column to lowercase
df_crd = df_crd.withColumn('fuelType', lower(df_crd['fuelType']))

# Exclude state of Texas
df_crd = df_crd.filter(
    (df_crd.state_name != 'Texas')
)

# Insert dataframe data into Hive table
df_crd.write.mode('overwrite') \
    .insertInto('car_rental_db.car_rental_analytics')
