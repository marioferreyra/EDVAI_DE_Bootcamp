from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql import HiveContext
from pyspark.sql.functions import to_date


SPARK_CONTEXT = SparkContext('local')
SPARK_SESSION = SparkSession(SPARK_CONTEXT)
HIVE_CONTEXT = HiveContext(SPARK_CONTEXT)

BASE_URL = "hdfs://172.17.0.2:9000/civil_aviation"
CV_2021_INFORME_CSV_FILE = f"{BASE_URL}/2021-informe-ministerio.csv"
CV_2022_INFORME_CSV_FILE = f"{BASE_URL}/202206-informe-ministerio.csv"

# Read Civil Aviations csv files
df_cv_2021 = SPARK_SESSION.read \
    .option('header', 'true') \
    .option('delimiter', ';').csv(CV_2021_INFORME_CSV_FILE)

df_cv_2022 = SPARK_SESSION.read \
    .option('header', 'true') \
    .option('delimiter', ';').csv(CV_2022_INFORME_CSV_FILE)

# Print dataframes schema
# df_cv_2021.printSchema()
# df_cv_2022.printSchema()

# Show first 5 rows of dataframes
# df_cv_2021.show(5)
# df_cv_2022.show(5)

# Join dataframes
df_cv_21_22 = df_cv_2021.union(df_cv_2022)

# Rename some columns
df_cv_21_22 = df_cv_21_22 \
    .withColumnRenamed('Fecha', 'fecha') \
    .withColumnRenamed('Hora UTC', 'horaUTC') \
    .withColumnRenamed('Clase de Vuelo (todos los vuelos)', 'clase_de_vuelo') \
    .withColumnRenamed('Clasificación Vuelo', 'clasificacion_de_vuelo') \
    .withColumnRenamed('Tipo de Movimiento', 'tipo_de_movimiento') \
    .withColumnRenamed('Aeropuerto', 'aeropuerto') \
    .withColumnRenamed('Origen / Destino', 'origen_destino') \
    .withColumnRenamed('Aerolinea Nombre', 'aerolinea_nombre') \
    .withColumnRenamed('Aeronave', 'aeronave') \
    .withColumnRenamed('Pasajeros', 'pasajeros') \
    .withColumnRenamed('Calidad dato', 'calidad_dato')

# Cast some dataframe columns
df_cv_21_22 = df_cv_21_22.withColumn(
    'fecha', to_date(df_cv_21_22.fecha, 'dd/MM/yyyy')
)
df_cv_21_22 = df_cv_21_22.withColumn(
    'horaUTC', df_cv_21_22.horaUTC.cast('string')
)
df_cv_21_22 = df_cv_21_22.withColumn(
    'clase_de_vuelo', df_cv_21_22.clase_de_vuelo.cast('string')
)
df_cv_21_22 = df_cv_21_22.withColumn(
    'clasificacion_de_vuelo', df_cv_21_22.clasificacion_de_vuelo.cast('string')
)
df_cv_21_22 = df_cv_21_22.withColumn(
    'tipo_de_movimiento', df_cv_21_22.tipo_de_movimiento.cast('string')
)
df_cv_21_22 = df_cv_21_22.withColumn(
    'aeropuerto', df_cv_21_22.aeropuerto.cast('string')
)
df_cv_21_22 = df_cv_21_22.withColumn(
    'origen_destino', df_cv_21_22.origen_destino.cast('string')
)
df_cv_21_22 = df_cv_21_22.withColumn(
    'aerolinea_nombre', df_cv_21_22.aerolinea_nombre.cast('string')
)
df_cv_21_22 = df_cv_21_22.withColumn(
    'aeronave', df_cv_21_22.aeronave.cast('string')
)
df_cv_21_22 = df_cv_21_22.withColumn(
    'pasajeros', df_cv_21_22.pasajeros.cast('int')
)
df_cv_21_22 = df_cv_21_22.withColumn(
    'calidad_dato', df_cv_21_22.calidad_dato.cast('string')
)

# Select columns
df_cv_21_22 = df_cv_21_22.select(
    df_cv_21_22.fecha,
    df_cv_21_22.horaUTC,
    df_cv_21_22.clase_de_vuelo,
    df_cv_21_22.clasificacion_de_vuelo,
    df_cv_21_22.tipo_de_movimiento,
    df_cv_21_22.aeropuerto,
    df_cv_21_22.origen_destino,
    df_cv_21_22.aerolinea_nombre,
    df_cv_21_22.aeronave,
    df_cv_21_22.pasajeros,
)

# Replace 0 for null on only pasajeros column
df_cv_21_22 = df_cv_21_22.na.fill(value=0, subset=['pasajeros'])

# Filter only 'Domestico' or 'Doméstico' value in clasificacion_de_vuelo column
df_cv_21_22_nac = df_cv_21_22.filter(
    (df_cv_21_22.clasificacion_de_vuelo != 'Internacional')
)

# Insert dataframes data into Hive table
df_cv_21_22_nac.write.mode('overwrite') \
    .insertInto('aeropuerto.aeropuerto_tabla')
