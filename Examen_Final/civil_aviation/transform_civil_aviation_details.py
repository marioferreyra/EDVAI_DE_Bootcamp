from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql import HiveContext


SPARK_CONTEXT = SparkContext('local')
SPARK_SESSION = SparkSession(SPARK_CONTEXT)
HIVE_CONTEXT = HiveContext(SPARK_CONTEXT)

BASE_URL = "hdfs://172.17.0.2:9000/civil_aviation"
CV_AERO_DET_CSV_FILE = f"{BASE_URL}/aeropuertos_detalle.csv"

# Read Civil Aviations csv files
df_aero_det = SPARK_SESSION.read \
    .option('header', 'true') \
    .option('delimiter', ';').csv(CV_AERO_DET_CSV_FILE)

# Print dataframes schema
# df_aero_det.printSchema()

# Show first 5 rows of dataframes
# df_aero_det.show(5)

# Rename some columns
df_aero_det = df_aero_det \
    .withColumnRenamed('local', 'aeropuerto') \
    .withColumnRenamed('oaci', 'oac')

# Cast some dataframe columns
df_aero_det = df_aero_det.withColumn(
    'aeropuerto', df_aero_det.aeropuerto.cast('string')
)
df_aero_det = df_aero_det.withColumn(
    'oac', df_aero_det.oac.cast('string')
)
df_aero_det = df_aero_det.withColumn(
    'iata', df_aero_det.iata.cast('string')
)
df_aero_det = df_aero_det.withColumn(
    'tipo', df_aero_det.tipo.cast('string')
)
df_aero_det = df_aero_det.withColumn(
    'denominacion', df_aero_det.denominacion.cast('string')
)
df_aero_det = df_aero_det.withColumn(
    'coordenadas', df_aero_det.coordenadas.cast('string')
)
df_aero_det = df_aero_det.withColumn(
    'latitud', df_aero_det.latitud.cast('string')
)
df_aero_det = df_aero_det.withColumn(
    'longitud', df_aero_det.longitud.cast('string')
)
df_aero_det = df_aero_det.withColumn(
    'elev', df_aero_det.elev.cast('string')
)
df_aero_det = df_aero_det.withColumn(
    'uom_elev', df_aero_det.uom_elev.cast('string')
)
df_aero_det = df_aero_det.withColumn(
    'ref', df_aero_det.ref.cast('string')
)
df_aero_det = df_aero_det.withColumn(
    'distancia_ref', df_aero_det.distancia_ref.cast('double')
)
df_aero_det = df_aero_det.withColumn(
    'direccion_ref', df_aero_det.direccion_ref.cast('string')
)
df_aero_det = df_aero_det.withColumn(
    'condicion', df_aero_det.condicion.cast('string')
)
df_aero_det = df_aero_det.withColumn(
    'control', df_aero_det.control.cast('string')
)
df_aero_det = df_aero_det.withColumn(
    'region', df_aero_det.region.cast('string')
)
df_aero_det = df_aero_det.withColumn(
    'fir', df_aero_det.fir.cast('string')
)
df_aero_det = df_aero_det.withColumn(
    'uso', df_aero_det.uso.cast('string')
)
df_aero_det = df_aero_det.withColumn(
    'trafico', df_aero_det.trafico.cast('string')
)
df_aero_det = df_aero_det.withColumn(
    'sna', df_aero_det.sna.cast('string')
)
df_aero_det = df_aero_det.withColumn(
    'concesionado', df_aero_det.concesionado.cast('string')
)
df_aero_det = df_aero_det.withColumn(
    'provincia', df_aero_det.provincia.cast('string')
)
df_aero_det = df_aero_det.withColumn(
    'inhab', df_aero_det.inhab.cast('string')
)

# Select columns
df_aero_det = df_aero_det.select(
    df_aero_det.aeropuerto,
    df_aero_det.oac,
    df_aero_det.iata,
    df_aero_det.tipo,
    df_aero_det.denominacion,
    df_aero_det.coordenadas,
    df_aero_det.latitud,
    df_aero_det.longitud,
    df_aero_det.elev,
    df_aero_det.uom_elev,
    df_aero_det.ref,
    df_aero_det.distancia_ref,
    df_aero_det.direccion_ref,
    df_aero_det.condicion,
    df_aero_det.control,
    df_aero_det.region,
    df_aero_det.uso,
    df_aero_det.trafico,
    df_aero_det.sna,
    df_aero_det.concesionado,
    df_aero_det.provincia,
)

# Replace 0 for null on only distancia_ref column
df_aero_det = df_aero_det.na.fill(value=0.0, subset=['distancia_ref'])

# Filter only 'Nacional' value in trafico column
df_aero_det_nac = df_aero_det.filter(
    (df_aero_det.trafico == 'Nacional')
)

# Insert dataframes data into Hive table
df_aero_det_nac.write.mode('overwrite') \
    .insertInto('aeropuerto.aeropuerto_detalles_tabla')
