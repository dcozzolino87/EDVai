from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql import HiveContext
from pyspark.sql import functions as F
from pyspark.sql.functions import col, when, to_date

sc = SparkContext('local')
spark = SparkSession(sc)
hc = HiveContext(sc)

## Leo los archivos ingestados en HDFS y los cargo en un DataFrame
df_im21 = spark.read.option("delimiter", ";").option("header","true").csv("/ingest/2021-informe-ministerio.csv")
df_im22 = spark.read.option("delimiter", ";").option("header","true").csv("/ingest/202206-informe-ministerio.csv")
df_ad = spark.read.option("delimiter", ";").option("header","true").csv("/ingest/aeropuertos_detalle.csv")

## Uno los archivos csv con la misma estructura para su posterior analisis
df_vr = df_im21.unionByName(df_im22)

## Elimino las columnas no requeridas para el analisis
df_ad = df_ad.drop("inhab", "fir")
df_vr = df_vr.drop("Calidad dato")

## Filtro solo los vuelos domesticos
df_domesticos = df_vr.filter((col("Clasificación Vuelo") == 'Doméstico') | (col("Clasificación Vuelo") == 'Domestico'))

## A los valores null de los campos Pasajeros y distancia_ref les asigno 0
df_domesticos_fix = df_domesticos.fillna({"Pasajeros":0})
df_ad_fix = df_ad.fillna({"distancia_ref":0})

## Modifico los nombres de las columnas
df_domesticos_final = df_domesticos_fix.withColumnRenamed('Fecha', 'fecha') \
                         .withColumnRenamed('Hora UTC', 'horaUTC') \
                         .withColumnRenamed('Clase de Vuelo (todos los vuelos)', 'clase_de_vuelo') \
                         .withColumnRenamed('Clasificación Vuelo', 'clasificacion_de_vuelo') \
                         .withColumnRenamed('Tipo de Movimiento', 'tipo_de_movimiento') \
                         .withColumnRenamed('Aeropuerto', 'aeropuerto') \
                         .withColumnRenamed('Origen / Destino', 'origen_destino') \
                         .withColumnRenamed('Aerolinea Nombre', 'aerolinea_nombre') \
                         .withColumnRenamed('Aeronave', 'aeronave') \
                         .withColumnRenamed('Pasajeros', 'pasajeros')

# Casteo la columna fecha a Date
df_domesticos_final = df_domesticos_final.withColumn('fecha', to_date(col('fecha'), 'dd/MM/yyyy'))

## Creo las vistas de los DF
df_domesticos_final.createOrReplaceTempView("v_vuelos")
df_ad_fix.createOrReplaceTempView("v_aeropuertos")

## Filtro el DF segun los datos que necesito guardar en la BD
df_vr_results = spark.sql("select fecha, horaUTC, clase_de_vuelo, clasificacion_de_vuelo,tipo_de_movimiento, aeropuerto, origen_destino, aerolinea_nombre, aeronave, cast(pasajeros as int) from v_vuelos")
df_ad_results = spark.sql("select local as aeropuerto, oaci as oac, iata, tipo, denominacion, coordenadas, latitud, longitud, cast(elev as float), uom_elev, ref, cast(distancia_ref as float), direccion_ref, condicion, control, region, uso, trafico, sna, concesionado, provincia from v_aeropuertos")

## Creo una vista del DF con los datos a insertar en la BD
df_vr_results.createOrReplaceTempView("v_insert_vr")
df_ad_results.createOrReplaceTempView("v_insert_ad")

## Inserto los datos en la BD
spark.sql("insert into anac.vuelos_realizados select * from v_insert_vr")
spark.sql("insert into anac.aeropuertos_detalle select * from v_insert_ad")
