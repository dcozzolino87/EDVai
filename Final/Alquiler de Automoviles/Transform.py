from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql import HiveContext
from pyspark.sql import functions as F
from pyspark.sql.functions import col, when, to_date
from pyspark.sql.functions import round

sc = SparkContext('local')
spark = SparkSession(sc)
hc = HiveContext(sc)

## Leo los archivos ingestados en HDFS y los cargo en un DataFrame
df_rental = spark.read.option("delimiter", ",").option("header","true").csv("/ingest/CarRentalData.csv")
df_georef = spark.read.option("delimiter", ";").option("header","true").csv("/ingest/georef-united-states-of-america-state.csv")

## Modifico los nombres de las columnas segun los requerimientos, que no contengan puntos y espacios por ej, o que no sean nombres largos
df_rental = df_rental.withColumnRenamed("fuelType", "fueltype") \
    .withColumnRenamed("location.city", "city") \
    .withColumnRenamed("location.country", "country") \
    .withColumnRenamed("location.latitude", "location_latitude") \
    .withColumnRenamed("location.longitude", "location_longitude") \
    .withColumnRenamed("location.state", "state_name") \
    .withColumnRenamed("owner.id", "owner_id") \
    .withColumnRenamed("rate.daily", "rate_daily") \
    .withColumnRenamed("vehicle.make", "make") \
    .withColumnRenamed("vehicle.model", "model") \
    .withColumnRenamed("vehicle.type", "type") \
    .withColumnRenamed("vehicle.year", "year")
    
df_georef = df_georef.withColumnRenamed("Geo Point", "geo_point") \
    .withColumnRenamed("Geo Shape", "geo_shape") \
    .withColumnRenamed("Year", "year_georef") \
    .withColumnRenamed("Official Code State", "official_code_state") \
    .withColumnRenamed("Official Name State", "official_name_state") \
    .withColumnRenamed("Iso 3166-3 Area Code", "iso_3166_3_area_code") \
    .withColumnRenamed("Type", "type") \
    .withColumnRenamed("United States Postal Service state abbreviation", "usps_state_abbreviation") \
    .withColumnRenamed("State FIPS Code", "state_fips_code") \
    .withColumnRenamed("State GNIS Code", "state_gnis_code")
    
## Hago el join de los dos archivos
df_innerjoin_rental = df_rental.join(df_georef, df_rental["state_name"] == df_georef["usps_state_abbreviation"], 'inner')

## Filtro que el rating no sea nulo y excluyo al Estado de Texas TX
df_filtered_rating = df_innerjoin_rental.filter(df_innerjoin_rental["rating"] != 0)
df_filtered_texas = df_filtered_rating_zero.filter(lower(df_filtered_rating["state_name"]) != "tx")

# Eliminamos las columnas que no son necesarias
df_filtered_texas = df_filtered_texas.drop("country") \
    .drop("location_latitude") \
    .drop("location_longitude") \
    .drop("type") \
    .drop("geo_point") \
    .drop("geo_shape") \
    .drop("official_code_state") \
    .drop("official_name_state") \
    .drop("iso_3166_3_area_code") \
    .drop("usps_state_abbreviation") \
    .drop("state_fips_code") \
    .drop("state_gnis_code") \
    .drop("year_georef")

## Creo la vista de la BD
df_filtered_texas.createOrReplaceTempView("v_analytics_data")

## Creo el DF para el Insert seleccionando y casteando los campos necesarios
df_insert_result = spark.sql("select fueltype as fuelType, cast(rating as int), cast(renterTripsTaken as int), cast(reviewCount as int), city, state_name, cast(owner_id as int), cast(rate_daily as int), make, model, cast(year as int) from v_analytics_data")

## Creo la vista para el insert
df_insert_result.createOrReplaceTempView("v_insert")

## Hago el insert en la BD
spark.sql("insert into car_rental_db.car_rental_analytics select * from v_insert")
