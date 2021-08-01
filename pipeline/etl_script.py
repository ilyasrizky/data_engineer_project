import os
import pandas as pd
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, dayofweek, sum, year


projectDir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Notes: .master("local[*]") bisa dihapus jika ingin berjalan di Spark Standalone Cluster (localhost:8181) spark://spark:7077
spark = SparkSession \
    .builder \
    .master("local[*]") \
    .config("spark.hadoop.dfs.client.use.datanode.hostname", "true") \
    .config('spark.driver.extraClassPath', os.path.join(projectDir, 'connectors/postgresql-9.4.1207.jar')) \
    .appName('End-to-end ETL Project').getOrCreate()

spark.sparkContext.setLogLevel('WARN')


# Extract Data Flight Delays and Cancellations From PostgresDB
conn: str = "jdbc:postgresql://postgres-db:5432/digitalskola"
properties: dict = {
    "user": "digitalskola",
    "password": "digitalskola",
    "driver": "org.postgresql.Driver"
}

df_airlines = spark.read.jdbc(conn, table="airlines", properties=properties)
df_airports = spark.read.jdbc(conn, table="airports", properties=properties)
df_flights = spark.read.jdbc(conn, table="flights", properties=properties)

df_airlines.show(5)
df_airports.show(5)
df_flights.show(5)

# Transforming Data

df_flightsTransform = df_flights \
    .fillna("This flight is not canceled", ["cancellation_reason"]) \
    .fillna("No Data", ["tail_number", "departure_time", "wheels_off", "air_time", "wheels_on", "arrival_time",
                        "air_system_delay", "security_delay", "airline_delay", "late_aircraft_delay", "weather_delay"])
date = pd.to_datetime(df_flights[["year", "month", "day"]])
days = datetime.strptime(dayofweek, "%A")

join_dfFlights_Airlines = df_airlines["iata_code"] == df_flightsTransform["airline"]
df_FlightsAirlines = df_airlines.join(
    df_flightsTransform, join_dfFlights_Airlines, "fullouter")

column = ["flight_number", "tail_number", "days", "date", "airline", "origin_airport", "destination_airport", "distance",
          "scheduled_departure", "departure_time", "wheels_off", "scheduled_time", "air_time", "wheels_on", "scheduled_arrival",
          "arrival_time", "diverted", "cancelled", "cancellation_reason", "air_system_delay", "security_delay", "airline_delay",
          "late_aircraft_delay", "weather_delay"]
df_flightsNew = df_FlightsAirlines.select(*column)

df_flightsNew.show(5)
df_flightsNew.printSchema()


# Write Parquet to Hadoop HDFS
df_flightsNew.write.save(f"hdfs://hadoop:9000/output/FlightDelaysandCancellationsData",
                         format="parquet",
                         mode='overwrite')

df_airlines.write.save(f"hdfs://hadoop:9000/output/AirlinesData",
                       format="parquet",
                       mode='overwrite')

df_airports.write.save(f"hdfs://hadoop:9000/output/AirportData",
                       format="parquet",
                       mode='overwrite')
