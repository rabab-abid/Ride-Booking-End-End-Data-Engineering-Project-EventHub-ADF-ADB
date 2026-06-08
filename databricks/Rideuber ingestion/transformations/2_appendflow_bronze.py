from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark import pipelines as dp

schema_bulkrides="STRUCT<ride_id STRING,confirmation_number STRING,passenger_id STRING,driver_id STRING,vehicle_id STRING,pickup_location_id STRING,dropoff_location_id STRING,vehicle_type_id BIGINT,vehicle_make_id BIGINT,payment_method_id BIGINT,ride_status_id BIGINT,pickup_city_id BIGINT,dropoff_city_id BIGINT,cancellation_reason_id BIGINT,passenger_name STRING,passenger_email STRING,passenger_phone STRING,driver_name STRING,driver_rating DOUBLE,driver_phone STRING,driver_license STRING,vehicle_model STRING,vehicle_color STRING,license_plate STRING,pickup_address STRING,pickup_latitude DOUBLE,pickup_longitude DOUBLE,dropoff_address STRING,dropoff_latitude DOUBLE,dropoff_longitude DOUBLE,distance_miles DOUBLE,duration_minutes BIGINT,booking_timestamp TIMESTAMP,pickup_timestamp STRING,dropoff_timestamp STRING,base_fare DOUBLE,distance_fare DOUBLE,time_fare DOUBLE,surge_multiplier DOUBLE,subtotal DOUBLE,tip_amount DOUBLE,total_fare DOUBLE,rating DOUBLE>"

dp.create_streaming_table('uber.bronze.bulk_stream_rides')

@dp.append_flow(
    target='bulk_stream_rides'
)
def process_bulkrides():
    df = spark.readStream.table('bulk_rides')\
              .withColumn("booking_timestamp", F.col('booking_timestamp').cast("timestamp"))
    return df

@dp.append_flow(
    target='bulk_stream_rides'
)
def process_streamrides():
    df = spark.readStream.table('rides_raw') \
            .select(F.from_json(F.col('value'), schema_bulkrides).alias('v')) \
            .select('v.*')
    return df
