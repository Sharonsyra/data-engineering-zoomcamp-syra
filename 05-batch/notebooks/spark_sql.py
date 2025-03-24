#!/usr/bin/env python
# coding: utf-8


from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .master("spark://macbookpro.lan:7077") \
    .appName('test') \
    .getOrCreate()

df_green = spark.read.parquet('../code/data/pq/green/*/*')

df_green.show()

df_green.printSchema()

df_yellow = spark.read.parquet('../code/data/pq/yellow/*/*')

df_yellow.show()

df_yellow.printSchema()

df_green.columns

df_yellow.columns

set(df_green.columns) & set(df_yellow.columns)

df_green = df_green \
    .withColumnRenamed('lpep_pickup_datetime', 'pickup_datetime') \
    .withColumnRenamed('lpep_dropoff_datetime', 'dropoff_datetime')

df_yellow = df_yellow \
    .withColumnRenamed('tpep_pickup_datetime', 'pickup_datetime') \
    .withColumnRenamed('tpep_dropoff_datetime', 'dropoff_datetime')

set(df_green.columns) & set(df_yellow.columns)

common_columns = []

yellow_columns = set(df_yellow.columns)

for col in df_green.columns:
    if col in yellow_columns:
        common_columns.append(col)

common_columns

df_green_with_service_type = df_green \
    .select(common_columns) \
    .withColumn('service_type', F.lit('green'))

df_yellow_with_service_type = df_yellow \
    .select(common_columns) \
    .withColumn('service_type', F.lit('yellow'))

df_trips_data = df_green_with_service_type.unionAll(df_yellow_with_service_type)

df_trips_data.groupBy('service_type').count().show()

df_trips_data.createOrReplaceTempView('trips_data')

spark.sql("""
SELECT * FROM trips_data LIMIT 10
""").show()

spark.sql("""
SELECT  service_type, COUNT(1)
FROM trips_data
GROUP BY service_type
""").show()

df_result = spark.sql("""
SELECT 
    -- Revenue grouping 
    PULocationID AS revenue_zone,
    TRUNC(pickup_datetime, 'MONTH') AS revenue_month,

    service_type, 

    -- Revenue calculation 
    SUM(fare_amount) AS revenue_monthly_fare,
    SUM(extra) AS revenue_monthly_extra,
    SUM(mta_tax) AS revenue_monthly_mta_tax,
    SUM(tip_amount) AS revenue_monthly_tip_amount,
    SUM(tolls_amount) AS revenue_monthly_tolls_amount,
    SUM(improvement_surcharge) AS revenue_monthly_improvement_surcharge,
    SUM(total_amount) AS revenue_monthly_total_amount,

    -- Additional calculations
    AVG(passenger_count) AS avg_monthly_passenger_count,
    AVG(trip_distance) AS avg_monthly_trip_distance

FROM trips_data
GROUP BY revenue_zone, revenue_month, service_type
""")

df_result.show()

df_result.coalesce(1).write.parquet('../code/data/report/revenue/', mode='overwrite')
