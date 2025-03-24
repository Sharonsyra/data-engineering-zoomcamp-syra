from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment


def create_session_sink_postgres(t_env):
    table_name = 'sessionized_trips'
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            PULocationID INT,
            DOLocationID INT,
            session_start TIMESTAMP(3),
            session_end TIMESTAMP(3),
            trip_count BIGINT,
            PRIMARY KEY (PULocationID, DOLocationID, session_start) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = '{table_name}',
            'username' = 'postgres',
            'password' = 'postgres',
            'driver' = 'org.postgresql.Driver'
        );
    """
    t_env.execute_sql(sink_ddl)
    return table_name


def create_taxi_kafka_source(t_env):
    table_name = "taxi_events"
    source_ddl = f"""
        CREATE TABLE {table_name} (
            VendorID INTEGER,
            lpep_pickup_datetime STRING,
            lpep_dropoff_datetime STRING,
            store_and_fwd_flag STRING,
            RatecodeID INTEGER,
            PULocationID INTEGER,
            DOLocationID INTEGER,
            passenger_count INTEGER,
            trip_distance DOUBLE,
            fare_amount DOUBLE,
            extra DOUBLE,
            mta_tax DOUBLE,
            tip_amount DOUBLE,
            tolls_amount DOUBLE,
            ehail_fee DOUBLE,
            improvement_surcharge DOUBLE,
            total_amount DOUBLE,
            payment_type INTEGER,
            trip_type INTEGER,
            congestion_surcharge DOUBLE,
            dropoff_ts AS TO_TIMESTAMP(lpep_dropoff_datetime, 'yyyy-MM-dd HH:mm:ss'),
            WATERMARK FOR dropoff_ts AS dropoff_ts - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'green-trips',
            'properties.bootstrap.servers' = 'redpanda-1:29092',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json'
        );
    """
    t_env.execute_sql(source_ddl)
    return table_name


def sessionize_taxi_trips():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10000)
    t_env = StreamTableEnvironment.create(env, environment_settings=EnvironmentSettings.new_instance().in_streaming_mode().build())

    source_table = create_taxi_kafka_source(t_env)
    sink_table = create_session_sink_postgres(t_env)

    query = f"""
           INSERT INTO {sink_table}
           SELECT 
               PULocationID,
               DOLocationID,
               SESSION_START(dropoff_ts, INTERVAL '5' MINUTES) AS session_start,
               SESSION_END(dropoff_ts, INTERVAL '5' MINUTES) AS session_end,
               COUNT(*) AS trip_count
           FROM {source_table}
           GROUP BY SESSION(dropoff_ts, INTERVAL '5' MINUTES), PULocationID, DOLocationID
       """
    try:
        t_env.execute_sql(query).wait()
    except Exception as e:
        print("Session window query failed:", str(e))


if __name__ == '__main__':
    sessionize_taxi_trips()
