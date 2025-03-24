import csv
import json
from kafka import KafkaProducer
from time import time

def main():
    # Create a Kafka producer
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    csv_file = '../data/green_tripdata_2019-10.csv'  # change to your CSV file path if needed

    def clean_value(value, field):
        if field == "passenger_count":
            return int(value) if value.strip().isdigit() else 0
        if field in ("trip_distance", "tip_amount"):
            return float(value) if value.strip() != "" else 0.0
        return value

    with open(csv_file, 'r', newline='', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        keys_to_pick = ['lpep_pickup_datetime', 'lpep_dropoff_datetime', 'PULocationID',
                        'DOLocationID', 'passenger_count', 'trip_distance', 'tip_amount']
        topic_name = 'green-trips'

        t0 = time()
        for row in reader:
            filtered = {k: clean_value(row[k], k) for k in keys_to_pick if k in row}

            # Each row will be a dictionary keyed by the CSV headers
            # Send data to Kafka topic "green-data"
            producer.send(topic_name, value=filtered)

    # Make sure any remaining messages are delivered
    producer.flush()
    t1 = time()
    took = t1 - t0
    print(took)
    producer.close()


if __name__ == "__main__":
    main()
