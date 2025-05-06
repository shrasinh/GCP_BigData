from kafka import KafkaProducer
from google.cloud import storage
import csv
import json
import time
import os

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = "34.58.97.69:9092"
KAFKA_TOPIC = "file-count-line-topic"

# GCS configuration
GCS_BUCKET_NAME = "assignmentibd"
GCS_FILE_PATH = "input.csv"


def download_from_gcs(bucket_name, source_blob_name, destination_file_name):
    """Downloads a file from GCS to local machine."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)

    blob.download_to_filename(destination_file_name)
    print(
        f"Downloaded gs://{bucket_name}/{source_blob_name} to {destination_file_name}"
    )


def read_local_file(file_path):
    """Reads CSV file and returns rows as list of dictionaries."""
    rows = []
    with open(file_path, "r") as file:
        csv_reader = csv.DictReader(file, fieldnames=["customer_id", "name", "city"])
        for row in csv_reader:
            rows.append(row)
    return rows


def create_producer():
    """Creates and returns a Kafka producer instance."""
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )


def send_to_kafka(producer, data_rows, batch_size=10, sleep_seconds=10):
    """Sends data to Kafka topic in batches with sleep time between batches."""
    sent_count = 0
    batches = [
        data_rows[i : i + batch_size] for i in range(0, len(data_rows), batch_size)
    ]

    for i, batch in enumerate(batches):

        # Determine how many records to send from this batch
        records_to_send = len(batch)
        batch = batch[:records_to_send]

        print(f"Sending batch {i+1} with {len(batch)} records to Kafka...")

        for record in batch:

            # Send to Kafka
            producer.send(KAFKA_TOPIC, value=record)
            sent_count += 1

        # Ensure all messages are sent
        producer.flush()

        print(f"Batch {i+1} sent. Total records sent: {sent_count}")

        # Sleep between batches (if not the last batch)
        if i < len(batches) - 1:
            print(f"Sleeping for {sleep_seconds} seconds...")
            time.sleep(sleep_seconds)

    return sent_count


def main():
    # File paths
    local_file = "input.csv"

    # Check if the file exists locally, if not, download from GCS
    if not os.path.exists(local_file):
        download_from_gcs(GCS_BUCKET_NAME, GCS_FILE_PATH, local_file)

    # Read data from file
    data_rows = read_local_file(local_file)
    print(f"Read {len(data_rows)} records from file")

    # Create Kafka producer
    producer = create_producer()

    # Send data to Kafka
    total_sent = send_to_kafka(
        producer=producer, data_rows=data_rows, batch_size=10, sleep_seconds=10
    )

    # Close the producer
    producer.close()

    print(f"Producer finished. Total records sent: {total_sent}")


if __name__ == "__main__":
    main()
