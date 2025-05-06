import json
import time
from google.cloud import pubsub_v1
from google.cloud import storage

# Configure the project, subscription
PROJECT_ID = "celtic-guru-448518-f8"
SUBSCRIPTION_NAME = "CountLineSub"

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_NAME)
storage_client = storage.Client()


def count_lines_in_file(bucket_name, file_name):
    """Download the file and count the lines."""
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)

    # Download the file contents
    contents = blob.download_as_string().decode("utf-8")

    # Count the lines
    line_count = len(contents.splitlines())

    print(f"File: {file_name}")
    print(f"Bucket: {bucket_name}")
    print(f"Line count: {line_count}")

    return line_count


def callback(message):
    """Process messages from the subscription."""
    print(f"Received message: {message}")

    try:
        # Parse the message data
        data = json.loads(message.data.decode("utf-8"))
        bucket_name = data["bucket"]
        file_name = data["file"]

        # Count the lines in the file
        line_count = count_lines_in_file(bucket_name, file_name)

        # Acknowledge the message
        message.ack()

        print(f"Successfully processed file {file_name} with {line_count} lines")

    except Exception as e:
        print(f"Error processing message: {e}")
        # The message is not acknowledged so it can be retried


def main():
    """Main entry point for the subscriber."""
    print(f"Listening for messages on {subscription_path}")

    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)

    # To keep the subscriber running
    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        streaming_pull_future.cancel()
        print("Subscriber stopped.")


if __name__ == "__main__":
    main()
