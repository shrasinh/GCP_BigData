from google.cloud import pubsub_v1
import functions_framework

# Configure the project and topic
PROJECT_ID = "celtic-guru-448518-f8"
TOPIC_NAME = "CountLine"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_NAME)


@functions_framework.cloud_event
def file_upload_trigger(event):
    """Cloud Function triggered by a new file in the bucket.
    Args:
        event (dict): Event payload.
    """
    file_name = event.data["name"]
    bucket_name = event.data["bucket"]

    print(f"Processing file: {file_name} from bucket: {bucket_name}")

    # Create a message with the file information
    message = {"bucket": bucket_name, "file": file_name}

    # Convert the message to JSON string and encode it
    import json

    message_data = json.dumps(message).encode("utf-8")

    # Publish the message to the topic
    future = publisher.publish(topic_path, data=message_data)
    message_id = future.result()

    print(f"Published message ID: {message_id} for file: {file_name}")
    return f"Published message for file: {file_name}"
