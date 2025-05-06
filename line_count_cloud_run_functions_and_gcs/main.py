import functions_framework
from google.cloud import storage


@functions_framework.cloud_event
def count_lines(cloud_event):

    try:

        # Extract event data
        data = cloud_event.data
        bucket_name = data["bucket"]
        file_name = data["name"]

        # Skip processing for output.txt to prevent recursion
        if file_name == "output.txt":
            print(f"Skipping {file_name} to avoid recursion")
            return "Skipped output file"

        # Initialize storage client and download file content
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_name)
        content = blob.download_as_text()

        # Count number of lines
        line_count = len(content.splitlines())

        # Upload output file with line count
        count_file_name = "output.txt"
        count_blob = bucket.blob(count_file_name)
        count_blob.upload_from_string(
            f"Number of lines in gs://{bucket_name}/{file_name}: {line_count}"
        )

    except Exception as e:
        print(f"Error processing file: {str(e)}")
        return "Error"
