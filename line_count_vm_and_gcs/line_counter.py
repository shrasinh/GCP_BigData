from google.cloud import storage


def count_lines_in_gcs(bucket_name: str, blob_name: str) -> int:
    """
    Count the number of lines in a file stored in Google Cloud Storage.

    Args:
        bucket_name (str): Name of the GCS bucket
        blob_name (str): Path to the file within the bucket

    Returns:
        int: Number of lines in the file

    Raises:
        Exception: If there's an error accessing the file or bucket
    """
    try:
        # Initialize the GCS client
        storage_client = storage.Client()

        # Get the bucket and blob
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)

        # Download and count lines
        content = blob.download_as_text()
        line_count = len(content.splitlines())

        return line_count

    except Exception as e:
        print(f"Error: {str(e)}")
        raise


def main():
    bucket_name = "assignment-1-gcs-bucket"
    blob_name = "input.txt"

    try:
        line_count = count_lines_in_gcs(bucket_name, blob_name)
        print(f"Number of lines in gs://{bucket_name}/{blob_name}: {line_count}")

    except Exception as e:
        print(f"Failed to count lines: {str(e)}")


if __name__ == "__main__":
    main()
