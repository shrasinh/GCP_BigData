import os
import time
import random
import pathlib
import urllib.request
import tarfile
from google.cloud import storage


class FlowerImageStreamer:
    def __init__(self, bucket_name):
        """
        Initialize the image streamer with GCS configuration

        :param bucket_name: Name of the Google Cloud Storage bucket
        """
        # Initialize GCS client
        self.storage_client = storage.Client()
        self.bucket = self.storage_client.bucket(bucket_name)

        # Locate or download flower images dataset
        self.data_dir = self._download_flower_dataset()

        # Scan and organize image paths by category
        self.image_paths = self._scan_image_paths()

    def _download_flower_dataset(self):
        """
        Download and extract flower dataset with robust path handling

        :return: Path to the downloaded and extracted dataset
        """
        # Predefined download URL and local paths
        download_url = "https://storage.googleapis.com/download.tensorflow.org/example_images/flower_photos.tgz"
        flower_dir = "flower_photos"
        tgz_path = "flower_photos.tgz"

        # Download if not already downloaded
        if not os.path.exists(flower_dir):
            print("Downloading flower dataset...")
            try:
                # Download the file
                print(f"Downloading from: {download_url}")
                urllib.request.urlretrieve(download_url, tgz_path)

                # Extract the tarball
                print("Extracting dataset...")
                with tarfile.open(tgz_path, "r:gz") as tar:
                    tar.extractall()

                # Remove the tarball after extraction
                os.remove(tgz_path)

            except Exception as e:
                print(f"Error downloading/extracting dataset: {e}")
                raise

        return flower_dir

    def _scan_image_paths(self):
        """
        Scan and organize image paths by flower category

        :return: Dictionary of flower categories and their image paths
        """
        image_paths = {}
        print(f"Scanning images in: {self.data_dir}")

        # Scan for subdirectories and JPG files
        for category_path in pathlib.Path(self.data_dir).glob("*"):
            if category_path.is_dir():
                category = category_path.name
                jpg_paths = list(category_path.glob("*.jpg"))

                # Only add category if it has images
                if jpg_paths:
                    image_paths[category] = jpg_paths
                    print(f"Found {len(jpg_paths)} images in {category} category")

        return image_paths

    def stream_images(self):
        """
        Stream images to GCS
        """
        # Flatten image paths
        all_images = []
        for category, paths in self.image_paths.items():
            for path in paths:
                all_images.append((category, path))

        # Shuffling
        random.shuffle(all_images)

        # Stream images
        for category, image_path in all_images:
            try:
                # Upload to GCS
                blob_name = f"{category}/{image_path.name}"
                blob = self.bucket.blob(blob_name)

                # Upload image
                blob.upload_from_filename(str(image_path))
                print(f"Uploaded {blob_name}")

                # 5 seconds interval between uploads
                sleep_time = 5
                print(f"Waiting {sleep_time:.2f} seconds...")
                time.sleep(sleep_time)

            except Exception as e:
                print(f"Error uploading {image_path}: {e}")


def main():
    # GCS bucket name
    BUCKET_NAME = "flower_images_ibd"

    # Initialize and start streaming
    streamer = FlowerImageStreamer(bucket_name=BUCKET_NAME)

    streamer.stream_images()


if __name__ == "__main__":
    main()
