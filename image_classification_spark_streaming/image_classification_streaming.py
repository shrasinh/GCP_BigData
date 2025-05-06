from pyspark.sql import SparkSession
from pyspark.sql.functions import col, pandas_udf, regexp_extract, PandasUDFType
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    BinaryType,
    TimestampType,
    LongType,
)

from pyspark.sql.functions import current_timestamp

import io
import torch
from torch.utils.data import Dataset, DataLoader
from torchvision import models, transforms
from PIL import Image
from tensorflow.keras.applications.imagenet_utils import decode_predictions
import pandas as pd


def create_spark_session():
    """
    Create a Spark Session with basic configuration
    """
    return (
        SparkSession.builder.appName("RealTimeImageClassification")
        .config("spark.executorEnv.TORCH_HOME", "/tmp/torch_cache")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .getOrCreate()
    )


class ImageNetDataset(Dataset):
    """
    Converts image contents into a PyTorch Dataset with standard ImageNet preprocessing
    """

    def __init__(self, contents):
        self.contents = contents

    def __len__(self):
        return len(self.contents)

    def __getitem__(self, index):
        return self._preprocess(self.contents[index])

    def _preprocess(self, content):
        """
        Preprocesses the input image content using standard ImageNet normalization
        """
        image = Image.open(io.BytesIO(content))
        transform = transforms.Compose(
            [
                transforms.Resize(256),
                transforms.CenterCrop(224),
                transforms.ToTensor(),
                transforms.Normalize(
                    mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225]
                ),
            ]
        )
        return transform(image)


def extract_label(path_col):
    """Extract label from file path using built-in SQL functions."""
    return regexp_extract(path_col, "([^/]+)/[^/]+$", 1)


def imagenet_model_udf(model_fn):
    """
    Wraps an ImageNet model into a Pandas UDF that makes predictions
    """

    def predict(content_series_iter):
        model = model_fn()
        model.eval()
        for content_series in content_series_iter:
            dataset = ImageNetDataset(list(content_series))
            loader = DataLoader(dataset)
            with torch.no_grad():
                for image_batch in loader:
                    predictions = model(image_batch).numpy()
                    predicted_labels = [
                        x[0] for x in decode_predictions(predictions, top=1)
                    ]
                    yield pd.DataFrame(predicted_labels)

    return_type = "class: string, desc: string, score:float"
    return pandas_udf(return_type, PandasUDFType.SCALAR_ITER)(predict)


def process_image_stream(input_path):
    """
    Set up real-time image classification streaming
    """
    # Create Spark Session
    spark = create_spark_session()

    # Define the schema for streaming input
    input_schema = StructType(
        [
            StructField("path", StringType(), False),
            StructField("modificationTime", TimestampType(), False),
            StructField("length", LongType(), False),
            StructField("content", BinaryType(), True),
        ]
    )

    # Read streaming data from the input directory
    stream_df = (
        spark.readStream.format("binaryFile")
        .schema(input_schema)
        .option("maxFilesPerTrigger", 1)  # Process one file at a time
        .load(input_path)
    )

    # Prepare classification UDF
    mobilenet_v2_udf = imagenet_model_udf(lambda: models.mobilenet_v2(pretrained=True))

    # Add prediction column
    classified_stream = (
        stream_df.withColumn("prediction", mobilenet_v2_udf(col("content")))
        .withColumn("label", extract_label(col("path")))
        .withColumn("processing_time", current_timestamp())
        .select(
            "label",
            "prediction",
            "processing_time",
        )
    )

    # Configure the streaming query
    query = (
        classified_stream.writeStream.outputMode("append")
        .format("console")
        .option("truncate", "false")
        .start()
    )

    # Await termination
    query.awaitTermination()


def main():
    input_path = "gs://flower_images_ibd/*"
    process_image_stream(input_path)


if __name__ == "__main__":
    main()
