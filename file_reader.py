# BOILERPLATE
import io
import logging
import os
from typing import Tuple

import boto3
import pandas as pd

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def read_csv_from_s3(bucket: str, key: str) -> Tuple[pd.DataFrame, str]:
    # BOILERPLATE — build S3 client using ambient Lambda execution role; no explicit credentials
    s3_client = boto3.client("s3")

    # LOGIC — download the S3 object at the given bucket/key
    logger.info("Downloading s3://%s/%s", bucket, key)
    response = s3_client.get_object(Bucket=bucket, Key=key)
    body_bytes = response["Body"].read()

    # LOGIC — parse CSV with all columns as strings to preserve raw values for downstream validation
    raw_df = pd.read_csv(
        io.BytesIO(body_bytes),
        dtype=str,
        keep_default_na=False,  # LOGIC — do not silently coerce empty strings to NaN at read time
    )

    logger.info(
        "Read %d rows from s3://%s/%s",
        len(raw_df),
        bucket,
        key,
    )

    # LOGIC — assign 1-based source row numbers reflecting original file position (header = row 0, first data row = 1)
    raw_df["_source_row"] = range(1, len(raw_df) + 1)

    # LOGIC — derive source file name as S3 key basename for downstream use
    source_file_name = os.path.basename(key)

    return raw_df, source_file_name