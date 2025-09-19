from google.cloud import storage
import json
import pandas as pd
import io

# --- Configuration ---
# Grouping configuration makes the script easier to read and manage.
PROJECT_ID = 'ad-dev-0800e668e8218823f35e'
SOURCE_BUCKET_NAME = 'geo_dump_oracledb'
DESTINATION_BUCKET_NAME = 'geo_processed_month'

# The blob name should be the object's path within the bucket.
BLOB_NAME = "objectstorage.uk-london-1.oraclecloud.com/p/Q6qHiynHIba5KfVQ6lqr-kZO-G3BXlBnZRTH_ntTl4IOMurhzf7I2pwc1SxQONwL/n/lrqxlmmrxdgw/b/GEO/o/test.csv"
BLOB_NAME_TEST = "test.csv"
PANDAS_CHUNK_SIZE_ROWS = 1024 * 1024 * 512  # 512MB chunks (adjust as needed)

def parse_json_column(json_string):
    """Safely parse a JSON string; return an empty dict on failure."""
    if pd.isna(json_string):
        return {}
    try:
        if isinstance(json_string, dict):
            return json_string
        return json.loads(json_string)
    except (json.JSONDecodeError, TypeError):
        return {}

def process_large_csv_to_parquet(project_id, source_bucket_name, dest_bucket_name, blob_name, chunk_rows):
    storage_client = storage.Client(project=project_id)
    source_bucket = storage_client.bucket(source_bucket_name)
    destination_bucket = storage_client.bucket(dest_bucket_name)

    # Use get_blob() to fetch the blob and its metadata.
    source_blob = source_bucket.get_blob(blob_name)
    # check for blob just in case
    if not source_blob:
        print(f"Error: Blob '{blob_name}' not found in bucket '{source_bucket_name}'.")
        return

    chunk_number = 0
    # Stream the blob and let pandas handle the chunking by row count.
    with source_blob.open("r", encoding="utf-8") as f:
        # The 'chunksize' parameter in read_csv returns an iterator of DataFrames.
        # This is the correct and safe way to process a large CSV.
        for df_chunk in pd.read_csv(f, sep=',', chunksize=chunk_rows, low_memory=False):
            print(f"Processing chunk {chunk_number} with {len(df_chunk)} rows...")

            # --- Efficient JSON Processing ---
            if 'json_column' in df_chunk.columns:
                # 1. Use .apply() for fast, vectorized operations.
                nested_data_series = df_chunk['json_column'].apply(parse_json_column)

                # 2. pd.json_normalize is the perfect tool for flattening a series of dicts.
                df_nested = pd.json_normalize(nested_data_series)

                # 3. Join the new nested columns back to the chunk DataFrame.
                df_chunk = df_chunk.join(df_nested)
                # df_chunk = df_chunk.drop(columns=['json_column']) # Optionally drop original

            # --- Convert to Parquet using an in-memory buffer ---
            # Using the high-level pandas `to_parquet` is simpler and recommended.
            parquet_buffer = io.BytesIO()
            df_chunk.to_parquet(parquet_buffer, engine='pyarrow', compression='snappy')

            # --- Upload Parquet Chunk ---
            chunk_blob_name = f"processed_data/part-{chunk_number:05d}.parquet"
            destination_blob = destination_bucket.blob(chunk_blob_name)

            # Go to the beginning of the in-memory buffer before uploading.
            parquet_buffer.seek(0)
            destination_blob.upload_from_file(parquet_buffer, content_type='application/octet-stream')

            print(f"Successfully uploaded: {chunk_blob_name}")
            chunk_number += 1

    print("Processing complete.")

# --- Main execution block ---
if __name__ == "__main__":
    process_large_csv_to_parquet(
        project_id=PROJECT_ID,
        source_bucket_name=SOURCE_BUCKET_NAME,
        dest_bucket_name=DESTINATION_BUCKET_NAME,
        blob_name=BLOB_NAME,
        chunk_rows=PANDAS_CHUNK_SIZE_ROWS
    )