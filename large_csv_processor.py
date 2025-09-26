from google.cloud import storage
import json
import pandas as pd
import os
import pyarrow as pa
import pyarrow.parquet as pq
import io

# Replace with your project and bucket information
project_id = 'ad-dev-0800e668e8218823f35e'

source_bucket_name = 'geo_dump_oracledb'
destination_bucket_name = 'geo_processed_month'
# got rid of unnecessary code assigning blob_name which was the overwritten (line 13)
blob_name = "objectstorage.uk-london-1.oraclecloud.com/p/Q6qHiynHIba5KfVQ6lqr-kZO-G3BXlBnZRTH_ntTl4IOMurhzf7I2pwc1SxQONwL/n/lrqxlmmrxdgw/b/GEO/o/test.csv"
blob_name_test = "test.csv"
chunk_size = 1024 * 1024 * 512  # 512MB chunks (adjust as needed)


def process_and_upload_parquet(project_id, source_bucket_name, destination_bucket_name, blob_name, chunk_size):
    storage_client = storage.Client(project=project_id)
    source_bucket = storage_client.bucket(source_bucket_name)
    destination_bucket = storage_client.bucket(destination_bucket_name)
    # blob = source_bucket.get_blob(blob_name_test)
    # blob_size = blob.size
    # the commented out code apparently sets blob size to None:
    # < Blob: geo_dump_oracledb, test.csv, None >
    # but using get_blob you do get the size - https://cloud.google.com/python/docs/reference/storage/2.0.0/buckets

    blob = source_bucket.get_blob(blob_name_test)
    # check for blob just in case
    if not blob:
        print(f"Error: Blob '{blob_name_test}' not found in bucket '{source_bucket_name}'.")
        return
    blob_size = blob.size

    myBlobs = source_bucket.list_blobs()
    blob_list = []
    for bb in myBlobs:
        d = source_bucket.blob(bb.name)
        blob_list.append(d)
        print(bb.name)

    for start_byte in range(0, blob_size, chunk_size):
        end_byte = min(start_byte + chunk_size - 1, blob_size - 1)
        range_str = f"bytes={start_byte}-{end_byte}"
        chunk_data = blob.download_as_bytes(start=start_byte, end=end_byte)

        try:
            # variable to load dictionaries in
            chunk_data_list = []
            power_events_list = []
            csv_chunk_str = chunk_data.decode('utf-8')

            df_chunk = pd.read_csv(io.StringIO(csv_chunk_str), sep=',', header=None)
            # Process nested JSON (same as before)
            for index, row in df_chunk.iterrows():
                try:
                    # Parse the JSON string
                    parsed_data = json.loads(row[1])
                    # Make array_index
                    array_index = list(range(0, len(parsed_data.get("attributes", {}).get("activePowerElec"))))
                    # Create timestamps for each event
                    individual_utc = []
                    for my_i in array_index:
                        individual_utc.append(parsed_data.get("utc") + (my_i*10))

                    # Extract required fields
                    result = {
                        "id": parsed_data.get("id"),
                        "resource": parsed_data.get("resource"),
                        "original_utc": parsed_data.get("utc"),
                        "array_index": array_index,
                        "activePowerElec": parsed_data.get("attributes", {}).get("activePowerElec"),
                        "individual_utc": individual_utc
                    }
                    data_indentifiers = {
                        "hub_id": parsed_data.get("id"),
                        "resource": parsed_data.get("resource"),
                        "original_utc": parsed_data.get("utc")
                    }
                    power_events = {
                        "array_index": array_index,
                        "activePowerElec": parsed_data.get("attributes", {}).get("activePowerElec"),
                        "individual_utc": individual_utc
                    }
                    chunk_data_list.append(result)
                    df_power_events = pd.DataFrame(power_events)
                    for key, value in data_indentifiers.items():
                        df_power_events[key] = value
                    power_events_list.append(df_power_events)

                except:
                    print("haha")

            # Convert to Parquet
            df_joined_power = pd.concat(power_events_list)
            table = pd.DataFrame(chunk_data_list)

            parquet_buffer = io.BytesIO()
            #pq.write_table(df_joined_power, where=parquet_buffer, compression='snappy')  # snappy compression
            df_joined_power.to_parquet(parquet_buffer, engine='pyarrow', compression='snappy')
            parquet_buffer.seek(0) # Go to the beginning of the buffer before uploading

            # Upload Parquet chunk to destination bucket
            chunk_blob_name = f"processed_data_{start_byte}-{end_byte}.parquet"
            destination_blob = destination_bucket.blob(chunk_blob_name)
            destination_blob.upload_from_file(
                parquet_buffer,
                content_type='application/octet-stream'
            )

            print(f"Uploaded: {chunk_blob_name}")


        except (pd.errors.ParserError, UnicodeDecodeError) as e:
            print(f"Processing Error: {e}, Range: {range_str}")


process_and_upload_parquet(project_id, source_bucket_name, destination_bucket_name, blob_name, chunk_size)