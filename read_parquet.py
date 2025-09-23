import pyarrow.parquet as pq
import pandas as pd

# Path to your local Parquet file
file_path = 'processed_data_part-00000.parquet'

try:
    # 1. Open the Parquet file
    parquet_file = pq.ParquetFile(file_path)
    dataframe = pd.read_parquet(file_path)

    # 2. Inspect the high-level metadata
    print("--- File Metadata ---")
    print(f"Number of row groups: {parquet_file.num_row_groups}")
    print(f"Total number of rows: {parquet_file.metadata.num_rows}")
    # The file_metadata object has more details like created_by, etc.
    print(parquet_file.metadata)
    print("\n")

    # 3. Inspect the schema (column names and data types)
    print("--- Schema ---")
    print(parquet_file.schema)
    print("\n")

    # 4. Read and display the first 5 rows
    # read_row_group() is efficient for grabbing a piece of the file
    # To get just a few rows, you can also use iter_batches()
    first_5_rows = next(parquet_file.iter_batches(batch_size=5)).to_pandas()
    print("--- First 5 Rows ---")
    print(first_5_rows)

except FileNotFoundError:
    print(f"Error: The file '{file_path}' was not found.")
except Exception as e:
    print(f"An error occurred: {e}")
