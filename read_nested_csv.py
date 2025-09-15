import pandas as pd
import json

try:
    df = pd.read_csv("test.csv")
except FileNotFoundError:
    print("Error: test.csv not found in the root folder.")
    exit()  # Exit if the file is not found
except pd.errors.ParserError as e:
    print(f"Error parsing CSV: {e}")
    exit()

all_data = []  # List to store extracted data

for index, row in df.iterrows():
    try:
        json_data = json.loads(row[1])
        row_data = {  # Create a dictionary for each row
            'original_date': row[0],  # Add the date from the first column
            'id': json_data.get('id'),  # Using .get() handles missing keys safely
            'utc': json_data.get('utc'),
            'duration': json_data.get('duration'),
            'resource': json_data.get('resource'),
            # ... extract other fields as needed ...
        }
        all_data.append(row_data)


    except (json.JSONDecodeError, KeyError) as e:
        print(f"Error processing JSON in row {index + 1}: {e}")

# Create the DataFrame outside the loop
result_df = pd.DataFrame(all_data)

# Print or process the DataFrame as needed
print(result_df)
