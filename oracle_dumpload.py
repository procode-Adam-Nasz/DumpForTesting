import re

def parse_dmp(filepath):
    data = []
    try:
        with open(filepath, 'r', encoding='utf-8') as f:  # Handle potential encoding
            for line in f:
                # Adapt this regex to your actual delimiter and data format
                match = re.match(r"^(\w+)\s+(\w+)\s+(\w+)$", line.strip())  # Example for 3 columns
                if match:
                    row = [match.group(1), match.group(2), match.group(3)]
                    data.append(row)

    except Exception as e:
        print(f"Error parsing file: {e}")
        return None

    return data


# Example usage:
filepath = "testtable.dmp"  # Replace with the actual file path
parsed_data = parse_dmp(filepath)

if parsed_data:
    for row in parsed_data:
        print(row)