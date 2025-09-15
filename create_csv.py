import csv
import random

# Function to generate random data and create a CSV file
def create_random_csv(filename, num_rows):
    # Random column headers
    headers = ["ID", "Name", "Age", "City", "Salary"]
    
    # List of sample data for names and cities
    sample_names = ["Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Hank", "Ivy", "Jack"]
    sample_cities = ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "San Diego", "Dallas", "Seattle", "Boston", "Miami"]
    
    # Open the file in write mode
    with open(filename, mode="w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(headers)  # Write the headers
        
        for i in range(1, num_rows + 1):
            # Generate random row data
            row = [
                i,  # ID
                random.choice(sample_names),  # Random name
                random.randint(20, 60),  # Random age
                random.choice(sample_cities),  # Random city
                round(random.uniform(30000, 150000), 2)  # Random salary
            ]
            writer.writerow(row)  # Write the row

# Example usage
create_random_csv("random_data.csv", 10)
