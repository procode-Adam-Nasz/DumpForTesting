import pandas as pd

def calculate_factorial(num):
    if num == 1:  # Base case for recursion
        return 1
    return num * calculate_factorial(num - 1)

def create_dataframe():
    """
    Creates and returns a DataFrame with some example data.
    """
    data = {"ID": [1, 2, 3], "Name": ["Alice", "Bob", "Charlie"], "Score": [85, 90, 78]}
    return pd.DataFrame(data)

def main():
    numbers = [3, 5, 10]  # List of numbers to calculate factorial
    results = []  # This will store the results
    
    for number in numbers:
        # Place a breakpoint inside the loop to inspect the 'number' and 'results' variables.
        print(f"Calculating factorial of {number}")  # Useful for observing the program flow
        factorial = calculate_factorial(number)
        results.append(factorial)
        print(f"Factorial of {number} is {factorial}")  # Observe changes in variables after calculation
    
    print("All calculations complete")
    print("Results:", results)  # Final result output

    # Create a DataFrame and display it
    df = create_dataframe()
    print("Generated DataFrame:")
    print(df)



if __name__ == "__main__":
    main()
