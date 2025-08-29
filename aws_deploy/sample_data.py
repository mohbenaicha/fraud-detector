import pandas as pd

# Load the dataset
input_file = '../Fraudulent_E-Commerce_Transaction_Data_2.csv'
output_file = './sample.csv'

# Read the CSV file
data = pd.read_csv(input_file)

# Sample 1000 records
sampled_data = data.sample(n=1000, random_state=42)

# Write the sampled data to a new CSV file
sampled_data.to_csv(output_file, index=False)

print(f"Sampled 1000 records and saved to {output_file}")