import pandas as pd
import os

# Create an output file with headers first
def initialize_output_file(output_file):
    # Read headers from first file to get column names
    sample = pd.read_csv(f"./yellow_tripdata_2015-01.csv", nrows=1)
    sample.to_csv(output_file, index=False, mode='w')
    print(f"Initialized output file {output_file} with headers")

# Process files one by one without storing all in memory
def process_files(output_file):
    for i in range(12):
        try:
            month = str(i+1).zfill(2)
            filename = f"./yellow_tripdata_2015-{month}.csv"
            print(f"Processing file - {filename}")
            
            # Process in chunks to reduce memory usage
            chunk_size = 100000  # Adjust based on your available memory
            chunk_counter = 0
            
            # Iterate through the file in chunks
            for chunk in pd.read_csv(filename, chunksize=chunk_size):
                chunk_counter += 1
                if chunk_counter % 10 == 0:
                    print(f"  Processing chunk {chunk_counter} of file {month}...")
                
                # Clean chunk by dropping NA values
                cleaned_chunk = chunk.dropna()
                
                # Append to output file without headers (except for first chunk)
                cleaned_chunk.to_csv(output_file, mode='a', header=False, index=False)
            
            print(f"Completed processing {filename}")
        except Exception as e:
            print(f"Error processing file {filename}: {str(e)}")

# Main execution
output_file = "./cleaned_data.csv"
initialize_output_file(output_file)
process_files(output_file)
print(f"All data has been processed and saved to {output_file}")