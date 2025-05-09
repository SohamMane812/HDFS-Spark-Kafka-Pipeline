import pandas as pd
import json
import time
import random
from kafka import KafkaProducer
import subprocess
import io
import pyarrow.parquet as pq
import tempfile
import argparse
import concurrent.futures
import threading

def get_data_from_parquet(row_limit=1000):
    """Get sample data from HDFS Parquet file
    
    Args:
        row_limit (int): Maximum number of rows to load, default 1000
    """
    # Create a temporary directory to store a small parquet file
    with tempfile.TemporaryDirectory() as temp_dir:
        # Use subprocess to call hdfs command and download a small parquet file
        cmd = ["hdfs", "dfs", "-get", "/nyc_taxi/parquet/VendorID=1/part-00000-*.parquet", temp_dir]
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = process.communicate()
        
        # Check if the command was successful
        if process.returncode != 0:
            print(f"Error getting parquet file from VendorID=1: {stderr.decode()}")
            # Try with VendorID=2
            cmd = ["hdfs", "dfs", "-get", "/nyc_taxi/parquet/VendorID=2/part-00000-*.parquet", temp_dir]
            process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            stdout, stderr = process.communicate()
            
            if process.returncode != 0:
                print(f"Error getting parquet file from VendorID=2: {stderr.decode()}")
                raise Exception("Could not get parquet files from HDFS")
        
        # Find the parquet file in the temp directory
        import os
        parquet_files = [f for f in os.listdir(temp_dir) if f.endswith('.parquet')]
        
        if not parquet_files:
            raise Exception("No parquet files found in the temporary directory")
        
        parquet_path = os.path.join(temp_dir, parquet_files[0])
        
        # Read the parquet file
        table = pq.read_table(parquet_path)
        df = table.to_pandas()
        
        # Limit rows based on parameter
        if len(df) > row_limit:
            df = df.head(row_limit)
        
        print(f"Loaded {len(df)} rows out of requested {row_limit}")
        return df

def serialize_record(record):
    """Convert a pandas Series to a JSON string"""
    # Convert pandas timestamp to string for JSON serialization
    record_dict = record.to_dict()
    for key, value in record_dict.items():
        if pd.isna(value):
            record_dict[key] = None
        elif isinstance(value, pd.Timestamp):
            record_dict[key] = value.strftime('%Y-%m-%d %H:%M:%S')
    return json.dumps(record_dict)

def send_message(producer, df, counter, lock):
    """Send a message to Kafka"""
    # Pick a random record
    record = df.iloc[random.randint(0, len(df)-1)]
    
    # Serialize and send to Kafka
    json_record = serialize_record(record)
    producer.send('taxi-trips', value=json_record)
    
    # Increment counter with thread safety
    with lock:
        counter[0] += 1
        if counter[0] % 100 == 0:
            print(f"Sent {counter[0]} records to Kafka")

def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Kafka Producer for NYC Taxi Data')
    parser.add_argument('--rows', type=int, default=1000,
                        help='Number of rows to load from Parquet (default: 1000)')
    parser.add_argument('--threads', type=int, default=5,
                        help='Number of concurrent sender threads (default: 5)')
    args = parser.parse_args()
    
    # Connect to Kafka
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                           value_serializer=lambda x: x.encode('utf-8'))
    
    # Get data
    print(f"Loading data from Parquet file (max {args.rows} rows)...")
    try:
        df = get_data_from_parquet(args.rows)
        print(f"Loaded {len(df)} records")
    except Exception as e:
        print(f"Error loading data: {e}")
        return
    
    # Simulate real-time data with concurrency
    print(f"Starting to stream data to Kafka using {args.threads} threads...")
    counter = [0]  # Use a list so we can modify it inside the function
    lock = threading.Lock()  # For thread-safe counter updates
    
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=args.threads) as executor:
            while True:
                # Submit multiple send tasks to the thread pool
                futures = [
                    executor.submit(send_message, producer, df, counter, lock)
                    for _ in range(args.threads)
                ]
                
                # Wait for all tasks to complete
                concurrent.futures.wait(futures)
                
                # Add a small delay to prevent overloading
                time.sleep(random.uniform(0.05, 0.2))
    except KeyboardInterrupt:
        print("Producer stopped by user")
    except Exception as e:
        print(f"Error in producer: {e}")
    finally:
        producer.close()
        print(f"Total records sent: {counter[0]}")

if __name__ == "__main__":
    main()