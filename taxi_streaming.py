from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Define schema for taxi data
taxi_schema = StructType([
    StructField("VendorID", IntegerType()),
    StructField("tpep_pickup_datetime", TimestampType()),
    StructField("tpep_dropoff_datetime", TimestampType()),
    StructField("passenger_count", IntegerType()),
    StructField("trip_distance", DoubleType()),
    StructField("pickup_longitude", DoubleType()),
    StructField("pickup_latitude", DoubleType()),
    StructField("RateCodeID", IntegerType()),
    StructField("store_and_fwd_flag", StringType()),
    StructField("dropoff_longitude", DoubleType()),
    StructField("dropoff_latitude", DoubleType()),
    StructField("payment_type", IntegerType()),
    StructField("fare_amount", DoubleType()),
    StructField("extra", DoubleType()),
    StructField("mta_tax", DoubleType()),
    StructField("tip_amount", DoubleType()),
    StructField("tolls_amount", DoubleType()),
    StructField("improvement_surcharge", DoubleType()),
    StructField("total_amount", DoubleType())
])

def process_batch(df, batch_id):
    """Process each batch of streaming data"""
    # Get count of records in this batch
    count = df.count()
    if count > 0:
        print(f"Batch {batch_id} received {count} records")
        
        # Calculate some basic metrics
        metrics = df.agg(
            avg("trip_distance").alias("avg_distance"),
            avg("fare_amount").alias("avg_fare"),
            avg("tip_amount").alias("avg_tip"),
            count("*").alias("trip_count")
        )
        
        # Show metrics
        metrics.show()
        
        # Optional: Save batch to HDFS
        batch_path = f"/nyc_taxi/streaming/batch_{batch_id}"
        df.write.mode("overwrite").parquet(f"hdfs://localhost:9000{batch_path}")
        print(f"Saved batch to {batch_path}")

def main():
    # Create Spark session
    spark = SparkSession.builder \
        .appName("TaxiStreamProcessor") \
        .getOrCreate()
    
    # Set log level to reduce verbosity
    spark.sparkContext.setLogLevel("WARN")
    
    # Read from Kafka
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "taxi-trips") \
        .option("startingOffsets", "latest") \
        .load()
    
    # Parse JSON data
    taxi_data = kafka_df.select(
        from_json(col("value").cast("string"), taxi_schema).alias("data")
    ).select("data.*")
    
    # Process the data in micro-batches
    query = taxi_data.writeStream \
        .foreachBatch(process_batch) \
        .outputMode("update") \
        .trigger(processingTime="10 seconds") \
        .start()
    
    # Wait for the streaming query to terminate
    query.awaitTermination()

if __name__ == "__main__":
    main()