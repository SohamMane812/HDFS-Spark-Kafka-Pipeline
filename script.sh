#!/usr/bin/bash
# 
sudo apt-get update && sudo apt-get upgrade -y
# 
sudo apt-get install -y default-jdk python3 python3-pip python3-venv git curl wget unzip
# Set up environment variables for java
echo "export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64" >> ~/.bashrc
echo "export PATH=\$PATH:\$JAVA_HOME/bin" >> ~/.bashrc
source ~/.bashrc

# Create directories for project
mkdir -p ~/data ~/downloads ~/logs ~/notebooks

# Install Hadoop:
cd ~/downloads
wget https://dlcdn.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz
tar -xzf hadoop-3.3.6.tar.gz -C ~
mv ~/hadoop-3.3.6 ~/hadoop

# Set Hadoop environment variables
echo "export HADOOP_HOME=~/hadoop" >> ~/.bashrc
echo "export HADOOP_CONF_DIR=\$HADOOP_HOME/etc/hadoop" >> ~/.bashrc
echo "export PATH=\$PATH:\$HADOOP_HOME/bin:\$HADOOP_HOME/sbin" >> ~/.bashrc
source ~/.bashrc

# Configure Hadoop
# Set JAVA_HOME in Hadoop configuration
echo "export JAVA_HOME=/usr/lib/jvm/default-java" >> ~/hadoop/etc/hadoop/hadoop-env.sh

# Create configuration files
cat > ~/hadoop/etc/hadoop/core-site.xml << EOF
<configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://localhost:9000</value>
    </property>
</configuration>
EOF

cat > ~/hadoop/etc/hadoop/hdfs-site.xml << EOF
<configuration>
    <property>
        <name>dfs.replication</name>
        <value>1</value>
    </property>
    <property>
        <name>dfs.namenode.name.dir</name>
        <value>file:///home/$(whoami)/hadoop/data/namenode</value>
    </property>
    <property>
        <name>dfs.datanode.data.dir</name>
        <value>file:///home/$(whoami)/hadoop/data/datanode</value>
    </property>
</configuration>
EOF

# Create HDFS directories
mkdir -p ~/hadoop/data/namenode ~/hadoop/data/datanode

# Setup SSH for Hadoop
ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
chmod 0600 ~/.ssh/authorized_keys

# Test SSH connection
ssh localhost
# Type 'yes' if prompted and then exit

# Format HDFS namenode
hdfs namenode -format

# Start Hadoop services:
~/hadoop/sbin/start-dfs.sh

# Verify Hadoop is running:
jps

# Create the .ssh directory
mkdir -p ~/.ssh

# Generate SSH key
ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa

# Add the key to authorized_keys
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys

# Set proper permissions
chmod 0600 ~/.ssh/authorized_keys

# Click "CREATE FIREWALL RULE" if not running

# Set up environment variables
echo "export HIVE_HOME=~/hive" >> ~/.bashrc
echo "export PATH=\$PATH:\$HIVE_HOME/bin" >> ~/.bashrc
source ~/.bashrc

# Install Spark
cd ~/downloads
wget https://downloads.apache.org/spark/spark-3.5.5/spark-3.5.5-bin-hadoop3.tgz
tar -xzf spark-3.5.5-bin-hadoop3.tgz -C ~
mv ~/spark-3.5.5-bin-hadoop3 ~/spark

# Install Kafka
cd ~/downloads
wget https://archive.apache.org/dist/kafka/2.8.2/kafka_2.13-2.8.2.tgz
tar -xzf kafka_2.13-2.8.2.tgz -C ~
mv ~/kafka_2.13-2.8.2 ~/kafka

# Update your PATH
echo 'export PATH=$PATH:~/kafka/bin' >> ~/.bashrc
source ~/.bashrc

# Configure ZooKeeper and Kafka
# Create data directories for ZooKeeper and Kafka
mkdir -p ~/zookeeper-data
mkdir -p ~/kafka-logs

# Configure ZooKeeper
cat > ~/kafka/config/zookeeper.properties << EOF
# the directory where the snapshot is stored.
dataDir=/home/$(whoami)/zookeeper-data
# the port at which the clients will connect
clientPort=2181
# disable the per-ip limit on the number of connections since this is a non-production config
maxClientCnxns=0
# Disable the adminserver by default to avoid port conflicts.
admin.enableServer=false
EOF

# Configure Kafka
cat > ~/kafka/config/server.properties << EOF
# The id of the broker. This must be set to a unique integer for each broker.
broker.id=0

# The address the socket server listens on
listeners=PLAINTEXT://:9092

# The address that will be advertised to produce and consume client applications
advertised.listeners=PLAINTEXT://localhost:9092

# Directory where log data will be stored
log.dirs=/home/$(whoami)/kafka-logs

# Number of partitions per topic
num.partitions=1

# Default replication factor
default.replication.factor=1

# ZooKeeper connection string
zookeeper.connect=localhost:2181

# ZooKeeper connection timeout
zookeeper.connection.timeout.ms=18000

# Enable auto creation of topic
auto.create.topics.enable=true
EOF

# Download the NYC Yellow Taxi data:
wget https://dask-data.s3.amazonaws.com/nyc-taxi/2015/yellow_tripdata_2015-01.csv
wget https://dask-data.s3.amazonaws.com/nyc-taxi/2015/yellow_tripdata_2015-02.csv
wget https://dask-data.s3.amazonaws.com/nyc-taxi/2015/yellow_tripdata_2015-03.csv
wget https://dask-data.s3.amazonaws.com/nyc-taxi/2015/yellow_tripdata_2015-04.csv
wget https://dask-data.s3.amazonaws.com/nyc-taxi/2015/yellow_tripdata_2015-05.csv
wget https://dask-data.s3.amazonaws.com/nyc-taxi/2015/yellow_tripdata_2015-06.csv
wget https://dask-data.s3.amazonaws.com/nyc-taxi/2015/yellow_tripdata_2015-07.csv
wget https://dask-data.s3.amazonaws.com/nyc-taxi/2015/yellow_tripdata_2015-08.csv
wget https://dask-data.s3.amazonaws.com/nyc-taxi/2015/yellow_tripdata_2015-09.csv
wget https://dask-data.s3.amazonaws.com/nyc-taxi/2015/yellow_tripdata_2015-10.csv
wget https://dask-data.s3.amazonaws.com/nyc-taxi/2015/yellow_tripdata_2015-11.csv
wget https://dask-data.s3.amazonaws.com/nyc-taxi/2015/yellow_tripdata_2015-12.csv


# Create a Python Script
vi merge_data.py

cat > /data/merge_data.py << EOF
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
EOF
# Create a requirements.txt
vi requirements.txt
cat > /data/requirements.txt << EOF
asttokens==3.0.0
colorama==0.4.6
comm==0.2.2
contourpy==1.3.2
cycler==0.12.1
debugpy==1.8.14
decorator==5.2.1
executing==2.2.0
fonttools==4.57.0
ipykernel==6.29.5
ipython==9.2.0
ipython_pygments_lexers==1.1.1
jedi==0.19.2
jupyter_client==8.6.3
jupyter_core==5.7.2
kiwisolver==1.4.8
matplotlib==3.10.1
matplotlib-inline==0.1.7
nest-asyncio==1.6.0
numpy==2.2.5
packaging==25.0
pandas==2.2.3
parso==0.8.4
pillow==11.2.1
platformdirs==4.3.7
prompt_toolkit==3.0.51
psutil==7.0.0
pure_eval==0.2.3
Pygments==2.19.1
pyparsing==3.2.3
python-dateutil==2.9.0.post0
pytz==2025.2
pyzmq==26.4.0
six==1.17.0
stack-data==0.6.3
tornado==6.4.2
traitlets==5.14.3
typing_extensions==4.13.2
tzdata==2025.2
wcwidth==0.2.13
EOF
# Make a virtual environment
python3 -m venv venv
source venv/bin/activate

# Run the Python Script
python merge_data.py

# Create HDFS directories for the taxi data
# Create directory structure
hdfs dfs -mkdir -p /nyc_taxi/raw
hdfs dfs -mkdir -p /user/hive/warehouse
hdfs dfs -chmod g+w /user/hive/warehouse

# 3. Upload the cleaned data to HDFS
hdfs dfs -put ~/data/cleaned_data.csv /nyc_taxi/raw/

# Check if the file exists in HDFS
hdfs dfs -ls /nyc_taxi/raw/
hdfs dfs -du -h /nyc_taxi/raw/cleaned_data.csv

# Start the Spark shell
~/spark/bin/spark-shell

# Set the JAVA_HOME environment variable for Spark:
bash# Set JAVA_HOME to your actual Java path
export JAVA_HOME=/usr/lib/jvm/default-java

# Verify it's set correctly
echo $JAVA_HOME

~/spark/bin/spark-shell

#  Run the below file to get analysis
# TaxiAnalysis.scala

# Start ZooKeeper
~/kafka/bin/zookeeper-server-start.sh -daemon ~/kafka/config/zookeeper.properties

# Wait for ZooKeeper to start
sleep 10

# Start Kafka
~/kafka/bin/kafka-server-start.sh -daemon ~/kafka/config/server.properties

# Wait for Kafka to start
sleep 10

# Verify that ZooKeeper and Kafka are running
ps -ef | grep zookeeper | grep -v grep
ps -ef | grep kafka | grep -v grep

# Create a topic named 'taxi-trips'
~/kafka/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic taxi-trips

# Verify the topic was created
~/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092

# Run the Kafka Produce
vi ~/streaming/taxi_producer.py

# Create a screen for kafka and run the taxi_producer.py
screen -S kafka
python taxi_producer.py

# Press Ctrl+A+D to come back to home screen
# Create the HDFS directory for streaming output
hdfs dfs -mkdir -p /nyc_taxi/streaming

# Create a new screen session for the Spark Streaming job
screen -S spark-streaming

# Create a Create the Spark Streaming script
hdfs dfs -mkdir -p /nyc_taxi/streaming
# Run the taxi_streaming.py file in spark-streaming screen

# Inside this new screen session, run the Spark Streaming job
~/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5 ~/streaming/taxi_streaming.py

# To reattach to the Kafka producer
screen -r kafka

# To reattach to the Spark Streaming job
screen -r spark-streaming