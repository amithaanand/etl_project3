import json
import requests
from kafka import KafkaProducer, KafkaConsumer

# Fetch data from the Random User Generator API
url = "https://randomuser.me/api/"
data = requests.get(url).json()

# Extract the results from the data
results = data["results"]

# Sort the results by a specific criterion, let's say by first name
sorted_results = sorted(results, key=lambda x: x["name"]["first"])

# Print the sorted results
# print("Sorted Results:")
# print(sorted_results)



# Extracted information
for result in sorted_results:
    extracted_info = {
        "Name": f"{result['name']['title']} {result['name']['first']} {result['name']['last']}",
        "Gender": result['gender'].capitalize(),
        "Email": result['email'],
        "Username": result['login']['username'],
        "Phone": result['phone'],
        "Cell": result['cell'],
        "ID (TFN)": result['id']['value'],
        "Nationality": result['nat'],
        "Picture": result['picture']['large']
    }

    # Printing the extracted information
    for key, value in extracted_info.items():
        print(f"{key}: {value}")

# Create a Kafka producer
producer = KafkaProducer(bootstrap_servers='localhost:9092')

# Serialize sorted results to JSON
# message = json.dumps(sorted_results).encode()  # Serialize message to bytes



message = json.dumps(extracted_info).encode()




# Send serialized message to Kafka topic
topic = 'my_topic'
producer.send(topic, message)

# Create a Kafka consumer
consumer = KafkaConsumer('my_topic', bootstrap_servers='localhost:9092', group_id='my_consumer_group')

# Consume messages from Kafka topic
for message in consumer:
    # Decode consumed message from bytes to JSON
    consumed_message = json.loads(message.value.decode())
    print("Consumed Message:", consumed_message)
    # Add break condition to exit loop after consuming one message
    break

# Close Kafka producer and consumer
producer.close()
consumer.close()

from pyspark.sql import SparkSession
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# MongoDB connection URI
mongo_uri = os.getenv("mongodb_url")

# Initialize Spark session
spark = SparkSession.builder \
    .appName("YourAppName") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1,org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
    .config("spark.jars.repositories", "https://repo.maven.apache.org/maven2") \
    .config("spark.mongodb.input.uri", mongo_uri) \
    .config("spark.mongodb.output.uri", mongo_uri)\
    .getOrCreate()

# Kafka parameters
kafka_bootstrap_servers = "localhost:9092"
kafka_topic = "my_topic"

# Read from Kafka
df = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .load()

# Process the data from Kafka
processed_df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

# Show the processed data
processed_df.show()

# Write processed data to MongoDB
processed_df.write \
    .format("com.mongodb.spark.sql.DefaultSource") \
    .mode("append") \
    .option("uri", mongo_uri) \
    .option("database", "test") \
    .option("collection", "demo") \
    .save()

# Stop the Spark session
spark.stop()