import time
from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import UnknownTopicOrPartitionError
from report_pb2 import Report
import weather

broker = 'localhost:9092'
admin_client = KafkaAdminClient(bootstrap_servers=[broker])

try:
    admin_client.delete_topics(["temperatures"])
    print("Deleted topics successfully")
except UnknownTopicOrPartitionError:
    print("Cannot delete topic/s (may not exist yet)")

time.sleep(3)  # Deletion sometimes takes a while to reflect

# Create topic 'temperatures' with 4 partitions and replication factor = 1
admin_client.create_topics([NewTopic(name="temperatures", num_partitions=4, replication_factor=1)])

print("Topics:", admin_client.list_topics())

# Create Kafka producer
producer = KafkaProducer(
    bootstrap_servers=broker,
    retries=10,  # retry up to 10 times on send failures
    acks='all'   # wait for all in-sync replicas to receive the data
)

for date, degrees in weather.get_next_weather(delay_sec=0.1):
    # Create protobuf message
    report = Report()
    report.date = date
    report.degrees = degrees

    # Extract month from the date (assuming date format is 'YYYY-MM-DD')
    month_name = time.strftime('%B', time.strptime(date, '%Y-%m-%d'))

    # Use the full month name as the key
    key = month_name

    value = report.SerializeToString()

    try:
        # Produce message to Kafka topic 'temperatures'
        producer.send(
            topic="temperatures",
            key=bytes(key, "utf-8"),
            value=value
        )
        print(f"Produced message: Key={key}, Value={value}")
    except Exception as e:
        print(f"Error producing message: {e}")

    time.sleep(0.1)

producer.close()
