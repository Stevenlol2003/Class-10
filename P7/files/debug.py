from kafka import KafkaConsumer
from report_pb2 import Report

broker = 'localhost:9092'
group_id = 'debug'

# Initialize Kafka consumer
consumer = KafkaConsumer(
    'temperatures',
    group_id=group_id,
    bootstrap_servers=[broker],
    enable_auto_commit=False  # Disable auto-commit to have more control
)

try:
    # Subscribe to the topic and let the broker assign partitions
    consumer.subscribe(['temperatures'])

    # Loop over messages forever
    for message in consumer:
        # Decode the value assuming it's a protobuf message
        try:
            report = Report()
            report.ParseFromString(message.value)
            message_dict = {
                'partition': message.partition,
                'key': message.key.decode('utf-8'),
                'date': report.date,
                'degrees': report.degrees,
            }
            print(message_dict)
        except Exception as e:
            print(f"Error decoding message: {e}")

except KeyboardInterrupt:
    pass
finally:
    consumer.close()

