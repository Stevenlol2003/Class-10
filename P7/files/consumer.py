import json
import os
import sys
import time
import datetime
from kafka import KafkaConsumer, TopicPartition
from kafka.errors import NoBrokersAvailable
from report_pb2 import Report

def load_partition_data(partition_number):
    file_path = f'/files/partition-{partition_number}.json'
    try:
        with open(file_path, 'r') as file:
            data = json.load(file)
    except FileNotFoundError:
        data = {"partition": partition_number, "offset": 0}
        with open(file_path, 'w') as file:
            json.dump(data, file)
    return data

def write_partition_data(partition_number, data):
    file_path = f'/files/partition-{partition_number}.json'
    temp_file_path = file_path + ".tmp"

    try:
        with open(temp_file_path, 'w') as temp_file:
            json.dump(data, temp_file)
            temp_file.flush()
            os.fsync(temp_file.fileno())

        os.rename(temp_file_path, file_path)
    except Exception as e:
        print(f"Error writing partition data: {e}")

def process_messages(consumer, partition_data):
    for message in consumer:
        # Process the message as needed
        print(f"Partition {message.partition}, Offset {message.offset}: {message.value}")

        # Deserialize the message value (assuming it's a protobuf object)
        report = Report()
        report.ParseFromString(message.value)

        # Check for duplicate dates
        current_date = report.date
        if current_date <= partition_data.get("last_date", ""):
            print(f"Duplicate date detected: {current_date}. Skipping.")
            continue
        
        # Update offset in partition_data
        partition_data["offset"] = message.offset + 1

        # Update statistics
        update_statistics(partition_data, report)

        # Write partition data to file
        write_partition_data(partition_data["partition"], partition_data)

def update_statistics(partition_data, report):
    # Extract month and year from the date
    month, year = report.date.split('-')[1], report.date.split('-')[0]
    month = datetime.datetime.strptime(month, "%m").strftime("%B")

    # Initialize stats if not present
    partition_data.setdefault(month, {}).setdefault(year, {
        "count": 0,
        "sum": 0,
        "avg": 0,
        "start": report.date,
        "end": report.date
    })

    # Update stats
    stats = partition_data[month][year]
    stats["count"] += 1
    stats["sum"] += report.degrees
    stats["avg"] = stats["sum"] / stats["count"]
    stats["end"] = report.date

if __name__ == "__main__":
    try:
        partitions = [int(partition) for partition in sys.argv[1:]]
    except ValueError:
        print("Invalid partition numbers.")
        exit(1)

    if not partitions:
        print("No partition numbers provided.")
        exit(1)

    # Kafka Consumer Configuration
    consumer = KafkaConsumer(
        # 'temperatures',
        bootstrap_servers='localhost:9092',
        enable_auto_commit=False,  # Disable automatic committing of offsets
        group_id='temperature-consumer',
    )

    # Assign partitions to the consumer
    consumer.assign([TopicPartition('temperatures', partition) for partition in partitions])

    # Load and initialize partition data
    partition_data = {partition: load_partition_data(partition) for partition in partitions}

    try:
        while True:
            for partition, data in partition_data.items():
                # Set the consumer to the correct offset
                consumer.seek(TopicPartition('temperatures', partition), data["offset"])

                # Process messages for the assigned partition
                process_messages(consumer, data)

            # Sleep before the next iteration
            time.sleep(1)

    except KeyboardInterrupt:
        print("Consumer terminated.")
    except NoBrokersAvailable:
        print("Error: No brokers available. Make sure Kafka is running.")
    finally:
        # Close the consumer
        consumer.close()
