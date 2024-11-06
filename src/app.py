import json
from confluent_kafka import Consumer, Producer, KafkaError
from datetime import datetime

# Load Kafka configuration
with open('../config/kafka_config.json') as f:
    kafka_conf = json.load(f)

# Initialize Kafka consumer and producer
consumer = Consumer(kafka_conf["consumer"])
producer = Producer(kafka_conf["producer"])

def calculate_age(dob_str):
    dob = datetime.strptime(dob_str, '%Y-%m-%d')
    today = datetime.today()
    return today.year - dob.year - ((today.month, today.day) < (dob.month, dob.day))

def process_message(message):
    try:
        payload = json.loads(message)
        name = payload['Name']
        dob = payload['DateOfBirth']
        age = calculate_age(dob)
        is_even_age = age % 2 == 0
        publish_message(is_even_age, payload)
    except json.JSONDecodeError:
        print("Failed to decode JSON message:", message)


def persist_to_file(payload, filename):
    """Persist message to a specific file based on age parity."""
    with open(filename, 'a') as f:
        f.write(json.dumps(payload) + '\n')

def publish_message(is_even_age, payload):
    """Publish message to Kafka and persist to a file."""
    topic = 'EVEN_TOPIC' if is_even_age else 'ODD_TOPIC'
    producer.produce(topic, json.dumps(payload).encode('utf-8'))
    producer.flush()
    print(f"Published to {topic}: {payload}")

    # Persist to the appropriate file
    filename = 'even_age.txt' if is_even_age else 'odd_age.txt'
    persist_to_file(payload, filename)


def consume_messages():
    consumer.subscribe(['INPUT_TOPIC'])
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(f"Error: {msg.error()}")
                break

        # Check if msg.value() is valid before processing
        message_value = msg.value()
        if message_value is not None:
            try:
                process_message(message_value.decode('utf-8'))
            except json.JSONDecodeError as e:
                print(f"Failed to decode JSON message: {message_value}")
                print(f"Error details: {e}")
        else:
            print("Received an empty message.")
    consumer.close()



if __name__ == '__main__':
    consume_messages()
