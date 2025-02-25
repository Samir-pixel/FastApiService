import json
from aiokafka import AIOKafkaProducer

KAFKA_BROKERS = "kafka:9092"
RESPONSE_TOPIC = "response_topic"

async def produce(message: dict):
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BROKERS)
    await producer.start()

    try:
        await producer.send_and_wait(RESPONSE_TOPIC, json.dumps(message).encode("utf-8"))
        print(f"Message sent:  {message}")
    finally:
        await producer.stop()