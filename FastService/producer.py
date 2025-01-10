import asyncio
import json
from aiokafka import AIOKafkaProducer

KAFKA_BROKERS = "localhost:9092"
RESPONSE_TOPIC = "response_topic"

async def produce(message: dict):
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BROKERS)
    await producer.start()

    try:
        await producer.send_and_wait(RESPONSE_TOPIC, json.dumps(message).encode("utf-8"))
    finally:
        await producer.stop()

if __name__ == "__main__":
    asyncio.run(produce({"status": "test message"}))
