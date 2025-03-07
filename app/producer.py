from aiokafka import AIOKafkaProducer
import json

async def produce(message):
    producer = AIOKafkaProducer(bootstrap_servers="kafka:9092")
    await producer.start()
    try:
        await producer.send_and_wait("search_topic", json.dumps(message).encode('utf-8'))
    finally:
        await producer.stop()