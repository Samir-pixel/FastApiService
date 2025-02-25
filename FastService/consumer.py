import json
import asyncio
import os
from aiokafka import AIOKafkaConsumer
from FastService.database import SessionLocal
from FastService.models import Game
from redis.asyncio import Redis

redis = Redis()

async def save_to_cache(key, value):
    await redis.set(key, value)

# Получаем адрес Kafka из переменной окружения
KAFKA_BROKERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = "query_topic"

async def consume():
    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKERS,
        group_id="fastapi_consumer"
    )
    await consumer.start()

    try:
        async for msg in consumer:
            message = json.loads(msg.value.decode("utf-8"))
            query = message.get("query")

            if query:
                games = search_games(query)
                print(f"Search results: {games}")
    finally:
        await consumer.stop()

def search_games(query: str):
    db = SessionLocal()
    try:
        return db.query(Game).filter(Game.name.ilike(f"%{query}%")).all()
    finally:
        db.close()

if __name__ == "__main__":
    asyncio.run(consume())
