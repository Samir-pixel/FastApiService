import json
import asyncio
from aiokafka import AIOKafkaConsumer
from FastService.database import SessionLocal
from FastService.models import Game
from redis.asyncio import Redis

redis = Redis()

async def save_to_cache(key, value):
    await redis.set(key, value)

KAFKA_BROKERS = "localhost:9092"
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
