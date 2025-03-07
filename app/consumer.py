import json
import asyncio
from aiokafka import AIOKafkaConsumer
from app.database import SessionLocal
from app.models import Game
from redis.asyncio import Redis
from app.producer import produce

redis = Redis()

async def save_to_cache(key, value):
    await redis.set(key, value)

KAFKA_BROKERS = "kafka:9092"
KAFKA_TOPIC = "query_topic"
RESPONSE_TOPIC = "response_topic"

async def consume():
    await asyncio.sleep(10) 
    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKERS,
        group_id="search_group"
    )
    await consumer.start()

    try:
        async for msg in consumer:
            message = json.loads(msg.value.decode("utf-8"))
            print(message)
            query = message.get("query")

            if query:
                games = search_games(query)
                print(f"Search results: {games}")

                # Отправляем результаты в ответный топик
                response_message = {
                    "query": query,
                    "results": [game.name for game in games]
                }
                await produce(response_message)
    finally:
        await consumer.stop()

def search_games(query: str):
    db = SessionLocal()
    try:
        return db.query(Game).filter(Game.name.ilike(f"%{query}%")).all()
    finally:
        db.close()