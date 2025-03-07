import asyncio
import json
import os
from sqlalchemy.orm import Session
from redis import Redis
from fastapi import FastAPI

from .database import SessionLocal
from .models import Game
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

app = FastAPI()
redis_host = os.getenv("REDIS_HOST", "redis")  # По умолчанию "redis"
redis_port = int(os.getenv("REDIS_PORT", 6379))  # По умолчанию 6379
redis = Redis(host=redis_host, port=redis_port, db=1)

KAFKA_BROKERS = "kafka:9092"
QUERY_TOPIC = "search_topic"
RESPONSE_TOPIC = "response_topic"

def search_games(db: Session, query: str):
    """
    Ищет игры по названию (name) в базе данных.
    """
    return db.query(Game).filter(
        Game.name.ilike(f"%{query}%")  # Поиск по названию (регистронезависимый)
    ).all()

async def consume():
    # Ждем, пока Kafka запустится
    await asyncio.sleep(10)

    consumer = AIOKafkaConsumer(
        QUERY_TOPIC,
        bootstrap_servers=KAFKA_BROKERS,
        group_id="fastapi_group",
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BROKERS,
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )

    try:
        await consumer.start()
        await producer.start()
        print("Kafka consumer and producer started")

        async for msg in consumer:
            try:
                print(f"Received message: {msg.value}")  # Логируем полученное сообщение
                query = msg.value.get('query')
                request_id = msg.value.get('request_id')  # Уникальный идентификатор запроса

                if not query or not request_id:
                    print("Invalid message format: missing 'query' or 'request_id'")
                    continue

                # Поиск в базе данных
                db = SessionLocal()
                try:
                    games = search_games(db, query)
                    results = [{"id": game.id, "name": game.name} for game in games]
                    print(f"Search results: {results}")  # Логируем результаты поиска
                except Exception as e:
                    print(f"Database error: {e}")
                    results = []
                finally:
                    db.close()

                try:
                    cache_key = f"search_query_{query}"
                    redis.set(cache_key, json.dumps(results), ex=3600)
                    print(f"Cache redis set {cache_key}")
                except Exception as e:
                    print(e)
                # Отправляем результаты обратно в Kafka
                try:
                    await producer.send_and_wait(
                        RESPONSE_TOPIC,
                        {"request_id": request_id, "query": query, "results": results}
                    )
                    print(f"Results sent to Kafka with request_id: {request_id}")  # Логируем отправку в Kafka
                except Exception as e:
                    print(f"Kafka producer error: {e}")

            except Exception as e:
                print(f"Error processing message: {e}")

    except Exception as e:
        print(f"Kafka consumer error: {e}")
    finally:
        await consumer.stop()
        await producer.stop()
        print("Kafka consumer and producer stopped")

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(consume())