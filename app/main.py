import asyncio
from fastapi import FastAPI
from .database import SessionLocal, engine
from app.models import Base
from app.consumer import consume
from app.producer import produce
from app.schema import SearchQuery
from fastapi.responses import JSONResponse

# Создаем все таблицы

app = FastAPI(
    title="Search Application",
    description="FastAPI application for searching with Redis caching and Kafka integration.",
    version="1.0.0"
)

@app.on_event("startup")
async def startup_event():
    # Запускаем потребителя (consumer) как фоновую задачу
    asyncio.create_task(consume())

# @app.post("/send/")
# async def send_message(data: dict):
#     await produce(data)
#     return {"status": "Message sent"}
@app.post("/search")
async def search(query: SearchQuery):
    await produce({"query": query.query})
    return JSONResponse(content={"query": query.query})