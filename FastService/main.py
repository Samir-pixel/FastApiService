import asyncio

from fastapi import FastAPI, Depends
from sqlalchemy.orm import Session
from FastService.database import SessionLocal, engine
from FastService.models import Base
from fastapi import FastAPI
from FastService.consumer import consume
from FastService.producer import produce

# Создаем все таблицы
Base.metadata.create_all(bind=engine)

app = FastAPI()

async def lifespan(app: FastAPI):
    # Действия при запуске приложения
    task = asyncio.create_task(consume())
    yield
    # Действия при остановке приложения
    task.cancel()

app = FastAPI(lifespan=lifespan)

@app.post("/send/")
async def send_message(data: dict):
    await produce(data)
    return {"status": "Message sent"}


