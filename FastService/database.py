from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base

# Настройки подключения к базе данных
DATABASE_URL = "postgresql://postgres:postgres@localhost:5432/FastApi_Search"

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()
