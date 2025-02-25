FROM python:3.10-alpine

WORKDIR /app

# Установка компилятора, зависимостей и zlib для aiokafka
RUN apk add --no-cache gcc musl-dev python3-dev zlib-dev

COPY requirements.txt .
RUN pip install -r requirements.txt

# Копируем весь проект, сохраняя структуру FastService/
COPY . .

# Открываем порт 8000 для FastAPI
EXPOSE 8000

# Запускаем приложение
CMD ["uvicorn", "FastService.main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]