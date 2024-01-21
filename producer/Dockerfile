FROM python:3.7-slim

WORKDIR /usr/src/app

COPY . .

RUN pip install --no-cache-dir --upgrade pip

RUN pip install --no-cache-dir -r requirements.txt

ENV KAFKA_BROKER="localhost:9092"
ENV SUBREDDIT="funny"

CMD ["python", "-u", "./main.py"]
