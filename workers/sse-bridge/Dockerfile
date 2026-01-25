FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir pika

COPY app.py .

CMD ["python", "-u", "app.py"]


