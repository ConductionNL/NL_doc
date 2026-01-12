FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN pip install --no-cache-dir pika==1.3.2

WORKDIR /app
COPY app.py /app/app.py

CMD ["python", "/app/app.py"]


