FROM python:3.11-slim
WORKDIR /app
RUN pip install --no-cache-dir pika minio
COPY app.py .
CMD ["python", "-u", "app.py"]

