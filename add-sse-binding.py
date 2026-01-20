#!/usr/bin/env python3
"""
Script to add SSE binding to RabbitMQ results stream.
Run this inside the RabbitMQ pod.
"""
import pika
import os
import sys

# Get credentials from environment
host = os.environ.get('RABBITMQ_HOST', 'localhost')
username = os.environ.get('RABBITMQ_DEFAULT_USER', 'guest')
password = os.environ.get('RABBITMQ_DEFAULT_PASS', 'guest')

print(f"Connecting to RabbitMQ at {host} as {username}")

try:
    credentials = pika.PlainCredentials(username, password)
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=host, credentials=credentials)
    )
    channel = connection.channel()
    
    # Bind nldoc.topics exchange to results queue with routing key specs.*
    print("Adding binding: nldoc.topics -> results (routing_key: specs.*)")
    channel.queue_bind(
        queue='results',
        exchange='nldoc.topics', 
        routing_key='specs.*'
    )
    print("SUCCESS: Binding added!")
    
    # Also add documents.* binding for initial events
    print("Adding binding: nldoc.topics -> results (routing_key: documents.*)")
    channel.queue_bind(
        queue='results',
        exchange='nldoc.topics',
        routing_key='documents.*'
    )
    print("SUCCESS: documents.* binding added!")
    
    connection.close()
    
except Exception as e:
    print(f"ERROR: {e}")
    sys.exit(1)

