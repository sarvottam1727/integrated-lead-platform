# send_lead.py
import pika
import json

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()
channel.queue_declare(queue='lead_queue', durable=True)

lead = {
    "SENDER_NAME": "John Doe",
    "SENDER_EMAIL": "john.doe@example.com",
    "SENDER_MOBILE": "9876543210",
    "SENDER_COMPANY": "Acme Corp"
}

channel.basic_publish(
    exchange='',
    routing_key='lead_queue',
    body=json.dumps(lead),
    properties=pika.BasicProperties(
        delivery_mode=2,  # make message persistent
    )
)

print("Lead sent")
connection.close()
