import pika
# importing os module for environment variables
import os
# importing necessary functions from dotenv library
from dotenv import load_dotenv, dotenv_values 
# loading variables from .env file
load_dotenv() 
 
def receive_message(ch, method, properties, body):
    print(body.decode())

#Config connection
URL = os.getenv("URL")
queue="datasets"
params = pika.URLParameters(URL)
connection = pika.BlockingConnection(params)
channel = connection.channel()
channel.queue_declare(queue = queue, durable = True)
channel.basic_consume(queue = queue, on_message_callback = receive_message, auto_ack = True)

try:
    channel.start_consuming()
except KeyboardInterrupt:
    channel.stop_consuming()





