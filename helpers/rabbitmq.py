import pika
import configparser
from loguru import logger


class RabbitMQ:
    def __init__(self):
        self.log = logger
        self.config = configparser.RawConfigParser()
        self.config.read("config.ini")
        self.user = self.config["RabbitMQ"]["user"]
        self.password = self.config["RabbitMQ"]["password"]
        self.host = self.config["RabbitMQ"]["host"]
        self.port = self.config["RabbitMQ"]["port"]
        self.vhost = self.config["RabbitMQ"]["vhost"]

        credential = pika.PlainCredentials(self.user, self.password)
        parameter = pika.ConnectionParameters(
            host=self.host,
            port=self.port,
            virtual_host=self.vhost,
            credentials=credential
        )
        self.connection = pika.BlockingConnection(parameter)
        self.channel = self.connection.channel()

    def queue_name(self, queue_name):
        self.channel.queue_declare(queue=queue_name)
    
    def send(self, exchange, routing_key, message):
        self.channel.basic_publish(exchange=exchange, routing_key=routing_key, body=message)
        self.log.success(f"Message sent to RabbitMQ: {message}")

    def close(self):
        self.connection.close()